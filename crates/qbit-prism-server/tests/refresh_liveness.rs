//! Issue #375: deterministic liveness races through public coordinator, ledger,
//! broadcaster and Stratum APIs. No sleeps decide when a race is injected.
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AuditBundle, FoundBlock, PayoutPolicy};
use qbit_prism_server::{
    broadcaster, codec,
    coordinator::Coordinator,
    ledger::{BlockObservation, Candidate, CandidateCtv, SignerKeys, WindowRef},
    metrics::Metrics,
    stratum::MiningBackend,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet, time::timeout};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
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

/// Produce authentic candidate and CTV artifacts through the ledger's public
/// writers. Only node chain observations are simulated.
async fn seed_candidate(f: &Fixture, ctv: bool) -> Result<(Candidate, AuditBundle)> {
    let snapshot = f.a.ledger.snapshot(1_000_000).await?;
    let manifest_key = ManifestSigningKey::from_seed_hex(&f.a.config.manifest_seed)?;
    let ledger_key = ManifestSigningKey::from_seed_hex(&f.a.config.ledger_seed)?;
    let found = FoundBlock {
        block_height: 101,
        coinbase_value_sats: 5_000_000_000,
        network_difficulty: 1_000_000,
        anchor_job_issued_at_ms: snapshot.anchor_ms,
    };
    let options = CandidateCtv {
        direct_floor_sats: u64::MAX,
        settlement_config: qbit_prism::SettlementModeConfig {
            max_fanout_recipients_per_transaction: 1,
            ..Default::default()
        },
        fanout_fee_policy: Some(qbit_prism::FanoutFeeRatePolicy::new(1000, 12000)),
    };
    let bundle = if ctv {
        qbit_prism::build_audit_bundle_with_ctv_settlement_options(
            snapshot.shares.clone(),
            found,
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            options.direct_floor_sats,
            options.settlement_config,
            options.fanout_fee_policy,
            None,
            vec![],
            &manifest_key,
            &ledger_key,
        )?
    } else {
        qbit_prism::build_audit_bundle(
            snapshot.shares.clone(),
            found,
            snapshot.prior_balances.clone(),
            PayoutPolicy::day_one_default(),
            &manifest_key,
            &ledger_key,
        )?
    };
    let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
        &bundle,
        &ledger_key.public_key_hex(),
    )?;
    let mut block = vec![0u8; 80];
    block[..4].copy_from_slice(&0x20000000u32.to_le_bytes());
    block[4..36].fill(0xab);
    let mut txid = hex::decode(&report.coinbase_txid)?;
    txid.reverse();
    block[36..68].copy_from_slice(&txid);
    block[68..72].copy_from_slice(&(chrono::Utc::now().timestamp() as u32).to_le_bytes());
    block[72..76].copy_from_slice(&0x207fffffu32.to_le_bytes());
    let hash = codec::hash_display(&codec::double_sha256(&block));
    block.push(1);
    block.extend(hex::decode(&report.coinbase_tx_hex)?);
    let candidate = Candidate {
        block_hash: hash,
        block_sha256: Candidate::block_digest_hex(&block),
        job_id: "startup-candidate".into(),
        payout_revision: snapshot.payout_revision,
        window: WindowRef::from_snapshot(&snapshot)?,
        bootstrap_share: None,
        found_block: bundle.found_block.clone(),
        payout_policy: bundle.payout_policy.clone(),
        ctv: ctv.then_some(options),
        audit_builder_version: qbit_prism::AUDIT_BUILDER_VERSION,
        signer_keys: SignerKeys::of(&manifest_key, &ledger_key),
        leased: false,
        coinbase_suffix_hex: bundle
            .coinbase_script_sig_suffix_hex
            .clone()
            .unwrap_or_else(|| "00".repeat(12)),
        deferred_share: None,
        block_bytes: block,
        as_issued_balances: snapshot.prior_balances,
    };
    f.a.ledger.enqueue_candidate(candidate.clone()).await?;
    Ok((candidate, bundle))
}

async fn mature_fanouts(f: &Fixture) -> Result<usize> {
    let (candidate, bundle) = seed_candidate(f, true).await?;
    let count = bundle
        .ctv_fanout_manifest_set
        .as_ref()
        .context("CTV fanouts")?
        .fanout_count as usize;
    ensure!(count > 2, "fixture needs multiple remaining rows");
    let claim =
        f.a.ledger
            .claim_candidate(60)
            .await?
            .context("parent claim")?
            .with_bundle(bundle);
    f.a.ledger
        .land_candidate(&claim, &f.a.config.ledger_public_key)
        .await?;
    f.a.ledger.finish_candidate(&claim, true, None).await?;
    f.a.ledger
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: candidate.block_hash.clone(),
                active: true,
            }],
            1101,
            f.a.ledger.payout_revision().await?,
        )
        .await?;
    f.node
        .set_tip(&"ab".repeat(32), &"cd".repeat(32), 1101, "02");
    f.node.set_reply(
        "getblockheader",
        json!([candidate.block_hash]),
        json!({"previousblockhash":"cd".repeat(32),"height":101,"confirmations":1001}),
    );
    f.node
        .set_reply("getblockhash", json!([101]), json!(candidate.block_hash));
    f.node
        .set_reply("getblockhash", json!([1101]), json!("ab".repeat(32)));
    // Recovery of already-confirmed fanouts exercises successful observations
    // and durable recheck scheduling without needing a wallet or real qbitd.
    sqlx::query("UPDATE qbit_ctv_fanout_artifacts SET settlement_status='confirmed',confirmed_block_hash=$1,confirmed_block_height=1101,confirmed_depth=1")
        .bind("ab".repeat(32)).execute(f.pool()).await?;
    f.a.refresh_once().await?;
    Ok(count)
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

async fn ctv_yield_case(f: &Fixture, publish_before_completion: bool) -> Result<()> {
    f.refresh(true).await?;
    let count = mature_fanouts(f).await?;
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
async fn ctv_settles_after_the_replacement_build_budget_when_publication_keeps_failing(
) -> Result<()> {
    run(qbit_prism_test_gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let count = mature_fanouts(f).await?;
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
