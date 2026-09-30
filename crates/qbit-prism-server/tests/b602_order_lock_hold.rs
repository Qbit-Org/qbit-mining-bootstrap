//! #602 step 1: measure how long each holder keeps `ORDER_LOCK`, read share
//! acknowledgements in the window after a pool block's acceptance apart from
//! steady state, and guard the settlement's hold over the share appends.
//!
//! Every test here lands a real block through two in-process frontends, one
//! fake node and PostgreSQL 16. A settlement is held open by a trigger that
//! sleeps inside its first-confirmation `UPDATE` of `qbit_pool_blocks`, so the
//! delay sits in the settlement transaction itself, under the locks it takes,
//! with no change to the product code.
//!
//! `appends_commit_within_budget_while_a_settlement_is_held` is an expected
//! failure: it passes while a held settlement still blocks the appends and
//! fails, saying "#602 looks fixed", once it does not.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test b602_order_lock_hold -- --nocapture
//! ```
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::AcceptedShare;
use qbit_prism_server::{coordinator::Coordinator, metrics::Metrics, stratum::MiningBackend};
use qbit_prism_test_gate as gate;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[allow(dead_code)]
#[path = "support/compact_runtime_e2e/mod.rs"]
mod support;
use support::{
    run,
    socket::{ordinary_submit, Client, Listener},
    Fixture, DIFFICULTY,
};

const HOLD: &str = "qbit_prism_database_order_lock_hold_seconds";
const LANDING: &str = "qbit_prism_share_ack_landing_window_seconds";
const ACK: &str = "qbit_prism_share_ack_seconds";

/// #602 R1: no acknowledgement over 2 s in the landing window, and a p99 at
/// or under 500 ms.
const NO_ACK_OVER: Duration = Duration::from_secs(2);
const P99_BUDGET: Duration = Duration::from_millis(500);

/// Long enough that an append blocked behind the held settlement is well
/// past `NO_ACK_OVER`, short enough to stay under the default 5 s
/// `PRISM_DATABASE_LOCK_TIMEOUT_MS`, so a blocked append still commits.
const SETTLEMENT_HOLD: Duration = Duration::from_secs(3);

/// Concurrent appends from the other frontend while the settlement is held.
const APPENDS: usize = 8;

/// A trigger that sleeps `hold` inside the transaction that first confirms a
/// pool block: the offering frontend's settlement, under `SETTLEMENT_LOCK`
/// and `ORDER_LOCK`.
async fn hold_first_confirmation(f: &Fixture, hold: Duration) -> Result<()> {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION b602_hold_settlement() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN PERFORM pg_sleep({}); RETURN NEW; END $$; \
         CREATE TRIGGER b602_hold_settlement BEFORE UPDATE ON qbit_pool_blocks FOR EACH ROW \
         WHEN (NEW.chain_state='confirmed' AND OLD.chain_state<>'confirmed') \
         EXECUTE FUNCTION b602_hold_settlement();",
        hold.as_secs_f64()
    ))
    .execute(f.pool())
    .await?;
    Ok(())
}

/// Until a backend is sleeping in the trigger.
async fn settlement_is_held(f: &Fixture) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let sleeping: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event='PgSleep'")
                .fetch_one(f.pool())
                .await?;
        if sleeping > 0 {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "the settlement never reached its first confirmation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Submit one block-solving share on `frontend` and claim its candidate,
/// without offering it.
async fn queue_block(
    frontend: &Arc<Coordinator>,
) -> Result<qbit_prism_server::ledger::CandidateClaim> {
    let worker = frontend.authorize("b602.rig").await?;
    let job = frontend
        .build_job(&worker, "1a2b3c4d", DIFFICULTY, 0.)
        .await?;
    frontend
        .persist_issued_job(&worker, &job, 0, Duration::from_secs(30))
        .await?;
    for nonce in 0..10_000u32 {
        let proof = job.wire.assemble_submission(
            &"00".repeat(job.wire.extranonce2_size),
            &format!("{:08x}", job.wire.ntime),
            &format!("{nonce:08x}"),
            None,
            0,
        )?;
        if proof.block_pass {
            frontend.submit(&worker, &job, proof, false.into()).await?;
            return frontend
                .ledger
                .claim_candidate(60)
                .await?
                .context("candidate missing");
        }
    }
    bail!("no block proof in bounded fixture search")
}

fn share(tag: &str, index: usize) -> AcceptedShare {
    AcceptedShare {
        share_seq: 0,
        share_id: format!("b602-{tag}:{index:064x}"),
        miner_id: "b602-miner".into(),
        order_key: "b602-miner".into(),
        p2mr_program_hex: "11".repeat(32),
        share_difficulty: 1,
        network_difficulty: 100,
        template_height: 100,
        job_id: "b602-job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    }
}

fn sample(metrics: &Metrics, key: &str) -> f64 {
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key} ")))
        .map_or(0., |value| value.parse().unwrap())
}

fn hold(metrics: &Metrics, holder: &str, part: &str) -> f64 {
    sample(metrics, &format!("{HOLD}_{part}{{holder=\"{holder}\"}}"))
}

/// Acknowledgements of both results in `family`.
fn acks(metrics: &Metrics, family: &str) -> f64 {
    ["accepted", "rejected"]
        .iter()
        .map(|result| sample(metrics, &format!("{family}_count{{result=\"{result}\"}}")))
        .sum()
}

/// R3: a landing's transactions record their `ORDER_LOCK` holds under their
/// own holder, and a hold runs from the grant to the end of its transaction,
/// not just across the lock statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landing_records_each_order_lock_hold_under_its_holder() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            let metrics = &f.a.metrics;
            ensure!(
                hold(metrics, "prepared", "count") >= 1.,
                "the refresh's window snapshot recorded no hold"
            );
            let injected = Duration::from_millis(300);
            hold_first_confirmation(f, injected).await?;
            f.node.accept_blocks();
            let appends = hold(metrics, "append", "count");
            let claim = queue_block(&f.a).await?;
            ensure!(
                hold(metrics, "append", "count") == appends + 1.,
                "the block-solving share's append recorded no hold"
            );
            let started = Instant::now();
            f.a.process_candidate(&claim).await?;
            let elapsed = started.elapsed();
            let state: String = sqlx::query_scalar(
                "SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1",
            )
            .bind(&claim.candidate.block_hash)
            .fetch_one(f.pool())
            .await?;
            ensure!(state == "submitted", "offer did not land: {state}");
            ensure!(
                hold(metrics, "first_confirmation", "count") == 1.,
                "the settlement that first confirmed the block is not a first_confirmation hold"
            );
            ensure!(
                hold(metrics, "settlement", "count") == 0.,
                "a first confirmation was recorded as another settlement"
            );
            let held = hold(metrics, "first_confirmation", "sum");
            ensure!(
                held >= injected.as_secs_f64() && held <= elapsed.as_secs_f64(),
                "the first confirmation held ORDER_LOCK {held} s, outside [{}, {}] s: a hold must \
                 cover the transaction's work through COMMIT",
                injected.as_secs_f64(),
                elapsed.as_secs_f64()
            );
            // The other frontend observed nothing of this settlement.
            ensure!(hold(&f.b.metrics, "first_confirmation", "count") == 0.);
            Ok(())
        })
    })
    .await
}

/// One Stratum session on `frontend`: log in, submit one ordinary share and
/// read its answer.
async fn acknowledge_one(frontend: &Arc<Coordinator>, name: &str) -> Result<()> {
    let mut listener = Listener::start(frontend, DIFFICULTY).await?;
    let result = async {
        let mut client = Client::connect(listener.address).await?;
        client.login(name).await?;
        let worker = frontend.authorize(name).await?;
        let job = frontend
            .resume_job(
                &worker,
                client.notify["params"][0].as_str().context("job id")?,
            )
            .await?
            .context("delivered job")?;
        let (request, _) = ordinary_submit(&job, &worker, 10)?;
        client.send(request).await?;
        client.response(10).await?;
        Ok(())
    }
    .await;
    listener.close().await?;
    result
}

/// R4: a Stratum acknowledgement is in the landing-window family only after a
/// pool block acceptance its frontend observed, and always in the steady
/// family as well. The offering frontend observes its own acceptance; the
/// other one first sees the block already confirmed, at the tip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledgements_after_an_acceptance_are_read_in_the_landing_window() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            for (frontend, name) in [(&f.a, "b602-steady-a.rig"), (&f.b, "b602-steady-b.rig")] {
                acknowledge_one(frontend, name).await?;
                ensure!(
                    acks(&frontend.metrics, ACK) == 1.,
                    "the steady acknowledgement was not recorded"
                );
                ensure!(
                    acks(&frontend.metrics, LANDING) == 0.,
                    "an acknowledgement with no acceptance was read as a landing window's"
                );
            }
            f.node.accept_blocks();
            let claim = queue_block(&f.a).await?;
            f.a.process_candidate(&claim).await?;
            f.a.refresh_once().await?;
            acknowledge_one(&f.a, "b602-landing-a.rig").await?;
            ensure!(acks(&f.a.metrics, ACK) == 2.);
            ensure!(
                acks(&f.a.metrics, LANDING) == 1.,
                "an acknowledgement right after its own acceptance is missing from the landing window"
            );
            ensure!(
                acks(&f.b.metrics, LANDING) == 0.,
                "the other frontend served no acknowledgement"
            );
            // The peer's reconciler first sees the block settled, at the tip.
            f.b.refresh_once().await?;
            acknowledge_one(&f.b, "b602-landing-b.rig").await?;
            ensure!(acks(&f.b.metrics, ACK) == 2.);
            ensure!(
                acks(&f.b.metrics, LANDING) == 1.,
                "an acknowledgement right after a peer's landing is missing from the landing window"
            );
            Ok(())
        })
    })
    .await
}

/// R6, an expected failure until #602 step 2. While a settlement is held open
/// in its first confirmation, the other frontend's share appends must commit
/// within #602's budget. Today they wait for the settlement's COMMIT behind
/// `ORDER_LOCK`, which this asserts; when they stop waiting, this fails with
/// "#602 looks fixed": then assert the budget directly instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appends_commit_within_budget_while_a_settlement_is_held() -> Result<()> {
    run(gate::site!(), |f| {
        Box::pin(async move {
            f.refresh(true).await?;
            hold_first_confirmation(f, SETTLEMENT_HOLD).await?;
            f.node.accept_blocks();
            let claim = queue_block(&f.a).await?;
            let settlement = tokio::spawn({
                let frontend = f.a.clone();
                async move { frontend.process_candidate(&claim).await }
            });
            let appended = async {
                settlement_is_held(f).await?;
                let appends = (0..APPENDS).map(|index| {
                    let ledger = f.b.ledger.clone();
                    async move {
                        let started = Instant::now();
                        let outcome = ledger.append(share("held", index), None).await;
                        (started.elapsed(), outcome.map(|result| result.inserted))
                    }
                });
                Ok::<_, anyhow::Error>(futures_util::future::join_all(appends).await)
            }
            .await;
            settlement.await??;
            let appended = appended?;
            let mut latencies: Vec<Duration> = Vec::with_capacity(APPENDS);
            for (latency, outcome) in &appended {
                match outcome {
                    Ok(true) => latencies.push(*latency),
                    Ok(false) => bail!("a fresh share was reported as already durable"),
                    Err(error) => bail!(
                        "an append failed after {latency:?} while a settlement was held: {error:#}"
                    ),
                }
            }
            latencies.sort();
            let slowest = latencies[APPENDS - 1];
            let p99 = latencies[(APPENDS * 99).div_ceil(100) - 1];
            eprintln!(
                "#602 R6: {APPENDS} appends during a {SETTLEMENT_HOLD:?} settlement hold: \
                 fastest {:?}, p99 {p99:?}, slowest {slowest:?}",
                latencies[0]
            );
            if slowest <= NO_ACK_OVER && p99 <= P99_BUDGET {
                bail!(
                    "#602 looks fixed: every append committed within {NO_ACK_OVER:?} (slowest \
                     {slowest:?}, p99 {p99:?}) while a settlement was held for \
                     {SETTLEMENT_HOLD:?}. Turn this expected failure into the budget assertion."
                );
            }
            // Blocked, and blocked by the held settlement: the fastest append
            // still waited for most of the hold.
            ensure!(
                latencies[0] >= SETTLEMENT_HOLD / 2,
                "appends missed the budget (slowest {slowest:?}) but the fastest took only \
                 {:?}, less than half the {SETTLEMENT_HOLD:?} hold: something other than the \
                 held settlement delayed them",
                latencies[0]
            );
            Ok(())
        })
    })
    .await
}

/// Every `ORDER_LOCK` acquisition in the ledger goes through the helper that
/// records its hold under a holder, and every hold guard lives until its
/// transaction ends. Two acquisitions are exempt: the schema cutover, whose
/// transaction its caller owns and commits, and archive's statement-scoped
/// drain, which holds nothing past its statement.
#[test]
fn every_order_lock_acquisition_records_its_hold() -> Result<()> {
    let root = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src/ledger"));
    let mut pending = vec![root.to_path_buf()];
    let mut plain = Vec::new();
    let mut labelled = 0;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let name = path.to_string_lossy().into_owned();
            if !name.ends_with(".rs") || name.ends_with("tests.rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path)?;
            let relative = path.strip_prefix(root)?.to_string_lossy().into_owned();
            let lines: Vec<&str> = source.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if line.contains("ORDER_LOCK") && line.contains("lock(") {
                    plain.push(format!("{relative}:{}", index + 1));
                }
                if line.contains(".lock_order(") || line.contains("lock_order(tx") {
                    labelled += 1;
                    // The guard is bound for the rest of the transaction; a
                    // bare `let _ =` would drop it at once and record the
                    // lock statement instead of the hold.
                    let statement = lines[index.saturating_sub(2)..=index].join(" ");
                    ensure!(
                        !statement.contains("let _ =") && !statement.contains("drop("),
                        "{relative}:{}: an ORDER_LOCK hold guard must live until its transaction ends",
                        index + 1
                    );
                }
            }
        }
    }
    plain.sort();
    ensure!(
        plain.len() == 2
            && plain[0].starts_with("archive.rs:")
            && plain[1].starts_with("migration.rs:"),
        "every ORDER_LOCK acquisition but the cutover and the drain must record its hold: {plain:?}"
    );
    ensure!(
        labelled >= 12,
        "found only {labelled} labelled ORDER_LOCK holds"
    );
    Ok(())
}
