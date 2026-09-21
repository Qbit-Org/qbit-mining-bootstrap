//! Issue #324 against PostgreSQL, through the real [`Coordinator`] and a
//! scripted qbit node.
//!
//! A share whose COMMIT is in flight at `share_commit_timeout` is reconciled
//! instead of dropped; an append that has not reached COMMIT is refused and
//! cancelled; an outcome still unknown at the acknowledgement deadline is
//! answered `ledger-outcome-unknown`, never `ledger-confirmation-failed`. A
//! block-only proof's acknowledgement follows its candidate's disposition up
//! to `block_only_ack_timeout`. Faults come from a deferred constraint trigger
//! that runs at COMMIT, the literal `ORDER_LOCK` key, a table lock and
//! `pg_terminate_backend`.

use super::d2_test_support::*;
use super::miner_tests::SharedLog;
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use sqlx::PgPool;
use std::collections::BTreeMap;
use tokio::time::Instant as TokioInstant;
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument::WithSubscriber;

/// The ledger's advisory locks are cluster-wide, so tests in other modules of
/// this binary can delay these by a second or two. No ceiling is shorter.
const PATIENCE: Duration = Duration::from_secs(20);
/// `ORDER_LOCK`, private to `ledger.rs`.
const ORDER_LOCK_KEY: &str = "x'505249534d000002'::bigint";

/// Deliberately long ledger transactions share advisory locks across schemas.
use super::test_serial::TEST_LOCK;

type Answer = std::result::Result<(), StratumError>;

// ---------------------------------------------------------------------------
// Scripted qbit node: it accepts every submitted block onto its active chain.
// ---------------------------------------------------------------------------

struct NodeState {
    chain: BTreeMap<u64, String>,
    chainwork: u128,
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let height = *node
        .chain
        .keys()
        .next_back()
        .expect("the scripted chain always retains its genesis");
    let tip = node.chain[&height].clone();
    let result = match request["method"]
        .as_str()
        .expect("the coordinator always names an RPC method")
    {
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":height,"headers":height,"bestblockhash":tip,
            "chainwork":format!("{:064x}",node.chainwork)}),
        "getbestblockhash" => json!(tip),
        "getblockhash" => request["params"][0]
            .as_u64()
            .and_then(|height| node.chain.get(&height))
            .map_or(Value::Null, |hash| json!(hash)),
        "getnetworkinfo" => json!({"connections":2}),
        "getblockheader" => {
            let hash = request["params"][0].as_str().unwrap_or_default().to_owned();
            match node
                .chain
                .iter()
                .find(|(_, known)| **known == hash)
                .map(|(height, _)| *height)
            {
                Some(height) => json!({"hash":hash,"height":height,
                    "previousblockhash":height.checked_sub(1).and_then(|parent| node.chain.get(&parent)).cloned()
                        .unwrap_or_else(|| "00".repeat(32))}),
                None => Value::Null,
            }
        }
        "getblocktemplate" => json!({"version":0x2000_0000u32,"bits":TEMPLATE_BITS,
            "height":height+1,"coinbasevalue":500_000_000u64,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":tip,"transactions":[]}),
        "submitblock" => {
            let block = hex::decode(
                request["params"][0]
                    .as_str()
                    .expect("submitblock carries a hex block"),
            )
            .expect("submitblock carries a hex block");
            node.chain.insert(
                height + 1,
                codec::hash_display(&codec::double_sha256(&block[..80])),
            );
            node.chainwork += 1;
            Value::Null
        }
        method => panic!("unexpected commit-reconcile RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    /// A second pool on the test schema, without the ledger's session
    /// timeouts, for holding locks and inspecting the server.
    side: PgPool,
    _server: AbortOnDropHandle<()>,
}

struct Proof {
    job: MiningJob<JobContext>,
    submission: codec::Submission,
    share_id: String,
    block_hash: String,
}

impl Fixture {
    /// A coordinator with built work. `share_commit_timeout` is 1 s unless
    /// `tune` changes it.
    async fn open(raw: &str, tune: impl FnOnce(&mut Config)) -> Result<Self> {
        let schema = TestSchema::create(raw, "prism_commit_reconcile").await?;
        let opened = async {
            let node = Arc::new(Mutex::new(NodeState {
                chain: BTreeMap::from([(0, "00".repeat(32)), (100, "aa".repeat(32))]),
                chainwork: 1,
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let mut config = test_config(
                schema.url(),
                rpc_url,
                "commit-reconcile",
                Duration::from_secs(1),
            )?;
            tune(&mut config);
            let app = Router::new().route("/", post(node_reply)).with_state(node);
            // Dropping the handle on any later failure aborts the server.
            let server = AbortOnDropHandle::new(tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            }));
            let coordinator =
                Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await?;
            let side = PgPool::connect(schema.url()).await?;
            coordinator.refresh_once().await?;
            Ok::<_, anyhow::Error>((coordinator, side, server))
        }
        .await;
        match opened {
            Ok((coordinator, side, server)) => Ok(Self {
                schema,
                coordinator,
                side,
                _server: server,
            }),
            Err(error) => Err(schema.abandon(error).await),
        }
    }

    async fn close(self) -> Result<()> {
        self.side.close().await;
        self.coordinator.ledger.pool.close().await;
        self.schema.remove().await
    }

    /// A current-work proof. The codec's pass flags select the path: an
    /// ordinary share-pass append, or a block-only proof below its share
    /// target.
    async fn proof(&self, block_only: bool) -> Result<Proof> {
        let worker = Worker {
            username: "miner.rig".into(),
            payout_address: "miner".into(),
            worker_name: Some("rig".into()),
            p2mr_program_hex: "ab".repeat(32),
        };
        let job = MiningBackend::build_job(&*self.coordinator, &worker, EXTRANONCE1, 1e-12, 0.0)
            .await
            .map_err(|error| anyhow::anyhow!("job build failed: {}", error.message))?;
        let mut submission = (0..200_000u32)
            .find_map(|nonce| {
                let submission = job
                    .wire
                    .assemble_submission(
                        &"00".repeat(EXTRANONCE2_SIZE),
                        &format!("{:08x}", job.wire.ntime),
                        &format!("{nonce:08x}"),
                        None,
                        0,
                    )
                    .ok()?;
                (submission.share_pass && submission.block_pass).then_some(submission)
            })
            .context("no proof in the nonce budget")?;
        if block_only {
            submission.share_pass = false;
        } else {
            submission.block_pass = false;
        }
        Ok(Proof {
            share_id: format!("{}:{}", worker.username, submission.block_hash_hex),
            block_hash: submission.block_hash_hex.clone(),
            job,
            submission,
        })
    }

    fn submit(&self, proof: Proof) -> (AbortOnDropHandle<Answer>, SharedLog) {
        let log = SharedLog::default();
        let coordinator = self.coordinator.clone();
        let Proof {
            job, submission, ..
        } = proof;
        let submit = async move {
            MiningBackend::submit(
                &*coordinator,
                &job.context.worker,
                &job,
                submission,
                false.into(),
            )
            .await
        };
        (
            AbortOnDropHandle::new(tokio::spawn(submit.with_subscriber(log.dispatch()))),
            log,
        )
    }

    /// Run `body` inside every COMMIT that inserted a ledger row.
    async fn hold_commit(&self, body: &str) -> Result<()> {
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION commit_reconcile_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {body}; RETURN NULL; END $$; \
             CREATE CONSTRAINT TRIGGER commit_reconcile_hold AFTER INSERT ON qbit_share_ledger \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION commit_reconcile_hold();"
        ))
        .execute(&self.side)
        .await?;
        Ok(())
    }

    async fn rows(&self, share_id: &str) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
                .bind(share_id)
                .fetch_one(&self.side)
                .await?,
        )
    }

    async fn outbox_state(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_optional(&self.side)
                .await?,
        )
    }

    /// The last share sequence number handed out. Sequences do not roll back,
    /// so this moves whenever an append reaches its INSERT, committed or not.
    async fn share_sequence(&self) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT pg_sequence_last_value(pg_get_serial_sequence('qbit_share_ledger','share_seq')::regclass)",
        )
        .fetch_one(&self.side)
        .await?)
    }

    fn late_confirmed(&self) -> u64 {
        self.coordinator
            .metrics
            .render()
            .lines()
            .find_map(|line| line.strip_prefix("qbit_prism_late_confirmed_shares_total "))
            .map_or(0, |value| {
                value.trim().parse::<f64>().unwrap_or(-1.0) as u64
            })
    }

    /// Land, submit and finish the queued candidate by hand; `submit_loop`
    /// never runs in these tests.
    async fn drive_candidate(&self) -> Result<()> {
        let claim = self
            .coordinator
            .ledger
            .claim_candidate(10)
            .await?
            .context("the enqueued candidate could not be claimed")?;
        self.coordinator.process_candidate(&claim).await
    }

    /// Backends waiting on a lock `pid` holds.
    async fn blocked_by(&self, pid: i32) -> Result<Vec<i32>> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(array_agg(pid),'{}') FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))",
        )
        .bind(pid)
        .fetch_one(&self.side)
        .await?)
    }

    /// Hold `ORDER_LOCK` on a dedicated session and wait until an append
    /// queues behind it. Returns the session and the queued backends.
    async fn hold_order_lock(&self) -> Result<(sqlx::pool::PoolConnection<sqlx::Postgres>, i32)> {
        let mut holder = self.side.acquire().await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *holder)
            .await?;
        sqlx::query(&format!("SELECT pg_advisory_lock({ORDER_LOCK_KEY})"))
            .execute(&mut *holder)
            .await?;
        Ok((holder, pid))
    }
}

/// Poll `probe` until it succeeds, with a hard ceiling instead of an open wait.
async fn until<F>(what: &str, mut probe: impl FnMut() -> F) -> Result<()>
where
    F: std::future::Future<Output = Result<bool>>,
{
    let deadline = TokioInstant::now() + PATIENCE;
    loop {
        if probe().await? {
            return Ok(());
        }
        ensure!(
            TokioInstant::now() < deadline,
            "timed out after {}s waiting for {what}",
            PATIENCE.as_secs()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn answer(submitted: AbortOnDropHandle<Answer>) -> Result<Answer> {
    Ok(tokio::time::timeout(PATIENCE, submitted)
        .await
        .context("the submission never resolved")??)
}

fn reason(answer: &Answer) -> Option<String> {
    answer
        .as_ref()
        .err()
        .and_then(|error| error.reason_id.as_deref().map(str::to_owned))
}

async fn wait_until_blocked(fixture: &Fixture, holder: i32) -> Result<Vec<i32>> {
    let deadline = TokioInstant::now() + PATIENCE;
    loop {
        let blocked = fixture.blocked_by(holder).await?;
        if !blocked.is_empty() {
            return Ok(blocked);
        }
        ensure!(
            TokioInstant::now() < deadline,
            "no append queued behind ORDER_LOCK"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Share-pass appends
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_commit_held_past_the_deadline_is_accepted_late() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |_| {}).await?;
    let outcome = async {
        let proof = fixture.proof(false).await?;
        let share_id = proof.share_id.clone();
        fixture.hold_commit("PERFORM pg_sleep(2)").await?;
        let started = TokioInstant::now();
        let (submitted, _log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            answer.is_ok(),
            "a COMMIT confirmed within the grace period was rejected with {:?}",
            reason(&answer)
        );
        ensure!(
            started.elapsed() >= Duration::from_secs(2),
            "the deferred trigger did not hold COMMIT past the share deadline"
        );
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "expected one ledger row"
        );
        ensure!(
            fixture.late_confirmed() == 1,
            "late_confirmed_shares_total is {}",
            fixture.late_confirmed()
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_commit_refused_by_the_server_is_rejected_without_a_row() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |_| {}).await?;
    let outcome = async {
        let proof = fixture.proof(false).await?;
        let share_id = proof.share_id.clone();
        fixture
            .hold_commit("RAISE EXCEPTION 'commit refused by the test trigger'")
            .await?;
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "a severity-ERROR COMMIT reply was answered {:?}",
            reason(&answer)
        );
        ensure!(
            log.text().contains("commit refused by the test trigger"),
            "the refusal did not come from COMMIT:\n{}",
            log.text()
        );
        ensure!(
            fixture.rows(&share_id).await? == 0,
            "a refused COMMIT left a row"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_append_queued_on_the_order_lock_is_aborted_at_the_deadline() -> Result<()>
{
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |_| {}).await?;
    let outcome = async {
        let proof = fixture.proof(false).await?;
        let share_id = proof.share_id.clone();
        let sequence = fixture.share_sequence().await?;
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let started = TokioInstant::now();
        let (submitted, _log) = fixture.submit(proof);
        let queued = wait_until_blocked(&fixture, holder_pid).await?;
        let answer = answer(submitted).await?;
        let answered = started.elapsed();
        ensure!(
            reason(&answer).as_deref() == Some("ledger-confirmation-failed"),
            "an append that never reached COMMIT was answered {:?}",
            reason(&answer)
        );
        ensure!(
            answered >= Duration::from_secs(1) && answered < Duration::from_secs(3),
            "answered after {answered:?}, not at the share deadline"
        );
        sqlx::query(&format!("SELECT pg_advisory_unlock({ORDER_LOCK_KEY})"))
            .execute(&mut *holder)
            .await?;
        // Every backend that queued behind the holder has now been granted
        // the lock or given up; the aborted append must run nothing more.
        until("the queued backends to stop waiting", || async {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE pid=ANY($1) AND wait_event_type='Lock'",
            )
            .bind(&queued)
            .fetch_one(&fixture.side)
            .await?;
            Ok(waiting == 0)
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        ensure!(fixture.rows(&share_id).await? == 0, "a refused append left a row");
        ensure!(
            fixture.share_sequence().await? == sequence,
            "the aborted append kept running after the lock was released"
        );
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_commit_held_past_the_grace_is_unknown_and_lands_later() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.share_commit_grace = Duration::from_secs(1)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(false).await?;
        let share_id = proof.share_id.clone();
        fixture.hold_commit("PERFORM pg_sleep(3)").await?;
        let started = TokioInstant::now();
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        let answered = started.elapsed();
        ensure!(
            reason(&answer).as_deref() == Some("ledger-outcome-unknown"),
            "a COMMIT still in flight after the grace period was answered {:?}",
            reason(&answer)
        );
        ensure!(
            answered >= Duration::from_secs(2) && answered < Duration::from_secs(3),
            "answered after {answered:?}, not at the share deadline plus grace"
        );
        let text = log.text();
        ensure!(
            text.contains("commit-in-flight") && text.contains(&share_id),
            "the unknown outcome was not logged with its share ID:\n{text}"
        );
        until("the in-flight COMMIT to land", || async {
            Ok(fixture.rows(&share_id).await? == 1)
        })
        .await?;
        ensure!(
            fixture.late_confirmed() == 0,
            "an unknown answer counted as late"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_backend_terminated_during_commit_is_unknown() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |_| {}).await?;
    let outcome = async {
        let proof = fixture.proof(false).await?;
        let share_id = proof.share_id.clone();
        fixture.hold_commit("PERFORM pg_sleep(3)").await?;
        let started = TokioInstant::now();
        let (submitted, log) = fixture.submit(proof);
        let deadline = TokioInstant::now() + PATIENCE;
        let pid: i32 = loop {
            let committing = sqlx::query_scalar(
                "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event='PgSleep' AND upper(btrim(query))='COMMIT'",
            )
            .fetch_optional(&fixture.side)
            .await?;
            if let Some(pid) = committing {
                break pid;
            }
            ensure!(
                TokioInstant::now() < deadline,
                "the append's backend never slept inside COMMIT"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&fixture.side)
            .await?;
        ensure!(terminated, "the committing backend could not be terminated");
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-outcome-unknown"),
            "a COMMIT whose backend was terminated was answered {:?}",
            reason(&answer)
        );
        // The reply to COMMIT was a severity-FATAL error, which is not proof
        // of a rollback.
        let text = log.text();
        ensure!(
            text.contains("commit-error") && text.contains("terminating connection"),
            "the FATAL COMMIT reply was not logged as an unknown outcome:\n{text}"
        );
        tokio::time::sleep_until(started + Duration::from_millis(3500)).await;
        ensure!(
            fixture.rows(&share_id).await? == 0,
            "a COMMIT terminated before its commit record left a row"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_ledger_hook_refusal_sends_no_commit() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |_| {}).await?;
    let outcome = async {
        let ledger = &fixture.coordinator.ledger;
        let revision = ledger.payout_revision().await?;
        let share = AcceptedShare {
            share_seq: 0,
            share_id: format!("miner:{}", "51".repeat(32)),
            miner_id: "miner".into(),
            order_key: "miner".into(),
            p2mr_program_hex: "ab".repeat(32),
            share_difficulty: 100,
            network_difficulty: 100,
            template_height: 100,
            job_id: "hook".into(),
            job_issued_at_ms: 1,
            accepted_at_ms: 0,
            ntime: 1_800_000_000,
            credit_policy: None,
        };
        let calls = AtomicU64::new(0);
        let refuse = || {
            calls.fetch_add(1, Ordering::SeqCst);
            false
        };
        let error = ledger
            .append_at_revision_gated(share.clone(), None, revision, &refuse)
            .await
            .expect_err("a refusing hook cannot commit");
        ensure!(
            error
                .downcast_ref::<crate::ledger::CommitGateClosed>()
                .is_some()
                && !error.to_string().contains("duplicate"),
            "the refusal surfaced as {error:#}"
        );
        ensure!(
            calls.load(Ordering::SeqCst) == 1,
            "the hook was not called exactly once"
        );
        ensure!(
            fixture.rows(&share.share_id).await? == 0,
            "COMMIT was sent after the hook refused it"
        );
        // The refusal rolled back and released ORDER_LOCK: the same share
        // appends normally through a permitting hook.
        let allow = || {
            calls.fetch_add(1, Ordering::SeqCst);
            true
        };
        let appended = ledger
            .append_at_revision_gated(share.clone(), None, revision, &allow)
            .await?;
        ensure!(appended.inserted, "the permitted append was not inserted");
        ensure!(
            calls.load(Ordering::SeqCst) == 2,
            "the hook was not called exactly once"
        );
        ensure!(
            fixture.rows(&share.share_id).await? == 1,
            "expected one ledger row"
        );
        let clock_before: i64 =
            sqlx::query_scalar("SELECT ledger_clock_ms FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&fixture.side)
                .await?;
        // The hook may close after the transaction has already verified an
        // identical durable share. Its read-only rollback must retain that
        // known duplicate outcome, without another credit or clock update.
        let duplicate = ledger
            .append_at_revision_gated(share.clone(), None, revision, &refuse)
            .await?;
        ensure!(
            !duplicate.inserted && duplicate.share == appended.share,
            "closed gate discarded the known identical duplicate"
        );
        ensure!(
            calls.load(Ordering::SeqCst) == 3,
            "duplicate did not reach the gate"
        );
        let clock_after: i64 =
            sqlx::query_scalar("SELECT ledger_clock_ms FROM qbit_prism_cluster WHERE singleton")
                .fetch_one(&fixture.side)
                .await?;
        ensure!(
            clock_after == clock_before && fixture.rows(&share.share_id).await? == 1,
            "read-only duplicate changed durable credit or its clock"
        );
        let mut mismatched = share.clone();
        mismatched.job_id = "different-immutable-input".into();
        let mismatch = ledger
            .append_at_revision_gated(mismatched, None, revision, &refuse)
            .await
            .expect_err("different payload cannot borrow prior credit");
        ensure!(
            mismatch
                .to_string()
                .contains("duplicate share_id payload mismatch")
                && calls.load(Ordering::SeqCst) == 3,
            "payload mismatch was reclassified as a known identical duplicate"
        );

        // The production adapter must preserve the private gate's actual
        // winning cause through a real SQL rollback. The public bool hook
        // and CommitGateClosed error intentionally carry no inferred cause.
        use super::submit_ledger::{CommitGate, GateClosure, SubmitLedger};
        for (index, expected) in [
            GateClosure::StaleAuthority,
            GateClosure::AuthorityUnavailable,
            GateClosure::DeadlineOrCancelled,
        ]
        .into_iter()
        .enumerate()
        {
            {
                let mut tip = fixture.coordinator.observed_tip.write().await;
                let sequence = tip.reserve();
                tip.observe(&"bb".repeat(32), sequence, true);
            }
            let admission = fixture
                .coordinator
                .submit_admission()
                .await
                .map_err(|error| anyhow::anyhow!("lease admission: {}", error.message))?;
            let lease = admission.lease.context("replacement lease missing")?;
            let gate = Arc::new(CommitGate::with_lease(Some(
                fixture.coordinator.lease_commit_fence(lease, None),
            )));
            if expected == GateClosure::DeadlineOrCancelled {
                ensure!(gate.close(), "the original deadline must win");
            }
            // Publish a new generation after admission, before the SQL hook.
            // The unavailable case holds the publication lock instead: a
            // failed try_read cannot prove whether any work became stale.
            let contended = if expected == GateClosure::AuthorityUnavailable {
                Some(fixture.coordinator.prepared.write().await)
            } else {
                let mut tip = fixture.coordinator.observed_tip.write().await;
                let sequence = tip.reserve();
                tip.observe(&"aa".repeat(32), sequence, true);
                tip.publish(&"aa".repeat(32))?;
                None
            };
            let mut refused = share.clone();
            refused.share_id = format!("miner:{}", format!("{:02x}", index + 0x61).repeat(32));
            let error = SubmitLedger::append_at_revision(
                ledger.as_ref(),
                refused.clone(),
                None,
                revision,
                gate.clone(),
            )
            .await
            .expect_err("a closed gate must roll back the new share");
            ensure!(
                error
                    .downcast_ref::<crate::ledger::CommitGateClosed>()
                    .is_some(),
                "expected a typed gate refusal: {error:#}"
            );
            ensure!(
                gate.closure() == Some(expected),
                "wrong closure: {:?}",
                gate.closure()
            );
            ensure!(
                fixture.rows(&refused.share_id).await? == 0,
                "refused share committed"
            );
            // A closed stale gate cannot erase a credit already confirmed by
            // the immutable-row match, nor turn that duplicate into stale.
            let duplicate = SubmitLedger::append_at_revision(
                ledger.as_ref(),
                share.clone(),
                None,
                revision,
                gate,
            )
            .await?;
            ensure!(!duplicate, "closed gate changed a confirmed duplicate");
            drop(contended);
            // Rollback also released ORDER_LOCK for a subsequent valid append.
            ensure!(
                ledger
                    .append_at_revision(refused.clone(), None, revision)
                    .await?
                    .inserted,
                "a refused transaction retained its lock or row"
            );
            ensure!(
                fixture.rows(&refused.share_id).await? == 1,
                "expected one later credit"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

// ---------------------------------------------------------------------------
// Block-only proofs
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_block_only_confirmed_after_the_share_deadline_is_accepted() -> Result<()>
{
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.block_only_ack_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let started = TokioInstant::now();
        let (submitted, _log) = fixture.submit(proof);
        until("the pending candidate", || async {
            Ok(fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"))
        })
        .await?;
        tokio::time::sleep_until(started + Duration::from_millis(1500)).await;
        ensure!(
            !submitted.is_finished(),
            "a block-only proof was answered at the share deadline"
        );
        fixture.drive_candidate().await?;
        let answer = answer(submitted).await?;
        ensure!(
            answer.is_ok(),
            "a confirmed block-only proof was answered {:?}",
            reason(&answer)
        );
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "expected one credit row"
        );
        ensure!(
            fixture.late_confirmed() == 0,
            "block-only counted a late confirmation"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_block_only_still_pending_at_its_bound_is_unknown() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.block_only_ack_timeout = Duration::from_millis(1500)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let started = TokioInstant::now();
        let (submitted, log) = fixture.submit(proof);
        let answer = answer(submitted).await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-outcome-unknown"),
            "a still-pending candidate was answered {:?}",
            reason(&answer)
        );
        ensure!(started.elapsed() >= Duration::from_millis(1500));
        let text = log.text();
        ensure!(
            text.contains("candidate-pending")
                && text.contains("block-only")
                && text.contains(&block_hash),
            "the unknown outcome was not logged with its block:\n{text}"
        );
        ensure!(fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"));
        // The candidate is still confirmed, and credited, afterwards.
        fixture.drive_candidate().await?;
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "the later confirmation did not credit the proof"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_block_only_enqueue_behind_the_order_lock_is_not_cut_off() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.block_only_ack_timeout = Duration::from_secs(10)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let started = TokioInstant::now();
        let (submitted, _log) = fixture.submit(proof);
        wait_until_blocked(&fixture, holder_pid).await?;
        // Past the share deadline, inside the 5 s lock_timeout.
        tokio::time::sleep_until(started + Duration::from_millis(1500)).await;
        ensure!(
            !submitted.is_finished(),
            "a block-only enqueue was cut off at the share deadline"
        );
        sqlx::query(&format!("SELECT pg_advisory_unlock({ORDER_LOCK_KEY})"))
            .execute(&mut *holder)
            .await?;
        until("the pending candidate", || async {
            Ok(fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"))
        })
        .await?;
        ensure!(!submitted.is_finished(), "answered before confirmation");
        fixture.drive_candidate().await?;
        let answer = answer(submitted).await?;
        ensure!(
            answer.is_ok(),
            "a block-only proof enqueued late was answered {:?}",
            reason(&answer)
        );
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "expected one credit row"
        );
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_block_only_poll_errors_are_retried_then_unknown() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.block_only_ack_timeout = Duration::from_secs(8)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let started = TokioInstant::now();
        let (submitted, log) = fixture.submit(proof);
        until("the pending candidate", || async {
            Ok(fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"))
        })
        .await?;
        // Every later disposition poll now waits out the ledger's 5 s
        // lock_timeout and fails.
        let mut lock = fixture.side.begin().await?;
        sqlx::query("LOCK TABLE qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await?;
        let answer = answer(submitted).await?;
        let answered = started.elapsed();
        lock.rollback().await?;
        ensure!(
            reason(&answer).as_deref() == Some("ledger-outcome-unknown"),
            "failed disposition polls were answered {:?}",
            reason(&answer)
        );
        ensure!(
            answered >= Duration::from_secs(8),
            "answered after {answered:?}, before the block-only bound"
        );
        let text = log.text();
        ensure!(
            text.contains("block-only disposition poll failed; retrying")
                && text.contains("poll-error"),
            "the failed polls were not retried and logged:\n{text}"
        );
        fixture.drive_candidate().await?;
        ensure!(
            fixture.rows(&share_id).await? == 1,
            "the later confirmation did not credit the proof"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

/// A degraded database must not hold the acknowledgement past the block-only
/// bound. The enqueue itself is never cut off, so the miner is answered unknown
/// while the candidate still lands afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reconcile_block_only_enqueue_past_the_bound_is_unknown() -> Result<()> {
    let Some(raw) = database_url()? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::open(&raw, |config| {
        config.block_only_ack_timeout = Duration::from_millis(1500)
    })
    .await?;
    let outcome = async {
        let proof = fixture.proof(true).await?;
        let (share_id, block_hash) = (proof.share_id.clone(), proof.block_hash.clone());
        let (mut holder, holder_pid) = fixture.hold_order_lock().await?;
        let started = TokioInstant::now();
        let (submitted, _log) = fixture.submit(proof);
        wait_until_blocked(&fixture, holder_pid).await?;
        let answer = answer(submitted).await?;
        let waited = started.elapsed();
        ensure!(
            reason(&answer).as_deref() == Some("ledger-outcome-unknown"),
            "an enqueue still running at the bound was answered {:?}",
            reason(&answer)
        );
        // Well inside the 5 s lock_timeout, so the answer came from the bound
        // rather than from the enqueue failing.
        ensure!(
            waited < Duration::from_secs(4),
            "the acknowledgement outlived the block-only bound by {waited:?}"
        );
        ensure!(
            fixture.outbox_state(&block_hash).await?.is_none(),
            "the candidate cannot be enqueued while the order lock is held"
        );
        // The enqueue was never cut off: it commits once the lock is released.
        sqlx::query(&format!("SELECT pg_advisory_unlock({ORDER_LOCK_KEY})"))
            .execute(&mut *holder)
            .await?;
        until("the pending candidate", || async {
            Ok(fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"))
        })
        .await?;
        ensure!(
            fixture.rows(&share_id).await? == 0,
            "a block-only proof must not be credited before confirmation"
        );
        drop(holder);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}
