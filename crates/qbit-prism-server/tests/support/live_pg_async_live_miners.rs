//! #474 slice B: an asynchronous PostgreSQL promotion while live miners,
//! their Stratum sessions and both frontends' caches survive.
//!
//! The pair, relays and procedure are the parent module's (#521 scenario
//! 3): a D3 primary with one dedicated asynchronous standby, both native
//! frontends on the stable writer endpoint, a real qbitd. Instead of CPU
//! miners stopped at the fence, four scripted Stratum miners
//! ([`ShareClient`]), each with its own payout address, submit shares on
//! their own sessions from before the replication cut, through the
//! unreplicated interval and across the fence, and keep those sessions
//! through the promotion. Every submission is recorded by identity with
//! the answer the miner received: acknowledged, rejected, or unknown (no
//! answer, a lost connection, or `ledger-outcome-unknown`).
//!
//! Submissions are in flight at the fence by construction: a deferred
//! trigger on the header map holds every share's COMMIT, already sent, on
//! an advisory lock the test takes just before it fences the writers. The
//! held COMMITs complete on the old primary after the fence, for clients
//! that are gone; nothing else is admitted there.
//!
//! D3 is asynchronous: acknowledged shares in the replication gap are lost
//! by design, and this case asserts no zero-loss property. It asserts that
//! every loss is accounted for exactly: each acknowledged share is on the
//! old primary; each acknowledged share the promoted ledger lacks was
//! committed inside the gap, the suffix of the commit order the standby
//! never received, and is named; no definite rejection was
//! committed anywhere; the old writer committed nothing after the fence
//! but the COMMITs it had already received.
//!
//! After the endpoint moves, the same sessions submit shares on their
//! current jobs and the work each miner kept from before the cut and from
//! inside the gap
//! (including a session opened inside the gap, whose job was issued there):
//! kept work is either resumed under its original authority or refused
//! truthfully as stale. A real block on work whose window the promoted
//! ledger holds then lands, and its audit, verified against the node's
//! coinbase, pays exactly a window read independently from the promoted
//! ledger, which holds sequence numbers lost shares had: no share the
//! promotion lost and no history a frontend cached before it. Then the lost
//! acknowledged shares are replayed. Each answer must match the promoted
//! ledger: accepted means credited exactly once, and a truthful stale
//! refusal means not credited. A surviving share replayed is a duplicate.
//! Once the final block has moved the tip, every surviving session receives
//! work for it and a share on that work is credited exactly once. A block
//! solved on the gap-issued job commits to the lost window; submitting it is #619's opt-in case below,
//! which asserts it is refused as stale or lands paying only promoted rows
//! (today it is offered and never lands), so here it is kept.
//! #466's replaced-history regression
//! (`ledger/window/snapshot_delta/tests/physical_failover.rs::
//! physical_async_failover_replaced_history_matches_full_read`) covers the
//! delta read itself and is not repeated here.
use super::*;
use crate::share_client::{
    reason_id, start_share_only_servers, Answer, Proof, ShareClient, Submitted,
};
use std::collections::{BTreeMap, BTreeSet};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_promotion_with_live_miners_accounts_for_every_share_and_lands_a_block() -> Result<()>
{
    drill(GapBlock::Kept).await
}

/// #619: the block solved on the gap-issued job, submitted after the
/// promotion, must be refused as stale authority or land paying only rows
/// the promoted ledger holds. Today it is accepted, offered and put on chain
/// with a coinbase paying the lost window, and never lands in the pool
/// (`window range incomplete`). Opt-in until #619 decides and fixes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "#619: a block on a job issued in the replication gap is offered but never lands"]
async fn issue_619_gap_issued_block_after_async_promotion_is_refused_or_pays_only_promoted_rows(
) -> Result<()> {
    drill(GapBlock::Submitted).await
}

/// What becomes of the block the gap miner solves on its gap-issued job.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GapBlock {
    /// Solved and kept, never submitted (#619).
    Kept,
    /// Submitted after the promotion; the #619 assertion.
    Submitted,
}

async fn drill(gap_block: GapBlock) -> Result<()> {
    let Some(bin) = gate::pg_bin_dir(gate::site!())? else {
        return Ok(());
    };
    let _drill = DRILLS.lock().await;
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
    let result = promotion(&mut fixture, &mut pair, &mut timeline, gap_block).await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
        eprintln!("{}", pair.diagnostics());
        eprintln!("promotion timeline: {}", timeline.render());
        pair.writer.route_to(pair.writable_port());
    }
    let cleanup = fixture.cleanup().await;
    drop(pair);
    result.and(cleanup)
}

/// A prepared record's window (first and last share sequence number and
/// share count) and how many accepted shares the ledger holds in that range
/// under the landing's predicate (accepted and issued by the window anchor).
type PreparedWindow = (Option<i64>, Option<i64>, Option<i64>, i64);

/// A candidate's window (first and last share sequence number, share count,
/// anchor), the accepted rows in its range, and those the landing reads.
type CandidateWindow = (Option<i64>, Option<i64>, Option<i64>, Option<i64>, i64, i64);

/// The scripted miners submitting through the promotion.
const MINERS: usize = 4;

/// Where the procedure was when a submission was sent or answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    BeforeCut,
    Gap,
    Fenced,
}

/// What the miner learned about one submission.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Acknowledged,
    Rejected(String),
    Unknown(String),
}

impl Outcome {
    fn of(answer: &Answer) -> Self {
        match answer {
            Answer::Accepted => Self::Acknowledged,
            Answer::Rejected(response) => match reason_id(response) {
                Some("ledger-outcome-unknown") => Self::Unknown("ledger-outcome-unknown".into()),
                Some(reason) => Self::Rejected(reason.into()),
                None => Self::Rejected(response["error"].to_string()),
            },
            other => Self::Unknown(other.to_string()),
        }
    }
}

/// One submission by identity, the phase it was sent and answered in, and
/// what the miner learned.
#[derive(Clone, Debug)]
struct Record {
    miner: usize,
    sent: Phase,
    answered: Phase,
    params: Value,
    share_id: String,
    outcome: Outcome,
}

impl Record {
    /// The submission again, for a replay on the same session.
    fn replay(&self) -> (Value, Submitted) {
        (
            self.params.clone(),
            Submitted {
                hash: self.share_id.rsplit(':').next().unwrap_or_default().into(),
                share_id: self.share_id.clone(),
                answer: Answer::TimedOut,
            },
        )
    }
}

/// Work a miner solved and kept without submitting it, and the phase it was
/// kept in.
struct Kept {
    miner: usize,
    kept: Phase,
    params: Value,
    submitted: Submitted,
}

#[derive(Default)]
struct Book {
    records: Vec<Record>,
    kept: Vec<Kept>,
}

/// One miner's loop: keep one share of work from before the cut and one
/// from inside the gap, and submit shares until one it sent after the fence
/// has been answered. Returns the session, which must still be connected.
async fn mine(
    miner: usize,
    mut client: ShareClient,
    phase: watch::Receiver<Phase>,
    book: Arc<Mutex<Book>>,
) -> Result<ShareClient> {
    let mut kept = BTreeSet::new();
    loop {
        let sent = *phase.borrow();
        if matches!(sent, Phase::BeforeCut | Phase::Gap) && kept.insert(sent) {
            let (params, submitted) = client.solve(Proof::Share).await?;
            book.lock().unwrap().kept.push(Kept {
                miner,
                kept: sent,
                params,
                submitted,
            });
        }
        let (params, submitted) = client.solve(Proof::Share).await?;
        let submitted = client.submit_solved(params.clone(), submitted).await;
        let answered = *phase.borrow();
        let lost = matches!(submitted.answer, Answer::Lost(_));
        book.lock().unwrap().records.push(Record {
            miner,
            sent,
            answered,
            params,
            share_id: submitted.share_id,
            outcome: Outcome::of(&submitted.answer),
        });
        ensure!(
            !lost,
            "miner {miner}'s session did not survive: {}",
            submitted.answer
        );
        if sent == Phase::Fenced {
            return Ok(client);
        }
        // Paced so the gap holds tens of shares, not thousands.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The miners that have kept work from `phase`.
fn kept_by(book: &Mutex<Book>, phase: Phase) -> BTreeSet<usize> {
    book.lock()
        .unwrap()
        .kept
        .iter()
        .filter(|work| work.kept == phase)
        .map(|work| work.miner)
        .collect()
}

/// Positive acknowledgements recorded so far that were sent in `phase` and
/// are not in `except`.
fn acknowledged(book: &Mutex<Book>, phase: Phase, except: &BTreeSet<String>) -> usize {
    book.lock()
        .unwrap()
        .records
        .iter()
        .filter(|record| {
            record.sent == phase
                && record.outcome == Outcome::Acknowledged
                && !except.contains(&record.share_id)
        })
        .count()
}

/// Client backends on `pool`'s cluster other than the observer's own.
async fn writer_backends(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND application_name<>'b474-observer'",
    )
    .fetch_one(pool)
    .await?)
}

/// A deferred trigger on the header map that holds every share's COMMIT,
/// once the client has sent it, while the test holds an advisory lock on
/// the old primary. Open, it takes the lock shared and holds nothing.
struct CommitHold {
    key: String,
    holder: Option<(sqlx::pool::PoolConnection<sqlx::Postgres>, i32)>,
}

impl CommitHold {
    async fn install(primary: &PgPool, schema: &str) -> Result<Self> {
        let key = format!("{schema}.b474_commit_hold");
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION {schema}.b474_commit_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock_shared(hashtext('{key}')::bigint); RETURN NULL; END $$; CREATE CONSTRAINT TRIGGER b474_commit_hold AFTER INSERT ON {schema}.qbit_prism_share_hashes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {schema}.b474_commit_hold();"
        ))
        .execute(primary)
        .await?;
        Ok(Self { key, holder: None })
    }

    async fn close(&mut self, primary: &PgPool) -> Result<()> {
        let mut connection = primary.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock(hashtext($1)::bigint)")
            .bind(&self.key)
            .execute(&mut *connection)
            .await?;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await?;
        self.holder = Some((connection, pid));
        Ok(())
    }

    /// COMMITs waiting on the closed hold.
    async fn held(&self, primary: &PgPool) -> Result<i64> {
        let (_, pid) = self.holder.as_ref().context("the hold is open")?;
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))",
        )
        .bind(pid)
        .fetch_one(primary)
        .await?)
    }

    async fn open(&mut self) -> Result<()> {
        let (mut connection, _) = self.holder.take().context("the hold is open")?;
        sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
            .bind(&self.key)
            .execute(&mut *connection)
            .await?;
        Ok(())
    }

    /// Remove the trigger, which replicated before the cut, from `pool`.
    async fn remove(pool: &PgPool, schema: &str) -> Result<()> {
        sqlx::raw_sql(&format!(
            "DROP TRIGGER b474_commit_hold ON {schema}.qbit_prism_share_hashes; DROP FUNCTION {schema}.b474_commit_hold();"
        ))
        .execute(pool)
        .await?;
        Ok(())
    }
}

/// Every share in the ledger of `schema` with its sequence number.
async fn sequences(pool: &PgPool, schema: &str) -> Result<BTreeMap<String, i64>> {
    Ok(sqlx::query_as::<_, (String, i64)>(&format!(
        "SELECT share_id,share_seq FROM {schema}.qbit_share_ledger"
    ))
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

/// A fresh payout address from the fixture's wallet.
async fn address(f: &Fixture) -> Result<String> {
    Ok(f.rpc("getnewaddress", json!(["", "p2mr"]))
        .await?
        .as_str()
        .context("address missing")?
        .into())
}

/// Verify `block`'s audit against the node's coinbase and return the shares
/// it pays, with their sequence numbers.
async fn paid_shares(f: &Fixture, block: &str) -> Result<BTreeSet<(String, i64)>> {
    let body: Value = f
        .client
        .get(format!(
            "http://127.0.0.1:{}/audit/blocks/{block}/bundle",
            f.api[1]
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let bundle: AuditBundle = serde_json::from_value(body["audit_bundle"].clone())?;
    let verbose = f.rpc("getblock", json!([block, 2])).await?;
    let coinbase = f
        .rpc(
            "getrawtransaction",
            json!([verbose["tx"][0]["txid"], false, block]),
        )
        .await?;
    let key = ManifestSigningKey::from_seed_hex(&"22".repeat(32))?.public_key_hex();
    verify_audit_bundle_against_coinbase_tx_hex(
        &bundle,
        coinbase.as_str().context("node coinbase missing")?,
        &key,
    )?;
    bundle
        .shares
        .iter()
        .map(|share| Ok((share.share_id.clone(), i64::try_from(share.share_seq)?)))
        .collect()
}

/// The window a block found on the job of `share_id` (the block's own
/// credited share) pays, read from the promoted ledger independently of the
/// block's audit: every share issued and accepted by that job's issuance,
/// newest first, until their weight reaches `PRISM_WINDOW_MULTIPLIER`
/// network difficulties.
async fn expected_window(f: &Fixture, share_id: &str) -> Result<BTreeSet<(String, i64)>> {
    let (anchor, network): (i64, String) = sqlx::query_as(
        "SELECT floor(extract(epoch FROM job_issued_at)*1000)::bigint,network_difficulty::text FROM qbit_share_ledger WHERE accepted AND share_id=$1",
    )
    .bind(share_id)
    .fetch_one(&f.pool)
    .await
    .with_context(|| format!("the block's share {share_id} is not credited"))?;
    let eligible: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT share_id,share_seq,share_difficulty::text FROM qbit_share_ledger WHERE accepted AND floor(extract(epoch FROM job_issued_at)*1000)<=$1 AND floor(extract(epoch FROM accepted_at)*1000)<=$1 ORDER BY share_seq DESC",
    )
    .bind(anchor)
    .fetch_all(&f.pool)
    .await?;
    let weight = |text: &str| -> Result<u128> {
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        ensure!(
            fraction.bytes().all(|digit| digit == b'0'),
            "fractional scaled difficulty {text}"
        );
        Ok(whole.parse()?)
    };
    let requested = weight(&network)?
        .checked_mul(qbit_prism::PRISM_WINDOW_MULTIPLIER)
        .context("window weight overflow")?;
    let mut total = 0u128;
    let mut window = BTreeSet::new();
    for (share, seq, difficulty) in eligible {
        if total >= requested {
            break;
        }
        total += weight(&difficulty)?;
        window.insert((share, seq));
    }
    Ok(window)
}

/// The prepared window behind issued job `job_id` on the promoted ledger,
/// with how many accepted shares that ledger holds in its range under the
/// landing's predicate; `None` when the job or its prepared record is absent.
async fn job_window(f: &Fixture, job_id: &str) -> Result<Option<PreparedWindow>> {
    Ok(sqlx::query_as(
        "SELECT p.window_first_share_seq,p.window_last_share_seq,p.window_share_count,(SELECT count(*) FROM qbit_share_ledger l WHERE l.accepted AND l.share_seq BETWEEN p.window_first_share_seq AND p.window_last_share_seq AND l.accepted_at<=to_timestamp(p.window_anchor_ms::double precision/1000) AND l.job_issued_at<=to_timestamp(p.window_anchor_ms::double precision/1000)) FROM qbit_prism_jobs j JOIN qbit_prism_jobs p ON p.job_id=j.payload->>'prepared_key' WHERE j.job_id=$1",
    )
    .bind(job_id)
    .fetch_optional(&f.pool)
    .await?)
}

/// Until `block` is confirmed on the promoted primary; a timeout names the
/// candidate's state and last error.
async fn landed(f: &Fixture, block: &str) -> Result<()> {
    let result = until(
        &format!("block {block} landed and confirmed on the promoted primary"),
        90,
        || async {
            Ok(sqlx::query_scalar::<_, String>(
                "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1",
            )
            .bind(block)
            .fetch_optional(&f.pool)
            .await?
            .as_deref()
                == Some("confirmed"))
        },
    )
    .await;
    if result.is_err() {
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT state,offer_outcome,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
        )
        .bind(block)
        .fetch_optional(&f.pool)
        .await?;
        let header = f.rpc("getblockheader", json!([block])).await;
        // The window the candidate carries, and what the ledger holds of it.
        let window: Option<CandidateWindow> = sqlx::query_as(
            "SELECT o.window_first_share_seq,o.window_last_share_seq,o.window_share_count,o.window_anchor_ms,(SELECT count(*) FROM qbit_share_ledger l WHERE l.accepted AND l.share_seq BETWEEN o.window_first_share_seq AND o.window_last_share_seq),(SELECT count(*) FROM qbit_share_ledger l WHERE l.accepted AND l.share_seq BETWEEN o.window_first_share_seq AND o.window_last_share_seq AND l.accepted_at<=to_timestamp(o.window_anchor_ms::double precision/1000) AND l.job_issued_at<=to_timestamp(o.window_anchor_ms::double precision/1000)) FROM qbit_block_candidate_outbox o WHERE o.block_hash=$1",
        )
        .bind(block)
        .fetch_optional(&f.pool)
        .await?;
        return result.with_context(|| {
            format!(
                "candidate (state, offer outcome, last error): {row:?}; window (first, last, count, anchor ms, rows in range, rows the landing predicate reads): {window:?}; node confirmations: {:?}",
                header.map(|header| header["confirmations"].clone())
            )
        });
    }
    Ok(())
}

async fn promotion(
    f: &mut Fixture,
    pair: &mut Pair,
    timeline: &mut Timeline,
    gap_block_disposition: GapBlock,
) -> Result<()> {
    let observer = |port: u16| format!("{}?application_name=b474-observer", pair.url(port));
    let primary = PgPool::connect(&observer(pair.primary.port)).await?;
    let standby = PgPool::connect(&observer(pair.standby.port)).await?;
    let schema = f.schema.clone();
    let names: String = sqlx::query_scalar("SHOW synchronous_standby_names")
        .fetch_one(&primary)
        .await?;
    ensure!(
        names.is_empty(),
        "D3 requires an asynchronous standby, found {names:?}"
    );

    start_share_only_servers(f, &[(0, Vec::new()), (1, Vec::new())]).await?;
    let mut hold = CommitHold::install(&primary, &schema).await?;
    let (phase, watcher) = watch::channel(Phase::BeforeCut);
    let book = Arc::new(Mutex::new(Book::default()));
    let mut miners = Vec::new();
    for miner in 0..MINERS {
        let username = format!("{}.b474-{miner}", address(f).await?);
        let client = ShareClient::connect(f.stratum[miner % 2], &username).await?;
        miners.push(tokio::spawn(mine(
            miner,
            client,
            watcher.clone(),
            book.clone(),
        )));
    }

    // Before the cut: shares the standby replays.
    // Every miner keeps work from each phase before the procedure leaves it.
    let every_miner: BTreeSet<usize> = (0..MINERS).collect();
    until(
        "shares on the primary, and work kept by every miner, before the cut",
        30,
        || async {
            Ok(shares(&primary, &schema).await?.len() >= 12
                && kept_by(&book, Phase::BeforeCut) == every_miner)
        },
    )
    .await?;
    let pre_cut = shares(&primary, &schema).await?;
    until("standby replay of the pre-cut shares", 15, || async {
        Ok(shares(&standby, &schema).await?.is_superset(&pre_cut))
    })
    .await?;

    // The unreplicated interval.
    phase.send_replace(Phase::Gap);
    pair.replication.fence();
    timeline.mark("replication cut");
    until("standby replay settled after the cut", 15, || async {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT pg_last_wal_replay_lsn()=pg_last_wal_receive_lsn()",
        )
        .fetch_one(&standby)
        .await?)
    })
    .await?;
    let frozen = shares(&standby, &schema).await?;
    ensure!(
        frozen.is_superset(&pre_cut),
        "the standby lost pre-cut shares"
    );
    // The promoted primary resumes the share sequence after the value the
    // standby last received. Grow the gap well past it, so the first shares
    // credited after the promotion reuse sequence numbers lost shares had.
    let resumes_after: i64 = sqlx::query_scalar(&format!(
        "SELECT last_value FROM {schema}.qbit_share_ledger_share_seq_seq"
    ))
    .fetch_one(&standby)
    .await?;
    until(
        "acknowledged shares inside the replication gap, past the standby's sequence",
        30,
        || async {
            let committed: Option<i64> = sqlx::query_scalar(&format!(
                "SELECT max(share_seq) FROM {schema}.qbit_share_ledger"
            ))
            .fetch_one(&primary)
            .await?;
            Ok(acknowledged(&book, Phase::Gap, &frozen) >= 8
                && committed.is_some_and(|seq| seq >= resumes_after + 20)
                && kept_by(&book, Phase::Gap) == every_miner)
        },
    )
    .await?;
    // A session opened inside the gap, once server 0 has prepared work over
    // it (it re-anchors every second): its job, and the payout its coinbase
    // commits to, come from a window the promotion will lose.
    let replicated: i64 = sqlx::query_scalar(&format!(
        "SELECT max(share_seq) FROM {schema}.qbit_share_ledger"
    ))
    .fetch_one(&standby)
    .await?;
    until("server 0's work prepared over the gap", 15, || async {
        Ok(sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM {schema}.qbit_prism_jobs WHERE job_id LIKE 'prepared:live-0:%' AND window_last_share_seq>$1)"
        ))
        .bind(replicated)
        .fetch_one(&primary)
        .await?)
    })
    .await?;
    let mut gap_miner =
        ShareClient::connect(f.stratum[0], &format!("{}.b474-gap", address(f).await?)).await?;
    let gap_share = gap_miner.solve(Proof::Share).await?;
    let gap_block = gap_miner.solve(Proof::Block).await?;
    let gap_window: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT p.window_last_share_seq FROM {schema}.qbit_prism_jobs j JOIN {schema}.qbit_prism_jobs p ON p.job_id=j.payload->>'prepared_key' WHERE j.job_id=$1"
    ))
    .bind(gap_block.0[1].as_str().context("gap job id missing")?)
    .fetch_optional(&primary)
    .await?
    .flatten();
    ensure!(
        gap_window.is_some_and(|last| last > replicated),
        "the gap-issued job's window (last share {gap_window:?}) does not reach past the replicated share {replicated}"
    );

    // Fence the old writer with COMMITs in flight.
    hold.close(&primary).await?;
    // Appends serialize on the ledger's own lock, so one COMMIT is held and
    // the other miners' appends queue behind it, before their COMMITs.
    until("a share COMMIT held in flight", 15, || async {
        Ok(hold.held(&primary).await? >= 1)
    })
    .await?;
    // Closing the hold waited for every COMMIT already past it, and every
    // later one waits on it: this is the old ledger at the fence, less the
    // held COMMITs.
    let held = hold.held(&primary).await?;
    let before_fence = shares(&primary, &schema).await?;
    phase.send_replace(Phase::Fenced);
    pair.writer.fence();
    timeline.mark("writers fenced with COMMITs in flight");
    // The held COMMITs were sent before the fence; they complete on the old
    // primary for clients that are gone.
    hold.open().await?;
    until(
        "no writer connection left on the old primary",
        15,
        || async { Ok(writer_backends(&primary).await? == 0) },
    )
    .await?;
    let refused = tokio::time::timeout(
        Duration::from_secs(10),
        PgPool::connect(&pair.url(pair.writer.port)),
    )
    .await;
    ensure!(
        !matches!(refused, Ok(Ok(_))),
        "the fenced writer endpoint still admits a connection"
    );
    let at_fence = shares(&primary, &schema).await?;
    let completed: BTreeSet<String> = at_fence.difference(&before_fence).cloned().collect();
    ensure!(
        before_fence.is_subset(&at_fence) && (1..=held).contains(&i64::try_from(completed.len())?),
        "after the fence the old primary committed {} share(s), but only {held} COMMIT(s) were held",
        completed.len()
    );
    // Every miner sends one more share after the fence and is answered.
    let mut sessions = Vec::new();
    for miner in miners {
        sessions.push(miner.await??);
    }
    timeline.mark("post-fence submissions answered");
    let old_sequences = sequences(&primary, &schema).await?;
    let old_shares: BTreeSet<String> = old_sequences.keys().cloned().collect();
    ensure!(
        old_shares == at_fence,
        "the fenced old primary committed {} share(s) after its writers were gone",
        old_shares.difference(&at_fence).count()
    );
    let flush: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&primary)
        .await?;
    let gap_bytes: i64 =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn,pg_last_wal_receive_lsn())::bigint")
            .bind(&flush)
            .fetch_one(&standby)
            .await?;
    primary.close().await;
    pair.primary.stop()?;
    timeline.mark("old primary stopped");
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
    CommitHold::remove(&standby, &schema).await?;

    // Reconcile every recorded identity with both ledgers.
    let new_shares = shares(&standby, &schema).await?;
    ensure!(
        new_shares == frozen,
        "promotion changed what the standby had received"
    );
    ensure!(
        new_shares.is_subset(&old_shares),
        "the promoted ledger holds shares the old primary never committed"
    );
    let lost: BTreeSet<String> = old_shares.difference(&new_shares).cloned().collect();
    ensure!(
        !lost.is_empty() && gap_bytes > 0,
        "the drill did not produce an unreplicated interval"
    );
    ensure!(lost.is_disjoint(&pre_cut), "a lost share predates the cut");
    let records = book.lock().unwrap().records.clone();
    let offered: BTreeSet<String> = records
        .iter()
        .map(|record| record.share_id.clone())
        .collect();
    ensure!(
        offered.len() == records.len(),
        "a share identity was offered twice"
    );
    ensure!(
        old_shares.is_subset(&offered),
        "the ledger holds a share no miner offered"
    );
    // Appends serialize on the ledger's lock, so sequence order is commit
    // order and the standby received an exact prefix of it: every lost share
    // comes after every replicated one. The gap is that suffix, the WAL the
    // standby never received, not the moment the test flipped its phase; a
    // share acknowledged just before the cut whose commit had not streamed
    // yet is lost with it, as D3 allows, and is named separately below.
    let replicated_through = new_shares
        .iter()
        .filter_map(|share| old_sequences.get(share))
        .max()
        .copied()
        .context("nothing was replicated")?;
    let early: Vec<(&String, i64)> = lost
        .iter()
        .filter_map(|share| old_sequences.get(share).map(|seq| (share, *seq)))
        .filter(|(_, seq)| *seq <= replicated_through)
        .collect();
    ensure!(
        early.is_empty(),
        "lost shares precede the replicated prefix (through share {replicated_through}): {early:?}"
    );
    let mut acked_missing = BTreeSet::new();
    let mut acked_before_cut = Vec::new();
    let mut unknown = BTreeMap::<&str, usize>::new();
    let mut rejected = BTreeMap::<String, usize>::new();
    for record in &records {
        let (on_old, on_new) = (
            old_shares.contains(&record.share_id),
            new_shares.contains(&record.share_id),
        );
        match &record.outcome {
            Outcome::Acknowledged => {
                ensure!(
                    on_old,
                    "{} was acknowledged but never committed: {record:?}",
                    record.share_id
                );
                if !on_new {
                    acked_missing.insert(record.share_id.clone());
                    if record.answered == Phase::BeforeCut {
                        acked_before_cut.push(record.share_id.clone());
                    }
                }
            }
            Outcome::Rejected(reason) => {
                ensure!(
                    !on_old,
                    "{} was rejected ({reason}) but committed on the old primary",
                    record.share_id
                );
                *rejected.entry(reason.clone()).or_default() += 1;
            }
            Outcome::Unknown(_) => {
                let class = match (on_old, on_new) {
                    (_, true) => "on the promoted ledger",
                    (true, false) => "lost in the gap",
                    (false, false) => "never committed",
                };
                *unknown.entry(class).or_default() += 1;
            }
        }
    }
    ensure!(
        !acked_missing.is_empty(),
        "no acknowledged share fell in the gap"
    );
    // The held COMMITs completed after the fence for clients that were gone:
    // their miners were never told they were credited.
    let held_records: Vec<&Record> = records
        .iter()
        .filter(|record| completed.contains(&record.share_id))
        .collect();
    ensure!(
        held_records.len() == completed.len()
            && held_records
                .iter()
                .all(|record| matches!(record.outcome, Outcome::Unknown(_))),
        "a COMMIT held across the fence was answered as known: {held_records:?}"
    );
    // Submissions sent before the fence and answered after it.
    let in_flight: Vec<&Record> = records
        .iter()
        .filter(|record| record.sent < Phase::Fenced && record.answered == Phase::Fenced)
        .collect();
    ensure!(
        records
            .iter()
            .filter(|record| record.sent == Phase::Fenced)
            .all(|record| record.outcome != Outcome::Acknowledged),
        "a share sent after the fence was acknowledged"
    );

    // Move the stable endpoint to the promoted primary.
    pair.writer.route_to(pair.standby.port);
    timeline.mark("endpoint moved");
    for index in 0..2 {
        until(
            &format!("server {index} readiness after the move"),
            60,
            || healthy(f, index),
        )
        .await?;
    }

    // What a brand-new session is issued right after the move: observed
    // only, since a window prepared inside the gap is #619's second mode.
    let mut first_jobs = Vec::new();
    for index in 0..2 {
        let mut probe = ShareClient::connect(
            f.stratum[index],
            &format!("{}.b474-probe", address(f).await?),
        )
        .await?;
        let (params, _) = probe.solve(Proof::Share).await?;
        let window = job_window(f, params[1].as_str().context("job id missing")?).await?;
        first_jobs.push(match window {
            Some((Some(_), Some(_), Some(count), held)) if count == held => {
                format!("server {index}: whole")
            }
            other => format!("server {index}: window not held {other:?}"),
        });
    }

    // The same sessions: their current jobs, kept work, and replays. No new
    // work is pushed to them until the tip moves; that is checked at the end.
    let mut after = Vec::new();
    for (miner, session) in sessions.iter_mut().enumerate() {
        let current = session.submit(Proof::Share).await?;
        ensure!(
            current.answer.accepted(),
            "miner {miner}'s surviving session was answered {} on its current job",
            current.answer
        );
        after.push(("on its current job", current));
    }
    let kept = std::mem::take(&mut book.lock().unwrap().kept);
    let mut each: Vec<(usize, Phase)> = kept.iter().map(|work| (work.miner, work.kept)).collect();
    each.sort();
    let expected: Vec<(usize, Phase)> = (0..MINERS)
        .flat_map(|miner| [(miner, Phase::BeforeCut), (miner, Phase::Gap)])
        .collect();
    ensure!(
        each == expected,
        "kept work is not exactly one share per miner per phase: {each:?}"
    );
    for work in kept {
        let label = match work.kept {
            Phase::BeforeCut => "kept before the cut",
            _ => "kept in the gap",
        };
        let answer = sessions[work.miner]
            .submit_solved(work.params, work.submitted)
            .await;
        after.push((label, answer));
    }
    let answer = gap_miner.submit_solved(gap_share.0, gap_share.1).await;
    after.push(("kept on a job issued in the gap", answer));

    // A real block on work whose window the promoted ledger holds, by a new
    // session. A frontend can still issue new jobs from a window it prepared
    // inside the gap (#619): the range's sequence numbers now belong to other
    // shares, accepted after that window's anchor, so the landing's window
    // predicate reads none of them. The session is replaced until its job's
    // window reads whole under that same predicate.
    // Its window must cover the shares just credited on the promoted
    // primary, which reuse sequence numbers lost shares had: server 1
    // re-anchors its prepared work every second.
    let credited: i64 = sqlx::query_scalar("SELECT max(share_seq) FROM qbit_share_ledger")
        .fetch_one(&f.pool)
        .await?;
    until(
        "server 1's work prepared over the post-promotion credits",
        15,
        || async {
            Ok(sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM qbit_prism_jobs WHERE job_id LIKE 'prepared:live-1:%' AND window_last_share_seq>=$1)",
            )
            .bind(credited)
            .fetch_one(&f.pool)
            .await?)
        },
    )
    .await?;
    let tip = f.rpc("getbestblockhash", json!([])).await?;
    let tip = tip.as_str().context("tip missing")?;
    let username = format!("{}.b474-final", address(f).await?);
    let started = Instant::now();
    let mut stale_windows = Vec::new();
    let (mut finisher, params, submitted) = loop {
        let mut finisher = ShareClient::connect(f.stratum[1], &username).await?;
        finisher.work_on(tip, Duration::from_secs(30)).await?;
        let (params, submitted) = finisher.solve(Proof::Block).await?;
        let window = job_window(f, params[1].as_str().context("job id missing")?).await?;
        match window {
            // Older work, prepared before the credits: not stale, just early.
            Some((Some(_), Some(last), Some(count), held)) if count == held && last < credited => {}
            Some((Some(first), Some(last), Some(count), held)) if count == held => {
                eprintln!(
                    "final block job {} prepared window {first}..={last}, {count} shares, all held",
                    params[1]
                );
                break (finisher, params, submitted);
            }
            other => stale_windows.push(format!("{other:?}")),
        }
        ensure!(
            started.elapsed() < Duration::from_secs(30),
            "#619: server 1 kept issuing jobs whose window the promoted ledger lacks (prepared first, last, count, held): {stale_windows:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let block = finisher.submit_solved(params, submitted).await;
    ensure!(
        block.answer.accepted(),
        "the block after the promotion was answered {}",
        block.answer
    );
    landed(f, &block.hash).await?;
    let paid = paid_shares(f, &block.hash).await?;
    // Sequence numbers the lost shares held that the promoted ledger has
    // since given to other shares: history a frontend must not reuse.
    let lost_sequences: BTreeSet<i64> = lost
        .iter()
        .filter_map(|share| old_sequences.get(share).copied())
        .collect();
    let replaced: Vec<i64> = sqlx::query_scalar(
        "SELECT share_seq FROM qbit_share_ledger WHERE share_seq=ANY($1) ORDER BY share_seq",
    )
    .bind(lost_sequences.iter().copied().collect::<Vec<_>>())
    .fetch_all(&f.pool)
    .await?;
    // The final block pays exactly the promoted ledger's window, read
    // independently of its audit, and that window holds sequence numbers the
    // lost shares had: the replaced history a frontend's cached window must
    // not stand in for (#466).
    let window = expected_window(f, &block.share_id).await?;
    ensure!(
        paid == window,
        "the final block's audit is not a full read of the promoted ledger's window: {} paid, {} expected; paid only {:?}, expected only {:?}",
        paid.len(),
        window.len(),
        paid.difference(&window).collect::<Vec<_>>(),
        window.difference(&paid).collect::<Vec<_>>()
    );
    let paid_reused: Vec<i64> = paid
        .iter()
        .map(|(_, seq)| *seq)
        .filter(|seq| lost_sequences.contains(seq))
        .collect();
    ensure!(
        !paid_reused.is_empty(),
        "the final block's window holds no sequence number a lost share had: {paid:?}"
    );

    for record in records
        .iter()
        .filter(|record| acked_missing.contains(&record.share_id))
    {
        let (params, submitted) = record.replay();
        let replay = sessions[record.miner]
            .submit_solved(params, submitted)
            .await;
        after.push(("lost acknowledged share replayed", replay));
    }
    for (label, submitted) in &after {
        let truthful = match &submitted.answer {
            Answer::Accepted => true,
            answer => matches!(answer.reason_id(), Some("stale-job" | "unknown-job")),
        };
        ensure!(
            truthful,
            "{label} share {} was answered {} after the promotion",
            submitted.share_id,
            submitted.answer
        );
    }
    // Every answer after the promotion matches the promoted ledger: an
    // accepted submission is credited exactly once, a refused one not at all.
    let ids: Vec<&str> = after
        .iter()
        .map(|(_, submitted)| submitted.share_id.as_str())
        .collect();
    let credited_after: BTreeMap<String, i64> = sqlx::query_as(
        "SELECT share_id,count(*) FROM qbit_share_ledger WHERE accepted AND share_id=ANY($1) GROUP BY share_id",
    )
    .bind(&ids)
    .fetch_all(&f.pool)
    .await?
    .into_iter()
    .collect();
    for (label, submitted) in &after {
        let credits = credited_after
            .get(&submitted.share_id)
            .copied()
            .unwrap_or(0);
        ensure!(
            credits == i64::from(submitted.answer.accepted()),
            "{label} share {} was answered {} but is credited {credits} time(s) on the promoted ledger",
            submitted.share_id,
            submitted.answer
        );
    }
    // The newest surviving acknowledgement, on the freshest job.
    let survivor = records
        .iter()
        .rev()
        .find(|record| {
            record.outcome == Outcome::Acknowledged && new_shares.contains(&record.share_id)
        })
        .context("no acknowledged share survived")?;
    let (params, submitted) = survivor.replay();
    let duplicate = sessions[survivor.miner]
        .submit_solved(params, submitted)
        .await;
    ensure!(
        duplicate.answer.reason_id() == Some("duplicate-share"),
        "a surviving acknowledged share replayed after the promotion was answered {}",
        duplicate.answer
    );

    // The block on the gap-issued job: its coinbase was built from the lost
    // window, so it must be refused as stale, or land paying only shares the
    // promoted ledger holds (#619; submitted only in that opt-in case).
    let stale_block = match gap_block_disposition {
        GapBlock::Kept => None,
        GapBlock::Submitted => Some(gap_miner.submit_solved(gap_block.0, gap_block.1).await),
    };
    let stale_paid = match stale_block.as_ref().map(|block| (block, &block.answer)) {
        None => None,
        Some((stale_block, Answer::Accepted)) => {
            landed(f, &stale_block.hash).await.with_context(|| {
                format!("the block on the gap-issued job (window through share {gap_window:?}, standby through {replicated}) was accepted")
            })?;
            Some(paid_shares(f, &stale_block.hash).await?)
        }
        Some((_, answer)) => {
            ensure!(
                matches!(answer.reason_id(), Some("stale-job" | "unknown-job")),
                "the block on the gap-issued job was answered {answer}"
            );
            None
        }
    };
    if let (Some(stale_block), Some(stale_paid)) = (&stale_block, &stale_paid) {
        let window = expected_window(f, &stale_block.share_id).await?;
        ensure!(
            *stale_paid == window,
            "the gap-issued block's audit is not a full read of the promoted ledger's window: {} paid, {} expected",
            stale_paid.len(),
            window.len()
        );
    }

    // Work issued after the promotion reaches the surviving sessions: the
    // final block moved the tip, each session is notified of work on it,
    // and a share on that work is accepted and credited exactly once.
    let tip = f.rpc("getbestblockhash", json!([])).await?;
    let tip = tip.as_str().context("tip missing")?.to_owned();
    let mut on_new_tip = 0;
    for (miner, session) in sessions.iter_mut().enumerate() {
        session
            .work_on(&tip, Duration::from_secs(30))
            .await
            .with_context(|| {
                format!("miner {miner}'s surviving session got no work on the new tip {tip}")
            })?;
        let share = session.submit(Proof::Share).await?;
        ensure!(
            share.answer.accepted(),
            "miner {miner}'s share on work for the new tip was answered {}",
            share.answer
        );
        let credits: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_share_ledger WHERE accepted AND share_id=$1",
        )
        .bind(&share.share_id)
        .fetch_one(&f.pool)
        .await?;
        ensure!(
            credits == 1,
            "miner {miner}'s share on the new tip is credited {credits} time(s)"
        );
        on_new_tip += 1;
    }

    f.quiesce().await?;
    let (rows, ids, seqs, headers): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT count(*),count(DISTINCT share_id),count(DISTINCT share_seq),(SELECT count(*) FROM qbit_prism_share_hashes h JOIN qbit_share_ledger l USING(share_id) WHERE l.accepted AND h.header_hash=right(l.share_id,64)) FROM qbit_share_ledger WHERE accepted",
    )
    .fetch_one(&f.pool)
    .await?;
    ensure!(
        rows == ids && rows == seqs && rows == headers,
        "share identities, sequence numbers or header map disagree after the promotion: {rows} rows, {ids} ids, {seqs} seqs, {headers} headers"
    );
    f.integrity().await?;
    let mut answered_after = BTreeMap::<String, usize>::new();
    for (label, submitted) in &after {
        *answered_after
            .entry(format!("{label}: {}", submitted.answer))
            .or_default() += 1;
    }
    let count = |outcome: fn(&Outcome) -> bool| {
        records
            .iter()
            .filter(|record| outcome(&record.outcome))
            .count()
    };
    eprintln!(
        "live async promotion with live miners: {} shares offered by {MINERS} miners on 2 frontends: {} acknowledged, {} rejected {rejected:?}, {} unknown {unknown:?}; \
         {} in flight at the fence, answered {:?}; {} committed before the cut, {} lost in the gap ({gap_bytes} WAL bytes), of which {} acknowledged: {acked_missing:?} ({} of them answered before the cut flag: {acked_before_cut:?}); \
         the old writer committed only its {} held COMMIT(s) after the fence; after the promotion: {answered_after:?}, {on_new_tip} surviving session(s) credited on work for the new tip, a surviving share replayed: {}; \
         the gap-issued block: {}; the final block {} paid {} shares, a full read of the promoted window (sessions replaced first for a stale window: {stale_windows:?}; new sessions right after the move: {first_jobs:?}) (of which sequence numbers a lost share had: {paid_reused:?}); {} lost sequence number(s) since reused: {replaced:?}; timeline: {}",
        records.len(),
        count(|outcome| *outcome == Outcome::Acknowledged),
        count(|outcome| matches!(outcome, Outcome::Rejected(_))),
        count(|outcome| matches!(outcome, Outcome::Unknown(_))),
        in_flight.len(),
        in_flight.iter().map(|record| &record.outcome).collect::<Vec<_>>(),
        pre_cut.len(),
        lost.len(),
        acked_missing.len(),
        acked_before_cut.len(),
        completed.len(),
        duplicate.answer,
        match (&stale_block, &stale_paid) {
            (None, _) => "kept, not submitted (#619)".to_owned(),
            (Some(block), Some(paid)) => format!("{} landed paying {} shares", block.hash, paid.len()),
            (Some(block), None) => format!("{} refused: {}", block.hash, block.answer),
        },
        block.hash,
        paid.len(),
        replaced.len(),
        timeline.render(),
    );
    standby.close().await;
    Ok(())
}
