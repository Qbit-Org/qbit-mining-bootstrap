//! Decision D2b, below-target block credit (issue #309), replayed through the
//! real [`Coordinator`] against PostgreSQL and a scripted qbit JSON-RPC node.
//!
//! A proof that meets the network target but misses its assigned share target
//! is credited at **network** difficulty, and only **after** the block it
//! solved is observed on the active chain. It earns nothing if the block is
//! abandoned, and a credit already published survives a later reorg. 2.x.x
//! instead credited the assigned share difficulty the moment the node accepted
//! the block; that difference is entry `d2b-below-target-credit` of
//! `docs/prism-rust-migration.md`.
//!
//! Every expectation here is read out of
//! `crates/qbit-prism/fixtures/vectors/below_target_credit.json`: the seeded
//! ledger, the node's disposition for the solved block, the credited
//! difficulty on both sides of the migration, and the payout projection of the
//! *next* block, which is where a wrong credit becomes a wrong payout. Only
//! harness constants (the template bits, the extranonce sizes, the nonce
//! budget) are written here.
//!
//! `crates/qbit-prism-server/tests/stratum_protocol.rs` covers the same shape
//! against an in-memory backend and so cannot see the credited amount or its
//! timing; this module is the accounting half of that pair.

use super::d2_test_support::*;
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use std::collections::BTreeMap;
use std::future::Future;
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument::WithSubscriber;

// ---------------------------------------------------------------------------
// Vectors
// ---------------------------------------------------------------------------

const VECTORS: &str = include_str!("../../../qbit-prism/fixtures/vectors/below_target_credit.json");
const D2_ENTRY: &str = "d2b-below-target-credit";
const NONCE_BUDGET: u32 = 200_000;
/// The ledger's advisory locks are cluster-wide constants, so `candidate_lease_tests`
/// in this same binary can hold `SETTLEMENT_LOCK` for a couple of seconds. No
/// deadline here is shorter than this.
const PATIENCE: Duration = Duration::from_secs(20);

/// Deliberately long ledger transactions share advisory locks across schemas.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// One vector case, looked up by name and checked for the invariants the
/// migration guide promises before any of its numbers are used.
fn vector_case(name: &str) -> Result<Value> {
    let document: Value =
        serde_json::from_str(VECTORS).context("below-target credit vectors are not valid JSON")?;
    let case = document["cases"]
        .as_array()
        .context("below-target credit vectors carry no cases")?
        .iter()
        .find(|case| case["name"] == json!(name))
        .cloned()
        .with_context(|| format!("below-target credit vectors carry no case named {name}"))?;
    if let Some(entry) = case.get("d2_entry") {
        ensure!(
            entry == &json!(D2_ENTRY),
            "case {name} records D2 entry {entry} instead of {D2_ENTRY}"
        );
        ensure!(
            case["expected_3xx"]["ok"].is_object(),
            "D2 case {name} carries no expected_3xx.ok projection"
        );
    }
    Ok(case)
}

/// The projection this case must reproduce: the 3.x.x side of a D2 case, and
/// the single frozen expectation of a case both versions agree on.
fn expectation(case: &Value) -> Result<Value> {
    let expected = if case.get("d2_entry").is_some() {
        &case["expected_3xx"]["ok"]
    } else {
        &case["expected"]["ok"]
    };
    ensure!(
        expected.is_object(),
        "case {} carries no payout projection",
        case["name"]
    );
    Ok(expected.clone())
}

/// Read a vector integer. `serde_json`'s `arbitrary_precision` keeps the
/// exported literal, so the 256-bit targets survive as exact decimals.
fn integer(value: &Value, what: &str) -> Result<BigUint> {
    BigUint::parse_bytes(value.to_string().as_bytes(), 10)
        .with_context(|| format!("{what} is not an unsigned integer: {value}"))
}

fn difficulty(value: &Value, what: &str) -> Result<u128> {
    integer(value, what)?
        .to_u128()
        .with_context(|| format!("{what} exceeds u128"))
}

// ---------------------------------------------------------------------------
// Scripted qbit node
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubmitMode {
    /// The block becomes the new tip and `submitblock` acknowledges it.
    Accept,
    /// The block becomes the new tip but the reply is lost: the node took the
    /// work and then answered with a JSON-RPC error.
    LoseReply,
    /// The node refuses the block and its tip does not move.
    Reject,
}

impl SubmitMode {
    fn from_outcome(outcome: &str) -> Result<Self> {
        Ok(match outcome {
            "accepted" | "reorged-after-acceptance" => Self::Accept,
            "confirmed-after-reconciliation" => Self::LoseReply,
            "rejected" => Self::Reject,
            other => anyhow::bail!("unsupported vector node outcome {other}"),
        })
    }
}

struct NodeState {
    /// The active chain, height to hash, always including the genesis at 0.
    chain: BTreeMap<u64, String>,
    /// Strictly increased on every tip change, including a reorg onto a
    /// foreign block: `observe_chain_view` rejects less work, and equal work
    /// on a different tip.
    chainwork: u128,
    coinbase_value: u64,
    submit: SubmitMode,
    submissions: usize,
}

impl NodeState {
    fn tip_height(&self) -> u64 {
        *self
            .chain
            .keys()
            .next_back()
            .expect("the scripted chain always retains its genesis")
    }

    fn tip(&self) -> String {
        self.chain[&self.tip_height()].clone()
    }

    /// Make `hash` the tip at `height`, discarding anything at or above it.
    fn adopt(&mut self, height: u64, hash: String) {
        self.chain.retain(|known, _| *known < height);
        self.chain.insert(height, hash);
        self.chainwork += 1;
    }
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let tip_height = node.tip_height();
    let tip = node.tip();
    let mut error = Value::Null;
    let result = match request["method"]
        .as_str()
        .expect("the coordinator always names an RPC method")
    {
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":tip_height,"headers":tip_height,"bestblockhash":tip,
            "chainwork":format!("{:064x}",node.chainwork)}),
        "getbestblockhash" => json!(tip),
        "getblockhash" => match request["params"][0].as_u64() {
            Some(height) => node
                .chain
                .get(&height)
                .map_or(Value::Null, |hash| json!(hash)),
            None => Value::Null,
        },
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
        "getblocktemplate" => {
            json!({"version":0x2000_0000u32,"bits":TEMPLATE_BITS,"height":tip_height+1,
            "coinbasevalue":node.coinbase_value,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":tip,"transactions":[]})
        }
        "submitblock" => {
            let block = hex::decode(
                request["params"][0]
                    .as_str()
                    .expect("submitblock carries a hex block"),
            )
            .expect("submitblock carries a hex block");
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            node.submissions += 1;
            match node.submit {
                SubmitMode::Reject => json!("rejected"),
                mode => {
                    node.adopt(tip_height + 1, hash);
                    if mode == SubmitMode::LoseReply {
                        error = json!({"code":-1,"message":"work queue depth exceeded"});
                    }
                    Value::Null
                }
            }
        }
        method => panic!("unexpected below-target credit RPC {method}"),
    };
    drop(node);
    Json(json!({"id":request["id"],"result":result,"error":error}))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    node: Arc<Mutex<NodeState>>,
    server: AbortOnDropHandle<()>,
}

impl Fixture {
    async fn open(raw: &str, case: &Value) -> Result<Self> {
        let scenario = &case["input"]["rule"]["scenario"];
        let next_height = scenario["next_block"]["block_height"]
            .as_u64()
            .context("vector next block has no height")?;
        let parent_height = next_height
            .checked_sub(2)
            .context("vector next block has no grandparent")?;
        let coinbase_value = scenario["next_block"]["coinbase_value_sats"]
            .as_u64()
            .context("vector next block has no coinbase value")?;
        let schema = TestSchema::create(raw, "prism_d2_credit").await?;
        let opened = async {
            // The node starts one block below the found block, so the first
            // refresh builds work for the height the vector solves.
            let node = Arc::new(Mutex::new(NodeState {
                chain: BTreeMap::from([(0, "00".repeat(32)), (parent_height, "aa".repeat(32))]),
                chainwork: 1,
                coinbase_value,
                submit: SubmitMode::from_outcome(
                    scenario["node_outcome"]
                        .as_str()
                        .context("vector scenario has no node outcome")?,
                )?,
                submissions: 0,
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            // The block-only branch of `submit` blocks until its credit row
            // exists, and this test drives the candidate by hand in between.
            // Well above 15s so a slow schema bootstrap cannot masquerade as a
            // missing credit.
            let config = test_config(
                schema.url(),
                rpc_url,
                "d2-below-target",
                Duration::from_secs(60),
            )?;
            let app = Router::new()
                .route("/", post(node_reply))
                .with_state(node.clone());
            // Dropping the handle on any later failure aborts the server.
            let server = AbortOnDropHandle::new(tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            }));
            let coordinator =
                Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await?;
            Ok::<_, anyhow::Error>((coordinator, node, server))
        }
        .await;
        match opened {
            Ok((coordinator, node, server)) => Ok(Self {
                schema,
                coordinator,
                node,
                server,
            }),
            Err(error) => Err(schema.abandon(error).await),
        }
    }

    fn ledger(&self) -> &Ledger {
        &self.coordinator.ledger
    }

    async fn credited(&self, share_id: &str) -> Result<Option<u128>> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT share_difficulty::text FROM qbit_share_ledger WHERE share_id=$1",
        )
        .bind(share_id)
        .fetch_optional(&self.ledger().pool)
        .await?;
        raw.map(|value| {
            value
                .parse()
                .context("credited difficulty is not an integer")
        })
        .transpose()
    }

    async fn credit_rows(&self, share_id: &str) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE share_id=$1")
                .bind(share_id)
                .fetch_one(&self.ledger().pool)
                .await?,
        )
    }

    async fn deferred_rows(&self, block_hash: &str) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT count(*) FROM qbit_prism_deferred_shares WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&self.ledger().pool)
        .await?)
    }

    async fn outbox_state(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_optional(&self.ledger().pool)
                .await?,
        )
    }

    async fn chain_state(&self, block_hash: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
                .bind(block_hash)
                .fetch_optional(&self.ledger().pool)
                .await?,
        )
    }

    async fn submissions(&self) -> usize {
        self.node.lock().await.submissions
    }

    /// Land, submit and finish the queued candidate. `submit_loop` never runs
    /// in these tests, so the claim is driven by hand.
    async fn drive_candidate(&self) -> Result<Result<()>> {
        let claim = self
            .coordinator
            .ledger
            .claim_candidate(10)
            .await?
            .context("the enqueued candidate could not be claimed")?;
        Ok(self.coordinator.process_candidate(&claim).await)
    }

    /// The payout consequence of the currently prepared work, in the shape
    /// `qbit-prism`'s `bundle_payout` vector entry point produces.
    async fn projection(&self) -> Result<Value> {
        let prepared = self
            .coordinator
            .prepared
            .read()
            .await
            .clone()
            .context("no prepared work after refresh")?;
        let bundle = prepared
            .bundle
            .clone()
            .context("prepared work carries no payout bundle")?;
        bundle_payout(&bundle)
    }

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        self.schema.remove().await
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Poll `probe` until it succeeds, with a hard ceiling instead of an open wait.
async fn until<P, F>(what: &str, mut probe: P) -> Result<()>
where
    P: FnMut() -> F,
    F: Future<Output = Result<bool>>,
{
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if probe().await? {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out after {}s waiting for {what}",
            PATIENCE.as_secs()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn assert_projection(case: &str, expected: &Value, actual: &Value) -> Result<()> {
    let departures = mismatches("payout", expected, actual);
    ensure!(
        departures.is_empty(),
        "{case}: next-block payout departs from the vector:\n  {}",
        departures.join("\n  ")
    );
    Ok(())
}

/// The stratum difficulty floor that produces the vector's assigned share
/// target.
///
/// `Job::reassign` clamps a target easier than the network target back to the
/// network target, so the `difficulty` argument alone can never yield a share
/// target *harder* than the block target; `minimum_difficulty` is the only
/// floor that can, and that is exactly the highdiff case D2b is about. The two
/// difficulty units also differ: `difficulty_target` divides the 0x1d00ffff
/// target by the float, while the vector's credited unit divides the
/// 0x207fffff target and scales by a million.
///
/// No `f64` divides the 0x1d00ffff target to exactly the 256-bit assigned
/// target, because the required ratio is not a binary fraction, so step the
/// floor up until it is at least as hard as the vector's. The remaining gap is
/// under one part in 2^52, far inside the one-part-in-4_000_000 window that
/// `scaled_target_difficulty`'s integer division rounds away.
fn assigned_share_floor(assigned_target: &BigUint) -> Result<f64> {
    let mut floor = codec::target_difficulty(assigned_target)?;
    for _ in 0..64 {
        if &codec::difficulty_target(floor)? <= assigned_target {
            return Ok(floor);
        }
        floor = floor.next_up();
    }
    anyhow::bail!("no representable stratum floor reaches the assigned share target")
}

/// A `tracing` sink the test can read back, used to capture the inner error
/// that `submit` deliberately hides from the miner.
#[derive(Clone, Default)]
struct SharedLog(Arc<std::sync::Mutex<Vec<u8>>>);

impl SharedLog {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer poisoned")).into_owned()
    }
}

impl std::io::Write for SharedLog {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer poisoned")
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLog {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Everything the common flow produces before a case takes over.
struct Solved {
    share_id: String,
    block_hash: String,
    submit: AbortOnDropHandle<std::result::Result<(), StratumError>>,
    log: SharedLog,
}

/// Steps 1 to 7 of the shared flow: seed the ledger, build the found-block
/// work, mine one proof of the shape the case needs, and start the submission.
async fn solve(fixture: &Fixture, case: &Value) -> Result<Solved> {
    let name = case["name"].as_str().context("vector case has no name")?;
    let rule = &case["input"]["rule"];
    let scenario = &rule["scenario"];
    let network_scaled = difficulty(
        &rule["decision_2xx"]["network_difficulty"],
        "network difficulty",
    )?;
    let assigned_scaled = difficulty(
        &rule["decision_2xx"]["assigned_share_difficulty"],
        "assigned share difficulty",
    )?;
    let found_height = scenario["next_block"]["block_height"]
        .as_u64()
        .context("vector next block has no height")?
        - 1;

    // 1. Seed the window the vector starts from, and prove the ledger agreed
    //    with the sequence numbers the expectation is written against.
    for seed in scenario["ledger_shares"]
        .as_array()
        .context("vector scenario seeds no ledger shares")?
    {
        let share: AcceptedShare = serde_json::from_value(seed.clone())
            .with_context(|| format!("{name}: invalid seeded ledger share {seed}"))?;
        let expected_seq = share.share_seq;
        let appended = fixture.ledger().append(share, None).await?;
        ensure!(
            appended.inserted && appended.share.share_seq == expected_seq,
            "{name}: seeded share {} took sequence {} instead of {expected_seq}",
            appended.share.share_id,
            appended.share.share_seq
        );
    }

    // 2 and 3. Build the found-block work at the vector's solved height.
    fixture.coordinator.refresh_once().await?;
    let prepared = fixture
        .coordinator
        .prepared
        .read()
        .await
        .clone()
        .context("no prepared work for the found block")?;
    let bundle = prepared
        .bundle
        .clone()
        .context("found-block work carries no payout bundle")?;
    ensure!(
        bundle.found_block.block_height == found_height
            && bundle.found_block.network_difficulty == network_scaled,
        "{name}: prepared work is block {} at network difficulty {}, wanted {found_height} at {network_scaled}",
        bundle.found_block.block_height,
        bundle.found_block.network_difficulty
    );

    // 4. The solving miner, as the vector names it.
    let solver = &scenario["solver"];
    let payout_address = solver["payout_address"]
        .as_str()
        .context("vector solver has no payout address")?
        .to_owned();
    let worker = Worker {
        username: format!("{payout_address}.rig"),
        payout_address: payout_address.clone(),
        worker_name: Some("rig".into()),
        p2mr_program_hex: solver["p2mr_program_hex"]
            .as_str()
            .context("vector solver has no P2MR program")?
            .to_owned(),
    };

    // 5. Work assigned above the network difficulty, which is what makes a
    //    valid block fall below its own share target.
    let network_target = codec::target_from_compact(codec::parse_u32_hex(TEMPLATE_BITS)?)?;
    let assigned_target =
        (network_target.clone() * BigUint::from(network_scaled)) / BigUint::from(assigned_scaled);
    let job = MiningBackend::build_job(
        &*fixture.coordinator,
        &worker,
        EXTRANONCE1,
        assigned_scaled as f64,
        assigned_share_floor(&assigned_target)?,
    )
    .await
    .map_err(|error| anyhow::anyhow!("{name}: job build failed: {}", error.message))?;
    ensure!(
        job.wire.network_target == integer(&scenario["network_target"], "vector network target")?,
        "{name}: job network target departs from the vector"
    );
    let vector_share_target = integer(&scenario["share_target"], "vector share target")?;
    ensure!(
        job.wire.share_target <= vector_share_target
            && (&vector_share_target - &job.wire.share_target) * BigUint::from(assigned_scaled)
                < vector_share_target,
        "{name}: assigned share target is outside the vector's rounding window"
    );
    ensure!(
        codec::scaled_target_difficulty(&job.wire.share_target)? == assigned_scaled,
        "{name}: assigned share difficulty is {} instead of {assigned_scaled}",
        codec::scaled_target_difficulty(&job.wire.share_target)?
    );

    // 6. Mine the proof this case is about.
    let want_share_pass = scenario["share_pass"]
        .as_bool()
        .context("vector scenario does not state share_pass")?;
    let submission = (0..NONCE_BUDGET)
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
            (submission.block_pass && submission.share_pass == want_share_pass)
                .then_some(submission)
        })
        .with_context(|| format!("{name}: no proof of the required shape in the nonce budget"))?;
    let share_id = format!("{}:{}", worker.username, submission.block_hash_hex);
    let block_hash = submission.block_hash_hex.clone();

    // 7. Start the submission. A block-only proof blocks in here until the
    //    credit row exists or the candidate reaches a non-pending state.
    let log = SharedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(log.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let submit = {
        let coordinator = fixture.coordinator.clone();
        let worker = worker.clone();
        async move { MiningBackend::submit(&*coordinator, &worker, &job, submission, false).await }
    };
    Ok(Solved {
        share_id,
        block_hash,
        submit: AbortOnDropHandle::new(tokio::spawn(submit.with_subscriber(subscriber))),
        log,
    })
}

/// Wait for the durable candidate intent, then prove that nothing has been
/// credited on the strength of it alone.
async fn assert_uncredited_intent(fixture: &Fixture, solved: &Solved, name: &str) -> Result<()> {
    until("the durable block candidate", || async {
        Ok(fixture.outbox_state(&solved.block_hash).await?.is_some())
    })
    .await?;
    ensure!(
        fixture.outbox_state(&solved.block_hash).await?.as_deref() == Some("pending"),
        "{name}: the block candidate did not start pending"
    );
    ensure!(
        fixture.deferred_rows(&solved.block_hash).await? == 1,
        "{name}: the below-target proof was not held as exactly one deferred share"
    );
    ensure!(
        fixture.credit_rows(&solved.share_id).await? == 0,
        "{name}: the below-target proof was credited before active-chain confirmation"
    );
    ensure!(
        !solved.submit.is_finished(),
        "{name}: the submission was acknowledged before active-chain confirmation"
    );
    Ok(())
}

async fn assert_credited(fixture: &Fixture, case: &Value, share_id: &str) -> Result<()> {
    let name = case["name"].as_str().context("vector case has no name")?;
    let rule = &case["input"]["rule"];
    let credited = difficulty(
        &rule["decision_3xx"]["credited_difficulty"],
        "3.x.x credited difficulty",
    )?;
    let legacy = difficulty(
        &rule["decision_2xx"]["credited_difficulty"],
        "2.x.x credited difficulty",
    )?;
    // Every case asserted here is a D2 case, so the vector itself must still
    // record a difference; otherwise the equality below would pin nothing that
    // tells 3.x.x from 2.x.x.
    ensure!(
        credited != legacy,
        "{name}: the vector records {credited} on both sides, so it no longer pins a D2b difference"
    );
    let actual = fixture
        .credited(share_id)
        .await?
        .with_context(|| format!("{name}: the confirmed proof produced no credit row"))?;
    ensure!(
        actual == credited,
        "{name}: credited {actual} instead of the proven {credited}"
    );
    Ok(())
}

/// Await the submission's answer under the shared ceiling.
async fn submission_result(
    solved: Solved,
    name: &str,
) -> Result<std::result::Result<(), StratumError>> {
    Ok(tokio::time::timeout(PATIENCE, solved.submit)
        .await
        .with_context(|| format!("{name}: the submission never resolved"))??)
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_only_proof_is_credited_network_work_only_once_the_block_is_active() -> Result<()> {
    let test = "block_only_proof_is_credited_network_work_only_once_the_block_is_active";
    let Some(raw) = database_url(test)? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case("block-only-proof-accepted")?;
    let fixture = Fixture::open(&raw, &case).await?;
    let outcome = async {
        let name = "block-only-proof-accepted";
        let solved = solve(&fixture, &case).await?;
        assert_uncredited_intent(&fixture, &solved, name).await?;

        fixture.drive_candidate().await??;
        ensure!(
            fixture.submissions().await == 1,
            "{name}: the block reached submitblock more than once"
        );
        ensure!(
            fixture.outbox_state(&solved.block_hash).await?.as_deref() == Some("submitted"),
            "{name}: the accepted candidate is not terminal"
        );
        assert_credited(&fixture, &case, &solved.share_id).await?;
        let share_id = solved.share_id.clone();
        let response = submission_result(solved, name).await?;
        ensure!(
            response.is_ok(),
            "{name}: a confirmed block-only proof was rejected"
        );

        // The next block is where a wrong credit becomes a wrong payout.
        fixture.coordinator.refresh_once().await?;
        assert_projection(name, &expectation(&case)?, &fixture.projection().await?)?;

        // A repeated observation of the same active block must not credit twice.
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.credit_rows(&share_id).await? == 1,
            "{name}: a repeated observation credited the proof more than once"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_only_proof_with_a_lost_submitblock_reply_is_credited_once_by_reconciliation(
) -> Result<()> {
    let test = "block_only_proof_with_a_lost_submitblock_reply_is_credited_once_by_reconciliation";
    let Some(raw) = database_url(test)? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case("block-only-proof-confirmed-after-reconciliation")?;
    let fixture = Fixture::open(&raw, &case).await?;
    let outcome = async {
        let name = "block-only-proof-confirmed-after-reconciliation";
        let solved = solve(&fixture, &case).await?;
        assert_uncredited_intent(&fixture, &solved, name).await?;

        // The node took the block and then lost its reply. The landing record
        // survives; nothing about the payout may advance on it.
        let error = fixture
            .drive_candidate()
            .await?
            .expect_err("a lost submitblock reply is not a completed candidate");
        ensure!(
            fixture.submissions().await == 1,
            "{name}: the block reached submitblock more than once"
        );
        ensure!(
            error.to_string().contains("submitblock"),
            "{name}: the candidate failed for an unrelated reason: {error}"
        );
        ensure!(
            fixture.chain_state(&solved.block_hash).await?.as_deref() == Some("prepared"),
            "{name}: an unacknowledged block was treated as confirmed"
        );
        ensure!(
            fixture.outbox_state(&solved.block_hash).await?.as_deref() == Some("pending"),
            "{name}: the unresolved candidate reached a terminal state"
        );
        ensure!(
            fixture.credit_rows(&solved.share_id).await? == 0,
            "{name}: an unacknowledged block was credited"
        );
        ensure!(
            !solved.submit.is_finished(),
            "{name}: the submission was acknowledged before reconciliation"
        );

        // Reconciliation observes the block on the active chain and credits it
        // inside the same confirmation transaction.
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.chain_state(&solved.block_hash).await?.as_deref() == Some("confirmed"),
            "{name}: reconciliation did not confirm the active block"
        );
        assert_credited(&fixture, &case, &solved.share_id).await?;
        let projection = fixture.projection().await?;
        let share_id = solved.share_id.clone();
        let response = submission_result(solved, name).await?;
        ensure!(
            response.is_ok(),
            "{name}: a reconciled block-only proof was rejected"
        );
        assert_projection(name, &expectation(&case)?, &projection)?;

        // A repeated observation of the same active block must not credit
        // twice.
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.credit_rows(&share_id).await? == 1,
            "{name}: a repeated observation credited the proof more than once"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_only_credit_survives_a_reorg_that_disconnects_its_block() -> Result<()> {
    let test = "block_only_credit_survives_a_reorg_that_disconnects_its_block";
    let Some(raw) = database_url(test)? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case("block-only-proof-reorged-after-acceptance")?;
    let fixture = Fixture::open(&raw, &case).await?;
    let outcome = async {
        let name = "block-only-proof-reorged-after-acceptance";
        let solved = solve(&fixture, &case).await?;
        assert_uncredited_intent(&fixture, &solved, name).await?;

        fixture.drive_candidate().await??;
        ensure!(
            fixture.submissions().await == 1,
            "{name}: the block reached submitblock more than once"
        );
        assert_credited(&fixture, &case, &solved.share_id).await?;
        let share_id = solved.share_id.clone();
        let block_hash = solved.block_hash.clone();
        ensure!(
            submission_result(solved, name).await?.is_ok(),
            "{name}: a confirmed block-only proof was rejected"
        );

        // A heavier foreign chain replaces the solved block.
        let height = {
            let mut node = fixture.node.lock().await;
            let height = node.tip_height();
            node.adopt(height, "bb".repeat(32));
            height
        };
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.chain_state(&block_hash).await?.as_deref() == Some("inactive"),
            "{name}: the disconnected block at {height} is still counted as active"
        );
        ensure!(
            fixture.credit_rows(&share_id).await? == 1,
            "{name}: the reorg disturbed an already published credit"
        );
        assert_credited(&fixture, &case, &share_id).await?;
        assert_projection(name, &expectation(&case)?, &fixture.projection().await?)?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_only_proof_the_node_rejects_fails_the_submission_without_credit() -> Result<()> {
    let test = "block_only_proof_the_node_rejects_fails_the_submission_without_credit";
    let Some(raw) = database_url(test)? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case("block-only-proof-rejected")?;
    let fixture = Fixture::open(&raw, &case).await?;
    let outcome = async {
        let name = "block-only-proof-rejected";
        let solved = solve(&fixture, &case).await?;
        assert_uncredited_intent(&fixture, &solved, name).await?;

        fixture.drive_candidate().await??;
        ensure!(
            fixture.submissions().await == 1,
            "{name}: the block reached submitblock more than once"
        );
        ensure!(
            fixture.outbox_state(&solved.block_hash).await?.as_deref() == Some("abandoned"),
            "{name}: a refused block did not end abandoned"
        );
        ensure!(
            fixture.credit_rows(&solved.share_id).await? == 0,
            "{name}: an abandoned block was credited"
        );
        let log = solved.log.clone();
        let share_id = solved.share_id.clone();
        let error = submission_result(solved, name)
            .await?
            .expect_err("a refused block cannot produce an accepted share");
        ensure!(
            error.reason_id.as_deref() == Some("ledger-confirmation-failed"),
            "{name}: the miner saw reason {:?}",
            error.reason_id
        );
        // The miner never sees the internal reason; the operator does.
        ensure!(
            log.text()
                .contains("block-only proof was not accepted on the active chain"),
            "{name}: the refusal reason was not recorded:\n{}",
            log.text()
        );

        // The network moves on without the refused block.
        {
            let mut node = fixture.node.lock().await;
            let height = node.tip_height() + 1;
            node.adopt(height, "bb".repeat(32));
        }
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.credit_rows(&share_id).await? == 0,
            "{name}: reconciliation credited a refused block"
        );
        assert_projection(name, &expectation(&case)?, &fixture.projection().await?)?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn share_passing_block_proof_is_credited_its_assigned_difficulty_at_once() -> Result<()> {
    let test = "share_passing_block_proof_is_credited_its_assigned_difficulty_at_once";
    let Some(raw) = database_url(test)? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case("share-and-block-proof-control")?;
    let fixture = Fixture::open(&raw, &case).await?;
    let outcome = async {
        let name = "share-and-block-proof-control";
        let solved = solve(&fixture, &case).await?;

        // A share-passing proof is credited on acceptance, with no wait on
        // confirmation and nothing deferred.
        let share_id = solved.share_id.clone();
        let block_hash = solved.block_hash.clone();
        let response = submission_result(solved, name).await?;
        ensure!(
            response.is_ok(),
            "{name}: a share-passing block proof was rejected"
        );
        ensure!(
            fixture.deferred_rows(&block_hash).await? == 0,
            "{name}: a share-passing proof was deferred"
        );
        ensure!(
            fixture.outbox_state(&block_hash).await?.as_deref() == Some("pending"),
            "{name}: the block candidate was not queued alongside the credit"
        );
        let credited = difficulty(
            &case["input"]["rule"]["decision_3xx"]["credited_difficulty"],
            "3.x.x credited difficulty",
        )?;
        ensure!(
            fixture.credited(&share_id).await? == Some(credited),
            "{name}: credited {:?} instead of the assigned {credited}",
            fixture.credited(&share_id).await?
        );

        fixture.drive_candidate().await??;
        ensure!(
            fixture.submissions().await == 1,
            "{name}: the block reached submitblock more than once"
        );
        ensure!(
            fixture.outbox_state(&block_hash).await?.as_deref() == Some("submitted"),
            "{name}: the accepted candidate is not terminal"
        );
        fixture.coordinator.refresh_once().await?;
        assert_projection(name, &expectation(&case)?, &fixture.projection().await?)?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(outcome, fixture.close().await)
}
