//! Decision D2 on #260: 3.x.x keeps its own payout behaviour during pool
//! bootstrap. Two entries of that decision are pinned here.
//!
//! * **D2a, bootstrap pooling.** The finder takes the whole block only while
//!   the ledger window is empty. From the first accepted share on, the
//!   proportional window applies, and 2.x.x's `PRISM_MIN_READY_MINERS=3`
//!   readiness gate is *not* restored.
//! * **D2c, prior balances during bootstrap.** Carried-forward balances stay
//!   in the payout of a bootstrap block. That combination -- an empty payout
//!   window beside a non-zero carry -- is the *migrated* state a pool reaches
//!   when it inherits balances without the share history that earned them, and
//!   3.x.x's own write paths cannot produce it in one schema. The test
//!   therefore records the carrying parent block's rows directly, from the
//!   engine's own verified payout manifest; `seed_carry_forward_block`
//!   documents why, and what that recording mirrors.
//!
//! The before/after examples live in `docs/prism-rust-migration.md`, section
//! "Payout differences from 2.x.x (decision D2)", and their machine-readable
//! form is `qbit-prism`'s `bootstrap_transition.json`. Every expectation below
//! is read out of that file rather than retyped, so a vector edit that changed
//! the decision would fail here instead of passing quietly.
//!
//! What is under test is the *selection*, not the arithmetic: these tests
//! drive [`Coordinator::refresh_once`] against a fake qbit JSON-RPC node and
//! then read the published [`Prepared`] work and the bundle that
//! [`MiningBackend::build_job`] actually hands a solver. Calling
//! [`Coordinator::build_bundle`] directly would compute the answer the
//! selection was supposed to make, so it is never the source of an assertion.
//!
//! The harness template carries bits `207fffff`, whose scaled difficulty is
//! 1_000_000 rather than the vectors' network difficulty of 100. Cases whose
//! payout depends on a *synthetic* bootstrap share (its difficulty is the
//! network difficulty) are therefore compared scale-normalized; cases whose
//! payout comes from seeded ledger shares are compared exactly, because the
//! 8x window weight counts every seeded share at either scale.
//!
//! PRISM_TEST_DATABASE_URL=postgres://postgres:prism@127.0.0.1:5432/postgres \
//!     cargo test -p qbit-prism-server --lib d2_bootstrap_tests -- --nocapture

use super::d2_test_support::*;
use super::*;
use anyhow::anyhow;
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism::{CarryForwardBalance, PayoutPolicy, PoolFeePolicy};
use tokio::task::JoinHandle;

/// The ledger's advisory locks (`ORDER_LOCK`, `SETTLEMENT_LOCK`) are
/// cluster-wide constants, not schema-scoped, so a private schema does not
/// isolate these tests from each other.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

// ---------------------------------------------------------------------------
// Harness constants
// ---------------------------------------------------------------------------

const TEMPLATE_VERSION: u32 = 0x2000_0000;
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The difficulty the harness scales every comparison by.
fn template_network_difficulty() -> Result<u128> {
    codec::scaled_target_difficulty(&codec::target_from_compact(codec::parse_u32_hex(
        TEMPLATE_BITS,
    )?)?)
}

// ---------------------------------------------------------------------------
// Vectors
// ---------------------------------------------------------------------------

const BOOTSTRAP_TRANSITION_VECTORS: &str =
    include_str!("../../../qbit-prism/fixtures/vectors/bootstrap_transition.json");

/// The named case from `bootstrap_transition.json`, checked against the D2
/// anchor it is expected to carry.
///
/// `d2_entry` is `Some` for a case that records a deliberate 2.x.x/3.x.x
/// difference (it must then carry a `expected_3xx` outcome) and `None` for a
/// case where both versions agree (it carries a single `expected`). A renamed
/// case, a re-anchored one, or one that lost its 3.x.x expectation fails here
/// rather than silently weakening the test below.
fn vector_case(name: &str, d2_entry: Option<&str>) -> Result<Value> {
    let file: Value = serde_json::from_str(BOOTSTRAP_TRANSITION_VECTORS)?;
    let case = file["cases"]
        .as_array()
        .context("bootstrap_transition.json has no cases array")?
        .iter()
        .find(|case| case["name"].as_str() == Some(name))
        .with_context(|| format!("bootstrap_transition.json has no case named {name}"))?
        .clone();
    ensure!(
        case["d2_entry"].as_str() == d2_entry,
        "case {name} records D2 entry {:?}, expected {d2_entry:?}",
        case["d2_entry"].as_str()
    );
    let expected = if d2_entry.is_some() {
        &case["expected_3xx"]
    } else {
        &case["expected"]
    };
    ensure!(
        expected["ok"].is_object(),
        "case {name} has no successful expected outcome to compare against"
    );
    Ok(case)
}

/// Everything the harness seeds for one case, read out of
/// `input.rule.scenario`.
struct Scenario {
    ledger_shares: Vec<AcceptedShare>,
    prior_balances: Vec<CarryForwardBalance>,
    solver: Worker,
    block_height: u64,
    coinbase_value_sats: u64,
    network_difficulty: u128,
}

fn scenario(case: &Value) -> Result<Scenario> {
    let rule = &case["input"]["rule"]["scenario"];
    let solver = &rule["solver"];
    let payout_address = solver["payout_address"]
        .as_str()
        .context("scenario solver has no payout address")?
        .to_owned();
    Ok(Scenario {
        ledger_shares: serde_json::from_value(rule["ledger_shares"].clone())?,
        prior_balances: serde_json::from_value(rule["prior_balances"].clone())?,
        solver: Worker {
            username: format!("{payout_address}.rig"),
            payout_address,
            worker_name: Some("rig".into()),
            p2mr_program_hex: solver["p2mr_program_hex"]
                .as_str()
                .context("scenario solver has no payout program")?
                .to_owned(),
        },
        block_height: rule["template"]["height"]
            .as_u64()
            .context("scenario template has no height")?,
        coinbase_value_sats: rule["template"]["coinbasevalue"]
            .as_u64()
            .context("scenario template has no coinbase value")?,
        network_difficulty: rule["network_difficulty"]
            .as_u64()
            .context("scenario has no network difficulty")?
            .into(),
    })
}

// ---------------------------------------------------------------------------
// Projection and comparison
// ---------------------------------------------------------------------------

/// One weight divided by the network difficulty it was measured against, so
/// that a synthetic bootstrap share computed at 1_000_000 can be compared
/// with a vector recorded at 100. Exact divisibility is asserted rather than
/// rounded away: a payout that is *not* a whole number of network difficulties
/// is a real difference, not a scale artifact.
fn scaled(path: &str, value: &Value, network_difficulty: u128) -> Result<Value> {
    let weight = u128::from(
        value
            .as_u64()
            .with_context(|| format!("{path} is not an unsigned integer: {value}"))?,
    );
    ensure!(
        weight % network_difficulty == 0,
        "{path}={weight} is not an exact multiple of network difficulty {network_difficulty}"
    );
    Ok(json!(u64::try_from(weight / network_difficulty)?))
}

/// The window half of a [`bundle_payout`] projection, with every difficulty
/// and weight expressed in network difficulties. The payout policy manifest
/// is left out: it is denominated in satoshis and compared exactly.
fn scale_normalized_window(projection: &Value, network_difficulty: u128) -> Result<Value> {
    let mut counted_shares = Vec::new();
    for (index, share) in projection["counted_shares"]
        .as_array()
        .context("projection has no counted shares")?
        .iter()
        .enumerate()
    {
        let mut share = share.clone();
        share["counted_difficulty"] = scaled(
            &format!("counted_shares[{index}].counted_difficulty"),
            &share["counted_difficulty"],
            network_difficulty,
        )?;
        counted_shares.push(share);
    }
    let mut entitlements = Vec::new();
    for (index, entitlement) in projection["entitlements"]
        .as_array()
        .context("projection has no entitlements")?
        .iter()
        .enumerate()
    {
        let mut entitlement = entitlement.clone();
        entitlement["weight"] = scaled(
            &format!("entitlements[{index}].weight"),
            &entitlement["weight"],
            network_difficulty,
        )?;
        entitlements.push(entitlement);
    }
    Ok(json!({
        "counted_window_weight": scaled(
            "counted_window_weight",
            &projection["counted_window_weight"],
            network_difficulty,
        )?,
        "counted_shares": counted_shares,
        "entitlements": entitlements,
    }))
}

// ---------------------------------------------------------------------------
// Fake qbit node
// ---------------------------------------------------------------------------

/// A minimal active chain: one hash per height, a tip, and cumulative work
/// that strictly increases whenever the tip advances.
struct NodeState {
    height: u64,
    hashes: HashMap<u64, String>,
    work: u64,
    coinbase_value_sats: u64,
    submissions: usize,
}

impl NodeState {
    fn at_tip(height: u64, tip: &str, coinbase_value_sats: u64) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            height,
            hashes: HashMap::from([(0, GENESIS_HASH.to_owned()), (height, tip.to_owned())]),
            work: 1,
            coinbase_value_sats,
            submissions: 0,
        }))
    }

    fn tip(&self) -> String {
        self.hashes[&self.height].clone()
    }
}

async fn node_reply(
    State(node): State<Arc<Mutex<NodeState>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut node = node.lock().await;
    let result = match request["method"].as_str().unwrap() {
        "getblockhash" => request["params"][0]
            .as_u64()
            .and_then(|height| node.hashes.get(&height))
            .map_or(Value::Null, |hash| json!(hash)),
        "getbestblockhash" => json!(node.tip()),
        "getblockchaininfo" => json!({"chain":"test","initialblockdownload":false,
            "blocks":node.height,"headers":node.height,"bestblockhash":node.tip(),
            "chainwork":format!("{:064x}",node.work)}),
        "getnetworkinfo" => json!({ "connections": 2 }),
        "getblockheader" => {
            let hash = request["params"][0].as_str().unwrap_or_default();
            let parent = node
                .hashes
                .iter()
                .find(|(_, known)| known.as_str() == hash)
                .and_then(|(height, _)| node.hashes.get(&height.saturating_sub(1)))
                .cloned()
                .unwrap_or_else(|| GENESIS_HASH.to_owned());
            json!({ "previousblockhash": parent })
        }
        "getblocktemplate" => json!({"version":TEMPLATE_VERSION,"bits":TEMPLATE_BITS,
            "height":node.height+1,"coinbasevalue":node.coinbase_value_sats,
            "curtime":unix_now().expect("the host clock precedes the epoch"),
            "previousblockhash":node.tip(),"transactions":[]}),
        "submitblock" => {
            let block = hex::decode(request["params"][0].as_str().unwrap()).unwrap();
            let hash = codec::hash_display(&codec::double_sha256(&block[..80]));
            node.height += 1;
            node.work += 1;
            let height = node.height;
            node.hashes.insert(height, hash);
            node.submissions += 1;
            Value::Null
        }
        method => panic!("unexpected bootstrap RPC {method}"),
    };
    Json(json!({"id":request["id"],"result":result,"error":null}))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    schema: TestSchema,
    coordinator: Arc<Coordinator>,
    node: Arc<Mutex<NodeState>>,
    server: JoinHandle<()>,
}

impl Fixture {
    /// A private schema, a fake node, and a coordinator wired to both. Every
    /// failure after the schema exists tears it down again, so a partially
    /// built fixture leaks neither a schema, a pool nor an `axum` task.
    async fn open(raw: &str, node: Arc<Mutex<NodeState>>) -> Result<Self> {
        let schema = TestSchema::create(raw, "prism_d2_boot").await?;
        let opened = async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let config = test_config(
                schema.url(),
                rpc_url,
                "d2-bootstrap",
                Duration::from_secs(15),
            )?;
            let app = Router::new()
                .route("/", post(node_reply))
                .with_state(node.clone());
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            match Coordinator::new(config, Arc::new(crate::metrics::Metrics::default())).await {
                Ok(coordinator) => Ok((coordinator, server)),
                Err(error) => {
                    server.abort();
                    Err(error)
                }
            }
        }
        .await;
        match opened {
            Ok((coordinator, server)) => Ok(Self {
                schema,
                coordinator,
                node,
                server,
            }),
            Err(error) => Err(schema.abandon(error).await),
        }
    }

    /// The bundle `refresh_once` published, if it selected the window.
    async fn prepared(&self) -> Result<Arc<Prepared>> {
        self.coordinator
            .prepared
            .read()
            .await
            .clone()
            .context("refresh_once published no prepared work")
    }

    /// The bundle a solver's job actually carries. This is the observation the
    /// tests assert on: it goes through the same selection a live miner does.
    async fn solver_bundle(&self, solver: &Worker) -> Result<Arc<AuditBundle>> {
        let job = self
            .coordinator
            .build_job(solver, EXTRANONCE1, 1.0, 0.0)
            .await
            .map_err(|error| anyhow!("build_job refused the solver: {error:?}"))?;
        Ok(job.context.bundle.clone())
    }

    async fn close(self) -> Result<()> {
        self.server.abort();
        self.coordinator.ledger.pool.close().await;
        self.schema.remove().await
    }
}

/// Append `shares` through the ledger, which assigns `share_seq` and
/// `accepted_at`, and confirm the ledger agreed with the vector's sequence.
async fn seed_window(ledger: &Ledger, shares: &[AcceptedShare]) -> Result<()> {
    for share in shares {
        let appended = ledger.append(share.clone(), None).await?;
        ensure!(
            appended.inserted && appended.share.share_seq == share.share_seq,
            "ledger assigned share_seq {} to {}, vector expects {}",
            appended.share.share_seq,
            share.share_id,
            share.share_seq
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// D2a: bootstrap pooling
// ---------------------------------------------------------------------------

/// Two miners, two accepted shares -- below 2.x.x's readiness gate of three
/// distinct miners, and above 3.x.x's rule of "any share at all". The window
/// must be selected, the solver's job must carry that same window bundle, and
/// the payout must be the proportional 3.x.x split rather than the 2.x.x
/// whole-coinbase-to-the-finder outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_shares_below_the_2xx_readiness_gate_still_pay_the_window() -> Result<()> {
    let Some(url) =
        database_url("accepted_shares_below_the_2xx_readiness_gate_still_pay_the_window")?
    else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case(
        "below-gate-with-other-miners-shares",
        Some("d2a-bootstrap-pooling"),
    )?;
    let scenario = scenario(&case)?;
    let bootstrap = case["input"]["rule"]["decision_3xx"]["bootstrap"]
        .as_bool()
        .context("case records no 3.x.x bootstrap decision")?;
    let node = NodeState::at_tip(
        scenario.block_height - 1,
        &"aa".repeat(32),
        scenario.coinbase_value_sats,
    );
    let fixture = Fixture::open(&url, node).await?;
    let result = async {
        seed_window(&fixture.coordinator.ledger, &scenario.ledger_shares).await?;
        fixture.coordinator.refresh_once().await?;

        let prepared = fixture.prepared().await?;
        ensure!(
            prepared.bundle.is_some() != bootstrap,
            "refresh_once selected {} for a window of {} share(s); the vector's 3.x.x decision \
             records bootstrap={bootstrap}",
            if prepared.bundle.is_some() {
                "the ledger window"
            } else {
                "a solver-only bootstrap bundle"
            },
            scenario.ledger_shares.len()
        );
        let published = prepared
            .bundle
            .clone()
            .context("the ledger window was not published as prepared work")?;
        let carried = fixture.solver_bundle(&scenario.solver).await?;
        ensure!(
            Arc::ptr_eq(&published, &carried),
            "the solver's job carried a different bundle than refresh_once published"
        );

        let projection = bundle_payout(&carried)?;
        let departures = mismatches("3xx", &case["expected_3xx"]["ok"], &projection);
        ensure!(
            departures.is_empty(),
            "payout departs from the recorded 3.x.x outcome:\n{}",
            departures.join("\n")
        );
        // The exact 3.x.x match above already rules out the 2.x.x outcome;
        // this keeps the vector itself honest about recording a difference.
        ensure!(
            !mismatches("2xx", &case["expected_2xx"]["ok"], &case["expected_3xx"]["ok"]).is_empty(),
            "the vector records the same payout on both sides, so it no longer pins a D2a difference"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(result, fixture.close().await)
}

/// The boundary the vectors do not cover: exactly one accepted share, from a
/// miner who is not the solver. 3.x.x has no readiness gate at all, so this
/// single share takes the whole coinbase and the solver is paid nothing. A
/// restored gate of two or more distinct miners would pay the solver instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_share_from_another_miner_takes_the_whole_coinbase() -> Result<()> {
    let Some(url) = database_url("a_single_share_from_another_miner_takes_the_whole_coinbase")?
    else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let miner_program = "01".repeat(32);
    let solver_program = "02".repeat(32);
    let coinbase_value_sats = 500_000_000;
    let block_height = 101;
    let share = AcceptedShare {
        share_seq: 1,
        share_id: "solitary-share".into(),
        miner_id: "miner-a".into(),
        order_key: "miner-a".into(),
        p2mr_program_hex: miner_program.clone(),
        share_difficulty: 30,
        network_difficulty: template_network_difficulty()?,
        template_height: block_height - 1,
        job_id: "solitary-job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 0,
        ntime: 1_800_000_000,
        credit_policy: None,
    };
    let solver = Worker {
        username: "miner-b.rig".into(),
        payout_address: "miner-b".into(),
        worker_name: Some("rig".into()),
        p2mr_program_hex: solver_program.clone(),
    };
    let node = NodeState::at_tip(block_height - 1, &"aa".repeat(32), coinbase_value_sats);
    let fixture = Fixture::open(&url, node).await?;
    let result = async {
        seed_window(&fixture.coordinator.ledger, std::slice::from_ref(&share)).await?;
        fixture.coordinator.refresh_once().await?;

        let published = fixture
            .prepared()
            .await?
            .bundle
            .clone()
            .context("one accepted share did not select the ledger window")?;
        let carried = fixture.solver_bundle(&solver).await?;
        ensure!(
            Arc::ptr_eq(&published, &carried),
            "the solver's job carried a different bundle than refresh_once published"
        );

        let accounts = &carried.payout_policy_manifest.accounts;
        ensure!(
            accounts.len() == 1
                && accounts[0].recipient_id == share.miner_id
                && accounts[0].p2mr_program_hex == miner_program
                && accounts[0].onchain_amount_sats == coinbase_value_sats,
            "the lone share holder was not paid the whole coinbase: {accounts:?}"
        );
        ensure!(
            !accounts
                .iter()
                .any(|account| account.p2mr_program_hex == solver_program),
            "the solver was paid from a window that holds no share of theirs"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(result, fixture.close().await)
}

// ---------------------------------------------------------------------------
// Bootstrap with an empty window
// ---------------------------------------------------------------------------

/// The one case both versions agree on: an empty ledger and no balances. No
/// window bundle is published, and the solver's job carries a synthetic
/// `bootstrap-share` that pays them the whole coinbase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_ledger_pays_the_solver_through_a_synthetic_bootstrap_share() -> Result<()> {
    let Some(url) =
        database_url("an_empty_ledger_pays_the_solver_through_a_synthetic_bootstrap_share")?
    else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case("empty-ledger", None)?;
    let scenario = scenario(&case)?;
    ensure!(
        scenario.ledger_shares.is_empty() && scenario.prior_balances.is_empty(),
        "the empty-ledger case stopped being empty"
    );
    let node = NodeState::at_tip(
        scenario.block_height - 1,
        &"aa".repeat(32),
        scenario.coinbase_value_sats,
    );
    let fixture = Fixture::open(&url, node).await?;
    let result = async {
        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.prepared().await?.bundle.is_none(),
            "an empty ledger window was published as payable prepared work"
        );
        let carried = fixture.solver_bundle(&scenario.solver).await?;
        assert_bootstrap_share(&carried, &scenario.solver)?;
        assert_bootstrap_payout(&carried, &case["expected"]["ok"], &scenario)?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(result, fixture.close().await)
}

// ---------------------------------------------------------------------------
// D2c: prior balances during bootstrap
// ---------------------------------------------------------------------------

/// An empty share window with one carry-only account whose balance is above
/// the day-one floor. 2.x.x dropped prior balances from its collection bundle
/// and did not pay that account; 3.x.x keeps the snapshot's prior balances in
/// the bootstrap bundle, so the account is paid out of this block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bootstrap_block_still_pays_carried_forward_balances() -> Result<()> {
    let Some(url) = database_url("a_bootstrap_block_still_pays_carried_forward_balances")? else {
        return Ok(());
    };
    let _serial = TEST_LOCK.lock().await;
    let case = vector_case(
        "bootstrap-carry-only-account-at-or-above-floor",
        Some("d2c-prior-balances-during-bootstrap"),
    )?;
    let scenario = scenario(&case)?;
    ensure!(
        scenario.ledger_shares.is_empty() && scenario.prior_balances.len() == 1,
        "the carry-only bootstrap case stopped being carry-only"
    );
    let network_difficulty = template_network_difficulty()?;
    // The carrying block is the parent of the block under test, so the node's
    // tip is both the scenario template's parent and the pool block that
    // produced the carry.
    let node = NodeState::at_tip(
        scenario.block_height - 1,
        &"aa".repeat(32),
        scenario.coinbase_value_sats,
    );
    let fixture = Fixture::open(&url, node).await?;
    let result = async {
        seed_carry_forward_block(&fixture, &scenario, network_difficulty).await?;
        let snapshot = fixture
            .coordinator
            .ledger
            .snapshot(network_difficulty)
            .await?;
        ensure!(
            snapshot.shares.is_empty(),
            "the seeding block left shares in the payout window"
        );
        ensure!(
            snapshot.prior_balances == scenario.prior_balances,
            "the seeded parent block produced {:?}, the vector expects {:?}",
            snapshot.prior_balances,
            scenario.prior_balances
        );

        fixture.coordinator.refresh_once().await?;
        ensure!(
            fixture.prepared().await?.bundle.is_none(),
            "an empty ledger window was published as payable prepared work"
        );
        let carried = fixture.solver_bundle(&scenario.solver).await?;
        ensure!(
            carried.found_block.block_height == scenario.block_height,
            "the bootstrap bundle was built for height {}, the vector expects {}",
            carried.found_block.block_height,
            scenario.block_height
        );
        assert_bootstrap_share(&carried, &scenario.solver)?;
        ensure!(
            carried.prior_balances == scenario.prior_balances,
            "the bootstrap bundle carried {:?}, the vector expects {:?}",
            carried.prior_balances,
            scenario.prior_balances
        );
        assert_bootstrap_payout(&carried, &case["expected_3xx"]["ok"], &scenario)?;
        // The exact 3.x.x match above already rules out the 2.x.x payout;
        // this keeps the vector itself honest about recording a difference.
        ensure!(
            !mismatches(
                "2xx",
                &case["expected_2xx"]["ok"]["payout_policy_manifest"],
                &case["expected_3xx"]["ok"]["payout_policy_manifest"],
            )
            .is_empty(),
            "the vector records the same payout on both sides, so it no longer pins a D2c difference"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    settle(result, fixture.close().await)
}

/// Give the ledger the scenario's carry-forward balance by recording the
/// parent block that produced it.
///
/// Every figure comes from the engine: the parent's payout is built with
/// `build_audit_bundle_with_coinbase_options` and verified with
/// `verify_audit_bundle_with_ledger_public_key`, and it is that manifest's own
/// accounts that are recorded, through `Ledger::land_candidate`'s own
/// statements and in its order -- the block row first as `prepared`, then its
/// payout and carry rows -- followed by the flip to `confirmed` that the
/// summary triggers turn into a balance. Nothing here invents a carry figure.
///
/// The seeding policy needs two settings working together. `min_output_sats`
/// is raised so the carried account's share of that block lands below the
/// floor and accrues instead of being paid. That alone cannot settle:
/// excluding the account leaves its satoshis unassigned and the engine rejects
/// the shortfall as `PayoutExceedsCandidateBalance`. A zero-bps pool fee gives
/// that dust somewhere to go -- the fee output absorbs exactly the excluded
/// amount and earns nothing else. Neither setting touches the block under
/// test, which runs on the coordinator's own `PayoutPolicy::day_one_default()`.
///
/// Recording those rows here, rather than landing a real block, is forced.
/// An empty payout window beside a non-zero carry is not reachable through any
/// 3.x.x write path within one schema, and all three exits are closed:
///
/// * A block that *accrues* must carry real ledger shares.
///   `persist_audit_snapshot` (`ledger/audit.rs:124-138`) re-reads the
///   bundle's share range out of `qbit_share_ledger` and requires it to match,
///   exempting only a single synthetic `bootstrap-share`.
/// * A bootstrap-shaped block cannot accrue at all. With one share there is
///   one entitlement, and `apply_payout_policy`
///   (`crates/qbit-prism/src/lib.rs:1079-1090`) rejects the block outright
///   when the miner reward is below the floor, while allocating that whole
///   reward to the single account otherwise -- so it always clears the floor
///   it was just checked against, and is always paid in full.
/// * Shares cannot be removed afterwards: the `qbit_share_ledger` trigger
///   `qbit_prism_immutable_share_history` refuses every UPDATE, DELETE and
///   TRUNCATE, and while even one accepted row exists the window is never
///   empty (`Ledger::snapshot` walks back from the newest share until
///   `8 * network_difficulty` of weight is spent -- eight million at this
///   template's difficulty).
///
/// So this is the migrated, or archived-history, state: the balances are
/// there and the shares that earned them are not. The three INSERTs below
/// repeat `ledger/blocks.rs` `land_candidate`'s statements for the block,
/// payout and carry rows verbatim -- same columns, same JSON extraction, same
/// `account_type = 'miner'` filter, same order -- so a schema change that moves
/// them fails here loudly instead of seeding a different state. The rest of
/// landing is left out because nothing under test reads it: the audit bundle
/// and share snapshot rows, the CTV fanout artifacts and the payout-revision
/// bump. The confirming UPDATE is the narrow form of `finish_candidate`'s: a
/// fresh, immature, `prepared` block needs neither `inactive_since` nor the
/// `inactive` branch.
async fn seed_carry_forward_block(
    fixture: &Fixture,
    scenario: &Scenario,
    network_difficulty: u128,
) -> Result<()> {
    let carried = scenario
        .prior_balances
        .first()
        .context("the scenario records no prior balance to seed")?;
    let balance_sats = u64::try_from(carried.balance_sats)?;
    let coinbase_value_sats = scenario.coinbase_value_sats;
    ensure!(
        balance_sats > 0 && coinbase_value_sats.is_multiple_of(balance_sats),
        "cannot seed a carry of {balance_sats} out of a {coinbase_value_sats} sat coinbase"
    );
    // One difficulty unit per `balance_sats` of coinbase, so the carried
    // account's single unit is worth exactly the balance the vector records.
    let units = u128::from(coinbase_value_sats / balance_sats);
    let block_height = scenario.block_height - 1;
    let seed_share = |share_seq: u64, miner: &str, program: &str, difficulty: u128| AcceptedShare {
        share_seq,
        share_id: format!("carry-seed-{miner}"),
        miner_id: miner.into(),
        order_key: miner.into(),
        p2mr_program_hex: program.into(),
        share_difficulty: difficulty,
        network_difficulty,
        template_height: block_height - 1,
        job_id: "carry-seed-job".into(),
        job_issued_at_ms: 1,
        accepted_at_ms: 1,
        ntime: 1_800_000_000,
        credit_policy: None,
    };
    let bundle = qbit_prism::build_audit_bundle_with_coinbase_options(
        vec![
            seed_share(1, &carried.recipient_id, &carried.p2mr_program_hex, 1),
            seed_share(2, "miner-seed", &"07".repeat(32), units - 1),
        ],
        FoundBlock {
            block_height,
            coinbase_value_sats,
            network_difficulty,
            anchor_job_issued_at_ms: 2,
        },
        vec![],
        PayoutPolicy {
            min_output_sats: Some(balance_sats + 10_000),
            pool_fee_policy: Some(PoolFeePolicy {
                fee_bps: 0,
                recipient_id: "pool-fee".into(),
                order_key: "pool-fee".into(),
                p2mr_program_hex: "0f".repeat(32),
            }),
            ..PayoutPolicy::day_one_default()
        },
        None,
        vec![],
        &ManifestSigningKey::from_seed_hex(&"11".repeat(32))?,
        &ManifestSigningKey::from_seed_hex(&"22".repeat(32))?,
    )?;
    let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
        &bundle,
        &fixture.coordinator.config.ledger_public_key,
    )?;
    let accrued = bundle
        .payout_policy_manifest
        .accounts
        .iter()
        .find(|account| account.recipient_id == carried.recipient_id)
        .context("the seeding payout has no account for the carried recipient")?;
    ensure!(
        accrued.carry_forward_balance_sats == carried.balance_sats,
        "the seeding payout accrued {} for {}, the vector expects {}",
        accrued.carry_forward_balance_sats,
        carried.recipient_id,
        carried.balance_sats
    );
    let (block_hash, parent_hash) = {
        let node = fixture.node.lock().await;
        (
            node.tip(),
            node.hashes
                .get(&(block_height - 1))
                .cloned()
                .unwrap_or_else(|| GENESIS_HASH.to_owned()),
        )
    };
    let accounts = serde_json::to_value(&bundle.payout_policy_manifest.accounts)?;
    let pool = &fixture.coordinator.ledger.pool;
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256) VALUES($1,$2,$3,$4,$5)")
        .bind(&block_hash).bind(i64::try_from(block_height)?).bind(&parent_hash)
        .bind(&report.coinbase_txid).bind(&report.coinbase_manifest_sha256_hex)
        .execute(&mut *tx).await?;
    sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) SELECT $1,$2,a->>'recipient_id',a->>'order_key',decode(a->>'p2mr_program_hex','hex'),(a->>'onchain_amount_sats')::bigint,(a->>'carry_forward_balance_sats')::numeric,a->>'action' FROM jsonb_array_elements($3::jsonb) a")
        .bind(&block_hash).bind(i64::try_from(block_height)?).bind(&accounts).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO qbit_payout_carry_forward(block_hash,block_height,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats,action) SELECT $1,$2,a->>'recipient_id',a->>'order_key',decode(a->>'p2mr_program_hex','hex'),(a->>'gross_amount_sats')::bigint,(a->>'prior_balance_sats')::numeric,(a->>'candidate_balance_sats')::numeric,(a->>'onchain_amount_sats')::bigint,COALESCE((a->>'settlement_fee_sats')::bigint,0),(a->>'carry_forward_balance_sats')::numeric,a->>'action' FROM jsonb_array_elements($3::jsonb) a WHERE COALESCE(a->>'account_type','miner')='miner'")
        .bind(&block_hash).bind(i64::try_from(block_height)?).bind(&accounts).execute(&mut *tx).await?;
    // Confirming is a separate statement, as in production: the summary
    // triggers count a block's carry rows exactly once, when it is flipped.
    sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=$1 AND chain_state='prepared'")
        .bind(&block_hash).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

/// A bootstrap bundle pays exactly one synthetic share, to the solver.
fn assert_bootstrap_share(bundle: &AuditBundle, solver: &Worker) -> Result<()> {
    ensure!(
        bundle.shares.len() == 1,
        "the bootstrap bundle carries {} shares, expected exactly one",
        bundle.shares.len()
    );
    let share = &bundle.shares[0];
    ensure!(
        share.share_id == "bootstrap-share"
            && share.miner_id == solver.payout_address
            && share.order_key == solver.payout_address
            && share.p2mr_program_hex == solver.p2mr_program_hex,
        "the bootstrap share is not the solver's synthetic share: {share:?}"
    );
    Ok(())
}

/// Compare a bootstrap payout with its vector. The policy manifest is
/// satoshi-denominated and must match exactly; the window is difficulty-
/// denominated and is compared in units of each side's own network difficulty,
/// because the synthetic share is worth one whole network difficulty at
/// whatever scale the template sets.
fn assert_bootstrap_payout(
    bundle: &AuditBundle,
    expected: &Value,
    scenario: &Scenario,
) -> Result<()> {
    let projection = bundle_payout(bundle)?;
    let departures = mismatches(
        "payout_policy_manifest",
        &expected["payout_policy_manifest"],
        &projection["payout_policy_manifest"],
    );
    ensure!(
        departures.is_empty(),
        "bootstrap payout departs from the recorded outcome:\n{}",
        departures.join("\n")
    );
    let departures = mismatches(
        "window",
        &scale_normalized_window(expected, scenario.network_difficulty)?,
        &scale_normalized_window(&projection, bundle.found_block.network_difficulty)?,
    );
    ensure!(
        departures.is_empty(),
        "bootstrap window departs from the recorded outcome:\n{}",
        departures.join("\n")
    );
    Ok(())
}
