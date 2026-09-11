//! Decision D2 on #260: 3.x.x keeps its own payout behaviour during pool
//! bootstrap. Two entries of that decision are pinned here.
//!
//! * **D2a, bootstrap pooling.** The finder takes the whole block only while
//!   the ledger window is empty. From the first accepted share on, the
//!   proportional window applies, and 2.x.x's `PRISM_MIN_READY_MINERS=3`
//!   readiness gate is *not* restored.
//! * **D2c, prior balances during bootstrap.** Carried-forward balances stay
//!   in the payout of a bootstrap block. The bundle a bootstrap job carries is
//!   pinned here for the balances the ledger actually holds; the carry-only
//!   case still needs a way to reach an empty window alongside a non-zero
//!   carry, which 3.x.x's own write paths cannot produce.
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

use super::*;
use anyhow::{anyhow, bail};
use axum::{extract::State, routing::post, Json, Router};
use qbit_prism::{CarryForwardBalance, PayoutPolicy};
use sqlx::PgPool;
use tokio::task::JoinHandle;

/// The ledger's advisory locks (`ORDER_LOCK`, `SETTLEMENT_LOCK`) are
/// cluster-wide constants, not schema-scoped, so a private schema does not
/// isolate these tests from each other.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

// ---------------------------------------------------------------------------
// Integration guard
// ---------------------------------------------------------------------------

/// Decides whether this file's tests run, fail or skip.
///
/// | `PRISM_TEST_DATABASE_URL` | other variables | result |
/// | --- | --- | --- |
/// | set and non-empty | -- | run against that database |
/// | unset or empty | `PRISM_TEST_REQUIRE_INTEGRATION=1` | fail, naming the variable |
/// | unset or empty | `GITHUB_JOB=prism-native-postgres` | fail, naming the variable |
/// | unset or empty | -- | print a skip line and return |
///
/// The three variables play different roles. This repository sets
/// `PRISM_TEST_DATABASE_URL` itself, in the `prism-native-postgres` job of
/// `.github/workflows/ci.yml`. GitHub sets `GITHUB_JOB` to the running job's
/// id, so matching it on `prism-native-postgres` means a database outage in
/// that job surfaces as a failure instead of a silent pass, even though
/// nothing in the repository writes that variable. Nothing sets
/// `PRISM_TEST_REQUIRE_INTEGRATION` yet: it is an opt-in switch proposed by
/// #286 for a run that wants every integration test to be mandatory, honoured
/// here in advance so that adopting it needs no change to this file.
///
/// Keying on `CI` instead would be wrong: GitHub sets `CI=true` in every job,
/// including `rust-tests`, which builds and runs the whole workspace with no
/// database at all.
///
/// An empty or whitespace-only URL counts as unset. A non-empty but malformed
/// URL is deliberately not second-guessed here; it reaches `sqlx` and fails
/// the test with the connection error, which is the diagnostic an operator
/// needs.
fn database_url(test_name: &str) -> Result<Option<String>> {
    let configured = std::env::var("PRISM_TEST_DATABASE_URL").unwrap_or_default();
    let configured = configured.trim();
    if !configured.is_empty() {
        return Ok(Some(configured.to_owned()));
    }
    let required_by = if matches!(
        std::env::var("PRISM_TEST_REQUIRE_INTEGRATION").as_deref(),
        Ok("1")
    ) {
        Some("PRISM_TEST_REQUIRE_INTEGRATION=1")
    } else if matches!(
        std::env::var("GITHUB_JOB").as_deref(),
        Ok("prism-native-postgres")
    ) {
        Some("GITHUB_JOB=prism-native-postgres")
    } else {
        None
    };
    if let Some(signal) = required_by {
        bail!(
            "{test_name} requires PostgreSQL: PRISM_TEST_DATABASE_URL is unset or empty while \
             {signal} demands the integration suite"
        );
    }
    eprintln!("skipping {test_name}: PRISM_TEST_DATABASE_URL is not set");
    Ok(None)
}

// ---------------------------------------------------------------------------
// Harness constants
// ---------------------------------------------------------------------------

/// Regtest-style compact bits. `codec::scaled_target_difficulty` reads these
/// as 1_000_000, and any header nonce passes the block target within a few
/// thousand tries.
const TEMPLATE_BITS: &str = "207fffff";
const TEMPLATE_VERSION: u32 = 0x2000_0000;
/// A future template time; `validate_template_age` accepts one.
const TEMPLATE_CURTIME: u64 = 1_800_000_000;
const EXTRANONCE1: &str = "00000000";
const EXTRANONCE2_SIZE: usize = 8;
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

/// The payout consequence of one bundle, in exactly the shape
/// `crates/qbit-prism/tests/money_path_vectors.rs` `bundle_payout` records:
/// the counted window, the entitlements it produces and the payout policy
/// manifest applied to them.
fn bundle_payout(bundle: &AuditBundle) -> Result<Value> {
    let reward = &bundle.reward_manifest;
    Ok(json!({
        "counted_window_weight": serde_json::to_value(reward.counted_window_weight)?,
        "counted_shares": reward
            .shares
            .iter()
            .map(|share| {
                Ok(json!({
                    "share_seq": share.share_seq,
                    "miner_id": share.miner_id,
                    "counted_difficulty": serde_json::to_value(share.counted_difficulty)?,
                }))
            })
            .collect::<Result<Vec<_>>>()?,
        "entitlements": serde_json::to_value(&reward.entitlements)?,
        "payout_policy_manifest": serde_json::to_value(&bundle.payout_policy_manifest)?,
    }))
}

/// Record every leaf where `actual` departs from `expected`.
fn diff(path: &str, expected: &Value, actual: &Value, mismatches: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(want), Value::Object(got)) => {
            for key in want
                .keys()
                .chain(got.keys().filter(|key| !want.contains_key(*key)))
            {
                let child = format!("{path}.{key}");
                match (want.get(key), got.get(key)) {
                    (Some(want), Some(got)) => diff(&child, want, got, mismatches),
                    (Some(want), None) => {
                        mismatches.push(format!("{child}: expected {want}, got nothing"))
                    }
                    (None, Some(got)) => {
                        mismatches.push(format!("{child}: expected nothing, got {got}"))
                    }
                    (None, None) => unreachable!(),
                }
            }
        }
        (Value::Array(want), Value::Array(got)) if want.len() == got.len() => {
            for (index, (want, got)) in want.iter().zip(got).enumerate() {
                diff(&format!("{path}[{index}]"), want, got, mismatches);
            }
        }
        _ if expected == actual => {}
        _ => mismatches.push(format!("{path}: expected {expected}, got {actual}")),
    }
}

fn mismatches(path: &str, expected: &Value, actual: &Value) -> Vec<String> {
    let mut found = Vec::new();
    diff(path, expected, actual, &mut found);
    found
}

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
            "curtime":TEMPLATE_CURTIME,"previousblockhash":node.tip(),"transactions":[]}),
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
    admin: PgPool,
    schema: String,
    coordinator: Arc<Coordinator>,
    server: JoinHandle<()>,
}

impl Fixture {
    /// A private schema, a fake node, and a coordinator wired to both. Every
    /// failure after the schema exists tears it down again, so a partially
    /// built fixture leaks neither a schema, a pool nor an `axum` task.
    async fn open(raw: &str, node: Arc<Mutex<NodeState>>) -> Result<Self> {
        let admin = PgPool::connect(raw).await?;
        let schema = format!("prism_d2_boot_{}", uuid::Uuid::new_v4().simple());
        let opened = async {
            sqlx::query(&format!("CREATE SCHEMA {schema}"))
                .execute(&admin)
                .await?;
            let mut url = url::Url::parse(raw)?;
            url.query_pairs_mut()
                .append_pair("options", &format!("-csearch_path={schema}"));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let rpc_url = format!("http://{}/", listener.local_addr()?);
            let app = Router::new()
                .route("/", post(node_reply))
                .with_state(node.clone());
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let config = Config {
                database_url: url.to_string(),
                instance_id: "d2-bootstrap".into(),
                database_connections: 6,
                initialize_schema: true,
                chain: "testnet".into(),
                expected_genesis_hash: None,
                min_peers: 1,
                template_max_age: Duration::from_secs(120),
                rpc_url,
                rpc_user: "test".into(),
                rpc_password: "test".into(),
                rpc_timeout: Duration::from_secs(10),
                block_submit_timeout: Duration::from_secs(10),
                poll_interval: Duration::from_secs(1),
                blockwait: false,
                build_workers: 1,
                runtime_workers: 2,
                snapshot_interval: Duration::from_secs(60),
                health_timeout: Duration::from_secs(60),
                share_commit_timeout: Duration::from_secs(15),
                extranonce2_size: EXTRANONCE2_SIZE,
                coinbase_tag: "/PRISM/".into(),
                manifest_seed: "11".repeat(32),
                ledger_seed: "22".repeat(32),
                ledger_public_key: ManifestSigningKey::from_seed_hex(&"22".repeat(32))?
                    .public_key_hex(),
                username_fallback: None,
                payout_policy: PayoutPolicy::day_one_default(),
                fee_address: None,
                ctv_enabled: false,
                ctv_config: qbit_prism::SettlementModeConfig::default(),
                ctv_direct_floor: 10_485_760,
                ctv_fee: None,
                ctv_fee_premium_bps: 12000,
                ctv_broadcast: false,
                ctv_broadcast_interval: Duration::from_secs(10),
                version_mask: codec::VERSION_ROLLING_MASK,
                audit_bind: "127.0.0.1".into(),
                audit_port: 0,
            };
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
                admin,
                schema,
                coordinator,
                server,
            }),
            Err(error) => {
                let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .execute(&admin)
                    .await;
                admin.close().await;
                Err(error)
            }
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
        let dropped = sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
        dropped?;
        Ok(())
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
            prepared.bundle.is_some() == !bootstrap,
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
        ensure!(
            !mismatches("2xx", &case["expected_2xx"]["ok"], &projection).is_empty(),
            "payout matched the 2.x.x whole-coinbase outcome, so D2a is not in force"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    fixture.close().await?;
    result
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
    fixture.close().await?;
    result
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
    fixture.close().await?;
    result
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
