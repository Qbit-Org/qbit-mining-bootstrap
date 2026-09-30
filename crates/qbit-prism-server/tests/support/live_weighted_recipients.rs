//! #521 scenario 7, the many-recipient share-to-spend item of #474: many
//! payout addresses mine with seeded, skewed weights and a spread of
//! per-worker difficulty through several found blocks, and every sat is
//! followed from the accepted share to the spent output.
//!
//! Regtest's proof-of-work limit leaves no room below the block target: a
//! share can be at most twice as easy as a block, so a spread of worker
//! difficulty means nothing there. The node is therefore restarted with
//! `-legacyretarget` and mined quickly until each epoch's 4x clamp has raised
//! the block target at least `RAMP_FACTOR` times, under a mock clock pinned
//! to the tip so the node's future-time rule never paces the ramp. Worker
//! difficulties then span about three orders of magnitude below the block,
//! as on mainnet.
//!
//! Every share is a real Stratum submission from a session this test drives,
//! on both servers, with the difficulty each worker asks for in its password.
//! The test chooses share-only proofs for ordinary shares and a block-bearing
//! proof once per round, so it decides when a block is found and which worker
//! finds it. Everything asserted about payouts is recomputed here from the
//! ledger's accepted shares and the chain, never read from the server's
//! bundles or summaries: the PPLNS window, the gross split, the payout floor
//! and carry-forward, the pool fee's dust sweep, the direct/fanout partition
//! and the fanout fee. The node then pays and the payees' wallet spends it.
use super::*;
use num_bigint::BigUint;
use qbit_prism_server::{
    broadcaster::amount_bits,
    codec::{
        difficulty_target, double_sha256, hash_display, parse_u32_hex, scaled_target_difficulty,
        target_from_compact,
    },
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// The per-PR case: 40 payout addresses (one large and one smaller whale, a
/// Zipf tail, one payee held just under the floor and four that submit a
/// single minimum-difficulty share) through four found blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_weighted_recipients_pay_exact_pplns_outputs_through_fanout_spend() -> Result<()> {
    weighted_case(&PER_PR).await
}

/// The nightly case: the mainnet floor from #521's production shape, about
/// 130 recipients with one whale near 85% and a Zipf tail, through six
/// found blocks. Opt-in: the per-PR job does not select it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly: 130 recipients through six blocks; run with --ignored"]
async fn nightly_mainnet_floor_weighted_recipients_share_to_spend() -> Result<()> {
    weighted_case(&MAINNET_FLOOR).await
}

pub(super) struct Scenario {
    pub(super) name: &'static str,
    pub(super) seed: u64,
    pub(super) payees: usize,
    pub(super) blocks: usize,
    /// Each whale's share of every round's work, in address order.
    pub(super) whales: &'static [f64],
    /// Addresses that submit one minimum-difficulty share in one round.
    pub(super) near_zero: usize,
}

const PER_PR: Scenario = Scenario {
    name: "per-PR",
    seed: 0x0521_0007,
    payees: 40,
    blocks: 4,
    whales: &[0.80, 0.07],
    near_zero: 4,
};

const MAINNET_FLOOR: Scenario = Scenario {
    name: "mainnet-floor",
    seed: 0x0521_0130,
    payees: 130,
    blocks: 6,
    whales: &[0.85],
    near_zero: 10,
};

/// The block target is raised at least this many times over regtest's limit.
const RAMP_FACTOR: f64 = 1024.0;
/// Each round submits this fraction of the PPLNS window's weight, so a later
/// block's window ends inside an earlier round and cuts one share.
const ROUND_WINDOW_FRACTION: f64 = 0.6;
/// The lowest worker difficulty: a share weight about 5.4e5 in the ledger's
/// scaled units, where an unraised regtest block weighs 1e6.
const MIN_DIFFICULTY: f64 = 2.5e-10;
/// The payout floor. Mainnet's is lower, but its coinbase is larger and its
/// shares far smaller against the block, so dust needs a higher floor here.
const FLOOR_BITS: u64 = 1_000_000;
/// The server's default direct-coinbase floor, as on mainnet.
const DIRECT_FLOOR_BITS: u64 = 10_485_760;
/// Fewer direct slots than direct-eligible recipients, so overflow reaches
/// the fanout by amount priority.
const MAX_DIRECT_OUTPUTS: usize = 4;
/// The server defaults for the coinbase budget and the fanout chunk size.
const MAX_SETTLEMENT_OUTPUTS: usize = 16;
const MAX_FANOUT_RECIPIENTS: usize = 1_000;
/// Mainnet runs with a pool fee; without one, any sub-floor balance makes the
/// selected recipients' balances fall short of the coinbase and no work can
/// be built (`repro_prism_stall`). The fee output fronts the swept dust.
const POOL_FEE_BPS: u64 = 200;
/// The fixture's CTV market rate and the server's default premium.
const FANOUT_RATE_PER_1000_WEIGHT: u64 = 1_000;
const FANOUT_PREMIUM_BPS: u64 = 12_000;
/// The fanout weight estimate: fixed bytes plus one P2MR output each.
const FANOUT_FIXED_WEIGHT: u64 = 90;
const FANOUT_OUTPUT_WEIGHT: u64 = 43;
/// The server's default `PRISM_STRATUM_MAX_CONNECTIONS`.
const DEFAULT_MAX_CONNECTIONS: usize = 384;

pub(super) async fn weighted_case(scenario: &Scenario) -> Result<()> {
    let Some(mut fixture) = Fixture::open_with_servers(true, false).await? else {
        return Ok(());
    };
    let started = Instant::now();
    let result = run(&mut fixture, scenario).await;
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
    }
    let cleanup = fixture.cleanup().await;
    eprintln!(
        "live weighted recipients ({}): {:.1}s including cleanup",
        scenario.name,
        started.elapsed().as_secs_f64()
    );
    result.and(cleanup)
}

async fn run(fixture: &mut Fixture, scenario: &Scenario) -> Result<()> {
    let ramp_address = harden_chain(fixture).await?;
    let template = fixture
        .rpc("getblocktemplate", json!([{"rules": ["segwit"]}]))
        .await?;
    let network = block_weight(template["bits"].as_str().context("template bits missing")?)?;
    let coinbase = template["coinbasevalue"]
        .as_u64()
        .context("coinbase value missing")?;
    let pool = identity_of(
        fixture,
        wallet_rpc(fixture, "prism", "getnewaddress", json!(["", "p2mr"]))
            .await?
            .as_str()
            .context("pool fee address missing")?,
    )
    .await?;
    fixture.rpc("createwallet", json!(["payees"])).await?;
    let mut payees = Vec::with_capacity(scenario.payees);
    for _ in 0..scenario.payees {
        let address = wallet_rpc(fixture, "payees", "getnewaddress", json!(["", "p2mr"])).await?;
        payees
            .push(identity_of(fixture, address.as_str().context("payee address missing")?).await?);
    }
    ensure!(
        payees
            .iter()
            .map(|payee| &payee.program)
            .collect::<BTreeSet<_>>()
            .len()
            == payees.len(),
        "payee programs are not distinct"
    );
    let plan = Plan::new(scenario, network, coinbase)?;
    start_servers(fixture, &pool, plan.workers.len()).await?;

    let mut sessions = Vec::with_capacity(plan.workers.len());
    for worker in &plan.workers {
        let username = format!("{}.{}", payees[worker.payee].recipient, worker.name);
        sessions.push(Some(
            Session::open(fixture.stratum[worker.server], username, worker.difficulty).await?,
        ));
    }
    let mut accepted: HashMap<String, usize> = HashMap::new();
    let mut stale_retries = 0;
    let mut found = Vec::with_capacity(plan.rounds.len());
    for (index, round) in plan.rounds.iter().enumerate() {
        let mut tasks = tokio::task::JoinSet::new();
        for &(worker, count) in &round.shares {
            let mut session = sessions[worker].take().context("session reused")?;
            tasks.spawn(async move {
                let mut shares = Vec::with_capacity(count);
                for _ in 0..count {
                    shares.push(session.submit(false).await?);
                }
                Ok::<_, anyhow::Error>((worker, session, shares))
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (worker, session, shares) = joined??;
            sessions[worker] = Some(session);
            for share in shares {
                stale_retries += share.stale_retries;
                ensure!(
                    accepted.insert(share.share_id, worker).is_none(),
                    "a share id was acknowledged twice"
                );
            }
        }
        // Let the one-second reanchor publish work over this round's shares
        // before the block proof, so the window covers most of the round.
        tokio::time::sleep(Duration::from_millis(1_600)).await;
        let finder = sessions[round.finder].as_mut().context("finder missing")?;
        finder.drain(Duration::from_millis(300)).await?;
        let block = finder.submit(true).await?;
        stale_retries += block.stale_retries;
        let hash = block.hash.clone();
        ensure!(
            accepted
                .insert(block.share_id.clone(), round.finder)
                .is_none(),
            "a block share id was acknowledged twice"
        );
        until(
            &format!("round {index} block {hash} confirmed"),
            90,
            || async {
                let state: Option<String> = sqlx::query_scalar(
                    "SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1",
                )
                .bind(&hash)
                .fetch_optional(&fixture.pool)
                .await?;
                Ok(state.as_deref() == Some("confirmed")
                    && fixture.rpc("getbestblockhash", json!([])).await? == json!(hash))
            },
        )
        .await?;
        for session in sessions.iter_mut().flatten() {
            session.wait_for_parent(&hash).await?;
        }
        found.push(Found {
            hash,
            share_id: block.share_id,
        });
    }
    drop(sessions);
    fixture.quiesce().await?;

    let shares = ledger_shares(fixture).await?;
    let attribution = check_attribution(&shares, &accepted, &plan, &payees)?;
    let blocks = chain_blocks(fixture, &found).await?;
    let config = PolicyConfig { pool: pool.clone() };
    let mut prior = Carried::new();
    let mut expected = Vec::with_capacity(blocks.len());
    for (block, found) in blocks.iter().zip(&found) {
        let anchor = shares
            .iter()
            .find(|share| share.share_id == found.share_id)
            .context("block share missing from the ledger")?
            .issued_ms;
        let window = pplns_window(&shares, anchor, block.network)?;
        let settled = settle(block.value, &window, &prior, &config)
            .with_context(|| format!("model of block {} at {}", block.hash, block.height))?;
        check_coinbase(block, &settled, &pool, &payees)?;
        check_carry_rows(fixture, block, &settled).await?;
        prior = settled.next_prior(&prior);
        expected.push((window, settled));
    }
    check_final_balances(fixture, &prior).await?;

    let last = blocks.last().context("no block found")?.height;
    mine(
        fixture,
        &ramp_address,
        last + qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS - tip_height(fixture).await?,
    )
    .await?;
    // The server's fanout ids only say what to wait for; the outputs checked
    // below are found on the chain by the covenant outpoints they spend.
    let fanouts: Vec<String> =
        sqlx::query_scalar("SELECT fanout_txid FROM qbit_ctv_fanout_artifacts")
            .fetch_all(&fixture.pool)
            .await?;
    ensure!(
        fanouts.len()
            == expected
                .iter()
                .map(|(_, settled)| settled.chunks.len())
                .sum::<usize>(),
        "fanout artifact count differs from the model's chunks"
    );
    // A fanout that matured before the last batch may already be mined.
    until("every mature fanout broadcast", 60, || async {
        let mempool = fixture.rpc("getrawmempool", json!([])).await?;
        let mempool = mempool.as_array().context("mempool missing")?;
        let confirmed: Vec<String> = sqlx::query_scalar(
            "SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed'",
        )
        .fetch_all(&fixture.pool)
        .await?;
        Ok(fanouts
            .iter()
            .all(|txid| confirmed.contains(txid) || mempool.contains(&json!(txid))))
    })
    .await?;
    mine(fixture, &ramp_address, 1).await?;
    until("every fanout confirmed in the read model", 60, || async {
        let confirmed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE settlement_status='confirmed'",
        )
        .fetch_one(&fixture.pool)
        .await?;
        Ok(usize::try_from(confirmed)? == fanouts.len())
    })
    .await?;
    let paid = check_fanouts(fixture, &blocks, &expected, &payees).await?;
    check_wallet_receipts(fixture, &expected, &paid, &payees).await?;
    let spent = spend_sample(fixture, &ramp_address, blocks.len(), &paid).await?;
    fixture.integrity().await?;

    let summary = Summary::new(&plan, &attribution, &expected, spent, stale_retries);
    summary.check(scenario)?;
    eprintln!("live weighted recipients ({}): {summary}", scenario.name);
    Ok(())
}

// ---------------------------------------------------------------------------
// The chain: a harder regtest, the servers, and wallet access.

/// Restarts the node with `-legacyretarget`, raises the block target at
/// least `RAMP_FACTOR` times and returns the address the ramp mined to. The
/// node's clock is left mocked at the tip, which later mining moves forward.
async fn harden_chain(fixture: &mut Fixture) -> Result<String> {
    fixture.rpc("stop", json!([])).await?;
    until("qbit node exit before restart", 60, || async {
        Ok(fixture.node.child.try_wait()?.is_some())
    })
    .await?;
    // The fixture's node arguments, plus the retargeting rule and the wallet.
    let mut command = Command::new(&fixture.qbitd);
    command
        .args([
            "-regtest",
            "-server=1",
            "-listen=0",
            "-dnsseed=0",
            "-discover=0",
            "-fallbackfee=0.00001",
            "-rpcuser=prismtest",
            "-rpcpassword=prismtest",
            "-txindex=0",
            "-legacyretarget",
            "-wallet=prism",
        ])
        .arg(format!("-datadir={}", fixture.directory.path().display()))
        .arg(format!("-rpcport={}", fixture.rpc_port));
    fixture.node = Process::spawn(
        &mut command,
        fixture.directory.path().join("qbit-retarget.log"),
    )?;
    until("retargeting qbit RPC", 30, || async {
        Ok(fixture.rpc("getblockchaininfo", json!([])).await?["chain"] == "regtest")
    })
    .await?;
    fixture.rpc("createwallet", json!(["ramp"])).await?;
    let address = wallet_rpc(fixture, "ramp", "getnewaddress", json!(["", "p2mr"]))
        .await?
        .as_str()
        .context("ramp address missing")?
        .to_owned();
    fixture.rpc("unloadwallet", json!(["ramp"])).await?;
    let base = fixture.rpc("getblockchaininfo", json!([])).await?["difficulty"]
        .as_f64()
        .context("difficulty missing")?;
    loop {
        let info = fixture.rpc("getblockchaininfo", json!([])).await?;
        let difficulty = info["difficulty"].as_f64().context("difficulty missing")?;
        if difficulty >= base * RAMP_FACTOR * 0.999 {
            break;
        }
        ensure!(
            info["blocks"].as_u64().context("height missing")? < 20_000,
            "legacy retargeting did not raise the target"
        );
        mine(fixture, &address, 500).await?;
    }
    Ok(address)
}

/// Mines `count` blocks to `address`, moving the mock clock to the tip first
/// so the node's future-time rule never waits on the wall clock.
async fn mine(fixture: &Fixture, address: &str, count: u64) -> Result<()> {
    let mut left = count;
    while left > 0 {
        let batch = left.min(250);
        let tip_time = fixture.rpc("getblockchaininfo", json!([])).await?["time"]
            .as_u64()
            .context("tip time missing")?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        fixture
            .rpc("setmocktime", json!([tip_time.max(now)]))
            .await?;
        generate(fixture, batch, address).await?;
        left -= batch;
    }
    Ok(())
}

/// The deadline of this file's node-heavy RPCs: ramp mining and the wallet's
/// many-input `sendall`. Both finish in seconds on an idle host, but a
/// 250-block batch outran the fixture client's 45 s timeout at a host load
/// average of 50 (#533). Neither call is idempotent, so they are never
/// retried, only given time to finish.
const HEAVY_RPC_TIMEOUT: Duration = Duration::from_secs(600);

/// `generatetoaddress`, under `HEAVY_RPC_TIMEOUT`.
async fn generate(fixture: &Fixture, batch: u64, address: &str) -> Result<()> {
    let payload: Value = fixture
        .client
        .post(format!("http://127.0.0.1:{}/", fixture.rpc_port))
        .basic_auth("prismtest", Some("prismtest"))
        .timeout(HEAVY_RPC_TIMEOUT)
        .json(&json!({"jsonrpc":"1.0","id":"live-test","method":"generatetoaddress","params":[batch, address]}))
        .send()
        .await?
        .json()
        .await?;
    ensure!(
        payload["error"].is_null(),
        "RPC generatetoaddress: {}",
        payload["error"]
    );
    Ok(())
}

async fn tip_height(fixture: &Fixture) -> Result<u64> {
    fixture
        .rpc("getblockcount", json!([]))
        .await?
        .as_u64()
        .context("tip height missing")
}

/// The ledger's scaled block weight for compact `bits`: regtest's limit
/// weighs one million, and a target `n` times harder weighs `n` million.
fn block_weight(bits: &str) -> Result<u128> {
    let limit = BigUint::from(0x7f_ffff_u32) << (8 * (0x20 - 3));
    let target = target_from_compact(parse_u32_hex(bits)?)?;
    u128::try_from(limit * 1_000_000_u32 / target).context("block weight exceeds u128")
}

/// A wallet RPC under `HEAVY_RPC_TIMEOUT`: the payees' `sendall` signs every
/// input it spends.
async fn wallet_rpc(fixture: &Fixture, wallet: &str, method: &str, params: Value) -> Result<Value> {
    let payload: Value = fixture
        .client
        .post(format!(
            "http://127.0.0.1:{}/wallet/{wallet}",
            fixture.rpc_port
        ))
        .basic_auth("prismtest", Some("prismtest"))
        .timeout(HEAVY_RPC_TIMEOUT)
        .json(&json!({"jsonrpc":"1.0","id":"live-test","method":method,"params":params}))
        .send()
        .await?
        .json()
        .await?;
    ensure!(
        payload["error"].is_null(),
        "RPC {wallet}/{method}: {}",
        payload["error"]
    );
    Ok(payload["result"].clone())
}

/// A payout identity as the ledger keys it: the address is both the miner id
/// and the order key, and the program is its P2MR witness program.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Identity {
    order_key: String,
    recipient: String,
    program: String,
}

impl Identity {
    fn script(&self) -> String {
        format!("5220{}", self.program)
    }
}

async fn identity_of(fixture: &Fixture, address: &str) -> Result<Identity> {
    let validation = fixture.rpc("validateaddress", json!([address])).await?;
    let script = validation["scriptPubKey"]
        .as_str()
        .context("script missing")?;
    ensure!(
        script.len() == 68 && script.starts_with("5220"),
        "{address} is not P2MR: {script}"
    );
    Ok(Identity {
        order_key: address.into(),
        recipient: address.into(),
        program: script[4..].into(),
    })
}

async fn start_servers(fixture: &mut Fixture, pool: &Identity, workers: usize) -> Result<()> {
    let mut env: Vec<(String, String)> = [
        // Fixed per-worker difficulty: each worker's password sets it.
        ("PRISM_STRATUM_VARDIFF", "0".to_owned()),
        ("PRISM_STRATUM_VARDIFF_MIN_DIFF", "1e-12".to_owned()),
        ("PRISM_PAYOUT_MIN_OUTPUT_BITS", FLOOR_BITS.to_string()),
        (
            "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS",
            DIRECT_FLOOR_BITS.to_string(),
        ),
        (
            "PRISM_MAX_DIRECT_COINBASE_OUTPUTS",
            MAX_DIRECT_OUTPUTS.to_string(),
        ),
        ("PRISM_POOL_FEE_ENABLED", "1".to_owned()),
        ("PRISM_POOL_FEE_BPS", POOL_FEE_BPS.to_string()),
        ("PRISM_POOL_FEE_ADDRESS", pool.recipient.clone()),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value))
    .collect();
    // #553's 2,000-wallet case has one session per worker, more than the
    // server's default of 384 connections.
    if workers > DEFAULT_MAX_CONNECTIONS {
        env.push((
            "PRISM_STRATUM_MAX_CONNECTIONS".to_owned(),
            (workers + 64).to_string(),
        ));
    }
    fixture.server_env = env;
    for index in 0..2 {
        let process = fixture.start_server(index)?;
        fixture.servers.push(process);
    }
    for index in 0..2 {
        until(
            &format!("PRISM HTTP readiness of server {index}"),
            30,
            || async {
                Ok(fixture
                    .client
                    .get(format!("http://127.0.0.1:{}/healthz", fixture.api[index]))
                    .send()
                    .await?
                    .status()
                    .is_success())
            },
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The seeded plan: addresses, workers, and the shares of each round.

struct WorkerPlan {
    payee: usize,
    name: String,
    server: usize,
    difficulty: f64,
    /// The ledger weight of one share at this difficulty.
    weight: u128,
}

struct Round {
    /// `(worker, share count)`; the finder's block proof is extra.
    shares: Vec<(usize, usize)>,
    finder: usize,
}

struct Plan {
    workers: Vec<WorkerPlan>,
    rounds: Vec<Round>,
}

fn share_weight(difficulty: f64) -> Result<u128> {
    scaled_target_difficulty(&difficulty_target(difficulty)?)
}

impl Plan {
    /// Payees in address order: the whales, a Zipf tail, one payee paced
    /// just under the floor each round (so its carried balance is paid once
    /// it crosses), then the near-zero payees.
    fn new(scenario: &Scenario, network: u128, coinbase: u64) -> Result<Self> {
        let mut rng = StdRng::seed_from_u64(scenario.seed);
        let whales = scenario.whales.len();
        let tail = scenario
            .payees
            .checked_sub(whales + 1 + scenario.near_zero)
            .context("scenario has too few payees")?;
        let threshold = 0.6 * FLOOR_BITS as f64 / coinbase as f64;
        let tail_total = 1.0 - scenario.whales.iter().sum::<f64>() - threshold;
        let zipf: Vec<f64> = (0..tail)
            .map(|rank| 1.0 / ((rank + 1) as f64).powf(1.1))
            .collect();
        let zipf_sum: f64 = zipf.iter().sum();
        let fractions: Vec<f64> = scenario
            .whales
            .iter()
            .copied()
            .chain(zipf.iter().map(|weight| tail_total * weight / zipf_sum))
            .chain([threshold])
            .collect();
        let round_work =
            network as f64 * qbit_prism::PRISM_WINDOW_MULTIPLIER as f64 * ROUND_WINDOW_FRACTION;
        // Share-only proofs need a share target easier than the block.
        let max_difficulty = network as f64 / 2.0 / share_weight(1.0)? as f64;
        ensure!(
            max_difficulty / MIN_DIFFICULTY >= 500.0,
            "block target too easy for a wide spread"
        );
        let clamp = |difficulty: f64| difficulty.clamp(MIN_DIFFICULTY, max_difficulty);

        let mut workers = Vec::new();
        let mut owned: Vec<Vec<usize>> = Vec::with_capacity(scenario.payees);
        for (payee, fraction) in fractions.iter().enumerate() {
            let count = if payee < whales {
                3
            } else if payee == fractions.len() - 1 {
                1
            } else {
                match rng.gen_range(0..10) {
                    0..=5 => 1,
                    6..=8 => 2,
                    _ => 3,
                }
            };
            let per_worker = round_work * fraction / count as f64;
            let mut ids = Vec::with_capacity(count);
            for index in 0..count {
                // Pace each worker at 2-30 shares a round, as vardiff would.
                let shares = (2.0f64).powf(rng.gen_range(1.0..4.9));
                let difficulty = if payee == 0 && index == 0 {
                    max_difficulty
                } else if payee == fractions.len() - 1 {
                    MIN_DIFFICULTY
                } else {
                    clamp(per_worker / shares / share_weight(1.0)? as f64)
                };
                ids.push(workers.len());
                workers.push(WorkerPlan {
                    payee,
                    name: format!("w{index}"),
                    server: (payee + index) % 2,
                    difficulty,
                    weight: share_weight(difficulty)?,
                });
            }
            owned.push(ids);
        }
        for payee in fractions.len()..scenario.payees {
            owned.push(vec![workers.len()]);
            workers.push(WorkerPlan {
                payee,
                name: "w0".into(),
                server: payee % 2,
                difficulty: MIN_DIFFICULTY,
                weight: share_weight(MIN_DIFFICULTY)?,
            });
        }

        let mut rounds: Vec<Round> = (0..scenario.blocks)
            .map(|_| Round {
                shares: Vec::new(),
                finder: 0,
            })
            .collect();
        for (payee, fraction) in fractions.iter().enumerate() {
            let always = payee < whales || payee == fractions.len() - 1;
            let mut active: Vec<bool> = (0..scenario.blocks)
                .map(|_| always || rng.gen_bool(0.8))
                .collect();
            if !active.contains(&true) {
                active[rng.gen_range(0..scenario.blocks)] = true;
            }
            for (round, active) in active.into_iter().enumerate() {
                if !active {
                    continue;
                }
                let work = round_work * fraction * rng.gen_range(0.7..1.3);
                let ids = &owned[payee];
                let mut any = false;
                for &worker in ids {
                    let count =
                        (work / ids.len() as f64 / workers[worker].weight as f64).round() as usize;
                    if count > 0 {
                        rounds[round].shares.push((worker, count));
                        any = true;
                    }
                }
                if !any {
                    let easiest = *ids
                        .iter()
                        .min_by_key(|worker| workers[**worker].weight)
                        .expect("every payee has a worker");
                    rounds[round].shares.push((easiest, 1));
                }
            }
        }
        for (index, payee) in (fractions.len()..scenario.payees).enumerate() {
            // Spread over every round but the last, so every block carries
            // some dust and each near-zero balance is carried at least once.
            rounds[index % (scenario.blocks - 1)]
                .shares
                .push((owned[payee][0], 1));
        }
        for (index, round) in rounds.iter_mut().enumerate() {
            round.finder = if index == 0 {
                owned[0][0]
            } else {
                round.shares[rng.gen_range(0..round.shares.len())].0
            };
        }
        Ok(Self { workers, rounds })
    }
}

// ---------------------------------------------------------------------------
// A Stratum session this test drives.

struct Session {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    buffer: Vec<u8>,
    username: String,
    extranonce1: String,
    extranonce2_size: usize,
    extranonce2: u64,
    next_id: u64,
    /// The difficulty this worker asked for. The server derives the share
    /// target from it exactly and advertises that target's difficulty, which
    /// can differ from it in the last bits.
    requested: f64,
    difficulty: f64,
    /// The latest notify and the difficulty advertised with it.
    notify: Value,
    job_difficulty: f64,
}

struct Submitted {
    share_id: String,
    hash: String,
    stale_retries: usize,
}

impl Session {
    async fn open(port: u16, username: String, difficulty: f64) -> Result<Self> {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        let (read, write) = stream.into_split();
        let mut session = Self {
            reader: BufReader::new(read),
            writer: write,
            buffer: Vec::new(),
            username,
            extranonce1: String::new(),
            extranonce2_size: 0,
            extranonce2: 0,
            next_id: 3,
            requested: difficulty,
            difficulty: 0.0,
            notify: Value::Null,
            job_difficulty: 0.0,
        };
        session
            .send(json!({"id":1,"method":"mining.subscribe","params":["weighted-regtest"]}))
            .await?;
        let password = format!("d={difficulty},md={difficulty}");
        session
            .send(json!({"id":2,"method":"mining.authorize","params":[session.username,password]}))
            .await?;
        tokio::time::timeout(Duration::from_secs(20), async {
            let mut authorized = false;
            loop {
                let message = session.read().await?;
                ensure!(
                    message.get("error").is_none_or(Value::is_null),
                    "handshake rejected: {message}"
                );
                if message["id"] == 1 {
                    session.extranonce1 = message["result"][1]
                        .as_str()
                        .context("extranonce1 missing")?
                        .into();
                    session.extranonce2_size = message["result"][2]
                        .as_u64()
                        .context("extranonce2 size missing")?
                        .try_into()?;
                }
                if message["id"] == 2 {
                    ensure!(message["result"] == true, "authorize refused: {message}");
                    authorized = true;
                }
                if authorized
                    && message["method"] == "mining.notify"
                    && (session.job_difficulty / difficulty - 1.0).abs() < 1e-9
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await
        .with_context(|| {
            format!(
                "{} never received work at difficulty {difficulty}",
                session.username
            )
        })??;
        Ok(session)
    }

    async fn send(&mut self, payload: Value) -> Result<()> {
        self.writer
            .write_all(format!("{payload}\n").as_bytes())
            .await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Value> {
        ensure!(
            self.reader.read_until(b'\n', &mut self.buffer).await? > 0,
            "{} connection closed",
            self.username
        );
        let message: Value = serde_json::from_slice(&self.buffer)?;
        self.buffer.clear();
        if message["method"] == "mining.set_difficulty" {
            self.difficulty = message["params"][0]
                .as_f64()
                .context("invalid difficulty")?;
        }
        if message["method"] == "mining.notify" {
            self.notify = message.clone();
            self.job_difficulty = self.difficulty;
        }
        Ok(message)
    }

    /// Reads whatever the server sent until it is quiet for `quiet`.
    async fn drain(&mut self, quiet: Duration) -> Result<()> {
        while let Ok(message) = tokio::time::timeout(quiet, self.read()).await {
            message?;
        }
        Ok(())
    }

    async fn wait_for_parent(&mut self, parent: &str) -> Result<()> {
        let mut wire = hex::decode(parent)?;
        wire.reverse();
        for word in wire.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let expected = hex::encode(wire);
        tokio::time::timeout(Duration::from_secs(30), async {
            while self.notify["params"][1].as_str() != Some(expected.as_str()) {
                self.read().await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .with_context(|| format!("{} got no work on parent {parent}", self.username))??;
        Ok(())
    }

    /// Submits one proof: a share-only proof, or with `block` one that also
    /// meets the block target. A `stale-job` refusal is retried on newer
    /// work and counted; any other refusal fails the test.
    async fn submit(&mut self, block: bool) -> Result<Submitted> {
        for stale_retries in 0..8 {
            let id = self.next_id;
            self.next_id += 1;
            let (request, hash) = self.solve(id, block)?;
            let job = request["params"][1].clone();
            self.send(request).await?;
            let response = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let message = self.read().await?;
                    if message["id"] == id {
                        return Ok::<_, anyhow::Error>(message);
                    }
                }
            })
            .await
            .with_context(|| format!("{} got no reply to a submit", self.username))??;
            if response["result"] == true {
                return Ok(Submitted {
                    share_id: format!("{}:{hash}", self.username),
                    hash,
                    stale_retries,
                });
            }
            ensure!(
                response["error"].to_string().contains("stale"),
                "{} share refused: {response}",
                self.username
            );
            // Work newer than the refused job may already have arrived.
            tokio::time::timeout(Duration::from_secs(20), async {
                while self.notify["params"][0] == job {
                    self.read().await?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("no new work after a stale refusal")??;
        }
        bail!("{} kept receiving stale refusals", self.username)
    }

    fn solve(&mut self, id: u64, block: bool) -> Result<(Value, String)> {
        let params = self.notify["params"]
            .as_array()
            .context("no work yet")?
            .clone();
        let field = |index: usize| params[index].as_str().context("invalid notify field");
        let mut previous = hex::decode(field(1)?)?;
        for word in previous.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let bits = parse_u32_hex(field(6)?)?;
        let ntime = parse_u32_hex(field(7)?)?;
        let version = parse_u32_hex(field(5)?)?;
        let network_target = target_bytes(&target_from_compact(bits)?);
        ensure!(
            (self.job_difficulty / self.requested - 1.0).abs() < 1e-9,
            "{} was sent work at difficulty {}",
            self.username,
            self.job_difficulty
        );
        let share_target = target_bytes(&difficulty_target(self.requested)?);
        for _ in 0..64 {
            let extranonce2 = format!(
                "{:0width$x}",
                self.extranonce2,
                width = self.extranonce2_size * 2
            );
            self.extranonce2 += 1;
            let coinbase = hex::decode(format!(
                "{}{}{extranonce2}{}",
                field(2)?,
                self.extranonce1,
                field(3)?
            ))?;
            let mut merkle = double_sha256(&coinbase);
            for sibling in params[4].as_array().context("merkle branch missing")? {
                let sibling = hex::decode(sibling.as_str().context("invalid sibling")?)?;
                merkle = double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
            }
            let mut header = [
                version.to_le_bytes().as_slice(),
                previous.as_slice(),
                merkle.as_slice(),
                ntime.to_le_bytes().as_slice(),
                bits.to_le_bytes().as_slice(),
                &[0; 4],
            ]
            .concat();
            for nonce in 0..=u16::MAX as u32 {
                header[76..].copy_from_slice(&nonce.to_le_bytes());
                let hash = double_sha256(&header);
                if at_most(&hash, &share_target) && at_most(&hash, &network_target) == block {
                    let request = json!({"id":id,"method":"mining.submit","params":[
                        self.username, field(0)?, extranonce2, format!("{ntime:08x}"), format!("{nonce:08x}")
                    ]});
                    return Ok((request, hash_display(&hash)));
                }
            }
        }
        bail!("no proof within the search budget")
    }
}

/// A target as 32 little-endian bytes, the order a header hash compares in.
fn target_bytes(target: &BigUint) -> [u8; 32] {
    let mut bytes = [0; 32];
    let le = target.to_bytes_le();
    bytes[..le.len()].copy_from_slice(&le);
    bytes
}

fn at_most(hash: &[u8; 32], target: &[u8; 32]) -> bool {
    hash.iter().rev().cmp(target.iter().rev()) != std::cmp::Ordering::Greater
}

struct Found {
    hash: String,
    share_id: String,
}

// ---------------------------------------------------------------------------
// What the ledger and the chain recorded.

struct LedgerShare {
    seq: i64,
    share_id: String,
    miner: String,
    order_key: String,
    program: String,
    weight: u128,
    issued_ms: i64,
    accepted_ms: i64,
    writer: String,
    credit_policy: Option<String>,
}

async fn ledger_shares(fixture: &Fixture) -> Result<Vec<LedgerShare>> {
    let rows = sqlx::query(
        "SELECT share_seq,share_id,miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,share_difficulty::text AS weight,
                round(extract(epoch FROM job_issued_at)*1000)::bigint AS issued_ms,round(extract(epoch FROM accepted_at)*1000)::bigint AS accepted_ms,
                writer_id,credit_policy
         FROM qbit_share_ledger WHERE accepted ORDER BY share_seq",
    )
    .fetch_all(&fixture.pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(LedgerShare {
                seq: row.try_get("share_seq")?,
                share_id: row.try_get("share_id")?,
                miner: row.try_get("miner_id")?,
                order_key: row.try_get("payout_order_key")?,
                program: row.try_get("program")?,
                weight: row.try_get::<String, _>("weight")?.parse()?,
                issued_ms: row.try_get("issued_ms")?,
                accepted_ms: row.try_get("accepted_ms")?,
                writer: row.try_get("writer_id")?,
                credit_policy: row.try_get("credit_policy")?,
            })
        })
        .collect()
}

struct Attribution {
    shares: usize,
    /// Payees whose accepted shares came through both servers.
    both_servers: usize,
    /// Payees with accepted shares from more than one worker difficulty.
    multi_difficulty: usize,
    /// Largest over smallest share weight among accepted shares.
    spread: f64,
}

/// The ledger holds exactly the shares the servers acknowledged, each under
/// the payout identity, weight and server of the worker that submitted it.
fn check_attribution(
    shares: &[LedgerShare],
    accepted: &HashMap<String, usize>,
    plan: &Plan,
    payees: &[Identity],
) -> Result<Attribution> {
    ensure!(
        shares.len() == accepted.len(),
        "ledger holds {} accepted shares, the servers acknowledged {}",
        shares.len(),
        accepted.len()
    );
    let mut servers: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
    let mut weights: BTreeMap<usize, BTreeSet<u128>> = BTreeMap::new();
    for share in shares {
        let worker = &plan.workers[*accepted
            .get(&share.share_id)
            .with_context(|| format!("ledger share {} was never acknowledged", share.share_id))?];
        let payee = &payees[worker.payee];
        ensure!(
            share.miner == payee.recipient
                && share.order_key == payee.order_key
                && share.program == payee.program,
            "share {} credited to {}/{}, not {}",
            share.share_id,
            share.miner,
            share.program,
            payee.recipient
        );
        ensure!(
            share.weight == worker.weight,
            "share {} weighs {}, its worker's difficulty {} weighs {}",
            share.share_id,
            share.weight,
            worker.difficulty,
            worker.weight
        );
        ensure!(
            share.writer == format!("live-{}", worker.server),
            "share {} written by {}, submitted to server {}",
            share.share_id,
            share.writer,
            worker.server
        );
        ensure!(
            share.credit_policy.is_none(),
            "share {} has a credit policy",
            share.share_id
        );
        ensure!(
            share.issued_ms <= share.accepted_ms,
            "share {} precedes its job",
            share.share_id
        );
        servers
            .entry(worker.payee)
            .or_default()
            .insert(share.writer.clone());
        weights
            .entry(worker.payee)
            .or_default()
            .insert(share.weight);
    }
    let all: Vec<u128> = shares.iter().map(|share| share.weight).collect();
    Ok(Attribution {
        shares: shares.len(),
        both_servers: servers
            .values()
            .filter(|servers| servers.len() == 2)
            .count(),
        multi_difficulty: weights.values().filter(|weights| weights.len() > 1).count(),
        spread: *all.iter().max().context("no shares")? as f64
            / *all.iter().min().context("no shares")? as f64,
    })
}

struct ChainBlock {
    hash: String,
    height: u64,
    network: u128,
    value: u64,
    coinbase_txid: String,
    /// `(vout, script hex, amount)` of every coinbase output.
    outputs: Vec<(u32, String, u64)>,
}

fn outputs_of(tx: &Value) -> Result<Vec<(u32, String, u64)>> {
    tx["vout"]
        .as_array()
        .context("outputs missing")?
        .iter()
        .map(|output| {
            Ok((
                output["n"].as_u64().context("vout missing")?.try_into()?,
                output["scriptPubKey"]["hex"]
                    .as_str()
                    .context("script missing")?
                    .into(),
                amount_bits(&output["value"])?,
            ))
        })
        .collect()
}

/// The found blocks as the active chain has them, in order.
async fn chain_blocks(fixture: &Fixture, found: &[Found]) -> Result<Vec<ChainBlock>> {
    let mut blocks: Vec<ChainBlock> = Vec::with_capacity(found.len());
    for found in found {
        let block = fixture.rpc("getblock", json!([found.hash, 2])).await?;
        let height = block["height"].as_u64().context("height missing")?;
        ensure!(
            fixture.rpc("getblockhash", json!([height])).await? == json!(found.hash),
            "found block {} is not on the active chain",
            found.hash
        );
        if let Some(previous) = blocks.last() {
            ensure!(height > previous.height, "found blocks out of order");
        }
        let coinbase = &block["tx"][0];
        let outputs = outputs_of(coinbase)?;
        blocks.push(ChainBlock {
            hash: found.hash.clone(),
            height,
            network: block_weight(block["bits"].as_str().context("bits missing")?)?,
            value: outputs.iter().map(|(_, _, amount)| amount).sum(),
            coinbase_txid: coinbase["txid"]
                .as_str()
                .context("coinbase txid missing")?
                .into(),
            outputs,
        });
    }
    Ok(blocks)
}

// ---------------------------------------------------------------------------
// The independent model: PPLNS window, payout policy and settlement.

struct Window {
    /// Counted weight per payout program.
    weights: BTreeMap<String, (Identity, u128)>,
    shares: usize,
    /// The oldest counted share was counted in part.
    cut: bool,
}

/// Newest first from the block's snapshot, eight blocks of weight, the last
/// share counted only up to what is left.
fn pplns_window(shares: &[LedgerShare], anchor_ms: i64, network: u128) -> Result<Window> {
    let mut remaining = network
        .checked_mul(qbit_prism::PRISM_WINDOW_MULTIPLIER)
        .context("window weight overflow")?;
    let mut eligible: Vec<&LedgerShare> = shares
        .iter()
        .filter(|share| share.issued_ms <= anchor_ms && share.accepted_ms <= anchor_ms)
        .collect();
    eligible.sort_by_key(|share| std::cmp::Reverse(share.seq));
    let mut window = Window {
        weights: BTreeMap::new(),
        shares: 0,
        cut: false,
    };
    for share in eligible {
        if remaining == 0 {
            break;
        }
        let counted = share.weight.min(remaining);
        remaining -= counted;
        window.cut = counted < share.weight;
        window.shares += 1;
        let identity = Identity {
            order_key: share.order_key.clone(),
            recipient: share.miner.clone(),
            program: share.program.clone(),
        };
        window
            .weights
            .entry(share.program.clone())
            .or_insert((identity, 0))
            .1 += counted;
    }
    ensure!(window.shares > 0, "empty window at anchor {anchor_ms}");
    Ok(window)
}

/// Splits `total` in proportion to the weights: the floor of each exact
/// share, then one sat each to the largest remainders, ties by identity.
fn allocate(total: u64, weights: &[(Identity, u128)]) -> Result<Vec<(Identity, u64)>> {
    let mut ordered = weights.to_vec();
    ordered.sort_by(|left, right| left.0.cmp(&right.0));
    let sum: u128 = ordered.iter().map(|(_, weight)| weight).sum();
    ensure!(sum > 0, "nothing to allocate over");
    let mut shares: Vec<(Identity, u64, u128)> = ordered
        .into_iter()
        .map(|(identity, weight)| {
            let product = u128::from(total) * weight;
            Ok((identity, u64::try_from(product / sum)?, product % sum))
        })
        .collect::<Result<_>>()?;
    let left = total - shares.iter().map(|(_, amount, _)| amount).sum::<u64>();
    let mut order: Vec<usize> = (0..shares.len()).collect();
    order.sort_by(|&left, &right| {
        shares[right]
            .2
            .cmp(&shares[left].2)
            .then_with(|| shares[left].0.cmp(&shares[right].0))
    });
    for index in order.into_iter().take(usize::try_from(left)?) {
        shares[index].1 += 1;
    }
    Ok(shares
        .into_iter()
        .map(|(identity, amount, _)| (identity, amount))
        .collect())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Account {
    identity: Identity,
    gross: u64,
    prior: i128,
    candidate: i128,
    onchain: u64,
    fee: u64,
    carry: i128,
}

struct PolicyConfig {
    pool: Identity,
}

struct Settled {
    /// Miner accounts by payout program.
    accounts: BTreeMap<String, Account>,
    pool_fee: u64,
    swept_dust: u64,
    /// Direct coinbase outputs by script.
    direct: BTreeMap<String, u64>,
    /// Fanout chunks: `(identity, gross, net)` in canonical order.
    chunks: Vec<Vec<(Identity, u64, u64)>>,
}

/// Carried balances by payout program, with the identity they belong to.
type Carried = BTreeMap<String, (Identity, i128)>;

impl Settled {
    /// Each block moves a balance by what it earned less what it was paid.
    fn next_prior(&self, prior: &Carried) -> Carried {
        let mut next = prior.clone();
        for (program, account) in &self.accounts {
            next.entry(program.clone())
                .or_insert_with(|| (account.identity.clone(), 0))
                .1 += i128::from(account.gross) - i128::from(account.onchain);
        }
        next.retain(|_, (_, balance)| *balance != 0);
        next
    }
}

fn settle(value: u64, window: &Window, prior: &Carried, config: &PolicyConfig) -> Result<Settled> {
    let earned_fee = u64::try_from(u128::from(value) * u128::from(POOL_FEE_BPS) / 10_000)?;
    let miner_value = value - earned_fee;
    ensure!(miner_value >= FLOOR_BITS, "coinbase below the floor");
    let weights: Vec<(Identity, u128)> = window.weights.values().cloned().collect();
    let mut accounts: BTreeMap<String, Account> = BTreeMap::new();
    for (identity, gross) in allocate(miner_value, &weights)? {
        let prior = prior
            .get(&identity.program)
            .map_or(0, |(_, balance)| *balance);
        accounts.insert(
            identity.program.clone(),
            Account {
                identity,
                gross,
                prior,
                candidate: prior + i128::from(gross),
                onchain: 0,
                fee: 0,
                carry: 0,
            },
        );
    }
    for (program, (identity, balance)) in prior {
        if !accounts.contains_key(program) {
            accounts.insert(
                program.clone(),
                Account {
                    identity: identity.clone(),
                    gross: 0,
                    prior: *balance,
                    candidate: *balance,
                    onchain: 0,
                    fee: 0,
                    carry: 0,
                },
            );
        }
    }
    let floor = i128::from(FLOOR_BITS);
    let mut selected: Vec<String> = accounts
        .iter()
        .filter(|(_, account)| account.candidate >= floor)
        .map(|(program, _)| program.clone())
        .collect();
    let eligible_sum: i128 = selected
        .iter()
        .map(|program| accounts[program].candidate)
        .sum();
    let mut swept_dust = 0;
    if selected.is_empty() {
        swept_dust = miner_value;
    } else if eligible_sum < i128::from(miner_value) {
        // The pool fee fronts the dust: every eligible balance is paid whole.
        swept_dust = miner_value - u64::try_from(eligible_sum)?;
        for program in &selected {
            let account = accounts.get_mut(program).expect("selected");
            account.onchain = u64::try_from(account.candidate)?;
        }
    } else {
        loop {
            ensure!(!selected.is_empty(), "no recipient reaches the floor");
            let weights: Vec<(Identity, u128)> = selected
                .iter()
                .map(|program| {
                    let account = &accounts[program];
                    Ok((account.identity.clone(), u128::try_from(account.candidate)?))
                })
                .collect::<Result<_>>()?;
            let paid = allocate(miner_value, &weights)?;
            let under: BTreeSet<String> = paid
                .iter()
                .filter(|(_, amount)| *amount < FLOOR_BITS)
                .map(|(identity, _)| identity.program.clone())
                .collect();
            if under.is_empty() {
                for (identity, amount) in paid {
                    let account = accounts.get_mut(&identity.program).expect("selected");
                    ensure!(
                        i128::from(amount) <= account.candidate,
                        "payout exceeds balance"
                    );
                    account.onchain = amount;
                }
                break;
            }
            selected.retain(|program| !under.contains(program));
        }
    }
    for account in accounts.values_mut() {
        account.carry = account.candidate - i128::from(account.onchain);
    }
    let pool_fee = earned_fee + swept_dust;

    // Settlement: the largest amounts at or above the direct floor take the
    // direct slots; everyone else is paid through fanout chunks.
    let mut recipients: Vec<(Identity, u64)> = accounts
        .values()
        .filter(|account| account.onchain > 0)
        .map(|account| (account.identity.clone(), account.onchain))
        .collect();
    if pool_fee > 0 {
        recipients.push((config.pool.clone(), pool_fee));
    }
    recipients.sort_by(|left, right| left.0.cmp(&right.0));
    let mut candidates: Vec<&(Identity, u64)> = recipients
        .iter()
        .filter(|(_, amount)| *amount >= DIRECT_FLOOR_BITS)
        .collect();
    candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let mut direct_count = candidates.len().min(MAX_DIRECT_OUTPUTS);
    while direct_count + (recipients.len() - direct_count).div_ceil(MAX_FANOUT_RECIPIENTS)
        > MAX_SETTLEMENT_OUTPUTS
    {
        direct_count -= 1;
    }
    let direct_keys: BTreeSet<&Identity> = candidates[..direct_count]
        .iter()
        .map(|(identity, _)| identity)
        .collect();
    let direct: BTreeMap<String, u64> = recipients
        .iter()
        .filter(|(identity, _)| direct_keys.contains(identity))
        .map(|(identity, amount)| (identity.script(), *amount))
        .collect();
    let fanout: Vec<(Identity, u64)> = recipients
        .iter()
        .filter(|(identity, _)| !direct_keys.contains(identity))
        .cloned()
        .collect();
    let mut chunks = Vec::new();
    for chunk in fanout.chunks(MAX_FANOUT_RECIPIENTS) {
        let fee = fanout_fee(chunk.len());
        let gross: Vec<(Identity, u128)> = chunk
            .iter()
            .map(|(identity, amount)| (identity.clone(), u128::from(*amount)))
            .collect();
        let total: u64 = chunk.iter().map(|(_, amount)| amount).sum();
        ensure!(fee < total, "fanout fee exceeds its chunk");
        // The fee's proportional split is the same largest-remainder rule.
        let fees = allocate(fee, &gross)?;
        let mut paid = Vec::with_capacity(chunk.len());
        for ((identity, amount), (fee_identity, fee)) in chunk.iter().zip(fees) {
            ensure!(identity == &fee_identity, "fee split out of order");
            let net = amount - fee;
            ensure!(
                net >= FLOOR_BITS,
                "fanout fee pushes {} below the floor",
                identity.recipient
            );
            if let Some(account) = accounts.get_mut(&identity.program) {
                account.fee = fee;
            }
            paid.push((identity.clone(), *amount, net));
        }
        chunks.push(paid);
    }
    Ok(Settled {
        accounts,
        pool_fee,
        swept_dust,
        direct,
        chunks,
    })
}

fn fanout_fee(recipients: usize) -> u64 {
    let compact = if recipients < 253 { 1 } else { 3 };
    let weight = FANOUT_FIXED_WEIGHT + compact + FANOUT_OUTPUT_WEIGHT * recipients as u64;
    (weight * FANOUT_RATE_PER_1000_WEIGHT * FANOUT_PREMIUM_BPS).div_ceil(1_000 * 10_000)
}

// ---------------------------------------------------------------------------
// Comparing the model with the chain, the ledger and the wallets.

/// The coinbase pays each direct recipient its exact amount, one covenant
/// output per fanout chunk carrying that chunk's gross, and nothing else.
fn check_coinbase(
    block: &ChainBlock,
    settled: &Settled,
    pool: &Identity,
    payees: &[Identity],
) -> Result<()> {
    let known: BTreeSet<String> = payees
        .iter()
        .chain(std::iter::once(pool))
        .map(Identity::script)
        .collect();
    let mut direct = BTreeMap::new();
    let mut covenants = Vec::new();
    for (_, script, amount) in &block.outputs {
        if *amount == 0 {
            continue;
        }
        if known.contains(script) {
            ensure!(
                direct.insert(script.clone(), *amount).is_none(),
                "block {} pays {script} twice",
                block.hash
            );
        } else {
            covenants.push(*amount);
        }
    }
    ensure!(
        direct == settled.direct,
        "block {} direct outputs differ from the recomputed PPLNS split:\n chain {direct:?}\n model {:?}",
        block.hash,
        settled.direct
    );
    let mut expected: Vec<u64> = settled
        .chunks
        .iter()
        .map(|chunk| chunk.iter().map(|(_, gross, _)| gross).sum())
        .collect();
    covenants.sort_unstable();
    expected.sort_unstable();
    ensure!(
        covenants == expected,
        "block {} covenant outputs {covenants:?}, recomputed chunks {expected:?}",
        block.hash
    );
    Ok(())
}

/// Every payee account the ledger recorded for the block matches the model;
/// accounts the model lacks carry nothing at all.
async fn check_carry_rows(fixture: &Fixture, block: &ChainBlock, settled: &Settled) -> Result<()> {
    let rows = sqlx::query(
        "SELECT miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,gross_amount_sats,prior_balance_sats::text AS prior,
                candidate_balance_sats::text AS candidate,onchain_amount_sats,settlement_fee_sats,carry_forward_balance_sats::text AS carry,action
         FROM qbit_payout_carry_forward WHERE block_hash=$1 AND maturity_state<>'reversed'",
    )
    .bind(&block.hash)
    .fetch_all(&fixture.pool)
    .await?;
    let mut seen = BTreeSet::new();
    for row in &rows {
        let program: String = row.try_get("program")?;
        let recorded = Account {
            identity: Identity {
                order_key: row.try_get("payout_order_key")?,
                recipient: row.try_get("miner_id")?,
                program: program.clone(),
            },
            gross: u64::try_from(row.try_get::<i64, _>("gross_amount_sats")?)?,
            prior: row.try_get::<String, _>("prior")?.parse()?,
            candidate: row.try_get::<String, _>("candidate")?.parse()?,
            onchain: u64::try_from(row.try_get::<i64, _>("onchain_amount_sats")?)?,
            fee: u64::try_from(row.try_get::<i64, _>("settlement_fee_sats")?)?,
            carry: row.try_get::<String, _>("carry")?.parse()?,
        };
        let action: String = row.try_get("action")?;
        match settled.accounts.get(&program) {
            Some(expected) => {
                ensure!(
                    &recorded == expected,
                    "block {} account differs from the model:\n ledger {recorded:?}\n model  {expected:?}",
                    block.hash
                );
                let paid = if expected.onchain > 0 {
                    "onchain"
                } else {
                    "accrued"
                };
                ensure!(
                    action == paid,
                    "block {} account {program} action {action}",
                    block.hash
                );
            }
            None => ensure!(
                recorded.gross == 0
                    && recorded.prior == 0
                    && recorded.onchain == 0
                    && recorded.carry == 0,
                "block {} records an account the model lacks: {recorded:?}",
                block.hash
            ),
        }
        seen.insert(program);
    }
    for (program, account) in &settled.accounts {
        ensure!(
            seen.contains(program) || (account.prior == 0 && account.gross == 0),
            "block {} ledger lacks account {program}",
            block.hash
        );
    }
    Ok(())
}

/// The ledger's current carry-forward per program equals the model's.
async fn check_final_balances(fixture: &Fixture, carried: &Carried) -> Result<()> {
    let model: BTreeMap<String, i128> = carried
        .iter()
        .map(|(program, (_, balance))| (program.clone(), *balance))
        .collect();
    let rows = sqlx::query(
        "SELECT encode(p2mr_program,'hex') AS program,sum(gross_amount_sats-onchain_amount_sats)::text AS balance
         FROM qbit_payout_carry_forward WHERE maturity_state<>'reversed' GROUP BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let mut recorded = BTreeMap::new();
    for row in rows {
        let balance: i128 = row.try_get::<String, _>("balance")?.parse()?;
        if balance != 0 {
            recorded.insert(row.try_get::<String, _>("program")?, balance);
        }
    }
    ensure!(
        recorded == model,
        "carried balances differ from the model:\n ledger {recorded:?}\n model  {model:?}"
    );
    Ok(())
}

/// A payee output on the chain: of which found block, direct in its coinbase
/// or through its fanout.
struct PaidOutput {
    block: usize,
    fanout: bool,
    txid: String,
    vout: u32,
    script: String,
    amount: u64,
}

type Paid = Vec<PaidOutput>;

/// Each block's fanout spends its coinbase covenant output and pays each
/// fanout recipient its exact net amount. Returns every payee output: the
/// direct coinbase ones and the fanout ones.
async fn check_fanouts(
    fixture: &Fixture,
    blocks: &[ChainBlock],
    expected: &[(Window, Settled)],
    payees: &[Identity],
) -> Result<Paid> {
    let mut covenants: HashMap<(String, u32), usize> = HashMap::new();
    let payee_scripts: BTreeSet<String> = payees.iter().map(Identity::script).collect();
    let mut paid = Paid::new();
    for (index, (block, (_, settled))) in blocks.iter().zip(expected).enumerate() {
        for (vout, script, amount) in &block.outputs {
            if payee_scripts.contains(script) {
                paid.push(PaidOutput {
                    block: index,
                    fanout: false,
                    txid: block.coinbase_txid.clone(),
                    vout: *vout,
                    script: script.clone(),
                    amount: *amount,
                });
            } else if *amount > 0 && !settled.direct.contains_key(script) {
                covenants.insert((block.coinbase_txid.clone(), *vout), index);
            }
        }
    }
    let first =
        blocks.first().context("no block")?.height + qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS;
    let tip = tip_height(fixture).await?;
    let mut spent: BTreeMap<usize, Vec<BTreeMap<String, u64>>> = BTreeMap::new();
    for height in first..=tip {
        let hash = fixture.rpc("getblockhash", json!([height])).await?;
        let block = fixture.rpc("getblock", json!([hash, 2])).await?;
        for tx in block["tx"]
            .as_array()
            .context("block txs missing")?
            .iter()
            .skip(1)
        {
            let spends: Vec<usize> = tx["vin"]
                .as_array()
                .context("inputs missing")?
                .iter()
                .filter_map(|input| {
                    let outpoint = (
                        input["txid"].as_str()?.to_owned(),
                        u32::try_from(input["vout"].as_u64()?).ok()?,
                    );
                    covenants.get(&outpoint).copied()
                })
                .collect();
            let [index] = spends[..] else {
                ensure!(spends.is_empty(), "a transaction spends several covenants");
                continue;
            };
            let txid = tx["txid"].as_str().context("txid missing")?;
            let mut outputs = BTreeMap::new();
            for (vout, script, amount) in outputs_of(tx)? {
                if amount == 0 {
                    continue;
                }
                ensure!(
                    outputs.insert(script.clone(), amount).is_none(),
                    "fanout pays {script} twice"
                );
                if payee_scripts.contains(&script) {
                    paid.push(PaidOutput {
                        block: index,
                        fanout: true,
                        txid: txid.into(),
                        vout,
                        script,
                        amount,
                    });
                }
            }
            spent.entry(index).or_default().push(outputs);
        }
    }
    for (index, (block, (_, settled))) in blocks.iter().zip(expected).enumerate() {
        let mut actual = spent.remove(&index).unwrap_or_default();
        let mut model: Vec<BTreeMap<String, u64>> = settled
            .chunks
            .iter()
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|(identity, _, net)| (identity.script(), *net))
                    .collect()
            })
            .collect();
        actual.sort();
        model.sort();
        ensure!(
            actual == model,
            "block {} fanout outputs differ from the model:\n chain {actual:?}\n model {model:?}",
            block.hash
        );
    }
    Ok(paid)
}

/// Each payee address received on the chain exactly what the model paid it
/// across all blocks, and the payees' wallet holds and can spend all of it.
async fn check_wallet_receipts(
    fixture: &Fixture,
    expected: &[(Window, Settled)],
    paid: &Paid,
    payees: &[Identity],
) -> Result<()> {
    let payee_scripts: BTreeSet<String> = payees.iter().map(Identity::script).collect();
    let mut model: BTreeMap<String, u64> = BTreeMap::new();
    for (_, settled) in expected {
        let fanout = settled
            .chunks
            .iter()
            .flatten()
            .map(|(identity, _, net)| (identity.script(), *net));
        let direct = settled
            .direct
            .iter()
            .map(|(script, amount)| (script.clone(), *amount));
        for (script, amount) in direct.chain(fanout) {
            if payee_scripts.contains(&script) {
                *model.entry(script).or_insert(0) += amount;
            }
        }
    }
    let mut chain: BTreeMap<String, u64> = BTreeMap::new();
    for output in paid {
        *chain.entry(output.script.clone()).or_insert(0) += output.amount;
    }
    ensure!(
        chain == model,
        "payee receipts on the chain differ from the model:\n chain {chain:?}\n model {model:?}"
    );
    let unspent = wallet_rpc(fixture, "payees", "listunspent", json!([1, 9_999_999])).await?;
    let mut wallet: BTreeMap<String, u64> = BTreeMap::new();
    for coin in unspent.as_array().context("wallet coins missing")? {
        ensure!(
            coin["spendable"] == true,
            "the payees' wallet cannot spend {coin}"
        );
        *wallet
            .entry(
                coin["scriptPubKey"]
                    .as_str()
                    .context("coin script missing")?
                    .into(),
            )
            .or_insert(0) += amount_bits(&coin["amount"])?;
    }
    ensure!(
        wallet == model,
        "the payees' wallet differs from the model:\n wallet {wallet:?}\n model  {model:?}"
    );
    Ok(())
}

/// The payees' wallet spends the smallest direct and the smallest fanout
/// payee output of every block, and the spend confirms.
async fn spend_sample(
    fixture: &Fixture,
    ramp_address: &str,
    blocks: usize,
    paid: &Paid,
) -> Result<usize> {
    let mut inputs = Vec::new();
    for block in 0..blocks {
        for fanout in [false, true] {
            let smallest = paid
                .iter()
                .filter(|output| output.block == block && output.fanout == fanout)
                .min_by_key(|output| output.amount);
            if let Some(output) = smallest {
                inputs.push(json!({"txid": output.txid, "vout": output.vout}));
            }
        }
    }
    ensure!(!inputs.is_empty(), "nothing to spend");
    let sink = wallet_rpc(fixture, "prism", "getnewaddress", json!(["", "p2mr"])).await?;
    let sent = wallet_rpc(
        fixture,
        "payees",
        "sendall",
        json!([[sink], null, "unset", null, {"inputs": inputs}]),
    )
    .await?;
    let txid = sent["txid"]
        .as_str()
        .context("spend txid missing")?
        .to_owned();
    mine(fixture, ramp_address, 1).await?;
    let spend = wallet_rpc(fixture, "payees", "gettransaction", json!([txid])).await?;
    ensure!(
        spend["confirmations"].as_u64().unwrap_or(0) >= 1,
        "the payees' spend did not confirm: {spend}"
    );
    for input in &inputs {
        ensure!(
            fixture
                .rpc("gettxout", json!([input["txid"], input["vout"], false]))
                .await?
                .is_null(),
            "spent payee output {input} is still unspent"
        );
    }
    Ok(inputs.len())
}

// ---------------------------------------------------------------------------
// The scenario was what it claims to be.

struct Summary {
    payees: usize,
    workers: usize,
    shares: usize,
    stale_retries: usize,
    both_servers: usize,
    multi_difficulty: usize,
    spread: f64,
    blocks: usize,
    cut_windows: usize,
    window_shares: Vec<usize>,
    direct: Vec<usize>,
    fanout: Vec<usize>,
    paid_payees: usize,
    dust_accounts: Vec<usize>,
    dust_later_paid: usize,
    swept: u64,
    pool_fee: u64,
    spent: usize,
}

impl Summary {
    fn new(
        plan: &Plan,
        attribution: &Attribution,
        expected: &[(Window, Settled)],
        spent: usize,
        stale_retries: usize,
    ) -> Self {
        let mut paid: BTreeSet<&String> = BTreeSet::new();
        let mut accrued: BTreeSet<&String> = BTreeSet::new();
        let mut dust_later_paid = BTreeSet::new();
        for (_, settled) in expected {
            for (program, account) in &settled.accounts {
                if account.onchain > 0 {
                    paid.insert(program);
                    if accrued.contains(program) && account.prior > 0 {
                        dust_later_paid.insert(program);
                    }
                } else if account.carry > 0 {
                    accrued.insert(program);
                }
            }
        }
        Self {
            payees: plan
                .workers
                .iter()
                .map(|worker| worker.payee)
                .collect::<BTreeSet<_>>()
                .len(),
            workers: plan.workers.len(),
            shares: attribution.shares,
            stale_retries,
            both_servers: attribution.both_servers,
            multi_difficulty: attribution.multi_difficulty,
            spread: attribution.spread,
            blocks: expected.len(),
            cut_windows: expected.iter().filter(|(window, _)| window.cut).count(),
            window_shares: expected.iter().map(|(window, _)| window.shares).collect(),
            direct: expected
                .iter()
                .map(|(_, settled)| settled.direct.len())
                .collect(),
            fanout: expected
                .iter()
                .map(|(_, settled)| settled.chunks.iter().map(Vec::len).sum())
                .collect(),
            paid_payees: paid.len(),
            dust_accounts: expected
                .iter()
                .map(|(_, settled)| {
                    settled
                        .accounts
                        .values()
                        .filter(|account| account.onchain == 0 && account.carry > 0)
                        .count()
                })
                .collect(),
            dust_later_paid: dust_later_paid.len(),
            swept: expected.iter().map(|(_, settled)| settled.swept_dust).sum(),
            pool_fee: expected.iter().map(|(_, settled)| settled.pool_fee).sum(),
            spent,
        }
    }

    /// The seeded scenario exercised what it is for; a change that made it
    /// degenerate (one recipient, no dust, no cut) fails here.
    fn check(&self, scenario: &Scenario) -> Result<()> {
        ensure!(
            self.payees == scenario.payees && self.blocks == scenario.blocks,
            "scenario size changed: {self}"
        );
        ensure!(
            self.spread >= 500.0,
            "share weights span only {:.0}x: {self}",
            self.spread
        );
        ensure!(
            self.both_servers > 0 && self.multi_difficulty > 0,
            "no address mined across servers and difficulties: {self}"
        );
        ensure!(
            self.cut_windows > 0,
            "no window ended inside a share: {self}"
        );
        ensure!(
            self.direct.iter().all(|count| *count > 0)
                && self
                    .fanout
                    .iter()
                    .all(|count| *count >= scenario.payees / 8),
            "blocks did not pay both direct and many fanout recipients: {self}"
        );
        ensure!(
            self.dust_accounts.iter().all(|count| *count > 0) && self.swept > 0,
            "no balance was carried below the floor: {self}"
        );
        ensure!(
            self.dust_later_paid > 0,
            "no carried balance was later paid: {self}"
        );
        ensure!(
            self.paid_payees * 3 >= scenario.payees,
            "too few payees were paid: {self}"
        );
        Ok(())
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} payees on {} workers submitted {} accepted shares ({} stale retries), {} payees across both servers, \
             {} at several difficulties, share weights spanning {:.0}x; {} blocks with window shares {:?} ({} cut), \
             direct outputs {:?}, fanout recipients {:?}, {} payees paid, sub-floor accounts {:?} ({} later paid, {} bits swept \
             into the {} bit pool fee), {} payee outputs spent by their wallet",
            self.payees,
            self.workers,
            self.shares,
            self.stale_retries,
            self.both_servers,
            self.multi_difficulty,
            self.spread,
            self.blocks,
            self.window_shares,
            self.cut_windows,
            self.direct,
            self.fanout,
            self.paid_payees,
            self.dust_accounts,
            self.dust_later_paid,
            self.swept,
            self.pool_fee,
            self.spent
        )
    }
}
