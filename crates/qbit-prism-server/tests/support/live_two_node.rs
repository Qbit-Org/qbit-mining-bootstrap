//! Own blocks through real two-node partitions (#521 scenarios 1 and 2).
//!
//! The fixture's node (A) serves the pool. A second regtest node (B) is
//! peered with it: B listens on loopback and A, started `-listen=0`, connects
//! out to it with `addnode … onetry`. A partition is B's
//! `setnetworkactive false`, which drops the connection and refuses a new
//! one; healing turns B's network back on and reconnects A. Blocks are mined
//! on a chosen node with `generatetoaddress`, and the pool's own blocks are
//! block-only proofs submitted through a frontend's highdiff port, so each
//! scenario controls exactly which own blocks exist and on which branch.
use super::highdiff_tests::HighdiffClient;
use super::*;

/// How long a connected miner may wait for work on a new tip. The native
/// current-work-gap warning fires at 15 s; the #413 stall held delivery for
/// 307 s. Each wait is measured from just before the call that moves the
/// pool's node to the new tip.
const NOTIFY_BOUND: Duration = Duration::from_secs(15);

/// `PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS` as the fixture runs it: the
/// servers start without the variable, so this is the production default.
const ORPHAN_CONFIRMATIONS: u64 = 6;

/// The second regtest node.
pub(super) struct PeerNode {
    process: Process,
    pub(super) rpc_port: u16,
    p2p_port: u16,
    client: reqwest::Client,
}

impl PeerNode {
    pub(super) async fn start(fixture: &Fixture) -> Result<Self> {
        let directory = fixture.directory.path().join("node-b");
        std::fs::create_dir_all(&directory)?;
        let rpc_port = fixture.ports.reserve()?;
        let p2p_port = fixture.ports.reserve()?;
        let mut command = Command::new(&fixture.qbitd);
        command
            .args([
                "-regtest",
                "-server=1",
                "-listen=1",
                "-bind=127.0.0.1",
                "-dnsseed=0",
                "-discover=0",
                "-rpcuser=prismtest",
                "-rpcpassword=prismtest",
                "-txindex=0",
            ])
            .arg(format!("-datadir={}", directory.display()))
            .arg(format!("-rpcport={rpc_port}"))
            .arg(format!("-port={p2p_port}"));
        fixture.ports.release(&[rpc_port, p2p_port]);
        let node = Self {
            process: Process::spawn(&mut command, fixture.directory.path().join("qbit-b.log"))?,
            rpc_port,
            p2p_port,
            client: fixture.client.clone(),
        };
        let ready = until("second qbit RPC", 30, || async {
            Ok(node.rpc("getblockchaininfo", json!([])).await?["chain"] == "regtest")
        })
        .await;
        match ready {
            Ok(()) => Ok(node),
            Err(error) => Err(error.context(node.diagnostics())),
        }
    }

    pub(super) async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let payload: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/", self.rpc_port))
            .basic_auth("prismtest", Some("prismtest"))
            .json(&json!({"jsonrpc":"1.0","id":"live-test-b","method":method,"params":params}))
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            payload["error"].is_null(),
            "node B RPC {method}: {}",
            payload["error"]
        );
        Ok(payload["result"].clone())
    }

    pub(super) async fn best(&self) -> Result<String> {
        best_of(self.rpc("getbestblockhash", json!([])).await?)
    }

    pub(super) async fn mine(&self, blocks: u64, address: &str) -> Result<Vec<String>> {
        mined(
            self.rpc("generatetoaddress", json!([blocks, address]))
                .await?,
        )
    }

    /// Connect A to B and wait until both hold the same active tip.
    pub(super) async fn heal(&self, fixture: &Fixture) -> Result<String> {
        self.rpc("setnetworkactive", json!([true])).await?;
        let address = format!("127.0.0.1:{}", self.p2p_port);
        until("node A connected to node B", 30, || async {
            if fixture.rpc("getconnectioncount", json!([])).await? == 0 {
                // `onetry` makes one attempt; repeat it until one holds.
                fixture
                    .rpc("addnode", json!([address.as_str(), "onetry"]))
                    .await?;
                return Ok(false);
            }
            Ok(self.rpc("getconnectioncount", json!([])).await? != 0)
        })
        .await?;
        until("nodes A and B on one active tip", 30, || async {
            Ok(node_a_best(fixture).await? == self.best().await?)
        })
        .await?;
        node_a_best(fixture).await
    }

    pub(super) async fn partition(&self, fixture: &Fixture) -> Result<()> {
        self.rpc("setnetworkactive", json!([false])).await?;
        until("nodes A and B partitioned", 30, || async {
            Ok(fixture.rpc("getconnectioncount", json!([])).await? == 0
                && self.rpc("getconnectioncount", json!([])).await? == 0)
        })
        .await
    }

    fn diagnostics(&self) -> String {
        diagnostics::report(&[("qbit node b".to_owned(), Some(&self.process))], None)
    }
}

fn best_of(value: Value) -> Result<String> {
    Ok(value
        .as_str()
        .context("best block hash missing")?
        .to_owned())
}

fn mined(value: Value) -> Result<Vec<String>> {
    value
        .as_array()
        .context("generated block hashes missing")?
        .iter()
        .map(|hash| Ok(hash.as_str().context("invalid block hash")?.to_owned()))
        .collect()
}

fn count(value: Value) -> Result<i64> {
    value.as_i64().context("block count missing")
}

async fn height(fixture: &Fixture, hash: &str) -> Result<i64> {
    count(fixture.rpc("getblockheader", json!([hash])).await?["height"].clone())
}

async fn new_address(fixture: &Fixture) -> Result<String> {
    Ok(fixture
        .rpc("getnewaddress", json!(["", "p2mr"]))
        .await?
        .as_str()
        .context("wallet address missing")?
        .to_owned())
}

pub(super) async fn node_a_best(fixture: &Fixture) -> Result<String> {
    best_of(fixture.rpc("getbestblockhash", json!([])).await?)
}

pub(super) async fn node_a_mine(fixture: &Fixture, blocks: u64) -> Result<Vec<String>> {
    mined(
        fixture
            .rpc("generatetoaddress", json!([blocks, fixture.address]))
            .await?,
    )
}

/// Start frontend `index` against the node whose RPC listens on `rpc_port`.
pub(super) fn start_frontend(fixture: &Fixture, index: usize, rpc_port: u16) -> Result<Process> {
    fixture.start_server_with(index, None, &[("QBIT_RPC_PORT", rpc_port.to_string())])
}

/// A check [`server_ready`] also waits on once the server is ready: #553
/// installs one while a session load runs, so a scenario's next step comes
/// under the whole load. `None` otherwise; live cases run one at a time.
pub(super) type ReadyGate = std::sync::Arc<dyn Fn() -> bool + Send + Sync>;
pub(super) static READY_GATE: std::sync::Mutex<Option<ReadyGate>> = std::sync::Mutex::new(None);

/// A session load's share offers, which #553 installs while a load runs and
/// [`deep_reorg`] pauses around its own blocks. `None` otherwise.
#[derive(Clone)]
pub(super) struct LoadShares {
    /// Pause (`true`) or resume (`false`) the load's share offers.
    pub pause: std::sync::Arc<dyn Fn(bool) + Send + Sync>,
    /// Whether no load share waits for its answer.
    pub answered: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
}
pub(super) static LOAD_SHARES: std::sync::Mutex<Option<LoadShares>> = std::sync::Mutex::new(None);

/// Pause a running load's share offers, until every offered share has its
/// answer, or resume them. Nothing without a load.
async fn pause_load_shares(paused: bool) -> Result<()> {
    let shares = LOAD_SHARES
        .lock()
        .map_err(|_| anyhow::anyhow!("load shares poisoned"))?
        .clone();
    let Some(shares) = shares else {
        return Ok(());
    };
    (shares.pause)(paused);
    if paused {
        until("the load's offered shares answered", 60, || async {
            Ok((shares.answered)())
        })
        .await?;
    }
    Ok(())
}

pub(super) async fn server_ready(fixture: &Fixture, index: usize) -> Result<()> {
    until(
        &format!("PRISM readiness of server {index}"),
        60,
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
    let gate = READY_GATE
        .lock()
        .map_err(|_| anyhow::anyhow!("ready gate poisoned"))?
        .clone();
    match gate {
        Some(gate) => until("the ready gate", 120, || async { Ok(gate()) }).await,
        None => Ok(()),
    }
}

/// Wait until `client` holds work on `parent`, and require that it arrived
/// within `NOTIFY_BOUND` of `changed`, taken just before the call that moved
/// the pool's node to that tip, so the bound is never measured short.
async fn delivered(client: &mut HighdiffClient, parent: &str, changed: Instant) -> Result<()> {
    client
        .wait_for_parent(parent)
        .await
        .with_context(|| format!("no mining.notify on new tip {parent}"))?;
    let latency = changed.elapsed();
    ensure!(
        latency <= NOTIFY_BOUND,
        "new-tip work for {parent} reached the miner {latency:?} after the tip changed, over the {NOTIFY_BOUND:?} bound"
    );
    eprintln!("two-node: new-tip work for {parent} delivered in {latency:?}");
    Ok(())
}

/// One own block: a block-only proof by worker `username` on frontend
/// `index`'s work for `parent`, acknowledged only once the block is durably
/// credited on the active chain.
pub(super) async fn own_block(
    fixture: &Fixture,
    index: usize,
    username: &str,
    parent: &str,
    id: u64,
) -> Result<String> {
    let mut miner = HighdiffClient::open_as(fixture, index, username.to_owned()).await?;
    miner.wait_for_parent(parent).await?;
    let (request, hash, _) = miner.solve(id, false)?;
    miner.send(request).await?;
    let response = tokio::time::timeout(Duration::from_secs(60), miner.response(id))
        .await
        .with_context(|| format!("own block {hash} was not acknowledged"))??;
    ensure!(
        response["result"] == true && response["error"].is_null(),
        "own block {hash} rejected: {response}"
    );
    Ok(hash)
}

pub(super) async fn chain_state(fixture: &Fixture, hash: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT chain_state FROM qbit_pool_blocks WHERE block_hash=$1")
            .bind(hash)
            .fetch_optional(&fixture.pool)
            .await?,
    )
}

async fn chain_states_are(fixture: &Fixture, hashes: &[&str], state: &str) -> Result<bool> {
    for hash in hashes {
        if chain_state(fixture, hash).await?.as_deref() != Some(state) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Accepted ledger rows for the share a block-only proof of `hash` defers.
pub(super) async fn credits(fixture: &Fixture, username: &str, hash: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger WHERE accepted AND share_id=$1")
            .bind(format!("{username}:{hash}"))
            .fetch_one(&fixture.pool)
            .await?,
    )
}

pub(super) async fn assert_no_duplicate_headers(fixture: &Fixture) -> Result<()> {
    // One statement, so one snapshot: under session load (#553) shares land
    // between two separate counts. An append writes both rows in one
    // transaction.
    let (accepted, unique): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_share_ledger WHERE accepted),\
                (SELECT count(*) FROM qbit_prism_share_hashes)",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        accepted == unique,
        "{accepted} accepted rows for {unique} headers: a header was credited twice"
    );
    Ok(())
}

/// What each of `miners` is owed as the public miner dashboard reports it
/// (carry-forward, lifetime earnings of confirmed blocks, and payouts
/// pending maturity), which every running frontend must agree on, and the
/// cluster's carry-forward balances by program.
async fn balances(
    fixture: &Fixture,
    miners: &[String],
) -> Result<(Vec<Value>, Vec<(String, String)>)> {
    let mut reported = Vec::new();
    for miner in miners {
        let mut views = Vec::new();
        for (index, server) in fixture.servers.iter().enumerate() {
            if server.child.try_wait()?.is_some() {
                continue;
            }
            let view: Value = fixture
                .client
                .get(format!(
                    "http://127.0.0.1:{}/public/v1/miners/{miner}",
                    fixture.api[index]
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            views.push(json!({
                "miner": miner,
                "owed": view["owed_balance_bits"],
                "lifetime": view["lifetime_earnings_bits"],
                "pending": view["pending_maturity_bits"],
            }));
        }
        let first = views.first().context("no running frontend")?.clone();
        ensure!(
            views.iter().all(|view| *view == first),
            "frontends disagree on {miner}'s balances: {views:?}"
        );
        reported.push(first);
    }
    let carry = sqlx::query_as(
        "SELECT encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances() ORDER BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    Ok((reported, carry))
}

#[derive(Debug)]
struct CandidateRow {
    state: String,
    outcome: Option<String>,
    reply: Option<String>,
    last_error: Option<String>,
    attempts: i32,
    claimed: bool,
}

async fn candidate(fixture: &Fixture, hash: &str) -> Result<CandidateRow> {
    let row = sqlx::query("SELECT state,offer_outcome,offer_reply,last_error,attempt_count,claim_token IS NOT NULL AS claimed FROM qbit_block_candidate_outbox WHERE block_hash=$1")
        .bind(hash)
        .fetch_one(&fixture.pool)
        .await?;
    Ok(CandidateRow {
        state: row.try_get("state")?,
        outcome: row.try_get("offer_outcome")?,
        reply: row.try_get("offer_reply")?,
        last_error: row.try_get("last_error")?,
        attempts: row.try_get("attempt_count")?,
        claimed: row.try_get("claimed")?,
    })
}

/// The candidate and landing gauges of one frontend's `/metrics`, the
/// inputs of the native candidate and landing pages.
#[derive(Debug)]
struct CandidateGauges {
    pending: f64,
    unacknowledged: f64,
    landing_failed: f64,
    revision_work_pending: f64,
    orphaned_total: f64,
}

async fn candidate_gauges(fixture: &Fixture, index: usize) -> Result<CandidateGauges> {
    let body = fixture
        .client
        .get(format!("http://127.0.0.1:{}/metrics", fixture.api[index]))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let sample = |name: &str| -> Result<f64> {
        let name = format!("qbit_prism_{name}");
        body.lines()
            .find_map(|line| {
                let (metric, value) = line.split_once(' ')?;
                (metric == name).then(|| value.trim().parse::<f64>())
            })
            .with_context(|| format!("{name} missing from /metrics"))?
            .with_context(|| format!("{name} is not a number"))
    };
    Ok(CandidateGauges {
        pending: sample("block_candidates_pending")?,
        unacknowledged: sample("block_candidate_oldest_unacknowledged_seconds")?,
        landing_failed: sample("block_candidate_oldest_landing_failed_seconds")?,
        revision_work_pending: sample("accepted_block_revision_work_pending_seconds")?,
        orphaned_total: sample("block_candidates_orphaned_total")?,
    })
}

/// Fail on a sample at a paging threshold of the native rules: the
/// candidate critical page at an unacknowledged age of 60 s, and the
/// landing-failed and revision-work critical pages at 307 s. With
/// `acknowledged`, the lost race is known to be settled and acknowledged,
/// so its unacknowledged and landing-failed ages must be exactly zero; an
/// unknown (-1) reading fails too.
fn unpaged(gauges: &CandidateGauges, acknowledged: bool) -> Result<()> {
    ensure!(
        gauges.unacknowledged < 60.0
            && gauges.landing_failed < 307.0
            && gauges.revision_work_pending < 307.0,
        "a candidate or landing gauge reached its paging threshold: {gauges:?}"
    );
    ensure!(
        !acknowledged || (gauges.unacknowledged == 0.0 && gauges.landing_failed == 0.0),
        "the acknowledged lost race counts toward a paging age: {gauges:?}"
    );
    Ok(())
}

/// `until`, sampling frontend 0's candidate gauges on every poll. Unlike an
/// error inside an `until` condition, which is only retried, a paging
/// sample (see `unpaged`) fails at once.
async fn until_unpaged<F, Fut>(
    fixture: &Fixture,
    label: &str,
    seconds: u64,
    acknowledged: bool,
    mut condition: F,
) -> Result<()>
where
    F: FnMut(CandidateGauges) -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let started = Instant::now();
    let mut last = None;
    loop {
        match candidate_gauges(fixture, 0).await {
            Ok(gauges) => {
                unpaged(&gauges, acknowledged)?;
                match condition(gauges).await {
                    Ok(true) => return Ok(()),
                    Ok(false) => {}
                    Err(error) => last = Some(error),
                }
            }
            Err(error) => last = Some(error),
        }
        if started.elapsed() > Duration::from_secs(seconds) {
            bail!(
                "timed out waiting for {label}: {}",
                last.map_or_else(|| "condition not met".into(), |e| e.to_string())
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Pause the offer of any reserved candidate at its last database step
/// before `submitblock`, the claim renewal at the external boundary, until
/// the returned transaction ends. The renewal times out after 5 s, so the
/// hold must stay short. No settlement or cluster lock is held meanwhile.
async fn hold_offers(
    fixture: &Fixture,
) -> Result<(sqlx::Transaction<'static, sqlx::Postgres>, i64)> {
    let key = i64::from(rand_key());
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION test_b521_hold_offer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.state='offer_reserved' AND NEW.state='offer_reserved' AND NEW.claim_token IS NOT DISTINCT FROM OLD.claim_token THEN PERFORM pg_advisory_xact_lock({key}); END IF; RETURN NEW; END $$; CREATE TRIGGER test_b521_hold_offer BEFORE UPDATE OF claim_expires_at ON qbit_block_candidate_outbox FOR EACH ROW EXECUTE FUNCTION test_b521_hold_offer();"
    ))
    .execute(&fixture.pool)
    .await?;
    let mut hold = fixture.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *hold)
        .await?;
    Ok((hold, key))
}

async fn release_offers(
    fixture: &Fixture,
    hold: sqlx::Transaction<'static, sqlx::Postgres>,
) -> Result<()> {
    hold.rollback().await?;
    sqlx::raw_sql(
        "DROP TRIGGER test_b521_hold_offer ON qbit_block_candidate_outbox; DROP FUNCTION test_b521_hold_offer();",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn offer_held(fixture: &Fixture, key: i64) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=0 AND objid::bigint=$1 AND objsubid=1)",
    )
    .bind(key)
    .fetch_one(&fixture.pool)
    .await?)
}

/// A positive advisory key below 2^31, so it is the lock's `objid`.
fn rand_key() -> u32 {
    (Uuid::new_v4().as_u128() as u32) & 0x7fff_ffff | 1
}

/// Scenario 1. The pool's own block reaches the pool's node after a
/// same-height competitor mined on the other side of a partition: the node
/// answers `inconclusive`, the block is a side-chain block, and the
/// competitor's chain then buries it. The offer is held at its
/// `submitblock` fence while the competitor reaches node A (by
/// `submitblock`, as a relayed block would, so the arrival order is exact),
/// which is the pool's real window between its supersession check and its
/// offer; nothing else is simulated. The candidate must stay acknowledged and unpaged while it
/// waits for its orphan proof, settle `orphaned` exactly at the default
/// confirmation depth, never stall job delivery, and be credited once when
/// a reorg back activates it.
pub(super) async fn lost_race(fixture: &mut Fixture, peer: &PeerNode) -> Result<()> {
    let base = peer.heal(fixture).await?;
    fixture.servers.push(fixture.start_server(0)?);
    server_ready(fixture, 0).await?;
    let mut watcher = HighdiffClient::open(fixture, 0).await?;
    let mut miner = HighdiffClient::open(fixture, 0).await?;
    miner.wait_for_parent(&base).await?;
    watcher.wait_for_parent(&base).await?;
    peer.partition(fixture).await?;
    let competitor = peer.mine(1, &fixture.address).await?.remove(0);

    // Our block on the same parent, offered only after the competitor is
    // node A's tip.
    let (request, own, _) = miner.solve(10, false)?;
    let (hold, key) = hold_offers(fixture).await?;
    miner.send(request).await?;
    until(
        "own block offer held at its submitblock fence",
        30,
        || async { offer_held(fixture, key).await },
    )
    .await?;
    ensure!(
        candidate(fixture, &own).await?.state == "offer_reserved",
        "held candidate is not reserved"
    );
    let competitor_bytes = peer.rpc("getblock", json!([competitor, 0])).await?;
    let changed = Instant::now();
    let reply = fixture
        .rpc("submitblock", json!([competitor_bytes]))
        .await?;
    ensure!(reply.is_null(), "node A refused the competitor: {reply}");
    ensure!(
        node_a_best(fixture).await? == competitor,
        "competitor is not node A's tip"
    );
    release_offers(fixture, hold).await?;
    delivered(&mut watcher, &competitor, changed).await?;

    until("lost race settled in reconciliation", 30, || async {
        Ok(candidate(fixture, &own).await?.state == "reconciliation")
    })
    .await?;
    let settled = candidate(fixture, &own).await?;
    ensure!(
        settled.outcome.as_deref() == Some("rejected")
            && settled.reply.as_deref() == Some("inconclusive"),
        "the node did not answer the lost race as a side-chain block: {settled:?}"
    );
    ensure!(
        chain_state(fixture, &own)
            .await?
            .is_some_and(|state| state != "confirmed"),
        "the lost race's audit did not land, or its block is credited"
    );
    ensure!(
        credits(fixture, &miner.username, &own).await? == 0,
        "a side-chain block's deferred share was credited"
    );
    // The row is unfinished but acknowledged: the node holds the block. It
    // counts as pending and never toward the unacknowledged or
    // landing-failed ages that page.
    // Samples taken before the settlement may still show the reservation's
    // age; the first one showing the row acknowledged is after it, and from
    // then on every sample must show zero for both paging ages.
    until_unpaged(
        fixture,
        "census of the acknowledged lost race",
        20,
        false,
        |gauges| async move { Ok(gauges.pending == 1.0 && gauges.unacknowledged == 0.0) },
    )
    .await?;
    unpaged(&candidate_gauges(fixture, 0).await?, true)?;

    // The competitor's chain reaches one confirmation short of the orphan
    // proof: the row must survive a retry at that depth.
    peer.heal(fixture).await?;
    let changed = Instant::now();
    let hashes = peer
        .mine(ORPHAN_CONFIRMATIONS - 2, &fixture.address)
        .await?;
    let short = hashes.last().context("no block mined")?.clone();
    until("node A on the deeper competitor chain", 30, || async {
        Ok(node_a_best(fixture).await? == short)
    })
    .await?;
    delivered(&mut watcher, &short, changed).await?;
    let before = candidate(fixture, &own).await?.attempts;
    until_unpaged(
        fixture,
        "a reconciliation retry one confirmation short",
        60,
        true,
        |_| async {
            let row = candidate(fixture, &own).await?;
            Ok(row.attempts > before && !row.claimed)
        },
    )
    .await?;
    let retried = candidate(fixture, &own).await?;
    ensure!(
        retried.state == "reconciliation",
        "a competitor with {} confirmations settled the row: {retried:?}",
        ORPHAN_CONFIRMATIONS - 1
    );

    // Exactly the default depth proves the orphan.
    let changed = Instant::now();
    let proof = peer.mine(1, &fixture.address).await?.remove(0);
    until("node A at the orphan-proof depth", 30, || async {
        Ok(node_a_best(fixture).await? == proof)
    })
    .await?;
    delivered(&mut watcher, &proof, changed).await?;
    until("lost race settled orphaned", 90, || async {
        Ok(candidate(fixture, &own).await?.state == "orphaned")
    })
    .await?;
    let orphaned = candidate(fixture, &own).await?;
    ensure!(
        orphaned.last_error.as_deref().is_some_and(
            |reason| reason.contains(&format!("with {ORPHAN_CONFIRMATIONS} confirmations"))
        ),
        "orphan proof at an unexpected depth: {orphaned:?}"
    );
    until_unpaged(
        fixture,
        "census after the orphan proof",
        20,
        false,
        |gauges| async move { Ok(gauges.pending == 0.0 && gauges.orphaned_total == 1.0) },
    )
    .await?;
    ensure!(
        credits(fixture, &miner.username, &own).await? == 0,
        "an orphaned block's deferred share was credited"
    );
    fixture.integrity().await?;

    // A reorg back: node A invalidates the competitor, extends our block
    // past the competitor's chain, and node B follows.
    fixture.rpc("invalidateblock", json!([competitor])).await?;
    ensure!(
        node_a_best(fixture).await? == own,
        "node A did not fall back to the pool's block"
    );
    let changed = Instant::now();
    let reorg = node_a_mine(fixture, ORPHAN_CONFIRMATIONS).await?;
    let back = reorg.last().context("no block mined")?.clone();
    until("node B reorged back onto the pool's block", 30, || async {
        Ok(peer.best().await? == back)
    })
    .await?;
    delivered(&mut watcher, &back, changed).await?;
    until("reactivated own block credited", 30, || async {
        Ok(
            chain_state(fixture, &own).await?.as_deref() == Some("confirmed")
                && credits(fixture, &miner.username, &own).await? == 1,
        )
    })
    .await?;
    // `reconsiderblock` makes the competitor's chain valid again; it has
    // less work, so nothing moves, and a further tip changes nothing either.
    fixture.rpc("reconsiderblock", json!([competitor])).await?;
    let changed = Instant::now();
    let next = peer.mine(1, &fixture.address).await?.remove(0);
    until("node A on the next tip", 30, || async {
        Ok(node_a_best(fixture).await? == next)
    })
    .await?;
    delivered(&mut watcher, &next, changed).await?;
    ensure!(
        credits(fixture, &miner.username, &own).await? == 1,
        "the reactivated block was credited more than once"
    );
    ensure!(
        candidate(fixture, &own).await?.state == "orphaned",
        "the terminal orphan disposition reopened"
    );
    assert_no_duplicate_headers(fixture).await?;
    fixture.integrity().await?;
    let gauges = candidate_gauges(fixture, 0).await?;
    unpaged(&gauges, false)?;
    ensure!(
        gauges.pending == 0.0,
        "a candidate is unfinished after the reorg back: {gauges:?}"
    );
    eprintln!("two-node lost race: {own} answered inconclusive, reconciled without paging at 1 and {} confirmations, orphaned at {ORPHAN_CONFIRMATIONS}, credited once after the reorg back", ORPHAN_CONFIRMATIONS - 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_own_block_loses_same_height_race_is_orphaned_and_credited_once_on_reorg_back(
) -> Result<()> {
    let Some(mut fixture) = Fixture::open_with_servers(false, false).await? else {
        return Ok(());
    };
    let peer = PeerNode::start(&fixture).await;
    let result = match &peer {
        Ok(peer) => lost_race(&mut fixture, peer).await,
        Err(error) => Err(anyhow::anyhow!("{error:#}")),
    };
    finish(fixture, peer, result).await
}

/// Scenario 2. The pool mines own blocks on both sides of a partition, the
/// frontend moving from node A to node B between them, and the partition
/// then heals onto A's branch, which reorganizes B nine blocks deep, past
/// the orphan-confirmation depth. Branch A's own blocks are reactivated and
/// credited once, branch B's are deactivated, every miner's reported
/// balances return exactly to the branch-A values with no payout
/// divergence, integrity stays clean, and the
/// reactivated blocks' CTV fanouts recover from `reorged` and confirm at
/// maturity while branch B's are never broadcast.
pub(super) async fn deep_reorg(fixture: &mut Fixture, peer: &PeerNode) -> Result<()> {
    // Two payout identities, so each own block splits its reward and the
    // miners' balances depend on which branch is active.
    let main = format!("{}.highdiff", fixture.address);
    let second = new_address(fixture).await?;
    let small = format!("{second}.small");
    let miners = [fixture.address.clone(), second];
    let mut tip = peer.heal(fixture).await?;
    fixture.servers.push(fixture.start_server(0)?);
    server_ready(fixture, 0).await?;
    // Under a session load (#553), its shares stop until branch B's own
    // blocks are paid. On regtest a load share weighs about half an own
    // block (credited at the network difficulty), so ~15 of them fill the
    // window: each own block would pay at most one earlier own share, by
    // timing, and branch B could pay both miners what branch A did.
    pause_load_shares(true).await?;
    // Shared own blocks before the partition seed the payout window, so
    // every later own block pays both miners through a CTV fanout.
    let mut seeds = Vec::new();
    for (id, username) in [(1, &main), (2, &main), (3, &small)] {
        tip = own_block(fixture, 0, username, &tip, id).await?;
        seeds.push((tip.clone(), username.clone()));
    }
    let fork = peer.heal(fixture).await?;
    ensure!(fork == tip, "node B did not take the shared own blocks");
    peer.partition(fixture).await?;

    // Branch A: two own blocks through frontend 0 on node A.
    let a1 = own_block(fixture, 0, &main, &fork, 4).await?;
    let a2 = own_block(fixture, 0, &main, &a1, 5).await?;
    let branch_a = [a1.as_str(), a2.as_str()];
    ensure!(
        chain_states_are(fixture, &branch_a, "confirmed").await?,
        "branch A's own blocks are not confirmed"
    );
    fixture.integrity().await?;
    let balances_a = balances(fixture, &miners).await?;

    // Node B outgrows branch A and the pool moves there: frontend 0 stops
    // and frontend 1 starts against node B.
    let b_tip = peer.mine(3, &fixture.address).await?.remove(2);
    fixture.servers[0].stop();
    let server = start_frontend(fixture, 1, peer.rpc_port)?;
    fixture.servers.push(server);
    server_ready(fixture, 1).await?;
    until("branch A's own blocks deactivated", 30, || async {
        chain_states_are(fixture, &branch_a, "inactive").await
    })
    .await?;
    fixture.integrity().await?;
    let b1 = own_block(fixture, 1, &main, &b_tip, 6).await?;
    let b2 = own_block(fixture, 1, &main, &b1, 7).await?;
    let branch_b = [b1.as_str(), b2.as_str()];
    ensure!(
        chain_states_are(fixture, &branch_b, "confirmed").await?,
        "branch B's own blocks are not confirmed"
    );
    fixture.integrity().await?;
    let balances_b = balances(fixture, &miners).await?;
    ensure!(
        balances_b != balances_a,
        "branch B's own blocks left the balances at branch A's, {balances_a:?}; the comparison below would prove nothing"
    );
    // Every own block is paid as found; the reorg runs under the whole load.
    pause_load_shares(false).await?;
    // Branch B: 3 + 2 own + 4 = 9 blocks past the fork, so B1 has
    // ORPHAN_CONFIRMATIONS confirmations; branch A grows to 10 unobserved.
    peer.mine(4, &fixture.address).await?;
    node_a_mine(fixture, 8).await?;
    let fork_height = height(fixture, &fork).await?;
    let b_height = count(peer.rpc("getblockcount", json!([])).await?)?;
    ensure!(
        b_height - fork_height > i64::try_from(ORPHAN_CONFIRMATIONS)?,
        "branch B is only {} blocks deep",
        b_height - fork_height
    );

    // Heal onto branch A: node B reorganizes past the orphan depth under
    // frontend 1.
    let winner = peer.heal(fixture).await?;
    ensure!(
        winner == node_a_best(fixture).await?,
        "the healed nodes did not take branch A"
    );
    until("deep reorg reconciled", 60, || async {
        Ok(chain_states_are(fixture, &branch_a, "confirmed").await?
            && chain_states_are(fixture, &branch_b, "inactive").await?)
    })
    .await?;
    fixture.integrity().await?;
    // Both frontends run again on the converged nodes, and agree.
    fixture.servers[0] = fixture.start_server(0)?;
    server_ready(fixture, 0).await?;
    let balances_healed = balances(fixture, &miners).await?;
    ensure!(
        balances_healed == balances_a,
        "balances after the deep reorg differ from branch A's: {balances_healed:?}, expected {balances_a:?}"
    );
    let divergences: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_prism_payout_divergences")
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(
        divergences == 0,
        "the deep reorg recorded {divergences} payout divergences"
    );
    let own = seeds
        .iter()
        .cloned()
        .chain([&a1, &a2, &b1, &b2].map(|hash| (hash.clone(), main.clone())));
    for (hash, username) in own {
        ensure!(
            credits(fixture, &username, &hash).await? == 1,
            "own block {hash} is not credited exactly once"
        );
    }
    assert_no_duplicate_headers(fixture).await?;

    // CTV fanouts: branch A's recover from `reorged`, branch B's stay.
    let fanouts = |hashes: [&str; 2]| {
        let pool = fixture.pool.clone();
        let hashes: Vec<String> = hashes.iter().map(|hash| (*hash).to_owned()).collect();
        async move {
            let rows: Vec<(String, String, i64)> = sqlx::query_as("SELECT fanout_txid,settlement_status,broadcast_attempt_count FROM qbit_ctv_fanout_artifacts WHERE block_hash=ANY($1) ORDER BY fanout_txid")
                .bind(&hashes)
                .fetch_all(&pool)
                .await?;
            anyhow::Ok(rows)
        }
    };
    let recovered = fanouts([&a1, &a2]).await?;
    ensure!(
        !recovered.is_empty()
            && recovered
                .iter()
                .all(|(_, status, _)| status == "awaiting_maturity"),
        "branch A's fanouts did not recover from the reorg: {recovered:?}"
    );
    let reorged = fanouts([&b1, &b2]).await?;
    ensure!(
        !reorged.is_empty() && reorged.iter().all(|(_, status, _)| status == "reorged"),
        "branch B's fanouts are not reorged: {reorged:?}"
    );
    let a1_height = height(fixture, &a1).await?;
    let tip = count(fixture.rpc("getblockcount", json!([])).await?)?;
    node_a_mine(fixture, u64::try_from(a1_height + 1001 - tip)?).await?;
    let txids: Vec<String> = recovered.iter().map(|(txid, _, _)| txid.clone()).collect();
    until("recovered fanouts broadcast at maturity", 90, || async {
        let mempool = fixture.rpc("getrawmempool", json!([])).await?;
        let mempool = mempool.as_array().context("mempool missing")?;
        Ok(txids.iter().all(|txid| mempool.contains(&json!(txid))))
    })
    .await?;
    node_a_mine(fixture, 1).await?;
    until("recovered fanouts confirmed", 60, || async {
        Ok(fanouts([&a1, &a2])
            .await?
            .iter()
            .all(|(_, status, _)| status == "confirmed"))
    })
    .await?;
    let reorged = fanouts([&b1, &b2]).await?;
    ensure!(
        reorged
            .iter()
            .all(|(_, status, attempts)| status == "reorged" && *attempts == 0),
        "a fanout of the losing branch was broadcast: {reorged:?}"
    );
    fixture.integrity().await?;
    eprintln!("two-node deep reorg: own blocks {a1} {a2} on branch A and {b1} {b2} on branch B; B reorganized {} deep onto A; balances {balances_a:?} exact (branch B had {balances_b:?}), reactivated blocks credited once, {} fanouts recovered and confirmed", b_height - fork_height, txids.len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_deep_reorg_with_own_blocks_on_both_branches_keeps_credit_balances_and_fanouts_exact(
) -> Result<()> {
    let Some(mut fixture) = Fixture::open_with_servers(true, false).await? else {
        return Ok(());
    };
    let peer = PeerNode::start(&fixture).await;
    let result = match &peer {
        Ok(peer) => deep_reorg(&mut fixture, peer).await,
        Err(error) => Err(anyhow::anyhow!("{error:#}")),
    };
    finish(fixture, peer, result).await
}

/// Report both nodes on failure, stop node B, then clean the fixture up,
/// which releases the serial guard only after every process is stopped.
pub(super) async fn finish(
    fixture: Fixture,
    peer: Result<PeerNode>,
    result: Result<()>,
) -> Result<()> {
    if result.is_err() {
        eprintln!("{}", fixture.diagnostics());
        if let Ok(peer) = &peer {
            eprintln!("{}", peer.diagnostics());
        }
    }
    drop(peer);
    result.and(fixture.cleanup().await)
}
