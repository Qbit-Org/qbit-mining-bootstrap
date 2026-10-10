//! PRISM 3.1 dual writer, against a real PostgreSQL: the carry owner guard
//! (`carry_owner.rs`) and its operator commands `status`, `release` and
//! `transfer` (`carry_owner/transfer.rs`), on a pair of nodes.
//!
//! Each node is its own fixture database, migrated and personalised as node
//! A or node B the way `node-identity set` does it, and each reads the other's
//! journal live through that database's URL, its `PRISM_PEER_DATABASE_URL`.
//! An unreachable peer is a URL no server listens on. Rows the peer sync would
//! copy (the other node's journal rows, a landed pool block and its audit) are
//! written by SQL.
//!
//! The chain both nodes follow is held in memory. A case mines foreign blocks
//! and pool blocks onto it; a pool block's coinbase carries the pool's tag,
//! `/PRISM/`, in its scriptSig.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test carry_owner_guard_postgres
//! ```
use anyhow::{bail, ensure, Context, Result};
use qbit_prism_server::{
    carry_owner::{
        append_role, check, read_latest_roles, seed_role,
        transfer::{self, ChainSource, CoinbaseView, Landing, PoolRecognizer, Report},
        CarryDecision, CarryFreeReason,
        CarryFreeReason::{
            ClaimNotVetted, ConfigMismatch, NoJournalRow, NodeUnidentified, NotOwner,
            OwnJournalBehindPeer, OwnJournalRolledBack, PeerClaimsOwnership, PeerNotSeeded,
            PeerUnconfirmed, SettingPending,
        },
        CarryOwnerSettings, Guard, LatestRoles, PeerJournal, RoleRow,
    },
    ledger::Ledger,
    ledger::SPONSOR_TAKEOVER_AFTER,
    metrics::Metrics,
    node_identity::{NodeIdentity, NodeIndex},
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{future::Future, sync::Mutex, time::Duration};

#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// A peer database URL no server listens on: every read of it fails.
const UNREACHABLE_PEER: &str = "postgresql://prism@127.0.0.1:1/unreachable";
/// The bound on one live read of a fixture peer. Generous, because the host
/// is shared and a read that timed out would count as an unreachable peer.
const LIVE_PEER_TIMEOUT: Duration = Duration::from_secs(30);
/// The bound on one read of the unreachable peer, which always fails.
const UNREACHABLE_PEER_TIMEOUT: Duration = Duration::from_millis(500);
/// The pool's coinbase tag.
const TAG: &str = "/PRISM/";
/// The orphan-confirmation depth `transfer` waits for past the peer's release.
const RELEASE_DEPTH: u64 = 6;
/// The first height `transfer` scans: the whole chain, the command's default.
const SCAN_FROM: u64 = 0;
/// The reason every `release` and `transfer` journals.
const HANDOVER: &str = "planned handover";
/// Who wrote the rows a case writes itself.
const RECORDED_BY: &str = "carry owner guard test";

const PAYING: CarryDecision = CarryDecision::Paying;

const fn carry_free(reason: CarryFreeReason) -> CarryDecision {
    CarryDecision::CarryFree(reason)
}

fn peer_timeout(url: &str) -> Duration {
    if url == UNREACHABLE_PEER {
        UNREACHABLE_PEER_TIMEOUT
    } else {
        LIVE_PEER_TIMEOUT
    }
}

/// The guard's settings as `node`'s frontend reads them, with `peer` as its
/// peer database URL.
fn settings(node: NodeIndex, carry_owner: bool, peer: &str) -> CarryOwnerSettings {
    CarryOwnerSettings {
        node_index: node.index(),
        carry_owner,
        peer_urls: vec![peer.to_owned()],
        interval: CarryOwnerSettings::INTERVAL,
        peer_timeout: peer_timeout(peer),
    }
}

/// The guard's live view of the journal at `url`.
fn peer_journal(url: &str) -> Result<PeerJournal> {
    PeerJournal::new(&[url.to_owned()], peer_timeout(url))
}

/// One row of `qbit_prism_node_roles`, every column but `recorded_at`.
#[derive(Clone, Debug, PartialEq)]
struct JournalRow {
    origin_node: i16,
    epoch: i64,
    carry_owner: bool,
    action: String,
    recorded_by: String,
    detail: Value,
}

impl JournalRow {
    /// The row as the guard reads it.
    fn role(&self) -> RoleRow {
        RoleRow {
            origin_node: self.origin_node,
            epoch: self.epoch,
            carry_owner: self.carry_owner,
            action: self.action.clone(),
            detail: self.detail.clone(),
        }
    }
}

/// Every row of the journal at `pool`, in (origin_node, epoch) order.
async fn journal(pool: &PgPool) -> Result<Vec<JournalRow>> {
    let rows: Vec<(i16, i64, bool, String, String, Value)> = sqlx::query_as(
        "SELECT origin_node,epoch,carry_owner,action,recorded_by,detail FROM qbit_prism_node_roles ORDER BY origin_node,epoch",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(origin_node, epoch, carry_owner, action, recorded_by, detail)| JournalRow {
                origin_node,
                epoch,
                carry_owner,
                action,
                recorded_by,
                detail,
            },
        )
        .collect())
}

/// Writes `row` into the journal at `pool` as the peer sync copies it: the
/// same identity and content, under the node that originated it.
async fn copy_row(pool: &PgPool, row: &JournalRow) -> Result<()> {
    sqlx::query("INSERT INTO qbit_prism_node_roles(origin_node,epoch,carry_owner,action,recorded_by,detail) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(row.origin_node)
        .bind(row.epoch)
        .bind(row.carry_owner)
        .bind(&row.action)
        .bind(&row.recorded_by)
        .bind(&row.detail)
        .execute(pool)
        .await?;
    Ok(())
}

/// Deletes `origin`'s journal rows from epoch `from` on, as a rollback of the
/// database under a running frontend would: the append-only trigger is off
/// for that one transaction.
async fn roll_back(pool: &PgPool, origin: i16, from: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    for statement in [
        "ALTER TABLE qbit_prism_node_roles DISABLE TRIGGER qbit_prism_node_roles_append_only",
        "DELETE FROM qbit_prism_node_roles WHERE origin_node=$1 AND epoch>=$2",
        "ALTER TABLE qbit_prism_node_roles ENABLE TRIGGER qbit_prism_node_roles_append_only",
    ] {
        let query = sqlx::query(statement);
        let query = if statement.starts_with("DELETE") {
            query.bind(origin).bind(from)
        } else {
            query
        };
        query.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// The journal row at `pool` with this identity, every column.
async fn row_json(pool: &PgPool, origin: i16, epoch: i64) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM qbit_prism_node_roles r WHERE origin_node=$1 AND epoch=$2",
    )
    .bind(origin)
    .bind(epoch)
    .fetch_optional(pool)
    .await?)
}

/// The cluster's payout revision.
async fn revision(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(pool)
            .await?,
    )
}

/// The database's wall clock in milliseconds, the floor of a new epoch.
async fn clock_ms(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(pool)
            .await?,
    )
}

/// One node of the pair: its database, and a frontend's ledger on it.
struct Node {
    node: NodeIndex,
    url: String,
    /// The peer's database URL.
    peer: String,
    ledger: Ledger,
}

impl Node {
    /// Migrates the database at `url` and, when `personalise`, makes it
    /// `node`'s.
    async fn open(url: &str, peer: &str, node: NodeIndex, personalise: bool) -> Result<Self> {
        let ledger = Ledger::connect(
            url,
            format!("carry-guard-{}", node.name().to_lowercase()),
            4,
            true,
        )
        .await?;
        let opened = Self {
            node,
            url: url.to_owned(),
            peer: peer.to_owned(),
            ledger,
        };
        if personalise {
            opened.personalise().await?;
        }
        Ok(opened)
    }

    /// `node-identity set --index N`.
    async fn personalise(&self) -> Result<()> {
        let record = self
            .ledger
            .set_node_identity(self.node, RECORDED_BY)
            .await?;
        ensure!(
            record.node == self.node,
            "the database was personalised as node {}, not {}",
            record.node,
            self.node
        );
        Ok(())
    }

    fn pool(&self) -> &PgPool {
        &self.ledger.pool
    }

    fn origin(&self) -> i16 {
        self.node.index()
    }

    /// This frontend's guard settings, with `carry_owner` as its
    /// `PRISM_CARRY_OWNER`.
    fn settings(&self, carry_owner: bool) -> CarryOwnerSettings {
        settings(self.node, carry_owner, &self.peer)
    }

    /// A frontend process of this node that has just started, with
    /// `carry_owner` as its `PRISM_CARRY_OWNER`.
    fn start(&self, carry_owner: bool) -> Process<'_> {
        Process {
            node: self,
            settings: self.settings(carry_owner),
            guard: Guard::default(),
            metrics: None,
            caught_up: true,
        }
    }

    async fn journal(&self) -> Result<Vec<JournalRow>> {
        journal(self.pool()).await
    }

    /// The latest rows of both nodes in this node's journal, as the guard
    /// reads them.
    async fn latest(&self) -> Result<LatestRoles> {
        let mut connection = self.pool().acquire().await?;
        Ok(read_latest_roles(&mut connection, self.origin()).await?)
    }

    /// Seeds this node's journal as its guard's first decision does.
    async fn seed(&self, carry_owner: bool) -> Result<JournalRow> {
        let epoch = seed_role(&self.ledger, self.origin(), carry_owner)
            .await?
            .with_context(|| format!("node {} was seeded already", self.node))?;
        self.row(epoch).await
    }

    /// Appends a row of this node's to its own journal, as `release` and
    /// `transfer` do, but without their checks.
    async fn append(&self, carry_owner: bool, action: &str, detail: Value) -> Result<JournalRow> {
        let mut connection = self.pool().acquire().await?;
        let epoch = append_role(
            &mut connection,
            self.origin(),
            carry_owner,
            action,
            RECORDED_BY,
            &detail,
        )
        .await?;
        drop(connection);
        self.row(epoch).await
    }

    /// This node's own row at `epoch`.
    async fn row(&self, epoch: i64) -> Result<JournalRow> {
        self.journal()
            .await?
            .into_iter()
            .find(|row| row.origin_node == self.origin() && row.epoch == epoch)
            .with_context(|| format!("node {}'s journal has no row at epoch {epoch}", self.node))
    }

    async fn revision(&self) -> Result<i64> {
        revision(self.pool()).await
    }
}

/// Requires `node`'s journal to hold exactly `rows`.
async fn expect_journal(node: &Node, rows: &[JournalRow], when: &str) -> Result<()> {
    let found = node.journal().await?;
    ensure!(
        found == rows,
        "{when}: node {}'s journal is {found:?}, not {rows:?}",
        node.node
    );
    Ok(())
}

/// One frontend process of a node: its guard's settings, and the state the
/// guard keeps between decisions. A restarted process is a new one.
struct Process<'a> {
    node: &'a Node,
    settings: CarryOwnerSettings,
    guard: Guard,
    metrics: Option<&'a Metrics>,
    /// The peer sync's own-log latch (D-8), as this process sees it.
    caught_up: bool,
}

impl Process<'_> {
    /// One decision, as the guard's loop takes it.
    async fn decide(&mut self, peer: &PeerJournal) -> Result<CarryDecision> {
        check(
            &self.node.ledger,
            &self.settings,
            peer,
            &mut self.guard,
            self.caught_up,
            self.metrics,
        )
        .await
    }

    /// One decision, which must be `expected`.
    async fn expect(
        &mut self,
        peer: &PeerJournal,
        expected: CarryDecision,
        when: &str,
    ) -> Result<()> {
        let decision = self.decide(peer).await?;
        ensure!(
            decision == expected,
            "{when}: the guard of node index {} decided {decision:?}, not {expected:?}",
            self.settings.node_index
        );
        Ok(())
    }

    /// One turn of the guard's loop: decide, then apply the decision to the
    /// carry gate. Returns the decision and whether the gate moved.
    async fn turn(&mut self, peer: &PeerJournal) -> Result<(CarryDecision, bool)> {
        let decision = self.decide(peer).await?;
        let ledger = &self.node.ledger;
        let moved = ledger.set_carry_paying(decision.paying()).await?;
        ensure!(
            ledger.carry_paying() == decision.paying() && !ledger.carry_fence_pending(),
            "after {decision:?} the gate pays: {}, its fence is pending: {}",
            ledger.carry_paying(),
            ledger.carry_fence_pending()
        );
        Ok((decision, moved))
    }
}

/// Nodes A and B, each personalised as itself.
struct Pair {
    a: Node,
    b: Node,
}

impl Pair {
    async fn open(a_url: &str, b_url: &str) -> Result<Self> {
        Ok(Self {
            a: Node::open(a_url, b_url, NodeIndex::A, true).await?,
            b: Node::open(b_url, a_url, NodeIndex::B, true).await?,
        })
    }

    async fn close(self) {
        self.a.ledger.pool.close().await;
        self.b.ledger.pool.close().await;
    }
}

/// Runs `case` on two fresh fixture databases, node A's and node B's, both
/// dropped however the case ends.
async fn run_pair<Case, Outcome>(raw: &str, prefix: &str, case: Case) -> Result<()>
where
    Case: FnOnce(String, String) -> Outcome,
    Outcome: Future<Output = Result<()>>,
{
    let a = FixtureDatabase::open(raw, &format!("{prefix}a_")).await?;
    let b = match FixtureDatabase::open(raw, &format!("{prefix}b_")).await {
        Ok(b) => b,
        Err(error) => return Err(a.abandon(error).await),
    };
    let result = case(a.url.clone(), b.url.clone()).await;
    let result = b.close(result).await;
    a.close(result).await
}

/// The chain both nodes follow, in memory: each height's block hash and
/// whether the pool mined it. A case appends blocks as it mines them.
#[derive(Default)]
struct FakeChain {
    blocks: Mutex<Vec<(String, bool)>>,
}

impl FakeChain {
    /// A chain of `length` blocks, the pool's at `pool_heights`.
    fn mined(length: u64, pool_heights: &[u64]) -> Self {
        let chain = Self::default();
        for height in 0..length {
            chain.mine(pool_heights.contains(&height));
        }
        chain
    }

    fn blocks(&self) -> std::sync::MutexGuard<'_, Vec<(String, bool)>> {
        self.blocks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Appends one block, the pool's when `pool`. Returns its height and hash.
    fn mine(&self, pool: bool) -> (u64, String) {
        let mut blocks = self.blocks();
        let height = blocks.len() as u64;
        let hash = format!("{:064x}", 0xb10c_0000_0000_u64 + height);
        blocks.push((hash.clone(), pool));
        (height, hash)
    }

    /// Appends `count` blocks of other miners.
    fn mine_foreign(&self, count: u64) {
        for _ in 0..count {
            self.mine(false);
        }
    }

    fn tip_height(&self) -> u64 {
        self.blocks().len() as u64 - 1
    }

    fn block(&self, height: u64) -> Result<(String, bool)> {
        usize::try_from(height)
            .ok()
            .and_then(|height| self.blocks().get(height).cloned())
            .with_context(|| format!("the chain has no block at height {height}"))
    }

    fn hash(&self, height: u64) -> Result<String> {
        Ok(self.block(height)?.0)
    }
}

impl ChainSource for FakeChain {
    async fn tip(&self) -> Result<(u64, String)> {
        let blocks = self.blocks();
        let (hash, _) = blocks.last().context("the chain has no blocks")?;
        Ok((blocks.len() as u64 - 1, hash.clone()))
    }

    async fn block_hash(&self, height: u64) -> Result<String> {
        self.hash(height)
    }

    async fn coinbase(&self, height: u64) -> Result<CoinbaseView> {
        let (block_hash, pool) = self.block(height)?;
        let tag: &[u8] = if pool {
            TAG.as_bytes()
        } else {
            b"/another pool/"
        };
        Ok(CoinbaseView {
            block_hash,
            // BIP34's height push, then the miner's tag.
            script_sig: [&[0x03][..], &height.to_le_bytes()[..3], tag].concat(),
            output_scripts: vec![[&[0x00, 0x14][..], &[0x5a; 20][..]].concat()],
        })
    }
}

/// The chain as [`FakeChain`] serves it, but with the block at any height
/// another one: the scanned tip was reorganised away after the scan, which
/// reads only the tips and coinbases of a chain that does not move.
struct ScannedTipReorged<'a>(&'a FakeChain);

impl ChainSource for ScannedTipReorged<'_> {
    async fn tip(&self) -> Result<(u64, String)> {
        self.0.tip().await
    }

    async fn block_hash(&self, height: u64) -> Result<String> {
        Ok(format!("{:064x}", 0x5eed_0000_0000_u64 + height))
    }

    async fn coinbase(&self, height: u64) -> Result<CoinbaseView> {
        self.0.coinbase(height).await
    }
}

/// Writes pool block `hash`'s row as a landing or the peer sync writes it,
/// under the node that found it.
async fn insert_block(
    pool: &PgPool,
    hash: &str,
    height: u64,
    origin: i16,
    chain_state: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,origin_node) VALUES($1,$2,repeat('00',32),repeat('ab',32),repeat('ac',32),$3,$4)")
        .bind(hash)
        .bind(i64::try_from(height)?)
        .bind(chain_state)
        .bind(origin)
        .execute(pool)
        .await?;
    Ok(())
}

/// The audit bundle a landing writes beside the block's row.
async fn insert_audit(pool: &PgPool, hash: &str, origin: i16) -> Result<()> {
    sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,origin_node) VALUES($1,'{}'::jsonb,repeat('ad',32),'00',$2)")
        .bind(hash)
        .bind(origin)
        .execute(pool)
        .await?;
    Ok(())
}

/// This node's reconciler confirms a landed block. A plain update, as a
/// writer that predates publication ordinals confirms: the trigger assigns
/// the ordinal.
async fn confirm_block(pool: &PgPool, hash: &str) -> Result<()> {
    let updated =
        sqlx::query("UPDATE qbit_pool_blocks SET chain_state='confirmed' WHERE block_hash=$1")
            .bind(hash)
            .execute(pool)
            .await?
            .rows_affected();
    ensure!(updated == 1, "confirming {hash} updated {updated} rows");
    Ok(())
}

/// The names of the checks `report` failed, in its order.
fn failed(report: &Report) -> Vec<&'static str> {
    report
        .checks
        .iter()
        .filter(|check| !check.ok)
        .map(|check| check.check)
        .collect()
}

/// The detail of `report`'s check `name`.
fn detail<'a>(report: &'a Report, name: &str) -> Result<&'a str> {
    report
        .checks
        .iter()
        .find(|check| check.check == name)
        .map(|check| check.detail.as_str())
        .with_context(|| format!("the report has no {name} check: {:?}", report.checks))
}

/// The pool blocks `transfer`'s chain scan found, each with its landing.
fn scanned(report: &Report) -> Result<Vec<(u64, String, Landing)>> {
    Ok(report
        .scan
        .as_ref()
        .context("the transfer report has no chain scan")?
        .pool_blocks
        .iter()
        .map(|block| (block.height, block.block_hash.clone(), block.landing))
        .collect())
}

/// `carry-owner release` on `node`, whose `PRISM_CARRY_OWNER` is
/// `carry_owner`.
async fn release_on(
    node: &Node,
    carry_owner: bool,
    chain: &FakeChain,
    confirm: bool,
) -> Result<Report> {
    let settings = node.settings(carry_owner);
    transfer::release(&node.ledger, &settings, chain, HANDOVER, confirm).await
}

/// `carry-owner transfer` on `node`, a non-owner by its setting, reading
/// the peer's journal at `peer`.
async fn transfer_on(node: &Node, peer: &str, chain: &FakeChain, confirm: bool) -> Result<Report> {
    transfer::transfer(
        &node.ledger,
        &settings(node.node, false, peer),
        chain,
        &PoolRecognizer::new(TAG, None)?,
        SCAN_FROM,
        RELEASE_DEPTH,
        HANDOVER,
        confirm,
    )
    .await
}

/// A confirmed `transfer` on `node` that only its chain scan refuses,
/// finding `landings` and reporting `unresolved`; it writes nothing.
async fn expect_scan_refusal(
    node: &Node,
    chain: &FakeChain,
    landings: &[(u64, String, Landing)],
    unresolved: &str,
) -> Result<()> {
    let report = transfer_on(node, &node.peer, chain, true).await?;
    ensure!(
        failed(&report) == ["chain_scan"] && report.written.is_none(),
        "a transfer with {unresolved} reported {:?}",
        report.checks
    );
    ensure!(
        scanned(&report)? == landings,
        "with {unresolved} the scan found {:?}",
        scanned(&report)?
    );
    ensure!(
        detail(&report, "chain_scan")?.contains(unresolved),
        "{:?}",
        report.checks
    );
    Ok(())
}

/// How many live reads of the peer's journal the guard counted as `result`.
fn peer_checks(metrics: &Metrics, result: &str) -> Result<f64> {
    let prefix = format!("qbit_prism_carry_owner_peer_checks_total{{result=\"{result}\"}} ");
    match metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
    {
        Some(count) => count
            .parse()
            .with_context(|| format!("peer check count {count:?} is not a number")),
        None => Ok(0.),
    }
}

/// Only a database personalised as this node is seeded, once, from
/// `PRISM_CARRY_OWNER`; later decisions, whatever the setting says by then,
/// read the seed and write nothing. Seeding writes nothing to the peer, and
/// an owner pays only once its peer has a row of its own.
#[tokio::test]
async fn an_identified_node_seeds_its_journal_once_from_its_setting_and_an_unidentified_one_never(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_seed_", seeding).await
}

async fn seeding(a_url: String, b_url: String) -> Result<()> {
    let a = Node::open(&a_url, &b_url, NodeIndex::A, true).await?;
    // Node B's database is not personalised yet.
    let b = Node::open(&b_url, &a_url, NodeIndex::B, false).await?;
    let to_a = peer_journal(&a.url)?;
    let to_b = peer_journal(&b.url)?;

    // No identity: nothing is seeded, whatever the setting says.
    for carry_owner in [false, true] {
        let mut unidentified = b.start(carry_owner);
        unidentified
            .expect(&to_a, carry_free(NodeUnidentified), "unpersonalised")
            .await?;
    }
    expect_journal(&b, &[], "unpersonalised").await?;
    // Personalised as the other node, as when a frontend's database URL
    // names its peer's database: nothing is seeded either.
    let mut misdirected = Process {
        settings: settings(NodeIndex::B, false, &b.url),
        ..a.start(false)
    };
    misdirected
        .expect(&to_b, carry_free(NodeUnidentified), "on node A's database")
        .await?;
    expect_journal(&a, &[], "node B's guard on node A's database").await?;

    // Node A, configured as the owner: its first decision seeds one row
    // from the setting. B answers but has no row: no confirmation.
    let before = clock_ms(a.pool()).await?;
    let mut owner = a.start(true);
    owner
        .expect(&to_b, carry_free(PeerNotSeeded), "first decision")
        .await?;
    let rows = a.journal().await?;
    let [seed] = rows.as_slice() else {
        bail!("node A's first decision left the journal {rows:?}, not one seed");
    };
    ensure!(
        seed.origin_node == 0
            && seed.carry_owner
            && seed.action == "seed"
            && seed.recorded_by == a.ledger.instance_id
            && seed.detail == json!({"setting": "PRISM_CARRY_OWNER"}),
        "node A's seed is {seed:?}"
    );
    ensure!(
        seed.epoch >= before,
        "the seed's epoch {} is below the wall clock {before} ms",
        seed.epoch
    );
    // Later decisions write nothing, even once the setting has changed: a
    // seed that disagrees with the setting is a misconfiguration.
    owner
        .expect(&to_b, carry_free(PeerNotSeeded), "second decision")
        .await?;
    let mut changed = a.start(false);
    changed
        .expect(&to_b, carry_free(ConfigMismatch), "setting changed")
        .await?;
    ensure!(
        seed_role(&a.ledger, a.origin(), false).await?.is_none(),
        "seed_role seeded a journal that has a row"
    );
    expect_journal(&a, &rows, "after later decisions").await?;

    // Personalised, node B seeds itself as configured: a non-owner, which
    // needs no confirmation.
    b.personalise().await?;
    let mut non_owner = b.start(false);
    for when in ["node B's first decision", "node B's second decision"] {
        non_owner.expect(&to_a, carry_free(NotOwner), when).await?;
    }
    let b_rows = b.journal().await?;
    let [b_seed] = b_rows.as_slice() else {
        bail!("node B's decisions left the journal {b_rows:?}, not one seed");
    };
    ensure!(
        b_seed.origin_node == 1
            && !b_seed.carry_owner
            && b_seed.action == "seed"
            && b_seed.recorded_by == b.ledger.instance_id,
        "node B's seed is {b_seed:?}"
    );
    expect_journal(&a, &rows, "after node B's seed").await?;
    // B seeded and not claiming: A pays.
    owner.expect(&to_b, PAYING, "once B is seeded").await?;
    expect_journal(&a, &rows, "once node A pays").await?;
    expect_journal(&b, &b_rows, "once node A pays").await?;
    Pair { a, b }.close().await;
    Ok(())
}

/// The owner start rule: an owner pays only after one live read since its
/// process started has shown the peer seeded and not claiming, and from then
/// on keeps paying while the peer is unreachable, since the peer can only
/// acquire after reading the owner's release. A non-owner never reads it.
#[tokio::test]
async fn an_owner_pays_after_one_live_peer_read_and_keeps_paying_while_the_peer_is_unreachable(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_start_", owner_start).await
}

async fn owner_start(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_a = peer_journal(&a.url)?;
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    let metrics = Metrics::default();
    let b_seed = b.seed(false).await?;

    // B answers, seeded and not claiming: A pays from its first decision.
    let mut first = Process {
        metrics: Some(&metrics),
        ..a.start(true)
    };
    first.expect(&to_b, PAYING, "B answering").await?;
    ensure!(
        first.guard.peer_confirmed(),
        "a paying owner has no peer confirmation"
    );

    // A process that starts while B is unreachable cannot know its journal
    // is current: it waits, however often it decides.
    let mut restarted = Process {
        metrics: Some(&metrics),
        ..a.start(true)
    };
    for _ in 0..2 {
        restarted
            .expect(&unreachable, carry_free(PeerUnconfirmed), "restarted")
            .await?;
        ensure!(
            !restarted.guard.peer_confirmed(),
            "an unreachable peer confirmed"
        );
    }
    // One live read confirms it, and it keeps paying while B is unreachable.
    restarted.expect(&to_b, PAYING, "first live read").await?;
    for _ in 0..3 {
        restarted
            .expect(&unreachable, PAYING, "confirmed, B unreachable")
            .await?;
    }
    let counted = (
        peer_checks(&metrics, "answered")?,
        peer_checks(&metrics, "failed")?,
    );
    ensure!(
        counted == (2., 5.),
        "the guard counted {counted:?} answered and failed peer reads, not (2, 5)"
    );

    // The non-owner never needs its peer, so it does not read it, reachable
    // or not.
    let mut non_owner = Process {
        metrics: Some(&metrics),
        ..b.start(false)
    };
    for peer in [&unreachable, &to_a] {
        non_owner
            .expect(peer, carry_free(NotOwner), "the non-owner")
            .await?;
    }
    let after = (
        peer_checks(&metrics, "answered")?,
        peer_checks(&metrics, "failed")?,
    );
    ensure!(
        after == counted,
        "the non-owner's decisions counted peer reads: {counted:?} became {after:?}"
    );

    // Deciding wrote nothing but A's seed.
    let rows = a.journal().await?;
    ensure!(
        matches!(rows.as_slice(), [seed] if seed.carry_owner && seed.action == "seed"),
        "node A's journal is {rows:?}, not its seed"
    );
    expect_journal(b, &[b_seed], "after the decisions").await?;
    pair.close().await;
    Ok(())
}

/// Two nodes seeded as owners (`PRISM_CARRY_OWNER` set on both) both build
/// carry-free work. With the peer unreachable a claim still counts once the
/// peer sync has copied it, for a process that never read the peer too.
#[tokio::test]
async fn two_nodes_seeded_as_owners_are_both_carry_free_also_through_the_synced_copy() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_both_", both_owners).await
}

async fn both_owners(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_a = peer_journal(&a.url)?;
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    let mut a_owner = a.start(true);
    a_owner
        .expect(&to_b, carry_free(PeerNotSeeded), "before B started")
        .await?;
    // B starts configured as the owner too: it seeds a claim and sees A's.
    let mut b_owner = b.start(true);
    b_owner
        .expect(&to_a, carry_free(PeerClaimsOwnership), "the second owner")
        .await?;
    let b_rows = b.journal().await?;
    let [b_seed] = b_rows.as_slice() else {
        bail!("node B's journal is {b_rows:?}, not one seed");
    };
    ensure!(
        b_seed.carry_owner && b_seed.action == "seed",
        "node B's seed is {b_seed:?}"
    );
    a_owner
        .expect(&to_b, carry_free(PeerClaimsOwnership), "two owners")
        .await?;
    ensure!(
        !a_owner.guard.peer_confirmed() && !b_owner.guard.peer_confirmed(),
        "a guard beside a second owner kept a confirmation"
    );

    // B unreachable, with no copy of its claim here: A waits for a live read.
    a_owner
        .expect(&unreachable, carry_free(PeerUnconfirmed), "uncopied claim")
        .await?;
    // The peer sync copies B's seed into A's journal: the claim counts
    // without a live read, for a restarted process too, and alerts.
    copy_row(a.pool(), b_seed).await?;
    let mut restarted = a.start(true);
    for process in [&mut a_owner, &mut restarted] {
        process
            .expect(
                &unreachable,
                carry_free(PeerClaimsOwnership),
                "copied claim",
            )
            .await?;
    }
    let status =
        transfer::status(&a.ledger, &settings(NodeIndex::A, true, UNREACHABLE_PEER)).await?;
    ensure!(
        status.decision == carry_free(PeerClaimsOwnership) && status.decision.alerts(),
        "carry-owner status decided {:?}",
        status.decision
    );
    ensure!(
        status.local.peer == Some(b_seed.role()) && status.peer.is_none(),
        "carry-owner status read {:?} locally and {:?} live",
        status.local,
        status.peer
    );
    pair.close().await;
    Ok(())
}

/// A paying owner whose peer becomes unreachable stops paying as soon as the
/// copy of the peer's journal the sync delivered shows a claim, and stays
/// carry-free while the claim stands.
#[tokio::test]
async fn a_claim_in_the_synced_copy_stops_a_paying_owner_while_the_peer_is_unreachable(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_synced_", synced_claim).await
}

async fn synced_claim(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    let b_seed = b.seed(false).await?;
    let mut owner = a.start(true);
    owner.expect(&to_b, PAYING, "B answering").await?;
    owner
        .expect(&unreachable, PAYING, "B unreachable since")
        .await?;

    // B claims ownership, the peer sync copies the claim into A's journal,
    // and B is unreachable again.
    let claim = b
        .append(true, "acquire", json!({"reason": "a second owner"}))
        .await?;
    ensure!(
        claim.epoch > b_seed.epoch,
        "node B's claim {claim:?} is not newer than its seed {b_seed:?}"
    );
    copy_row(a.pool(), &claim).await?;
    owner
        .expect(
            &unreachable,
            carry_free(PeerClaimsOwnership),
            "copied claim",
        )
        .await?;
    ensure!(
        !owner.guard.peer_confirmed(),
        "the copied claim left node A's confirmation"
    );
    for (peer, when) in [
        (&unreachable, "B still unreachable"),
        (&to_b, "B answering"),
    ] {
        owner
            .expect(peer, carry_free(PeerClaimsOwnership), when)
            .await?;
    }
    ensure!(
        a.latest().await?.peer == Some(claim.role()),
        "node A's journal does not show B's claim as B's latest row"
    );
    pair.close().await;
    Ok(())
}

/// An owner whose own journal is older than the peer's copy of it, as after
/// a restore from a backup taken before its release, is carry-free and
/// alerts, and loses the confirmation it had. The guard never repairs the
/// journal; once the lost row is recovered the guard follows it.
#[tokio::test]
async fn an_owner_whose_journal_is_behind_the_peers_copy_of_it_is_carry_free() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_behind_", rolled_back).await
}

async fn rolled_back(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    let a_seed = a.seed(true).await?;
    let b_seed = b.seed(false).await?;
    // The peer sync has copied A's seed into B's journal: the copy is current.
    copy_row(b.pool(), &a_seed).await?;
    let mut owner = a.start(true);
    owner.expect(&to_b, PAYING, "a current copy at B").await?;

    // A released, B copied the release, and A's database was then restored
    // from a backup that predates it: A's own journal still shows its claim.
    let lost = JournalRow {
        origin_node: a.origin(),
        epoch: a_seed.epoch + 1,
        carry_owner: false,
        action: "release".into(),
        recorded_by: "carry-owner release".into(),
        detail: json!({"reason": "lost in a restore", "tip_height": 100}),
    };
    copy_row(b.pool(), &lost).await?;
    owner
        .expect(&to_b, carry_free(OwnJournalBehindPeer), "behind B's copy")
        .await?;
    ensure!(
        !owner.guard.peer_confirmed(),
        "node A kept its confirmation with a stale journal"
    );
    // B unreachable now: without its confirmation A waits rather than paying
    // on a journal it knows to be stale.
    owner
        .expect(&unreachable, carry_free(PeerUnconfirmed), "B unreachable")
        .await?;
    let mut restarted = a.start(true);
    restarted
        .expect(&to_b, carry_free(OwnJournalBehindPeer), "restarted")
        .await?;
    let status = transfer::status(&a.ledger, &a.settings(true)).await?;
    ensure!(
        status.decision == carry_free(OwnJournalBehindPeer) && status.decision.alerts(),
        "carry-owner status decided {:?}",
        status.decision
    );
    let local = LatestRoles {
        own: Some(a_seed.role()),
        peer: None,
    };
    let at_peer = LatestRoles {
        own: Some(lost.role()),
        peer: Some(b_seed.role()),
    };
    ensure!(
        status.local == local && status.peer.as_ref() == Some(&at_peer),
        "carry-owner status read {:?} locally and {:?} at the peer",
        status.local,
        status.peer
    );
    expect_journal(
        a,
        std::slice::from_ref(&a_seed),
        "after the guard's decisions",
    )
    .await?;

    // The own-log recovery copies the lost release back: the guard follows
    // the journal again, a release waiting for its setting.
    copy_row(a.pool(), &lost).await?;
    owner
        .expect(&to_b, carry_free(SettingPending), "release recovered")
        .await?;
    let mut following = a.start(false);
    following
        .expect(&to_b, carry_free(NotOwner), "setting follows")
        .await?;
    pair.close().await;
    Ok(())
}

/// `carry-owner release` on the owner journals a `release` row recording the
/// chain's tip and bumps the payout revision exactly once, in one
/// transaction; the guard then builds carry-free work. Without `confirm` it
/// writes nothing, and a node that does not hold ownership is refused.
#[tokio::test]
async fn release_journals_the_tip_with_one_revision_bump_and_is_refused_without_ownership(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_release_", release_case).await
}

async fn release_case(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_b = peer_journal(&b.url)?;
    let chain = FakeChain::mined(120, &[]);
    let tip = chain.tip_height();
    let a_seed = a.seed(true).await?;
    let b_seed = b.seed(false).await?;
    let mut owner = a.start(true);
    owner.expect(&to_b, PAYING, "before the release").await?;
    let r0 = a.revision().await?;

    // Without confirm: the checks pass and nothing is written.
    let dry = release_on(a, true, &chain, false).await?;
    let names: Vec<_> = dry.checks.iter().map(|check| check.check).collect();
    ensure!(
        dry.command == "release"
            && dry.passed()
            && names == ["node_identity", "this_node_owner"]
            && dry.decision == PAYING
            && dry.written.is_none(),
        "the unconfirmed release reported {dry:?}"
    );
    expect_journal(
        a,
        std::slice::from_ref(&a_seed),
        "after an unconfirmed release",
    )
    .await?;
    ensure!(
        a.revision().await? == r0,
        "an unconfirmed release bumped the revision"
    );

    // Confirmed: one release row at the chain's tip, and one revision bump.
    let report = release_on(a, true, &chain, true).await?;
    ensure!(
        report.passed(),
        "the release was refused: {:?}",
        report.checks
    );
    let rows = a.journal().await?;
    let [seed, released] = rows.as_slice() else {
        bail!("after the release node A's journal is {rows:?}");
    };
    ensure!(
        *seed == a_seed
            && released.origin_node == 0
            && !released.carry_owner
            && released.action == "release"
            && released.recorded_by == "carry-owner release"
            && released.epoch > a_seed.epoch,
        "the release row is {released:?}"
    );
    ensure!(
        released.detail == json!({"reason": HANDOVER, "tip_height": tip}),
        "the release recorded {}, not the tip {tip}",
        released.detail
    );
    let r1 = a.revision().await?;
    ensure!(
        r1 == r0 + 1,
        "the release moved the payout revision from {r0} to {r1}, not by one"
    );
    let written = json!({
        "origin_node": 0,
        "epoch": released.epoch,
        "carry_owner": false,
        "action": "release",
        "detail": released.detail,
        "payout_revision": r1,
    });
    ensure!(
        report.written.as_ref() == Some(&written),
        "the release reported writing {:?}, not {written}",
        report.written
    );
    let own = a.latest().await?.own.context("node A has no own row")?;
    ensure!(
        own.tip_height() == Some(tip),
        "the guard reads the release's tip as {:?}, not {tip}",
        own.tip_height()
    );

    // The journal says released while the setting still says owner:
    // carry-free until the setting follows, then a non-owner.
    owner
        .expect(&to_b, carry_free(SettingPending), "released")
        .await?;
    let mut restarted = a.start(true);
    restarted
        .expect(&to_b, carry_free(SettingPending), "released, restarted")
        .await?;
    let mut following = a.start(false);
    following
        .expect(&to_b, carry_free(NotOwner), "setting follows")
        .await?;

    // A no longer holds ownership: a second release is refused.
    let again = release_on(a, true, &chain, true).await?;
    ensure!(
        failed(&again) == ["this_node_owner"] && again.written.is_none(),
        "a second release reported {again:?}"
    );
    expect_journal(a, &rows, "after a refused release").await?;
    ensure!(
        a.revision().await? == r1,
        "a refused release bumped the revision"
    );
    // Nor does the non-owner.
    let rb = b.revision().await?;
    let refused = release_on(b, false, &chain, true).await?;
    ensure!(
        failed(&refused) == ["this_node_owner"] && refused.written.is_none(),
        "a release on the non-owner reported {refused:?}"
    );
    expect_journal(b, &[b_seed], "after a release on the non-owner").await?;
    ensure!(
        b.revision().await? == rb,
        "a refused release on the non-owner bumped its revision"
    );
    pair.close().await;
    Ok(())
}

/// `carry-owner transfer` on the non-owner refuses while the peer is
/// unreachable, while it holds ownership, until its release is buried by
/// the orphan depth, and while a pool block on the active chain is not
/// landed with its audit and confirmed here. Once every check passes it
/// journals an `acquire`, and the guards follow the journals.
#[tokio::test]
async fn transfer_acquires_only_once_the_peer_released_deep_enough_and_every_pool_block_landed(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_transfer_", transfer_case).await
}

async fn transfer_case(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    // Heights 0 to 99. Node B found the pool block at 50, which its ledger
    // landed and confirmed long ago.
    let chain = FakeChain::mined(100, &[50]);
    let early = chain.hash(50)?;
    insert_block(b.pool(), &early, 50, b.origin(), "confirmed").await?;
    insert_audit(b.pool(), &early, b.origin()).await?;
    a.seed(true).await?;
    let b_seed = b.seed(false).await?;
    let untouched = [b_seed.clone()];

    // The peer must answer.
    let report = transfer_on(b, UNREACHABLE_PEER, &chain, true).await?;
    ensure!(
        failed(&report) == ["peer_answered"] && report.peer.is_none() && report.written.is_none(),
        "a transfer with the peer unreachable reported {:?}",
        report.checks
    );
    ensure!(
        scanned(&report)? == [(50, early.clone(), Landing::Confirmed)],
        "the scan found {:?}",
        scanned(&report)?
    );
    expect_journal(b, &untouched, "the peer unreachable").await?;
    // Over PRISM_PEER_DATABASE_URL only: with that path down, a fallback
    // that answers, which may be a copy lagging the peer's journal, is not
    // read (here it would show node A's claim).
    let report = transfer::transfer(
        &b.ledger,
        &CarryOwnerSettings {
            peer_urls: vec![UNREACHABLE_PEER.to_owned(), a.url.clone()],
            ..settings(b.node, false, &a.url)
        },
        &chain,
        &PoolRecognizer::new(TAG, None)?,
        SCAN_FROM,
        RELEASE_DEPTH,
        HANDOVER,
        true,
    )
    .await?;
    ensure!(
        failed(&report) == ["peer_answered"] && report.peer.is_none() && report.written.is_none(),
        "a transfer with only the fallback path answering reported {:?}",
        report.checks
    );
    expect_journal(b, &untouched, "only the fallback path answering").await?;

    // The peer must not hold ownership.
    let report = transfer_on(b, &a.url, &chain, true).await?;
    ensure!(
        failed(&report) == ["peer_not_owner"] && report.written.is_none(),
        "a transfer from the owner reported {:?}",
        report.checks
    );
    ensure!(
        detail(&report, "peer_not_owner")?.contains("run carry-owner release on it first"),
        "{:?}",
        report.checks
    );
    expect_journal(b, &untouched, "the peer the owner").await?;

    // A finds its last carry-paying block at 100, B's peer sync lands it,
    // and A releases at that tip.
    let (paid_height, paid) = chain.mine(true);
    insert_block(b.pool(), &paid, paid_height, a.origin(), "confirmed").await?;
    insert_audit(b.pool(), &paid, a.origin()).await?;
    let released = release_on(a, true, &chain, true).await?;
    ensure!(
        released.passed() && released.written.is_some(),
        "node A's release reported {:?}",
        released.checks
    );
    let release = a.latest().await?.own.context("node A has no own row")?;
    ensure!(
        release.action == "release" && release.tip_height() == Some(paid_height),
        "node A's release is {release:?}"
    );

    // Three blocks on, the release is not buried deep enough.
    chain.mine_foreign(3);
    let report = transfer_on(b, &a.url, &chain, true).await?;
    ensure!(
        failed(&report) == ["release_depth"] && report.written.is_none(),
        "a transfer 3 blocks past the release reported {:?}",
        report.checks
    );
    let due = paid_height + RELEASE_DEPTH;
    let shallow = format!(
        "released at height {paid_height}; the tip is {} and must reach {due}",
        paid_height + 3
    );
    ensure!(
        detail(&report, "release_depth")?.contains(&shallow),
        "{:?}",
        report.checks
    );
    expect_journal(b, &untouched, "a shallow release").await?;

    // A finds a carry-free block at 104 and the tip reaches the release
    // depth, but B has not landed the new block.
    let (free_height, free) = chain.mine(true);
    chain.mine_foreign(2);
    ensure!(
        chain.tip_height() == due,
        "the tip is not at the release depth"
    );
    let missing = format!("{free} at {free_height} (no landing rows)");
    let mut landings = vec![
        (50, early.clone(), Landing::Confirmed),
        (paid_height, paid.clone(), Landing::Confirmed),
        (free_height, free.clone(), Landing::Missing),
    ];
    expect_scan_refusal(b, &chain, &landings, &missing).await?;
    // A block row without its audit is no landing either.
    insert_block(b.pool(), &free, free_height, a.origin(), "prepared").await?;
    expect_scan_refusal(b, &chain, &landings, &missing).await?;
    // With its audit it is landed, but B's reconciler has not confirmed it.
    insert_audit(b.pool(), &free, a.origin()).await?;
    landings[2].2 = Landing::Unconfirmed;
    let unconfirmed = format!("{free} at {free_height} (landed, not confirmed)");
    expect_scan_refusal(b, &chain, &landings, &unconfirmed).await?;
    expect_journal(b, &untouched, "a pool block not landed and confirmed").await?;

    // B's reconciler confirms it: every check passes, and without confirm
    // nothing is written.
    confirm_block(b.pool(), &free).await?;
    landings[2].2 = Landing::Confirmed;
    let dry = transfer_on(b, &a.url, &chain, false).await?;
    ensure!(
        dry.passed() && dry.written.is_none(),
        "the unconfirmed transfer reported {:?}",
        dry.checks
    );
    ensure!(scanned(&dry)? == landings, "{:?}", scanned(&dry)?);
    expect_journal(b, &untouched, "an unconfirmed transfer").await?;

    // The scanned tip is reorganised away before the acquire: refused under
    // the lock, nothing acquired. A's release, copied just before, stands.
    let reorged = transfer::transfer(
        &b.ledger,
        &b.settings(false),
        &ScannedTipReorged(&chain),
        &PoolRecognizer::new(TAG, None)?,
        SCAN_FROM,
        RELEASE_DEPTH,
        HANDOVER,
        true,
    )
    .await;
    ensure!(
        reorged.as_ref().is_err_and(|error| error
            .to_string()
            .contains("is no longer on the active chain")),
        "a transfer whose scanned tip was reorganised away returned {reorged:?}"
    );
    let held = b.journal().await?;
    ensure!(
        held.len() == 2 && held.contains(&b_seed) && held.iter().all(|row| row.action != "acquire"),
        "after a reorganised scan node B's journal is {held:?}"
    );

    // Confirmed: B journals its acquire.
    let report = transfer_on(b, &a.url, &chain, true).await?;
    ensure!(
        report.passed(),
        "the transfer was refused: {:?}",
        report.checks
    );
    let rows = b.journal().await?;
    let [copied, seed, acquired] = rows.as_slice() else {
        bail!("after the transfer node B's journal is {rows:?}");
    };
    // A's release, copied whole with the acquire (D1's journal tail): a node
    // A rebuilt from B's database gets its release back.
    ensure!(
        copied.role() == release
            && row_json(b.pool(), a.origin(), release.epoch).await?
                == row_json(a.pool(), a.origin(), release.epoch).await?,
        "node B's copy of node A's release is {copied:?}"
    );
    ensure!(
        *seed == b_seed
            && acquired.origin_node == 1
            && acquired.carry_owner
            && acquired.action == "acquire"
            && acquired.recorded_by == "carry-owner transfer"
            && acquired.epoch > b_seed.epoch,
        "the acquire row is {acquired:?}"
    );
    let recorded = json!({
        "reason": HANDOVER,
        "tip_height": due,
        "peer_epoch": release.epoch,
        "scan_from_height": SCAN_FROM,
        "pool_blocks": 3,
    });
    ensure!(
        acquired.detail == recorded,
        "the acquire recorded {}, not {recorded}",
        acquired.detail
    );
    let written = json!({
        "origin_node": 1,
        "epoch": acquired.epoch,
        "carry_owner": true,
        "action": "acquire",
        "detail": recorded,
        // Copied by the reorganised attempt above.
        "peer_row": {"epoch": release.epoch, "copied": false},
    });
    ensure!(
        report.written.as_ref() == Some(&written),
        "the transfer reported writing {:?}, not {written}",
        report.written
    );

    // B's guard: acquired, but carry-free until its setting follows; then it
    // pays, A's latest row being its release. A, its setting following its
    // release, is the non-owner.
    let to_a = peer_journal(&a.url)?;
    let mut pending = b.start(false);
    pending
        .expect(&to_a, carry_free(SettingPending), "acquired")
        .await?;
    let mut new_owner = b.start(true);
    new_owner.expect(&to_a, PAYING, "setting follows").await?;
    let mut old_owner = a.start(false);
    old_owner
        .expect(&peer_journal(&b.url)?, carry_free(NotOwner), "released")
        .await?;

    // B holds ownership now: another transfer is refused.
    let again = transfer_on(b, &a.url, &chain, true).await?;
    ensure!(
        failed(&again) == ["this_node_not_owner"] && again.written.is_none(),
        "a transfer to the owner reported {:?}",
        again.checks
    );
    expect_journal(b, &rows, "a transfer to the owner").await?;
    pair.close().await;
    Ok(())
}

/// The guard's decisions move the dual-writer carry gate the way its loop
/// applies them: the gate opens on the first paying decision and closes on
/// the first carry-free one, each change fenced by exactly one payout
/// revision bump, and a repeated decision moves and bumps nothing.
#[tokio::test]
async fn each_guard_decision_that_moves_the_carry_gate_is_fenced_by_one_revision_bump() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_gate_", gate_fencing).await
}

async fn gate_fencing(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    a.ledger.set_dual_writer_identity(NodeIdentity {
        node: NodeIndex::A,
        carry_owner: true,
    })?;
    ensure!(
        a.ledger.dual_writer() && a.ledger.own_node() == Some(0),
        "node A's ledger is not in dual-writer mode as node A"
    );
    ensure!(
        !a.ledger.carry_paying() && !a.ledger.carry_fence_pending(),
        "the dual-writer gate must start closed with nothing to fence"
    );
    a.seed(true).await?;
    b.seed(false).await?;
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    let r0 = a.revision().await?;
    let mut owner = a.start(true);

    // Started with B unreachable: carry-free, and the closed gate stays
    // closed.
    let waiting = owner.turn(&unreachable).await?;
    ensure!(
        waiting == (carry_free(PeerUnconfirmed), false) && a.revision().await? == r0,
        "a waiting owner's turn was {waiting:?}"
    );
    // The first live read: paying, and the gate opens with one bump.
    let opened = owner.turn(&to_b).await?;
    ensure!(
        opened == (PAYING, true),
        "the first live read's turn was {opened:?}"
    );
    ensure!(
        a.revision().await? == r0 + 1,
        "opening the gate moved the revision from {r0} to {}, not by one",
        a.revision().await?
    );
    // Later turns, B answering or not, keep it open and bump nothing.
    for peer in [&to_b, &unreachable, &to_b] {
        let kept = owner.turn(peer).await?;
        ensure!(kept == (PAYING, false), "a paying turn was {kept:?}");
    }
    ensure!(
        a.revision().await? == r0 + 1,
        "a repeated paying decision bumped the revision"
    );

    // B claims ownership as well: A's next turn is carry-free, and the gate
    // closes with one more bump.
    b.append(true, "acquire", json!({"reason": "a second owner"}))
        .await?;
    let closed = owner.turn(&to_b).await?;
    ensure!(
        closed == (carry_free(PeerClaimsOwnership), true),
        "the turn after B's claim was {closed:?}"
    );
    ensure!(
        a.revision().await? == r0 + 2,
        "closing the gate moved the revision from {} to {}, not by one",
        r0 + 1,
        a.revision().await?
    );
    // Carry-free turns after it move and bump nothing.
    for (peer, expected) in [
        (&unreachable, carry_free(PeerUnconfirmed)),
        (&to_b, carry_free(PeerClaimsOwnership)),
    ] {
        let kept = owner.turn(peer).await?;
        ensure!(kept == (expected, false), "a carry-free turn was {kept:?}");
    }
    ensure!(
        a.revision().await? == r0 + 2 && a.ledger.fence_carry_change().await?.is_none(),
        "a repeated carry-free decision bumped the revision or left a fence"
    );
    pair.close().await;
    Ok(())
}

/// A journal rolled back under a running guard, in a way the peer sync's
/// lineage check cannot see (a filesystem snapshot that keeps the WAL
/// timeline), may have lost a release the peer acted on: its latest epoch
/// falls below one the guard already read, so the guard is carry-free and
/// alerts, whether or not the peer answers, and never seeds an emptied
/// journal again. Once the lost rows are recovered it follows them.
#[tokio::test]
async fn a_journal_rolled_back_under_a_running_guard_is_carry_free_until_its_rows_return(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_rollback_", rolled_back_under_the_guard).await
}

async fn rolled_back_under_the_guard(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    let a_seed = a.seed(true).await?;
    b.seed(false).await?;
    let mut owner = a.start(true);
    owner.expect(&to_b, PAYING, "before the release").await?;
    let release = a
        .append(
            false,
            "release",
            json!({"reason": HANDOVER, "tip_height": 100}),
        )
        .await?;
    owner
        .expect(&unreachable, carry_free(SettingPending), "released")
        .await?;

    // Rolled back to before the release while B is unreachable: the old
    // claim is A's latest row again.
    roll_back(a.pool(), a.origin(), release.epoch).await?;
    for (peer, when) in [(&unreachable, "B unreachable"), (&to_b, "B answering")] {
        owner
            .expect(peer, carry_free(OwnJournalRolledBack), when)
            .await?;
        ensure!(
            !owner.guard.peer_confirmed(),
            "{when}: a rolled-back guard kept its confirmation"
        );
    }
    let alert = CarryDecision::CarryFree(OwnJournalRolledBack);
    ensure!(alert.alerts(), "a rollback does not alert");
    // Rolled back past the seed as well: the guard does not seed the empty
    // journal again, though B answers and holds no row of A's.
    roll_back(a.pool(), a.origin(), a_seed.epoch).await?;
    owner
        .expect(&to_b, carry_free(OwnJournalRolledBack), "emptied")
        .await?;
    expect_journal(a, &[], "an emptied journal").await?;

    // Own-log recovery copies the rows back: the guard follows the journal.
    copy_row(a.pool(), &a_seed).await?;
    copy_row(a.pool(), &release).await?;
    owner
        .expect(&to_b, carry_free(SettingPending), "rows recovered")
        .await?;
    pair.close().await;
    Ok(())
}

/// A node seeds its first journal row only once its own log is caught up
/// (D-8) and a live read shows the peer holds no row of this node: one
/// restored from a backup older than its first row, whose rows the peer
/// still holds, waits for own-log recovery instead of claiming anew.
#[tokio::test]
async fn a_node_seeds_only_with_its_own_log_caught_up_and_none_of_its_rows_at_the_peer(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_reseed_", seeding_guarded).await
}

async fn seeding_guarded(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    b.seed(false).await?;
    let mut waiting = Process {
        caught_up: false,
        ..a.start(true)
    };
    waiting
        .expect(&to_b, carry_free(NoJournalRow), "own log not caught up")
        .await?;
    let mut alone = a.start(true);
    alone
        .expect(&unreachable, carry_free(NoJournalRow), "B unreachable")
        .await?;
    expect_journal(a, &[], "no seed yet").await?;

    // A's database was restored from a backup older than its first row:
    // B still holds A's seed and its later release, which the sync copied.
    let at = clock_ms(a.pool()).await? - 60_000;
    let old_seed = JournalRow {
        origin_node: a.origin(),
        epoch: at,
        carry_owner: true,
        action: "seed".into(),
        recorded_by: RECORDED_BY.into(),
        detail: json!({"setting": "PRISM_CARRY_OWNER"}),
    };
    let old_release = JournalRow {
        epoch: at + 1,
        carry_owner: false,
        action: "release".into(),
        detail: json!({"reason": HANDOVER, "tip_height": 100}),
        ..old_seed.clone()
    };
    copy_row(b.pool(), &old_seed).await?;
    copy_row(b.pool(), &old_release).await?;
    let mut restored = a.start(true);
    restored
        .expect(
            &to_b,
            carry_free(NoJournalRow),
            "restored, B holding its rows",
        )
        .await?;
    expect_journal(a, &[], "restored before own-log recovery").await?;
    // Own-log recovery brings them back: a release waiting for its setting.
    copy_row(a.pool(), &old_seed).await?;
    copy_row(a.pool(), &old_release).await?;
    restored
        .expect(&to_b, carry_free(SettingPending), "rows recovered")
        .await?;
    pair.close().await;
    Ok(())
}

/// Two nodes seeded as owners: once the operator releases one, the other's
/// seed is still no vetted claim, so it stays carry-free and alerts, until
/// `release` and then `transfer` on it, which waits for the peer's release
/// to be buried and scans the chain, write an `acquire` made after that
/// release; then it pays. The transfer copies the peer's release into its
/// journal.
#[tokio::test]
async fn after_two_claims_only_a_claim_transferred_after_the_peers_release_pays() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_vetted_", vetted_claim).await
}

async fn vetted_claim(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_a = peer_journal(&a.url)?;
    let to_b = peer_journal(&b.url)?;
    let chain = FakeChain::mined(100, &[]);
    a.seed(true).await?;
    b.seed(true).await?;
    let mut a_owner = a.start(true);
    a_owner
        .expect(&to_b, carry_free(PeerClaimsOwnership), "two owners")
        .await?;

    // The operator releases B.
    let released = release_on(b, true, &chain, true).await?;
    ensure!(
        released.passed() && released.written.is_some(),
        "node B's release reported {:?}",
        released.checks
    );
    let b_release = b.latest().await?.own.context("node B has no own row")?;
    let decision = a_owner.decide(&to_b).await?;
    ensure!(
        decision == carry_free(ClaimNotVetted) && decision.alerts(),
        "node A's seed facing B's release decided {decision:?}"
    );
    let mut restarted = a.start(true);
    restarted
        .expect(&to_b, carry_free(ClaimNotVetted), "restarted")
        .await?;

    // `release` then `transfer` on A, once B's release is buried.
    let a_released = release_on(a, true, &chain, true).await?;
    ensure!(
        a_released.passed() && a_released.written.is_some(),
        "node A's release reported {:?}",
        a_released.checks
    );
    chain.mine_foreign(RELEASE_DEPTH);
    let report = transfer_on(a, &b.url, &chain, true).await?;
    ensure!(
        report.passed() && report.written.is_some(),
        "node A's transfer reported {:?}",
        report.checks
    );
    let acquired = a.latest().await?.own.context("node A has no own row")?;
    ensure!(
        acquired.action == "acquire" && acquired.acquired_after(&b_release),
        "node A's acquire {acquired:?} was not made after B's release {b_release:?}"
    );
    ensure!(
        row_json(a.pool(), b.origin(), b_release.epoch).await?
            == row_json(b.pool(), b.origin(), b_release.epoch).await?,
        "node A's journal does not hold B's release as B wrote it"
    );
    let mut transferred = a.start(true);
    transferred
        .expect(&to_b, PAYING, "transferred after B's release")
        .await?;
    let mut b_following = b.start(false);
    b_following
        .expect(&to_a, carry_free(NotOwner), "B's setting follows")
        .await?;
    pair.close().await;
    Ok(())
}

/// The broadcaster's view of a finder's liveness: how long ago a node
/// published its newest work, read live from its own database and aged by
/// that database's clock, or nothing from an unreachable one; and the same
/// from the copy another node holds.
#[tokio::test]
async fn a_nodes_newest_work_is_aged_by_its_own_database_clock() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run_pair(&raw, "guard_work_age_", work_age).await
}

async fn work_age(a_url: String, b_url: String) -> Result<()> {
    let pair = Pair::open(&a_url, &b_url).await?;
    let (a, b) = (&pair.a, &pair.b);
    let to_b = peer_journal(&b.url)?;
    let unreachable = peer_journal(UNREACHABLE_PEER)?;
    ensure!(
        to_b.work_age(b.origin()).await == Some(None),
        "node B's database holds work of B's before any was published"
    );
    ensure!(
        unreachable.work_age(b.origin()).await.is_none(),
        "an unreachable peer answered"
    );
    sqlx::query(
        "INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,origin_node) \
         VALUES('prepared:work-age','carry owner guard test',repeat('ab',32),0,'{}'::jsonb,clock_timestamp()+interval '1 hour',$1)",
    )
    .bind(b.origin())
    .execute(b.pool())
    .await?;
    let fresh = to_b
        .work_age(b.origin())
        .await
        .flatten()
        .context("node B's newest work was not read")?;
    ensure!(
        fresh < Duration::from_secs(60),
        "work published just now is {fresh:?} old"
    );
    sqlx::query("UPDATE qbit_prism_jobs SET created_at=clock_timestamp()-interval '31 minutes' WHERE job_id='prepared:work-age'")
        .execute(b.pool())
        .await?;
    let old = to_b
        .work_age(b.origin())
        .await
        .flatten()
        .context("node B's newest work was not read")?;
    ensure!(
        old >= SPONSOR_TAKEOVER_AFTER,
        "work published 31 minutes ago is {old:?} old"
    );
    // Node A holds no copy of B's work yet; its own database says so.
    ensure!(
        a.ledger.work_age(b.origin()).await?.is_none()
            && b.ledger.work_age(b.origin()).await? >= Some(SPONSOR_TAKEOVER_AFTER),
        "the local reads of node B's newest work disagree with the live one"
    );
    pair.close().await;
    Ok(())
}
