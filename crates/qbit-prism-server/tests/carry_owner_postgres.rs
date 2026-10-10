//! PRISM 3.1 dual writer, against a real PostgreSQL: the carry gate work
//! reads its prior balances through, and a peer's settlement as this node's
//! derived state.
//!
//! Every case starts from canonical balances a 3.0 settlement left: a block
//! this node confirmed at height 100 that owes miner-a 1,000 sats and miner-b
//! 500, and leaves miner-c 300 sats in debt. Work reads its prior balances
//! through the carry gate (`ledger/carry.rs`). Single-writer mode reads the
//! canonical balances exactly as 3.0 did. A dual-writer node builds
//! carry-free work, on no prior balances, until its owner guard opens the
//! gate, and each change of the gate is fenced by one payout revision bump.
//!
//! A peer's carry-free block arrives the way peer sync writes it: the block's
//! immutable columns, its carry rows and its payout entries in one
//! transaction, with no chain state of its own. Only this node's reconciler
//! makes it count, retracts it on a reorg, matures it, and halts the cluster
//! when it disconnects once mature. Confirming carry-free rows in dual mode
//! records no payout divergence; single-writer mode keeps 3.0's record.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test carry_owner_postgres
//! ```
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::{prior_balances_digest, CarryForwardBalance, WindowCut};
use qbit_prism_server::node_identity::{NodeIdentity, NodeIndex};
use qbit_prism_server::{
    carry_owner::{append_role, seed_role},
    coordinator::{Coordinator, Prepared},
    ledger::{
        carry_free_prior_digest, rows_divergence, BalanceSource, BlockObservation, CarryRow,
        Ledger, PayoutState, WindowError, WindowRef,
    },
    metrics::Metrics,
};
use qbit_prism_test_gate as gate;
use sqlx::PgPool;
use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
use ledger_database::FixtureDatabase;

/// Every snapshot's network difficulty. No case has shares, so it only has
/// to be positive.
const NETWORK_DIFFICULTY: u128 = 100;
/// The pool's carry-forward debt as this frontend last read it from the
/// canonical balances, or -1 before it read them.
const DEBT_GAUGE: &str = "qbit_prism_carry_forward_debt_sats";
/// miner-c's debt in the local settlement.
const LOCAL_DEBT: f64 = 300.;

/// Until D1's migration 031 lands, a dual-writer window cut refuses to read
/// the share ledger without the `(origin_node, share_seq)` index; create it
/// the way D2's tests do. A no-op once the migration has made one.
async fn ensure_origin_index(pool: &PgPool) -> Result<()> {
    let indexed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] \
         JOIN pg_attribute b ON b.attrelid=i.indrelid AND b.attnum=i.indkey[1] \
         WHERE i.indrelid='qbit_share_ledger'::regclass AND i.indisvalid AND a.attname='origin_node' AND b.attname='share_seq')",
    )
    .fetch_one(pool)
    .await?;
    if !indexed {
        sqlx::query("CREATE INDEX qbit_share_ledger_origin_seq_until_031 ON qbit_share_ledger (origin_node, share_seq)")
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Run `ledger` as dual-writer node B: its gate starts closed, and the rows
/// the peer sync inserts (origin_node 0, the column default) are node A's.
/// Its journal claims ownership, so the gate alone decides what work pays.
async fn as_node_b(ledger: &Ledger) -> Result<()> {
    ledger.set_dual_writer_identity(NodeIdentity {
        node: NodeIndex::B,
        carry_owner: false,
    })?;
    claim_ownership(ledger, NodeIndex::B).await
}

/// Run `ledger` as dual-writer node A, whose rows the column default names;
/// as for [`as_node_b`], its journal claims ownership.
async fn as_node_a(ledger: &Ledger) -> Result<()> {
    ledger.set_dual_writer_identity(NodeIdentity {
        node: NodeIndex::A,
        carry_owner: true,
    })?;
    claim_ownership(ledger, NodeIndex::A).await
}

/// The origin index a dual-writer snapshot needs, and a journal row of
/// `node`'s that claims ownership, as an owner's guard seeds it.
async fn claim_ownership(ledger: &Ledger, node: NodeIndex) -> Result<()> {
    ensure_origin_index(&ledger.pool).await?;
    seed_role(ledger, node.index(), true).await?;
    Ok(())
}

/// One account of a block's settlement: one carry row and, for a peer's
/// block, one payout entry.
#[derive(Clone, Copy)]
struct Account {
    miner: &'static str,
    /// The account's 32-byte P2MR program is this byte, repeated.
    program: u8,
    /// The prior balance the block's work was issued with.
    prior: i64,
    gross: i64,
    onchain: i64,
}

impl Account {
    const fn new(miner: &'static str, program: u8, prior: i64, gross: i64, onchain: i64) -> Self {
        Self {
            miner,
            program,
            prior,
            gross,
            onchain,
        }
    }

    fn program_hex(&self) -> String {
        format!("{:02x}", self.program).repeat(32)
    }
}

/// The 3.0 settlement every case starts from, already confirmed. miner-c was
/// paid 400 sats on chain against a 300-sat prior no confirmed block credits.
const LOCAL: [Account; 3] = [
    // miner, program, prior, gross, on chain
    Account::new("miner-a", 0x11, 0, 1_000, 0),
    Account::new("miner-b", 0x22, 0, 500, 0),
    Account::new("miner-c", 0x33, 300, 100, 400),
];
const LOCAL_HEIGHT: i64 = 100;

/// A carry-free block the peer settled: every issued prior is zero and no
/// account was paid more than its gross. miner-a's canonical balance is
/// 1,000 sats, which this block was not built on, so 3.0's rule calls it
/// divergent.
const PEER: [Account; 2] = [
    Account::new("miner-a", 0x11, 0, 600, 500),
    Account::new("miner-d", 0x44, 0, 50, 0),
];

fn block_hash(tag: u8) -> String {
    format!("{tag:02x}").repeat(32)
}

/// How a block's rows reach this node's database.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// This node's own 3.0 settlement, written already confirmed. Only its
    /// carry rows, which are all the balances read.
    LocalConfirmed,
    /// Copied by peer sync: the block's immutable columns, its carry rows and
    /// its payout entries. Chain state, maturity and the publication ordinal
    /// are this node's own, at their defaults until its reconciler observes
    /// the block.
    PeerSync,
}

/// Writes one block's settlement rows in one transaction.
async fn insert_block(
    pool: &PgPool,
    hash: &str,
    height: i64,
    origin: Origin,
    accounts: &[Account],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let block = match origin {
        Origin::LocalConfirmed => "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state) VALUES($1,$2,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed')",
        Origin::PeerSync => "INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,as_issued_audit_sha256) VALUES($1,$2,repeat('00',32),repeat('ab',32),repeat('ac',32),repeat('ad',32))",
    };
    sqlx::query(block)
        .bind(hash)
        .bind(height)
        .execute(&mut *tx)
        .await?;
    for account in accounts {
        let candidate = account.prior + account.gross;
        let carry = candidate - account.onchain;
        let action = if account.onchain > 0 {
            "onchain"
        } else {
            "accrued"
        };
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,$2,$3,$3,decode($4,'hex'),$5,$6,$7,$8,$9,$10)")
            .bind(height)
            .bind(hash)
            .bind(account.miner)
            .bind(account.program_hex())
            .bind(account.gross)
            .bind(account.prior)
            .bind(candidate)
            .bind(account.onchain)
            .bind(carry)
            .bind(action)
            .execute(&mut *tx)
            .await?;
        if origin == Origin::PeerSync {
            sqlx::query("INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action) VALUES($1,$2,$3,$3,decode($4,'hex'),$5,$6,$7)")
                .bind(hash)
                .bind(height)
                .bind(account.miner)
                .bind(account.program_hex())
                .bind(account.onchain)
                .bind(carry)
                .bind(action)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

async fn seed_local(pool: &PgPool) -> Result<()> {
    insert_block(
        pool,
        &block_hash(0xa0),
        LOCAL_HEIGHT,
        Origin::LocalConfirmed,
        &LOCAL,
    )
    .await
}

/// `qbit_current_carry_forward_balances()`, decoded as a snapshot decodes it.
async fn canonical_balances(pool: &PgPool) -> Result<Vec<CarryForwardBalance>> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT miner_id,payout_order_key,encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances()",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(recipient_id, order_key, p2mr_program_hex, balance)| {
            Ok(CarryForwardBalance {
                recipient_id,
                order_key,
                p2mr_program_hex,
                balance_sats: balance.parse()?,
            })
        })
        .collect()
}

fn by_program(balances: &[CarryForwardBalance]) -> BTreeMap<String, i128> {
    balances
        .iter()
        .map(|balance| (balance.p2mr_program_hex.clone(), balance.balance_sats))
        .collect()
}

/// The balances `blocks` leave once they all count: per program, every
/// gross less every on-chain payment. A zero sum is no balance.
fn expected(blocks: &[&[Account]]) -> BTreeMap<String, i128> {
    let mut balances = BTreeMap::new();
    for account in blocks.iter().flat_map(|accounts| accounts.iter()) {
        *balances.entry(account.program_hex()).or_insert(0) +=
            i128::from(account.gross - account.onchain);
    }
    balances.retain(|_, balance| *balance != 0);
    balances
}

async fn ensure_balances(pool: &PgPool, blocks: &[&[Account]], when: &str) -> Result<()> {
    let actual = by_program(&canonical_balances(pool).await?);
    let wanted = expected(blocks);
    ensure!(
        actual == wanted,
        "{when}: the canonical balances are {actual:?}, not {wanted:?}"
    );
    // The per-label summary the balances are read from must still match
    // its recomputation from the carry rows of the blocks that count.
    let drift: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_carry_forward_current_drift()")
        .fetch_one(pool)
        .await?;
    ensure!(
        drift == 0,
        "{when}: the balance summary drifted from its recomputation on {drift} program(s)"
    );
    Ok(())
}

/// The cluster's payout revision, read whether or not it is halted.
async fn revision(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(pool)
            .await?,
    )
}

/// The window reference of carry-free work with no shares.
fn carry_free_window(anchor_ms: i64) -> WindowRef {
    WindowRef {
        anchor_ms,
        prior_balances_digest: carry_free_prior_digest(),
        shares: None,
        cut: None,
    }
}

fn debt_gauge(metrics: &Metrics) -> Result<f64> {
    let prefix = format!("{DEBT_GAUGE} ");
    metrics
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .with_context(|| format!("{DEBT_GAUGE} has no sample"))?
        .parse()
        .with_context(|| format!("{DEBT_GAUGE} is not a number"))
}

fn observed(hash: &str, active: bool) -> [BlockObservation; 1] {
    [BlockObservation {
        block_hash: hash.into(),
        active,
    }]
}

#[derive(Debug)]
struct BlockState {
    chain: String,
    maturity: String,
    ordinal: Option<i64>,
    /// Whether the block records a disconnection.
    inactive_since: bool,
    matured_at: bool,
}

async fn block_state(pool: &PgPool, hash: &str) -> Result<BlockState> {
    let (chain, maturity, ordinal, inactive_since, matured_at) = sqlx::query_as(
        "SELECT chain_state,maturity_state,audit_publication_sequence,inactive_since IS NOT NULL,matured_at IS NOT NULL FROM qbit_pool_blocks WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?;
    Ok(BlockState {
        chain,
        maturity,
        ordinal,
        inactive_since,
        matured_at,
    })
}

/// The distinct maturity states of a block's carry rows and of its payout
/// entries.
async fn row_maturities(pool: &PgPool, hash: &str) -> Result<(Vec<String>, Vec<String>)> {
    let carry = sqlx::query_scalar(
        "SELECT DISTINCT maturity_state FROM qbit_payout_carry_forward WHERE block_hash=$1 ORDER BY 1",
    )
    .bind(hash)
    .fetch_all(pool)
    .await?;
    let payouts = sqlx::query_scalar(
        "SELECT DISTINCT maturity_state FROM qbit_pool_payout_entries WHERE block_hash=$1 ORDER BY 1",
    )
    .bind(hash)
    .fetch_all(pool)
    .await?;
    Ok((carry, payouts))
}

/// A block's #478 record: its divergent accounts, the debt it created, the
/// revision a candidate gave it, and the canonical balances it met.
type DivergenceRecord = (Option<i32>, Option<String>, Option<i64>, Option<String>);

async fn divergence_record(pool: &PgPool, hash: &str) -> Result<Option<DivergenceRecord>> {
    Ok(sqlx::query_as(
        "SELECT divergent_accounts,overpay_sats::text,candidate_payout_revision,confirmed_prior_balances_sha256 FROM qbit_prism_payout_divergences WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?)
}

async fn divergence_rows(pool: &PgPool, hash: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_prism_payout_divergences WHERE block_hash=$1),(SELECT count(*) FROM qbit_prism_payout_divergence_accounts WHERE block_hash=$1)",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

fn carry_rows(accounts: &[Account]) -> Vec<CarryRow> {
    accounts
        .iter()
        .map(|account| CarryRow {
            p2mr_program_hex: account.program_hex(),
            miner_id: account.miner.into(),
            issued_prior_sats: account.prior.into(),
            gross_sats: account.gross.into(),
            onchain_sats: account.onchain.into(),
        })
        .collect()
}

/// Runs `case` on a fresh fixture database, dropped however the case ends.
async fn run<Case, Outcome>(raw: &str, prefix: &str, case: Case) -> Result<()>
where
    Case: FnOnce(String) -> Outcome,
    Outcome: Future<Output = Result<()>>,
{
    let fixture = FixtureDatabase::open(raw, prefix).await?;
    let result = case(fixture.url.clone()).await;
    fixture.close(result).await
}

/// Single-writer mode is 3.0: work reads the canonical balances, the gauge
/// records their debt, and the gate never moves or bumps the revision.
#[tokio::test]
async fn single_writer_work_reads_the_canonical_balances_and_the_gate_never_moves() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_single_", single_writer_parity).await
}

async fn single_writer_parity(url: String) -> Result<()> {
    let metrics = Arc::new(Metrics::default());
    let ledger =
        Ledger::connect_with_metrics(&url, "single-writer".into(), 4, true, Some(metrics.clone()))
            .await?;
    let pool = ledger.pool.clone();
    seed_local(&pool).await?;
    ensure_balances(&pool, &[&LOCAL], "the local settlement").await?;
    let canonical = canonical_balances(&pool).await?;
    let digest = prior_balances_digest(&canonical);
    ensure!(
        digest != carry_free_prior_digest(),
        "the seeded balances must not digest like no balances"
    );
    ensure!(
        !ledger.dual_writer() && ledger.carry_paying() && !ledger.carry_fence_pending(),
        "a ledger never put in dual-writer mode must pay carried balances"
    );
    let r0 = revision(&pool).await?;

    let snapshot = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        snapshot.prior_balances == canonical,
        "the snapshot's prior balances {:?} are not the canonical {canonical:?}",
        snapshot.prior_balances
    );
    ensure!(
        snapshot.payout_revision == r0,
        "a snapshot moved the revision"
    );
    let window = WindowRef::from_snapshot(&snapshot)?;
    ensure!(
        window.prior_balances_digest == digest,
        "the snapshot's reference does not digest the canonical balances"
    );
    ensure!(
        debt_gauge(&metrics)? == LOCAL_DEBT,
        "a canonical snapshot must record the pool's debt, {LOCAL_DEBT}, not {}",
        debt_gauge(&metrics)?
    );
    let state = ledger.payout_state().await?;
    ensure!(
        state
            == PayoutState {
                payout_revision: r0,
                prior_balances_digest: digest,
            },
        "payout_state {state:?} is not the canonical balances at revision {r0}"
    );
    let read = ledger.read_window(&window, BalanceSource::Current).await?;
    ensure!(
        read.prior_balances == canonical,
        "read_window(Current) did not return the canonical balances"
    );
    // A carry-free reference does not describe this node's balances.
    match ledger
        .read_window(
            &carry_free_window(snapshot.anchor_ms),
            BalanceSource::Current,
        )
        .await
    {
        Err(WindowError::PriorBalancesChanged { expected, actual })
            if expected == carry_free_prior_digest() && actual == digest => {}
        other => bail!(
            "a single-writer read of a carry-free reference must fail as changed balances: {other:?}"
        ),
    }

    // The gate is open for good: no change, no bump, nothing pending.
    for paying in [false, true, false] {
        ensure!(
            !ledger.set_carry_paying(paying).await?,
            "set_carry_paying({paying}) reported a change in single-writer mode"
        );
        ensure!(
            ledger.carry_paying() && !ledger.carry_fence_pending(),
            "set_carry_paying({paying}) moved the single-writer gate"
        );
    }
    ensure!(
        ledger.fence_carry_change().await?.is_none(),
        "single-writer mode fenced a carry change"
    );
    ensure!(
        revision(&pool).await? == r0,
        "the single-writer gate bumped the payout revision"
    );
    let again = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        again.prior_balances == canonical && again.payout_revision == r0,
        "the snapshot changed after set_carry_paying in single-writer mode"
    );
    ensure!(
        ledger.payout_state().await? == state,
        "payout_state changed after set_carry_paying in single-writer mode"
    );
    ledger.pool.close().await;
    Ok(())
}

/// A closed gate builds carry-free work: the snapshot, payout_state and a
/// current-balance window read all see no prior balances, while the
/// canonical balances are untouched and still what single-writer work reads.
#[tokio::test]
async fn dual_writer_work_reads_no_prior_balances_while_the_canonical_ones_stay() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_free_", dual_writer_carry_free).await
}

async fn dual_writer_carry_free(url: String) -> Result<()> {
    let metrics = Arc::new(Metrics::default());
    let ledger =
        Ledger::connect_with_metrics(&url, "dual-writer".into(), 4, true, Some(metrics.clone()))
            .await?;
    let pool = ledger.pool.clone();
    seed_local(&pool).await?;
    let canonical = canonical_balances(&pool).await?;
    let canonical_digest = prior_balances_digest(&canonical);
    let r0 = revision(&pool).await?;
    as_node_b(&ledger).await?;
    ensure!(
        ledger.dual_writer() && !ledger.carry_paying() && !ledger.carry_fence_pending(),
        "the dual-writer gate must start closed with nothing to fence"
    );
    ensure!(
        revision(&pool).await? == r0,
        "closing the gate before any work is built bumped the revision"
    );
    ensure!(
        debt_gauge(&metrics)? == -1.,
        "the debt gauge was set before any balance read"
    );

    let snapshot = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        snapshot.prior_balances.is_empty(),
        "carry-free work was built on prior balances {:?}",
        snapshot.prior_balances
    );
    ensure!(
        snapshot.payout_revision == r0,
        "a snapshot moved the revision"
    );
    // A dual-writer window always records its cut; this ledger holds no
    // share of either node yet.
    let window = WindowRef::from_snapshot(&snapshot)?;
    ensure!(
        window
            == WindowRef {
                cut: Some(WindowCut::default()),
                ..carry_free_window(snapshot.anchor_ms)
            },
        "the carry-free snapshot's reference is {window:?}"
    );
    // Not 0: carry-free work reads no balances, so it says nothing about the
    // debt.
    ensure!(
        debt_gauge(&metrics)? == -1.,
        "a carry-free snapshot recorded a debt of {}",
        debt_gauge(&metrics)?
    );
    let state = ledger.payout_state().await?;
    ensure!(
        state
            == PayoutState {
                payout_revision: r0,
                prior_balances_digest: carry_free_prior_digest(),
            },
        "payout_state {state:?} is not carry-free at revision {r0}"
    );
    let read = ledger
        .read_window(&window, BalanceSource::Current)
        .await
        .context("read_window(Current) of carry-free work")?;
    ensure!(
        read.prior_balances.is_empty() && read.shares.is_empty() && read.payout_revision == r0,
        "read_window(Current) of carry-free work returned {:?} at revision {}",
        read.prior_balances,
        read.payout_revision
    );
    // A window read from current balances, as a landing whose as-issued
    // snapshot is gone reads it, follows the reference's own mode, not the
    // gate: an owner's work built on the canonical balances still meets them
    // while the gate is closed (a restarted guard before its first peer
    // read, or `candidates recover`, which runs no guard).
    let paying_window = WindowRef {
        prior_balances_digest: canonical_digest,
        ..window
    };
    let read = ledger
        .read_window(&paying_window, BalanceSource::Current)
        .await
        .context("read_window(Current) of an owner's work with the gate closed")?;
    ensure!(
        read.prior_balances == canonical,
        "with the gate closed an owner's reference read {:?}, not the canonical balances",
        read.prior_balances
    );

    // The canonical balances are untouched, and a single-writer frontend on
    // the same database still builds on them.
    ensure_balances(&pool, &[&LOCAL], "after carry-free reads").await?;
    let single = Ledger::connect(&url, "single-writer-view".into(), 2, false).await?;
    ensure!(
        single.snapshot(NETWORK_DIFFICULTY).await?.prior_balances == canonical,
        "a single-writer snapshot beside the dual-writer one lost the canonical balances"
    );
    ensure!(
        single.payout_state().await?.prior_balances_digest == canonical_digest,
        "a single-writer payout_state beside the dual-writer one lost the canonical balances"
    );
    single.pool.close().await;

    // Control: the same frontend records the debt once its work pays carried
    // balances, so the unset gauge above was this registry's.
    ensure!(
        ledger.set_carry_paying(true).await?,
        "opening the gate was not a change"
    );
    ensure!(
        ledger.snapshot(NETWORK_DIFFICULTY).await?.prior_balances == canonical,
        "a paying snapshot did not read the canonical balances"
    );
    ensure!(
        debt_gauge(&metrics)? == LOCAL_DEBT,
        "a paying snapshot recorded a debt of {}, not {LOCAL_DEBT}",
        debt_gauge(&metrics)?
    );
    // And carry-free work still reads no balances while the gate is open.
    let read = ledger
        .read_window(&window, BalanceSource::Current)
        .await
        .context("read_window(Current) of carry-free work with the gate open")?;
    ensure!(
        read.prior_balances.is_empty(),
        "with the gate open a carry-free reference read {:?}",
        read.prior_balances
    );
    ledger.pool.close().await;
    Ok(())
}

/// Each change of the gate is fenced by exactly one payout revision bump,
/// so work built under the old mode is always at a superseded revision; a
/// repeated setting changes nothing and bumps nothing.
#[tokio::test]
async fn every_carry_gate_change_is_fenced_by_exactly_one_revision_bump() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_fence_", gate_changes_are_fenced).await
}

async fn gate_changes_are_fenced(url: String) -> Result<()> {
    let ledger = Ledger::connect(&url, "carry-owner".into(), 4, true).await?;
    let pool = ledger.pool.clone();
    seed_local(&pool).await?;
    as_node_b(&ledger).await?;
    let canonical = canonical_balances(&pool).await?;
    let canonical_digest = prior_balances_digest(&canonical);
    let r0 = revision(&pool).await?;
    let carry_free = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        carry_free.prior_balances.is_empty() && carry_free.payout_revision == r0,
        "the closed gate's work is not carry-free at revision {r0}"
    );

    ensure!(
        ledger.set_carry_paying(true).await?,
        "opening the closed gate was not a change"
    );
    ensure!(
        ledger.carry_paying() && !ledger.carry_fence_pending(),
        "the opened gate is not paying with its fence committed"
    );
    ensure!(
        revision(&pool).await? == r0 + 1,
        "opening the gate must bump the revision exactly once, from {r0} to {}",
        revision(&pool).await?
    );
    ensure!(
        ledger.clone().carry_paying(),
        "a clone of the frontend's ledger did not see the opened gate"
    );
    let paying = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        paying.prior_balances == canonical && paying.payout_revision == r0 + 1,
        "the opened gate's work is not built on the canonical balances at revision {}",
        r0 + 1
    );
    ensure!(
        ledger.payout_state().await?
            == PayoutState {
                payout_revision: r0 + 1,
                prior_balances_digest: canonical_digest,
            },
        "payout_state does not follow the opened gate"
    );

    ensure!(
        !ledger.set_carry_paying(true).await?,
        "opening the open gate again was reported as a change"
    );
    ensure!(
        ledger.fence_carry_change().await?.is_none(),
        "a repeated setting left a fence to commit"
    );
    ensure!(
        revision(&pool).await? == r0 + 1,
        "a repeated setting bumped the revision"
    );

    ensure!(
        ledger.set_carry_paying(false).await?,
        "closing the open gate was not a change"
    );
    ensure!(
        !ledger.carry_paying() && !ledger.carry_fence_pending(),
        "the closed gate still pays, or its fence is pending"
    );
    ensure!(
        revision(&pool).await? == r0 + 2,
        "closing the gate must bump the revision exactly once more"
    );
    let closed = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        closed.prior_balances.is_empty() && closed.payout_revision == r0 + 2,
        "the closed gate's work is not carry-free at revision {}",
        r0 + 2
    );
    ensure!(
        ledger.payout_state().await?
            == PayoutState {
                payout_revision: r0 + 2,
                prior_balances_digest: carry_free_prior_digest(),
            },
        "payout_state does not follow the closed gate"
    );
    ensure!(
        !ledger.set_carry_paying(false).await?,
        "closing the closed gate again was reported as a change"
    );
    ensure!(
        revision(&pool).await? == r0 + 2,
        "a repeated close bumped the revision"
    );
    ensure_balances(&pool, &[&LOCAL], "after the gate moved twice").await?;
    ledger.pool.close().await;
    Ok(())
}

/// A gate change whose bump cannot commit stays in force with its fence
/// pending, and `fence_carry_change` commits that one bump later.
#[tokio::test]
async fn a_carry_change_whose_bump_fails_stays_pending_until_its_fence_commits() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_pending_", failed_fence_stays_pending).await
}

async fn failed_fence_stays_pending(url: String) -> Result<()> {
    let ledger = Ledger::connect(&url, "carry-owner".into(), 4, true).await?;
    let pool = ledger.pool.clone();
    seed_local(&pool).await?;
    as_node_b(&ledger).await?;
    let canonical = canonical_balances(&pool).await?;
    let r0 = revision(&pool).await?;
    // A halted cluster refuses the bump.
    sqlx::query(
        "UPDATE qbit_prism_cluster SET fatal_error='carry fence test: halted' WHERE singleton",
    )
    .execute(&pool)
    .await?;
    let error = match ledger.set_carry_paying(true).await {
        Ok(changed) => bail!("a halted cluster committed the fence (changed: {changed})"),
        Err(error) => error,
    };
    ensure!(
        format!("{error:#}").contains("cluster halted"),
        "the fence failed for another reason: {error:#}"
    );
    ensure!(
        ledger.carry_paying() && ledger.carry_fence_pending(),
        "a change whose bump failed must stay in force with its fence pending"
    );
    ensure!(
        ledger.fence_carry_change().await.is_err() && ledger.carry_fence_pending(),
        "a retry that cannot commit must keep the fence pending"
    );
    ensure!(
        revision(&pool).await? == r0,
        "a failed fence moved the revision"
    );

    sqlx::query("UPDATE qbit_prism_cluster SET fatal_error=NULL WHERE singleton")
        .execute(&pool)
        .await?;
    ensure!(
        ledger.fence_carry_change().await? == Some(r0 + 1),
        "the retried fence did not bump to {}",
        r0 + 1
    );
    ensure!(
        !ledger.carry_fence_pending() && revision(&pool).await? == r0 + 1,
        "the committed fence is still pending, or bumped more than once"
    );
    ensure!(
        ledger.fence_carry_change().await?.is_none() && revision(&pool).await? == r0 + 1,
        "a committed fence was committed again"
    );
    let paying = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        paying.prior_balances == canonical && paying.payout_revision == r0 + 1,
        "work after the committed fence is not paying at revision {}",
        r0 + 1
    );
    ledger.pool.close().await;
    Ok(())
}

/// A peer's carry-free block is this node's derived state: it counts only
/// once this node's reconciler observes it active, with one revision bump and
/// a local publication ordinal and, in dual mode, no divergence record; a
/// reorg retracts it and a reactivation counts it again.
#[tokio::test]
async fn a_peer_settled_block_counts_only_through_this_nodes_reconciler() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_peer_", peer_settlement).await
}

async fn peer_settlement(url: String) -> Result<()> {
    let ledger = Ledger::connect(&url, "receiver".into(), 4, true).await?;
    let pool = ledger.pool.clone();
    as_node_b(&ledger).await?;
    seed_local(&pool).await?;
    let local_ordinal = block_state(&pool, &block_hash(0xa0))
        .await?
        .ordinal
        .context("the local confirmed block has no publication ordinal")?;
    let hash = block_hash(0xb1);
    let height = 200;
    let r0 = revision(&pool).await?;

    insert_block(&pool, &hash, height, Origin::PeerSync, &PEER).await?;
    let synced = block_state(&pool, &hash).await?;
    ensure!(
        synced.chain == "prepared"
            && synced.maturity == "immature"
            && synced.ordinal.is_none()
            && !synced.inactive_since
            && !synced.matured_at,
        "a synced block must start as this node's prepared, immature block with no ordinal: {synced:?}"
    );
    ensure_balances(&pool, &[&LOCAL], "a synced, unobserved peer block").await?;
    ensure!(
        revision(&pool).await? == r0,
        "writing a peer's settlement moved the revision"
    );
    ensure!(
        ledger
            .pool_blocks_for_reconcile()
            .await?
            .iter()
            .any(|block| block.block_hash == hash
                && block.height == height as u64
                && block.chain_state == "prepared"),
        "the reconciler does not list the synced peer block"
    );

    let first = ledger
        .reconcile_blocks_at_revision(&observed(&hash, true), 201, r0)
        .await?;
    // Confirmed, but not found here: the found-block count leaves it out.
    ensure!(
        first == 0,
        "a peer block's first confirmation counted {first} found blocks, not 0"
    );
    ensure!(
        revision(&pool).await? == r0 + 1,
        "the confirmation must bump the revision exactly once"
    );
    let confirmed = block_state(&pool, &hash).await?;
    let ordinal = confirmed
        .ordinal
        .context("the confirmed peer block has no publication ordinal")?;
    ensure!(
        confirmed.chain == "confirmed" && confirmed.maturity == "immature",
        "the observed peer block is {confirmed:?}"
    );
    ensure!(
        ordinal > local_ordinal,
        "the peer block's ordinal {ordinal} is not this node's next one after {local_ordinal}"
    );
    ensure_balances(&pool, &[&LOCAL, &PEER], "the confirmed peer block").await?;
    ensure!(
        divergence_rows(&pool, &hash).await? == (0, 0),
        "a dual-writer confirmation of carry-free rows recorded a divergence"
    );

    let reorged = ledger
        .reconcile_blocks_at_revision(&observed(&hash, false), 202, r0 + 1)
        .await?;
    ensure!(reorged == 0, "a deactivation counted as a confirmation");
    ensure!(
        revision(&pool).await? == r0 + 2,
        "the reorg must bump the revision exactly once"
    );
    let inactive = block_state(&pool, &hash).await?;
    ensure!(
        inactive.chain == "inactive"
            && inactive.inactive_since
            && inactive.ordinal == Some(ordinal),
        "the reorged peer block is {inactive:?}"
    );
    ensure_balances(&pool, &[&LOCAL], "the reorged peer block").await?;

    let reactivated = ledger
        .reconcile_blocks_at_revision(&observed(&hash, true), 203, r0 + 2)
        .await?;
    ensure!(
        reactivated == 0,
        "a reactivation counted as a first confirmation"
    );
    ensure!(
        revision(&pool).await? == r0 + 3,
        "the reactivation must bump the revision exactly once"
    );
    let again = block_state(&pool, &hash).await?;
    ensure!(
        again.chain == "confirmed" && !again.inactive_since && again.ordinal == Some(ordinal),
        "the reactivated peer block is {again:?}; its ordinal must be the first one"
    );
    ensure_balances(&pool, &[&LOCAL, &PEER], "the reactivated peer block").await?;
    ensure!(
        divergence_rows(&pool, &hash).await? == (0, 0),
        "a dual-writer reactivation of carry-free rows recorded a divergence"
    );
    ensure!(
        row_maturities(&pool, &hash).await?
            == (vec!["immature".to_owned()], vec!["immature".to_owned()]),
        "the peer block's rows matured below its maturity height"
    );
    ledger.pool.close().await;
    Ok(())
}

/// A confirmed peer block matures at this node's reconciler like a local
/// one, and a disconnection observed once it is mature halts the cluster.
#[tokio::test]
async fn a_peer_settled_block_matures_here_and_its_mature_disconnect_halts_the_cluster(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_mature_", peer_maturity_and_halt).await
}

async fn peer_maturity_and_halt(url: String) -> Result<()> {
    let ledger = Ledger::connect(&url, "receiver".into(), 4, true).await?;
    let pool = ledger.pool.clone();
    as_node_b(&ledger).await?;
    seed_local(&pool).await?;
    let hash = block_hash(0xb2);
    let height: u64 = 300;
    insert_block(&pool, &hash, height as i64, Origin::PeerSync, &PEER).await?;
    let r0 = revision(&pool).await?;
    ensure!(
        ledger
            .reconcile_blocks_at_revision(&observed(&hash, true), height + 1, r0)
            .await?
            == 0,
        "a peer block counted as found here"
    );
    let r1 = revision(&pool).await?;
    ensure!(r1 == r0 + 1, "the confirmation bumped {} times", r1 - r0);

    // One block short of maturity nothing of the peer block matures.
    ledger
        .reconcile_blocks_at_revision(&observed(&hash, true), height + 999, r1)
        .await?;
    let young = block_state(&pool, &hash).await?;
    ensure!(
        young.chain == "confirmed" && young.maturity == "immature" && !young.matured_at,
        "the peer block matured one block early: {young:?}"
    );
    ensure!(
        revision(&pool).await? == r1,
        "a reconciliation that matured no payout bumped the revision"
    );

    ledger
        .reconcile_blocks_at_revision(&observed(&hash, true), height + 1000, r1)
        .await?;
    let mature = block_state(&pool, &hash).await?;
    ensure!(
        mature.chain == "confirmed" && mature.maturity == "mature" && mature.matured_at,
        "the peer block did not mature 1,000 blocks deep: {mature:?}"
    );
    ensure!(
        row_maturities(&pool, &hash).await?
            == (vec!["mature".to_owned()], vec!["mature".to_owned()]),
        "the peer block's carry rows and payout entries did not mature with it"
    );
    let r2 = revision(&pool).await?;
    ensure!(
        r2 == r1 + 1,
        "maturing the payouts must bump the revision exactly once"
    );
    ensure_balances(&pool, &[&LOCAL, &PEER], "the matured peer block").await?;
    ensure!(
        ledger
            .pool_blocks_for_reconcile()
            .await?
            .iter()
            .any(|block| block.block_hash == hash && block.maturity_state == "mature"),
        "the reconciler stopped observing the latest mature block"
    );

    let error = match ledger
        .reconcile_blocks_at_revision(&observed(&hash, false), height + 1001, r2)
        .await
    {
        Ok(confirmed) => {
            bail!("a mature peer block's disconnection was accepted (confirmed {confirmed})")
        }
        Err(error) => error,
    };
    ensure!(
        format!("{error:#}").contains(&format!("mature pool block disconnected: {hash}")),
        "the mature disconnection failed for another reason: {error:#}"
    );
    let fatal: Option<String> =
        sqlx::query_scalar("SELECT fatal_error FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(&pool)
            .await?;
    ensure!(
        fatal.as_deref().is_some_and(
            |fatal| fatal.contains("mature pool block disconnected") && fatal.contains(&hash)
        ),
        "the mature disconnection did not halt the cluster: fatal_error is {fatal:?}"
    );
    let halted = block_state(&pool, &hash).await?;
    ensure!(
        halted.chain == "confirmed" && halted.maturity == "mature",
        "the halt changed the mature block: {halted:?}"
    );
    ensure!(revision(&pool).await? == r2, "the halt bumped the revision");
    ensure_balances(&pool, &[&LOCAL, &PEER], "the halted cluster").await?;
    ensure!(
        ledger.snapshot(NETWORK_DIFFICULTY).await.is_err() && ledger.payout_state().await.is_err(),
        "a halted cluster still builds work"
    );
    ledger.pool.close().await;
    Ok(())
}

/// The divergence control: confirming carry-free rows against canonical
/// balances they were not built on is a divergent landing by 3.0's rule, and
/// single-writer mode still records it; dual mode records nothing for such a
/// block, and its re-confirmation rewrites a record left by a single-writer
/// confirmation as not divergent.
#[tokio::test]
async fn only_single_writer_mode_records_a_carry_free_confirmation_as_divergent() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_divergence_", divergence_control).await
}

async fn divergence_control(url: String) -> Result<()> {
    let single = Ledger::connect(&url, "single-writer".into(), 4, true).await?;
    // Node A: the blocks below carry the column default origin, so they are
    // its own carry-free blocks (a peer's are covered at the end).
    let dual = Ledger::connect(&url, "dual-writer".into(), 4, false).await?;
    as_node_a(&dual).await?;
    let pool = single.pool.clone();
    seed_local(&pool).await?;
    let (first, second) = (block_hash(0xb3), block_hash(0xb4));
    insert_block(&pool, &first, 400, Origin::PeerSync, &PEER).await?;
    insert_block(&pool, &second, 401, Origin::PeerSync, &PEER).await?;

    // Single writer: 3.0's record. miner-a's issued prior, 0, is not its
    // canonical 1,000; nothing was overpaid.
    let met = canonical_balances(&pool).await?;
    single
        .reconcile_blocks_at_revision(&observed(&first, true), 402, revision(&pool).await?)
        .await?;
    let record = divergence_record(&pool, &first).await?;
    ensure!(
        record
            == Some((
                Some(1),
                Some("0".into()),
                None,
                Some(hex::encode(prior_balances_digest(&met))),
            )),
        "single-writer mode must record the carry-free confirmation as one divergent account and no overpay against the balances it met: {record:?}"
    );
    ensure!(
        divergence_rows(&pool, &first).await? == (1, 0),
        "single-writer mode recorded overpaid accounts for a carry-free block"
    );

    // Dual writer: an identical block records nothing, though 3.0's rule
    // finds it divergent on both accounts against the balances it meets,
    // miner-a's 1,100 sats and the 50 the first block left miner-d.
    let met = canonical_balances(&pool).await?;
    let by_3_0 = rows_divergence(carry_rows(&PEER), &met);
    ensure!(
        by_3_0.divergent_accounts == 2 && by_3_0.overpay_sats == 0,
        "the control block is not divergent by 3.0's rule: {by_3_0:?}"
    );
    dual.reconcile_blocks_at_revision(&observed(&second, true), 402, revision(&pool).await?)
        .await?;
    ensure!(
        block_state(&pool, &second).await?.chain == "confirmed",
        "the dual-writer reconciler did not confirm the block"
    );
    ensure!(
        divergence_rows(&pool, &second).await? == (0, 0),
        "a dual-writer confirmation of carry-free rows recorded a divergence"
    );
    ensure_balances(&pool, &[&LOCAL, &PEER, &PEER], "both peer blocks").await?;

    // A record that exists is rewritten, not left behind: after a reorg the
    // dual writer's re-confirmation records the block as not divergent, and
    // the divergence report stops counting it.
    single
        .reconcile_blocks_at_revision(&observed(&first, false), 403, revision(&pool).await?)
        .await?;
    dual.reconcile_blocks_at_revision(&observed(&first, true), 404, revision(&pool).await?)
        .await?;
    ensure!(
        block_state(&pool, &first).await?.chain == "confirmed",
        "the dual-writer reconciler did not reactivate the block"
    );
    let rewritten = divergence_record(&pool, &first).await?;
    ensure!(
        matches!(&rewritten, Some((Some(0), Some(overpay), None, Some(_))) if overpay == "0"),
        "the dual-writer re-confirmation must rewrite the existing record as not divergent: {rewritten:?}"
    );
    let landings: String =
        sqlx::query_scalar("SELECT qbit_prism_payout_divergence_report()->>'divergent_landings'")
            .fetch_one(&pool)
            .await?;
    ensure!(
        landings == "0",
        "the divergence report still counts {landings} divergent landing(s)"
    );
    ensure_balances(&pool, &[&LOCAL, &PEER, &PEER], "after the re-confirmation").await?;

    // A peer's block is its own node's to record: node B, confirming node
    // A's block, writes nothing and leaves the existing record as it is.
    let peer = Ledger::connect(&url, "dual-writer-b".into(), 4, false).await?;
    as_node_b(&peer).await?;
    single
        .reconcile_blocks_at_revision(&observed(&first, false), 405, revision(&pool).await?)
        .await?;
    peer.reconcile_blocks_at_revision(&observed(&first, true), 406, revision(&pool).await?)
        .await?;
    ensure!(
        block_state(&pool, &first).await?.chain == "confirmed",
        "node B's reconciler did not reactivate node A's block"
    );
    ensure!(
        divergence_record(&pool, &first).await? == rewritten,
        "node B rewrote node A's divergence record"
    );
    ensure_balances(
        &pool,
        &[&LOCAL, &PEER, &PEER],
        "after node B's confirmation",
    )
    .await?;
    peer.pool.close().await;
    dual.pool.close().await;
    single.pool.close().await;
    Ok(())
}

async fn published(coordinator: &Coordinator) -> Result<Arc<Prepared>> {
    coordinator
        .prepared
        .read()
        .await
        .clone()
        .context("the refresh published no work")
}

/// How many published rebuilds `trigger` caused, by the refresh histogram.
fn refreshes(metrics: &Metrics, trigger: &str) -> f64 {
    let label = format!("trigger=\"{trigger}\"");
    metrics
        .render()
        .lines()
        .filter_map(|line| line.strip_prefix("qbit_prism_refresh_seconds_count{"))
        .filter(|line| line.contains(&label))
        .filter_map(|line| line.rsplit_once(' '))
        .filter_map(|(_, count)| count.parse::<f64>().ok())
        .sum()
}

/// Sets miner-a's canonical balance, as a settlement on another block
/// would, without bumping the revision. Written to the per-label summary
/// the balances are read from, so the coordinator has no pool block to
/// reconcile against the fake node's chain.
async fn set_canonical_balance(pool: &PgPool, sats: i64) -> Result<()> {
    sqlx::query("INSERT INTO qbit_payout_carry_forward_current(miner_id,payout_order_key,p2mr_program,balance_sats,active_row_count) VALUES('miner-a','miner-a',decode(repeat('11',32),'hex'),$1,1) ON CONFLICT (miner_id,payout_order_key,p2mr_program) DO UPDATE SET balance_sats=EXCLUDED.balance_sats,updated_at=clock_timestamp()")
        .bind(sats)
        .execute(pool)
        .await?;
    Ok(())
}

/// The coordinator's refresh probe reads its balances through the gate:
/// carry-free work survives a change of the canonical balances, where the
/// same change replaces paying work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dual_writer_refresh_keeps_carry_free_work_when_only_the_canonical_balances_move(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_refresh_", refresh_probe_follows_the_gate).await
}

async fn refresh_probe_follows_the_gate(url: String) -> Result<()> {
    let node = fake_qbitd::FakeNode::open().await?;
    let mut config = fake_qbitd::coordinator_config(url, &node, "carry-refresh")?;
    // Neither a reanchor nor an aged template may replace the work on a slow
    // host: only the refresh probe decides whether it is reused.
    config.snapshot_interval = Duration::from_secs(3600);
    config.template_max_age = Duration::from_secs(3600);
    let metrics = Arc::new(Metrics::default());
    let coordinator = Coordinator::new(config, metrics.clone()).await?;
    let ledger = coordinator.ledger.clone();
    let pool = ledger.pool.clone();
    // Closed before any work is built.
    as_node_b(&ledger).await?;
    set_canonical_balance(&pool, 12_345).await?;

    coordinator.refresh_once().await?;
    let carry_free = published(&coordinator).await?;
    ensure!(
        carry_free.window.prior_balances_digest == carry_free_prior_digest(),
        "the dual-writer coordinator published work on prior balances"
    );
    set_canonical_balance(&pool, 23_456).await?;
    coordinator.refresh_once().await?;
    ensure!(
        Arc::ptr_eq(&carry_free, &published(&coordinator).await?),
        "a change of the canonical balances replaced carry-free work: the refresh probe read them"
    );
    ensure!(
        refreshes(&metrics, "balances") == 0.,
        "a carry-free refresh was triggered by balances"
    );

    // Control: once the gate is open, the same change replaces the work.
    ensure!(
        ledger.set_carry_paying(true).await?,
        "opening the gate was not a change"
    );
    coordinator.refresh_once().await?;
    let paying = published(&coordinator).await?;
    ensure!(
        paying.window.prior_balances_digest
            == prior_balances_digest(&canonical_balances(&pool).await?),
        "the opened gate's work is not built on the canonical balances"
    );
    set_canonical_balance(&pool, 34_567).await?;
    coordinator.refresh_once().await?;
    let rebuilt = published(&coordinator).await?;
    ensure!(
        !Arc::ptr_eq(&paying, &rebuilt)
            && rebuilt.window.prior_balances_digest
                == prior_balances_digest(&canonical_balances(&pool).await?),
        "a change of the canonical balances did not replace paying work"
    );
    ensure!(
        refreshes(&metrics, "balances") == 1.,
        "the paying rebuild was not triggered by balances ({} balance refreshes)",
        refreshes(&metrics, "balances")
    );
    ledger.pool.close().await;
    Ok(())
}

/// A `carry-owner release` takes effect at its own commit. The owner's guard
/// still holds the gate open until its next check, but every snapshot reads
/// this node's journal under `SETTLEMENT_LOCK`, which the release also
/// takes, so the next work built, and the payout state the refresh compares
/// with, are carry-free at once.
#[tokio::test]
async fn a_release_makes_work_carry_free_at_its_commit_while_the_gate_is_still_open() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "carry_release_", release_at_commit).await
}

async fn release_at_commit(url: String) -> Result<()> {
    let ledger = Ledger::connect(&url, "owner".into(), 4, true).await?;
    let pool = ledger.pool.clone();
    seed_local(&pool).await?;
    as_node_a(&ledger).await?;
    let canonical = canonical_balances(&pool).await?;
    let canonical_digest = prior_balances_digest(&canonical);
    // The owner: its journal claims ownership (`as_node_a`), and its guard
    // opened the gate.
    ensure!(
        ledger.set_carry_paying(true).await?,
        "opening the gate was not a change"
    );
    ensure!(
        ledger.snapshot(NETWORK_DIFFICULTY).await?.prior_balances == canonical,
        "the owner's work did not pay carried balances"
    );
    ensure!(
        ledger.payout_state().await?.prior_balances_digest == canonical_digest,
        "the owner's payout state is not the canonical balances"
    );
    // `carry-owner release` commits its row; the guard has not run since.
    let mut tx = ledger.settlement_transaction().await?;
    append_role(
        &mut tx,
        0,
        false,
        "release",
        "carry-owner release",
        &serde_json::json!({"reason": "handover", "tip_height": 100}),
    )
    .await?;
    tx.commit().await?;
    ensure!(ledger.carry_paying(), "the gate closed by itself");
    let snapshot = ledger.snapshot(NETWORK_DIFFICULTY).await?;
    ensure!(
        snapshot.prior_balances.is_empty(),
        "work built after the release paid carried balances: {:?}",
        snapshot.prior_balances
    );
    ensure!(
        ledger.payout_state().await?.prior_balances_digest == carry_free_prior_digest(),
        "the payout state after the release is not carry-free"
    );
    ledger.pool.close().await;
    Ok(())
}
