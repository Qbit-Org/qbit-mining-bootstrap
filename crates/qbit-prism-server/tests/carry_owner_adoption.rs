//! PRISM 3.1 dual writer, scenario S8 (CONTRACT.md §5; D-10, D-11), end to
//! end against a real PostgreSQL and the in-process fake qbit node: a pool
//! block is on the active chain, buried as deep as adoption waits
//! (`ADOPT_AFTER_CONFIRMATIONS`, about an hour), and this node holds none
//! of its landing rows, because the node that found it died before they
//! reached here. It holds what peer sync copied before the block was offered
//! (D-19): the prepared records on the block's parent, their template and
//! balance blobs, and the window's shares. `Coordinator::adoption_pass` finds
//! the block on the chain and `Coordinator::adopt_block` rebuilds its audit
//! from the prepared record it was found on and lands it through
//! `Ledger::land_adopted_block`.
//!
//! The coordinator is a dual-writer frontend, node B, on a database
//! personalised as node B, with a 1% pool fee. It publishes work on a
//! non-empty window (the fixture's shares and a small miner's below-floor
//! share), the template then churns (newer work on the same parent and
//! window, other transactions), and a miner solves the job it holds from the
//! first work, with its own extranonce2. The block is submitted, offered and
//! landed the ordinary way, and that landing is the expected result. Then
//! this node "never landed it": every row the landing and its confirmation
//! wrote, and the finder's outbox row and issued job, are deleted, children
//! before parents. The fake node serves the active chain as `RpcChain` and
//! `adopt_block` read it.
//!
//! The adopted rows must be the finder's rows, with direct coinbase payouts
//! and under CTV settlement alike, `prepared` until this node's reconciler
//! confirms them from the chain, after which the canonical balances are the
//! ones the finder's confirmation left. Adopting and confirming it credits
//! no share: the solving share was the finder's deferred row, which never
//! reached here. A block no held record rebuilds lands nothing and stays
//! reported at every pass.
//!
//! A block found on the peer's work (its coinbase's extranonce1 is node A's)
//! that this node landed is the peer's to sponsor: this node reserves CPFP
//! funding for its fanouts only once they are overdue for a takeover.
//!
//! ```text
//! PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
//!   cargo test --locked -p qbit-prism-server --test carry_owner_adoption
//! ```
use anyhow::{bail, ensure, Context, Result};
use qbit_prism::{AcceptedShare, FanoutFeeRatePolicy, PoolFeePolicy};
use qbit_prism_server::{
    broadcaster::sponsors_fanout,
    carry_owner::PeerJournal,
    codec,
    config::{DualWriterConfig, DEFAULT_PEER_SYNC_BATCH_ROWS, DEFAULT_PEER_SYNC_INTERVAL_MS},
    coordinator::{
        adoption::{Adoption, AdoptionState, ADOPT_AFTER_CONFIRMATIONS},
        Coordinator, JobContext, Prepared,
    },
    ledger::{
        bump_payout_revision, carry_free_prior_digest, coinbase_witness_reserved_value,
        BlockObservation, FanoutClaim, FanoutSponsor, IdentityCheck, Ledger, PreparedTemplate,
    },
    metrics::Metrics,
    node_identity::{NodeIdentity, NodeIndex},
    peer_sync::{PeerSyncPublisher, TableSyncStatus},
    stratum::{MiningBackend, MiningJob, Worker},
};
use qbit_prism_test_gate as gate;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[path = "support/fake_qbitd.rs"]
#[allow(dead_code)]
mod fake_qbitd;
#[path = "support/ledger_database.rs"]
#[allow(dead_code)]
mod ledger_database;
#[path = "support/window_fixture.rs"]
#[allow(dead_code)]
mod window_fixture;
use fake_qbitd::FakeNode;
use ledger_database::FixtureDatabase;

/// This frontend: node B, the non-owner, so its work is carry-free.
const NODE: NodeIdentity = NodeIdentity {
    node: NodeIndex::B,
    carry_owner: false,
};
/// The window fixture's shares: exactly this many fill the window.
const WINDOW_SHARES: u64 = 16;
/// A small miner's one share, appended after them. Its gross, about 4,950
/// sats, is below the payout floor of 14,720, so it accrues as carry and
/// its dust is swept into the pool fee; the oldest fixture share is then
/// only partly counted, and the window holds all seventeen.
const SMALL_SHARE_DIFFICULTY: u128 = 8;
const SMALL_PROGRAM: u8 = 0x5a;
/// The pool's fee, 1%, paid to its own program: what lets the dust be swept,
/// and the coinbase output `PoolRecognizer` also knows the pool's blocks by.
const POOL_FEE_BPS: u16 = 100;
const POOL_FEE_PROGRAM: u8 = 0xfe;
/// The fake node's stock tip, the parent of every block found here.
const PARENT_HEIGHT: u64 = 100;
const COINBASE_VALUE: u64 = 5_000_000_000;
/// The miner's assigned difficulty: every hash is a share, and the network
/// target of the fake node's bits passes about half of them.
const SHARE_DIFFICULTY: f64 = 1e-12;
/// A miner rolls its extranonce2; the block's coinbase carries it, and the
/// adopted audit must take it from the block, not the record's placeholder.
const EXTRANONCE2: &str = "a1b2c3d4e5f60718";

fn parent_hash() -> String {
    "ab".repeat(32)
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

/// How the block's coinbase pays the miners.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Settlement {
    /// One coinbase output per paid miner.
    Direct,
    /// CTV settlement: every paid miner through covenant fanouts, two per
    /// fanout transaction, so the landing writes a fanout set and three
    /// fanout artifacts.
    Ctv,
}

/// A dual-writer frontend as `qbit-prism-server` starts one, on a database
/// that `node-identity set` personalised, and the fake node it reads.
struct Fixture {
    chain: FakeNode,
    coordinator: Arc<Coordinator>,
    pool: PgPool,
}

impl Fixture {
    async fn open(url: String, instance: &str, settlement: Settlement) -> Result<Self> {
        let chain = FakeNode::open().await?;
        let mut config = fake_qbitd::coordinator_config(url, &chain, instance)?;
        // Only a template, revision or tip change may replace work, and the
        // fixed templates below must not age out on a slow host.
        config.snapshot_interval = Duration::from_secs(3600);
        config.template_max_age = Duration::from_secs(3600);
        config.health_timeout = Duration::from_secs(600);
        if settlement == Settlement::Ctv {
            config.ctv_enabled = true;
            config.ctv_direct_floor = u64::MAX;
            config.ctv_config.max_fanout_recipients_per_transaction = 2;
            config.ctv_fee = Some(FanoutFeeRatePolicy::new(1000, 12000));
        }
        config.payout_policy.pool_fee_policy = Some(PoolFeePolicy {
            fee_bps: POOL_FEE_BPS,
            recipient_id: "pool-fee".into(),
            order_key: "pool-fee".into(),
            p2mr_program_hex: format!("{POOL_FEE_PROGRAM:02x}").repeat(32),
        });
        // `Coordinator::new` gives the ledger this identity, as at startup.
        // The peer DSN is never dialled by the coordinator.
        config.dual_writer = Some(DualWriterConfig {
            identity: NODE,
            peer_database_url: "postgresql://prism_sync@peer.invalid/prism".into(),
            peer_database_url_fallback: None,
            peer_sync_interval: Duration::from_millis(DEFAULT_PEER_SYNC_INTERVAL_MS),
            peer_sync_batch_rows: DEFAULT_PEER_SYNC_BATCH_ROWS,
        });
        let coordinator = Coordinator::new(config, Arc::new(Metrics::default())).await?;
        let pool = coordinator.ledger.pool.clone();
        // The window's shares were written before this database became node
        // B's, so they keep origin 0: the peer's, as peer sync copies them.
        window_fixture::WindowPlan::new(WINDOW_SHARES)?
            .load(&pool, "adoption-window")
            .await?;
        coordinator
            .ledger
            .set_node_identity(NODE.node, "carry_owner_adoption")
            .await?;
        ensure_origin_index(&pool).await?;
        // The fixture's shares are the peer's, as peer sync would have
        // copied them: mark them pulled, as the sync's share cursor does, so
        // the window cut admits them (D-14).
        sqlx::query(
            "INSERT INTO qbit_prism_peer_sync_cursors(stream,peer_node,scanned_through,ingested_through) SELECT 'shares',$1,max(share_seq),max(share_seq) FROM qbit_share_ledger WHERE origin_node=$1 ON CONFLICT(stream) DO UPDATE SET peer_node=EXCLUDED.peer_node,scanned_through=EXCLUDED.scanned_through,ingested_through=EXCLUDED.ingested_through",
        )
        .bind(NODE.node.peer().index())
        .execute(&pool)
        .await?;
        ensure!(
            matches!(
                coordinator.ledger.check_node_identity(NODE.node).await?,
                IdentityCheck::Ready(_)
            ),
            "the database is not personalised as node {}",
            NODE.node
        );
        ensure!(
            coordinator.ledger.own_node() == Some(NODE.node.index())
                && !coordinator.ledger.carry_paying(),
            "the coordinator's ledger is not dual-writer node B with its carry gate closed"
        );
        // This node's own share, appended the way the share path appends one.
        coordinator
            .ledger
            .append(
                AcceptedShare {
                    share_seq: 0,
                    share_id: format!("small.rig:{}", "5a".repeat(32)),
                    miner_id: "small".into(),
                    order_key: "small".into(),
                    p2mr_program_hex: format!("{SMALL_PROGRAM:02x}").repeat(32),
                    share_difficulty: SMALL_SHARE_DIFFICULTY,
                    network_difficulty: window_fixture::FAKE_NODE_NETWORK_DIFFICULTY,
                    template_height: PARENT_HEIGHT,
                    job_id: "small-job".into(),
                    job_issued_at_ms: chrono::Utc::now().timestamp_millis() - 1_000,
                    accepted_at_ms: 0,
                    ntime: u32::try_from(chrono::Utc::now().timestamp())?,
                    credit_policy: None,
                },
                None,
            )
            .await?;
        Ok(Self {
            chain,
            coordinator,
            pool,
        })
    }

    /// Publish work on the stock parent with exactly `transactions`, as the
    /// coordinator's refresh does whenever the node's template changes.
    async fn publish(&self, transactions: &[Vec<u8>]) -> Result<Arc<Prepared>> {
        self.chain.set_template(Some(template(transactions)));
        self.coordinator.refresh_once().await?;
        self.coordinator
            .prepared
            .read()
            .await
            .clone()
            .context("the refresh published no work")
    }

    /// A miner's job on the published work, issued and persisted as the
    /// Stratum session does, on `extranonce1` from this node's half.
    async fn issue(&self, worker: &Worker, extranonce1: &str) -> Result<MiningJob<JobContext>> {
        let job = self
            .coordinator
            .build_job(worker, extranonce1, SHARE_DIFFICULTY, 0.0)
            .await?;
        self.coordinator
            .persist_issued_job(worker, &job, 0, Duration::from_secs(120))
            .await?;
        Ok(job)
    }

    async fn close(self) {
        self.coordinator.ledger.pool.close().await;
    }
}

/// The fake node's stock template on its stock tip, holding `transactions`.
fn template(transactions: &[Vec<u8>]) -> Value {
    let now = chrono::Utc::now().timestamp();
    json!({
        "height": PARENT_HEIGHT + 1,
        "coinbasevalue": COINBASE_VALUE,
        "previousblockhash": parent_hash(),
        "version": 0x2000_0000u32,
        "bits": fake_qbitd::TEMPLATE_BITS,
        "curtime": now,
        "mintime": now - 1,
        "transactions": transactions
            .iter()
            .map(|tx| json!({"data": hex::encode(tx)}))
            .collect::<Vec<_>>(),
    })
}

/// A pre-segwit transaction spending `tag` repeated: one input, one
/// `OP_TRUE` output; `locktime` tells otherwise equal ones apart.
fn legacy_tx(tag: u8, locktime: u32) -> Vec<u8> {
    let mut tx = vec![2, 0, 0, 0, 1];
    tx.extend([tag; 32]);
    tx.extend(0u32.to_le_bytes());
    tx.push(0);
    tx.extend(u32::MAX.to_le_bytes());
    tx.push(1);
    tx.extend(1_000u64.to_le_bytes());
    tx.extend([1, 0x51]);
    tx.extend(locktime.to_le_bytes());
    tx
}

/// The same shape with the segwit marker, flag and a one-item witness, so
/// its witness merkle leaf is not its txid.
fn segwit_tx(tag: u8) -> Vec<u8> {
    let mut tx = vec![2, 0, 0, 0, 0, 1, 1];
    tx.extend([tag; 32]);
    tx.extend(1u32.to_le_bytes());
    tx.push(0);
    tx.extend(u32::MAX.to_le_bytes());
    tx.push(1);
    tx.extend(2_000u64.to_le_bytes());
    tx.extend([1, 0x51]);
    tx.extend([1, 2, 0xaa, 0xbb]);
    tx.extend(0u32.to_le_bytes());
    tx
}

fn txid(tx: &[u8]) -> Result<String> {
    Ok(codec::hash_display(&codec::double_sha256(
        &codec::strip_witness_transaction(tx)?,
    )))
}

/// Search the job's nonces for a block, as a miner does, with
/// [`EXTRANONCE2`] rolled into the coinbase.
fn solve(job: &MiningJob<JobContext>) -> Result<codec::Submission> {
    ensure!(
        EXTRANONCE2.len() == job.wire.extranonce2_size * 2,
        "the fixture's extranonce2 does not fit the job"
    );
    let ntime = format!("{:08x}", job.wire.ntime);
    for nonce in 0..100_000u32 {
        let proof =
            job.wire
                .assemble_submission(EXTRANONCE2, &ntime, &format!("{nonce:08x}"), None, 0)?;
        if proof.block_pass {
            return Ok(proof);
        }
    }
    bail!("no block in a bounded nonce search")
}

fn header_parent(block: &[u8]) -> String {
    hex::encode(block[4..36].iter().rev().copied().collect::<Vec<_>>())
}

fn read_count(bytes: &[u8], at: &mut usize) -> Result<usize> {
    let first = *bytes.get(*at).context("truncated transaction")?;
    *at += 1;
    let width = match first {
        0xfd => 2,
        0xfe => 4,
        0xff => 8,
        value => return Ok(usize::from(value)),
    };
    let mut value = [0u8; 8];
    value[..width].copy_from_slice(bytes.get(*at..*at + width).context("truncated count")?);
    *at += width;
    Ok(usize::try_from(u64::from_le_bytes(value))?)
}

/// A transaction's outputs, value and script, as a decoder reads them.
fn outputs(tx: &[u8]) -> Result<Vec<(u64, Vec<u8>)>> {
    let mut at = 4;
    if tx.get(4) == Some(&0) && tx.get(5).is_some_and(|flag| *flag != 0) {
        at += 2;
    }
    for _ in 0..read_count(tx, &mut at)? {
        at += 36;
        let script = read_count(tx, &mut at)?;
        at += script + 4;
    }
    let mut outputs = Vec::new();
    for _ in 0..read_count(tx, &mut at)? {
        let value = u64::from_le_bytes(
            tx.get(at..at + 8)
                .context("truncated output value")?
                .try_into()?,
        );
        at += 8;
        let script = read_count(tx, &mut at)?;
        outputs.push((
            value,
            tx.get(at..at + script)
                .context("truncated output script")?
                .to_vec(),
        ));
        at += script;
    }
    Ok(outputs)
}

/// The decoded transaction `getrawtransaction <txid> true <block>` answers.
fn decoded(txid: &str, script_sig: &[u8], outputs: &[(u64, Vec<u8>)]) -> Value {
    json!({
        "txid": txid,
        "vin": [{"coinbase": hex::encode(script_sig), "sequence": u32::MAX}],
        "vout": outputs.iter().enumerate().map(|(n, (value, script))| json!({
            "value": *value as f64 / 1e8,
            "n": n,
            "scriptPubKey": {"hex": hex::encode(script)},
        })).collect::<Vec<_>>(),
    })
}

/// The active chain the fake node serves, by height.
struct Chain {
    hashes: Vec<String>,
}

impl Chain {
    fn tip_height(&self) -> u64 {
        self.hashes.len() as u64 - 1
    }

    fn tip(&self) -> &str {
        self.hashes.last().expect("a chain has a tip")
    }
}

/// Make the fake node's active chain run from its genesis to `block`, at
/// [`PARENT_HEIGHT`] + 1 on the stock tip, and `depth` blocks past it, and
/// answer every call `RpcChain`, `adopt_block` and the reconciler make about
/// it: `getbestblockhash` and the tip's `getblockheader`, `getblockhash` at
/// every height, each block's `getblockheader`, `getblock <hash> 1` and its
/// coinbase's `getrawtransaction <txid> true <hash>`, and the raw block,
/// `getblock <hash> 0`. Every other block is another miner's, with a
/// coinbase that carries neither the pool's tag nor its fee program.
fn serve_chain(node: &FakeNode, block: &[u8], depth: u64) -> Result<Chain> {
    ensure!(
        header_parent(block) == parent_hash(),
        "the block is not on the stock tip"
    );
    let height = PARENT_HEIGHT + 1;
    let block_hash = codec::hash_display(&codec::double_sha256(&block[..80]));
    let hashes: Vec<String> = (0..=height + depth)
        .map(|at| match at {
            0 => "00".repeat(32),
            at if at == PARENT_HEIGHT => parent_hash(),
            at if at == height => block_hash.clone(),
            at => format!("{:056x}{at:08x}", 0x0c4a1u64),
        })
        .collect();
    let tip_height = height + depth;
    for (at, hash) in hashes.iter().enumerate() {
        let at = at as u64;
        let previous = at.checked_sub(1).map(|below| &hashes[below as usize]);
        node.set_reply("getblockhash", json!([at]), json!(hash));
        node.set_reply(
            "getblockheader",
            json!([hash]),
            json!({"hash": hash, "height": at, "previousblockhash": previous, "confirmations": tip_height - at + 1}),
        );
        let (txids, coinbase_txid, coinbase) = if at == height {
            let coinbase = codec::coinbase_from_block(block)?;
            let coinbase_txid = txid(coinbase)?;
            let mut txids = vec![coinbase_txid.clone()];
            // The template's transactions follow the coinbase verbatim.
            let mut rest = &block[80..];
            let mut at_tx = 0;
            let count = read_count(rest, &mut at_tx)?;
            rest = &rest[at_tx + coinbase.len()..];
            for _ in 1..count {
                let tx_len = transaction_len(rest)?;
                txids.push(txid(&rest[..tx_len])?);
                rest = &rest[tx_len..];
            }
            ensure!(rest.is_empty(), "the block has trailing bytes");
            let decoded = decoded(
                &coinbase_txid,
                codec::coinbase_script_sig(coinbase)?,
                &outputs(coinbase)?,
            );
            node.set_reply("getblock", json!([hash, 0]), json!(hex::encode(block)));
            (txids, coinbase_txid, decoded)
        } else {
            let coinbase_txid = format!("{:056x}{at:08x}", 0x7c0b5u64);
            let mut script_sig = vec![3];
            script_sig.extend(&(at as u32).to_le_bytes()[..3]);
            script_sig.extend(b"/another-miner/");
            let mut payee = vec![0x52, 0x20];
            payee.extend([0x77; 32]);
            let decoded = decoded(&coinbase_txid, &script_sig, &[(COINBASE_VALUE, payee)]);
            (vec![coinbase_txid.clone()], coinbase_txid, decoded)
        };
        node.set_reply(
            "getblock",
            json!([hash, 1]),
            json!({"hash": hash, "height": at, "previousblockhash": previous, "confirmations": tip_height - at + 1, "tx": txids}),
        );
        node.set_reply(
            "getrawtransaction",
            json!([coinbase_txid, true, hash]),
            coinbase,
        );
    }
    let chain = Chain { hashes };
    node.set_tip(
        chain.tip(),
        &chain.hashes[chain.hashes.len() - 2],
        tip_height,
        &format!("{:x}", 0x100 + tip_height),
    );
    Ok(chain)
}

/// The byte length of the serialized transaction at the start of `bytes`.
fn transaction_len(bytes: &[u8]) -> Result<usize> {
    let mut at = 4;
    let witness = bytes.get(4) == Some(&0) && bytes.get(5).is_some_and(|flag| *flag != 0);
    if witness {
        at += 2;
    }
    let inputs = read_count(bytes, &mut at)?;
    for _ in 0..inputs {
        at += 36;
        let script = read_count(bytes, &mut at)?;
        at += script + 4;
    }
    for _ in 0..read_count(bytes, &mut at)? {
        at += 8;
        let script = read_count(bytes, &mut at)?;
        at += script;
    }
    if witness {
        for _ in 0..inputs {
            for _ in 0..read_count(bytes, &mut at)? {
                let item = read_count(bytes, &mut at)?;
                at += item;
            }
        }
    }
    at += 4;
    ensure!(at <= bytes.len(), "truncated transaction");
    Ok(at)
}

/// Every row a block's landing writes, by table, as it reads back, without
/// what two landings of the same block differ in by design: sequence values,
/// timestamps, and the chain state and broadcast bookkeeping this node's
/// reconciler and broadcaster own.
type Landing = BTreeMap<&'static str, Vec<Value>>;

const LANDING_ROWS: [(&str, &str); 7] = [
    (
        "qbit_pool_blocks",
        "SELECT to_jsonb(b)-'{found_at,chain_state,maturity_state,matured_at,disconnected_at,inactive_since,audit_publication_sequence,sync_seq}'::text[] FROM qbit_pool_blocks b WHERE block_hash=$1",
    ),
    (
        "qbit_pool_audit_bundles",
        "SELECT to_jsonb(a)-'created_at' FROM qbit_pool_audit_bundles a WHERE block_hash=$1",
    ),
    (
        "qbit_prism_audit_snapshots",
        "SELECT to_jsonb(s)-'created_at' FROM qbit_prism_audit_snapshots s JOIN qbit_pool_audit_bundles a ON a.share_snapshot_sha256=s.snapshot_sha256 WHERE a.block_hash=$1",
    ),
    (
        "qbit_pool_payout_entries",
        "SELECT to_jsonb(p)-'{payout_entry_seq,created_at}'::text[] FROM qbit_pool_payout_entries p WHERE block_hash=$1 ORDER BY miner_id,payout_order_key,p2mr_program,action",
    ),
    (
        "qbit_payout_carry_forward",
        "SELECT to_jsonb(c)-'{carry_forward_seq,created_at}'::text[] FROM qbit_payout_carry_forward c WHERE block_hash=$1 ORDER BY miner_id,payout_order_key,p2mr_program,action",
    ),
    (
        "qbit_ctv_fanout_sets",
        "SELECT to_jsonb(s)-'created_at' FROM qbit_ctv_fanout_sets s WHERE block_hash=$1",
    ),
    (
        "qbit_ctv_fanout_artifacts",
        "SELECT jsonb_build_object('fanout_txid',fanout_txid,'manifest_set_sha256',manifest_set_sha256,'manifest',manifest,'manifest_sha256',manifest_sha256,'precommitment_sha256',precommitment_sha256,'ctv_hash',ctv_hash,'chunk_index',chunk_index,'parent_coinbase_txid',parent_coinbase_txid,'parent_coinbase_vout',parent_coinbase_vout,'fanout_tx_hex',fanout_tx_hex,'settlement_status',settlement_status,'origin_node',origin_node) FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1 ORDER BY fanout_txid",
    ),
];

async fn landing(pool: &PgPool, hash: &str) -> Result<Landing> {
    let mut landing = Landing::new();
    for (table, rows) in LANDING_ROWS {
        landing.insert(
            table,
            sqlx::query_scalar(rows).bind(hash).fetch_all(pool).await?,
        );
    }
    Ok(landing)
}

/// Name the first table whose rows differ between two landings.
fn same_landing(adopted: &Landing, original: &Landing) -> Result<()> {
    for (table, rows) in original {
        let adopted = &adopted[table];
        ensure!(
            adopted == rows,
            "the adopted {table} rows differ from the finder's:\nadopted:  {}\noriginal: {}",
            json!(adopted),
            json!(rows)
        );
    }
    Ok(())
}

/// Every landing row's row version: a statement that rewrote one changes it.
async fn row_versions(pool: &PgPool, hash: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT 'block '||xmin::text FROM qbit_pool_blocks WHERE block_hash=$1 \
         UNION ALL SELECT 'audit '||xmin::text FROM qbit_pool_audit_bundles WHERE block_hash=$1 \
         UNION ALL SELECT 'payout '||payout_entry_seq||' '||xmin::text FROM qbit_pool_payout_entries WHERE block_hash=$1 \
         UNION ALL SELECT 'carry '||carry_forward_seq||' '||xmin::text FROM qbit_payout_carry_forward WHERE block_hash=$1 \
         ORDER BY 1",
    )
    .bind(hash)
    .fetch_all(pool)
    .await?)
}

/// What this node holds of a block anywhere a landing or an outbox writes.
async fn held_rows(pool: &PgPool, hash: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM qbit_pool_blocks WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_pool_audit_bundles WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_pool_payout_entries WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_payout_carry_forward WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_ctv_fanout_sets WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_prism_payout_divergences WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_block_candidate_outbox WHERE block_hash=$1) \
         + (SELECT count(*) FROM qbit_prism_deferred_shares WHERE block_hash=$1)",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

/// S8 on this node: the finder died before any of the block's landing rows
/// reached it. Everything the ordinary landing and confirmation wrote goes,
/// children before parents. The carry rows go while their block still
/// counts, so each deletion takes its delta back out of the balance summary.
/// So do the finder's own outbox row and issued job, which no peer copies.
/// The prepared records, their template and balance blobs and the window's
/// shares stay: peer sync copied them before the block was offered (D-19).
async fn forget_landing(pool: &PgPool, hash: &str, issued_job: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let snapshot: Option<String> = sqlx::query_scalar(
        "SELECT share_snapshot_sha256 FROM qbit_pool_audit_bundles WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    for statement in [
        "DELETE FROM qbit_ctv_fanout_broadcast_attempts WHERE fanout_txid IN (SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1)",
        "DELETE FROM qbit_prism_cpfp_packages WHERE fanout_txid IN (SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1)",
        "DELETE FROM qbit_prism_cpfp_retired_funding WHERE fanout_txid IN (SELECT fanout_txid FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1)",
        "DELETE FROM qbit_ctv_fanout_artifacts WHERE block_hash=$1",
        "DELETE FROM qbit_ctv_fanout_sets WHERE block_hash=$1",
        "DELETE FROM qbit_pool_payout_entries WHERE block_hash=$1",
        "DELETE FROM qbit_payout_carry_forward WHERE block_hash=$1",
        "DELETE FROM qbit_prism_payout_divergence_accounts WHERE block_hash=$1",
        "DELETE FROM qbit_prism_payout_divergences WHERE block_hash=$1",
        "DELETE FROM qbit_pool_audit_bundles WHERE block_hash=$1",
        "DELETE FROM qbit_pool_blocks WHERE block_hash=$1",
        "DELETE FROM qbit_prism_deferred_shares WHERE block_hash=$1",
        "DELETE FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    ] {
        sqlx::query(statement)
            .bind(hash)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("forgetting the landing: {statement}"))?;
    }
    if let Some(snapshot) = snapshot {
        sqlx::query("DELETE FROM qbit_prism_audit_snapshots s WHERE snapshot_sha256=$1 AND NOT EXISTS(SELECT 1 FROM qbit_pool_audit_bundles a WHERE a.share_snapshot_sha256=s.snapshot_sha256)")
            .bind(snapshot)
            .execute(&mut *tx)
            .await?;
    }
    ensure!(
        sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id=$1 AND template_sha256 IS NULL")
            .bind(issued_job)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1,
        "the finder's issued job {issued_job} was not held"
    );
    tx.commit().await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct BlockState {
    chain: String,
    maturity: String,
    ordinal: Option<i64>,
    origin: i16,
}

async fn block_state(pool: &PgPool, hash: &str) -> Result<BlockState> {
    let (chain, maturity, ordinal, origin) = sqlx::query_as(
        "SELECT chain_state,maturity_state,audit_publication_sequence,origin_node FROM qbit_pool_blocks WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?;
    Ok(BlockState {
        chain,
        maturity,
        ordinal,
        origin,
    })
}

/// `qbit_current_carry_forward_balances()`, by account.
async fn canonical_balances(pool: &PgPool) -> Result<Vec<(String, String, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT miner_id,payout_order_key,encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances() ORDER BY 1,2,3",
    )
    .fetch_all(pool)
    .await?)
}

async fn revision(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT payout_revision FROM qbit_prism_cluster WHERE singleton")
            .fetch_one(pool)
            .await?,
    )
}

/// A block's #478 record: its divergence rows and their account rows.
async fn divergence_rows(pool: &PgPool, hash: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM qbit_prism_payout_divergences WHERE block_hash=$1),(SELECT count(*) FROM qbit_prism_payout_divergence_accounts WHERE block_hash=$1)",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?)
}

async fn outbox_state(pool: &PgPool, hash: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT state FROM qbit_block_candidate_outbox WHERE block_hash=$1")
            .bind(hash)
            .fetch_optional(pool)
            .await?,
    )
}

/// The share ledger, summarised: its row count and a digest of every row.
async fn share_ledger(pool: &PgPool) -> Result<(i64, String)> {
    Ok(sqlx::query_as(
        "SELECT count(*),md5(COALESCE(string_agg(to_jsonb(l)::text,',' ORDER BY share_seq),'')) FROM qbit_share_ledger l",
    )
    .fetch_one(pool)
    .await?)
}

/// The outbox row's state and last error, for a failed expectation.
async fn outbox_report(pool: &PgPool, hash: &str) -> Result<Option<(String, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT state,last_error FROM qbit_block_candidate_outbox WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?)
}

/// The ledger's own checks of the balances and the block's evidence.
async fn ensure_consistent(pool: &PgPool, hash: &str, when: &str) -> Result<()> {
    let drift: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_carry_forward_current_drift()")
        .fetch_one(pool)
        .await?;
    ensure!(
        drift == 0,
        "{when}: the balance summary drifted from its recomputation on {drift} account(s)"
    );
    let mismatches: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM qbit_carry_forward_integrity_mismatches() WHERE block_hash=$1",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?;
    ensure!(
        mismatches == 0,
        "{when}: {mismatches} carry-forward integrity mismatch(es) for the block"
    );
    Ok(())
}

/// The adoption a pass or `adopt_block` reports for a block that landed
/// from `prepared`.
fn landed(hash: &str, prepared: &str) -> Adoption {
    Adoption::Landed {
        block_hash: hash.into(),
        prepared: prepared.into(),
    }
}

/// S8, the adoption itself: a pool block found on this node's first work
/// after its template had churned, landed the ordinary way, and then
/// forgotten. The adoption pass lands it from the record it was found on,
/// skipping the newer record on the same parent whose template holds other
/// transactions, with exactly the finder's rows, `prepared`; another pass
/// and another `adopt_block` rewrite nothing; and the reconciler confirms it
/// into the balances the finder's confirmation left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_block_whose_landing_rows_never_arrived_is_adopted_as_its_finder_landed_it(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "adoption_direct_", |url| {
        adopted_as_its_finder_landed_it(url, Settlement::Direct)
    })
    .await
}

/// The same under CTV settlement: the adopted landing also writes the
/// block's fanout set and every fanout artifact exactly as the finder's did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ctv_pool_block_whose_landing_rows_never_arrived_is_adopted_with_its_fanouts(
) -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "adoption_ctv_", |url| {
        adopted_as_its_finder_landed_it(url, Settlement::Ctv)
    })
    .await
}

async fn adopted_as_its_finder_landed_it(url: String, settlement: Settlement) -> Result<()> {
    let f = Fixture::open(url, "adoption-landed", settlement).await?;
    let result = adopted_as_landed(&f, settlement).await;
    f.close().await;
    result
}

async fn adopted_as_landed(f: &Fixture, settlement: Settlement) -> Result<()> {
    let c = &f.coordinator;
    let pool = &f.pool;
    let first = f.publish(&[legacy_tx(0x11, 1), segwit_tx(0x22)]).await?;
    ensure!(
        first.window.shares.map(|range| range.share_count) == Some(WINDOW_SHARES + 1)
            && first.window.prior_balances_digest == carry_free_prior_digest(),
        "the first work is not carry-free work on the fixture's window and the small share: {:?}",
        first.window
    );
    // A miner's session, from this node's half of the extranonce1 space, and
    // its job on the first work.
    let session = c.new_session_id().await?;
    ensure!(
        NODE.node.extranonce1_range().contains(&session.value()),
        "extranonce1 {:08x} is not node B's",
        session.value()
    );
    let worker = c.authorize("finder.rig").await?;
    let job = f
        .issue(&worker, &format!("{:08x}", session.value()))
        .await?;
    ensure!(
        job.context.prepared.storage_key == first.storage_key,
        "the job is not on the first work"
    );
    // The node's template churns: newer work on the same parent and window.
    let second = f.publish(&[legacy_tx(0x33, 2)]).await?;
    ensure!(
        second.storage_key != first.storage_key
            && second.window == first.window
            && second.template["previousblockhash"] == first.template["previousblockhash"]
            && PreparedTemplate::encode(&second.template)?.sha256()
                != PreparedTemplate::encode(&first.template)?.sha256(),
        "the churned template did not publish newer work on the same parent and window"
    );

    // The miner solves the job it holds, and the coordinator enqueues,
    // offers and lands the block the ordinary way.
    let proof = solve(&job)?;
    let hash = proof.block_hash_hex.clone();
    let block = hex::decode(&proof.block_hex)?;
    c.submit(&worker, &job, proof, false.into()).await?;
    let claim = c
        .ledger
        .claim_candidate(60)
        .await?
        .context("the found block was not enqueued")?;
    ensure!(
        claim.candidate.block_hash == hash,
        "another candidate was claimed"
    );
    f.chain.accept_blocks();
    c.process_candidate(&claim).await?;
    ensure!(
        outbox_state(pool, &hash).await?.as_deref() == Some("submitted"),
        "the offer did not land: outbox {:?}",
        outbox_report(pool, &hash).await?
    );
    let finder = block_state(pool, &hash).await?;
    ensure!(
        finder.chain == "confirmed" && finder.origin == NODE.node.index(),
        "the ordinary landing is {finder:?}"
    );
    let original = landing(pool, &hash).await?;
    let rows = |table: &str| original[table].len();
    // Five paid miners; under CTV settlement two per fanout transaction.
    let fanouts = match settlement {
        Settlement::Direct => 0,
        Settlement::Ctv => 3,
    };
    ensure!(
        rows("qbit_pool_blocks") == 1
            && rows("qbit_pool_audit_bundles") == 1
            && rows("qbit_prism_audit_snapshots") == 1
            && rows("qbit_payout_carry_forward") == 6
            && rows("qbit_pool_payout_entries") == 7
            && rows("qbit_ctv_fanout_sets") == usize::from(fanouts > 0)
            && rows("qbit_ctv_fanout_artifacts") == fanouts,
        "the ordinary landing wrote {:?} rows",
        original
            .iter()
            .map(|(table, rows)| (*table, rows.len()))
            .collect::<BTreeMap<_, _>>()
    );
    // Found on this node's work: its fanouts are this node's to sponsor.
    for artifact in &original["qbit_ctv_fanout_artifacts"] {
        let txid = artifact["fanout_txid"]
            .as_str()
            .context("a fanout artifact has no txid")?;
        ensure!(
            c.ledger.fanout_sponsor(txid).await? == Some(FanoutSponsor::Finder),
            "the finder does not sponsor its own fanout {txid}"
        );
    }
    // The block's coinbase commits to the audit the finder landed.
    let root = hex::encode(coinbase_witness_reserved_value(
        codec::coinbase_from_block(&block)?,
    )?);
    ensure!(
        original["qbit_pool_audit_bundles"][0]["audit_bundle"]["audit_commitment_root_hex"]
            == json!(root),
        "the landed audit's commitment root is not the block's witness reserved value {root}"
    );
    // Every fixture miner is paid on chain; the small miner's gross accrues,
    // and the finder's confirmation made it the one canonical balance.
    let accrued: Vec<&Value> = original["qbit_payout_carry_forward"]
        .iter()
        .filter(|row| row["action"] != "onchain")
        .collect();
    let small_carry = match accrued.as_slice() {
        [row]
            if row["miner_id"] == "small"
                && row["action"] == "accrued"
                && row["onchain_amount_sats"] == 0
                && row["carry_forward_balance_sats"]
                    .as_i64()
                    .is_some_and(|carry| carry > 0) =>
        {
            row["carry_forward_balance_sats"].to_string()
        }
        _ => bail!("the landing did not accrue exactly the small miner's gross: {accrued:?}"),
    };
    let balances = canonical_balances(pool).await?;
    ensure!(
        balances
            == [(
                "small".to_owned(),
                "small".to_owned(),
                format!("{SMALL_PROGRAM:02x}").repeat(32),
                small_carry.clone(),
            )],
        "the finder's confirmation left balances {balances:?}, not the small miner's {small_carry} sats"
    );
    let divergences = divergence_rows(pool, &hash).await?;
    ensure_consistent(pool, &hash, "the finder's landing").await?;

    // S8: this node never landed it.
    forget_landing(pool, &hash, &job.wire.job_id).await?;
    ensure!(
        held_rows(pool, &hash).await? == 0
            && c.ledger.block_rows_present(&hash).await? == (false, false),
        "the block's rows were not all forgotten"
    );
    ensure!(
        canonical_balances(pool).await?.is_empty(),
        "forgetting the only block left balances behind"
    );
    let held: Vec<String> = c
        .ledger
        .compact_prepared_on_parent(&parent_hash())
        .await?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    ensure!(
        held == [second.storage_key.clone(), first.storage_key.clone()],
        "the prepared records on the parent, newest first, are {held:?}"
    );
    let revision_before = revision(pool).await?;
    let shares_before = share_ledger(pool).await?;

    // One block short of the adoption depth the finder's rows may still
    // arrive: the pass reads the block and waits.
    serve_chain(&f.chain, &block, ADOPT_AFTER_CONFIRMATIONS - 1)?;
    let mut state = AdoptionState::default();
    let early = c.adoption_pass(&mut state).await?;
    ensure!(
        early.is_empty() && held_rows(pool, &hash).await? == 0,
        "a pass adopted a block {} deep: {early:?}",
        ADOPT_AFTER_CONFIRMATIONS - 1
    );
    // The next block buries it exactly as deep as adoption waits. While the
    // peer is reachable and its blocks are still being pulled, the rows may
    // be in flight: the pass adopts nothing.
    let chain = serve_chain(&f.chain, &block, ADOPT_AFTER_CONFIRMATIONS)?;
    let (sync, status) = PeerSyncPublisher::new();
    ensure!(
        c.peer_sync.set(status).is_ok(),
        "the fixture's coordinator already has a peer sync status"
    );
    sync.update(|status| {
        status.peer_reachable = true;
        status.own_log_caught_up = true;
        status.per_table.insert(
            "qbit_pool_blocks".into(),
            TableSyncStatus {
                lag_rows: 3,
                lag_seconds: 4.,
                last_success: Some(chrono::Utc::now()),
            },
        );
    });
    let held_back = c.adoption_pass(&mut state).await?;
    ensure!(
        held_back.is_empty() && held_rows(pool, &hash).await? == 0,
        "a pass adopted while the peer's blocks were being pulled: {held_back:?}"
    );
    // A refused or failing sync keeps publishing its last lag; once its last
    // successful pull is minutes old, the lag holds nothing back.
    sync.update(|status| {
        if let Some(blocks) = status.per_table.get_mut("qbit_pool_blocks") {
            blocks.last_success = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
        }
    });
    let adoptions = c.adoption_pass(&mut state).await?;
    ensure!(
        adoptions == [landed(&hash, &first.storage_key)],
        "the pass did not adopt the block from the record it was found on: {adoptions:?}"
    );
    let adopted = landing(pool, &hash).await?;
    same_landing(&adopted, &original)?;
    let state_after = block_state(pool, &hash).await?;
    ensure!(
        state_after
            == BlockState {
                chain: "prepared".into(),
                maturity: "immature".into(),
                ordinal: None,
                origin: NODE.node.index(),
            },
        "the adopted block is {state_after:?}, not this node's prepared block"
    );
    ensure!(
        revision(pool).await? == revision_before && canonical_balances(pool).await?.is_empty(),
        "a prepared landing moved the revision or the balances"
    );
    ensure!(
        outbox_state(pool, &hash).await?.is_none(),
        "adoption wrote an outbox row"
    );

    // Idempotent: the next pass, and a restarted frontend's first, find the
    // rows and land nothing; adopt_block on a block whose audit landed
    // meanwhile compares that audit and rewrites nothing.
    let versions = row_versions(pool, &hash).await?;
    let again = c.adoption_pass(&mut state).await?;
    ensure!(again.is_empty(), "a second pass reported {again:?}");
    let fresh = c.adoption_pass(&mut AdoptionState::default()).await?;
    ensure!(fresh.is_empty(), "a fresh pass reported {fresh:?}");
    let repeated = c.adopt_block(&hash).await?;
    ensure!(
        repeated == landed(&hash, &first.storage_key),
        "adopting the landed block again reported {repeated:?}"
    );
    ensure!(
        row_versions(pool, &hash).await? == versions,
        "adopting the landed block again rewrote its rows"
    );
    same_landing(&landing(pool, &hash).await?, &original)?;

    // The reconciler confirms it from the chain like any other block: one
    // first confirmation of this node's own block, one revision bump, and
    // the canonical balances the finder's confirmation left.
    let found = c.blocks.load(Ordering::Relaxed);
    c.reconcile(chain.tip(), chain.tip_height(), revision_before)
        .await?;
    let confirmed = block_state(pool, &hash).await?;
    ensure!(
        confirmed.chain == "confirmed" && confirmed.ordinal.is_some(),
        "the reconciler did not confirm the adopted block: {confirmed:?}"
    );
    ensure!(
        c.blocks.load(Ordering::Relaxed) == found + 1,
        "the confirmation was not counted as this node's first confirmation"
    );
    ensure!(
        revision(pool).await? == revision_before + 1,
        "the confirmation must bump the revision exactly once"
    );
    ensure!(
        canonical_balances(pool).await? == balances,
        "the adopted block's confirmation left balances {:?}, not the finder's {balances:?}",
        canonical_balances(pool).await?
    );
    ensure!(
        divergence_rows(pool, &hash).await? == divergences,
        "the adopted block's confirmation recorded another divergence"
    );
    // The solving share was the finder's deferred row: nothing here credits
    // it (the dead node's tail, D-11).
    ensure!(
        share_ledger(pool).await? == shares_before,
        "adopting and confirming the block changed the share ledger"
    );
    ensure_consistent(pool, &hash, "the adopted block's confirmation").await?;
    session.release().await
}

/// Reserving CPFP funding for `fanout`, which is the peer's to sponsor, is
/// refused and writes nothing.
async fn refused_reservation(
    c: &Coordinator,
    fanout: &FanoutClaim,
    funding: &str,
    when: &str,
) -> Result<()> {
    match c
        .ledger
        .reserve_cpfp_funding(fanout, "sponsor", funding, 0, 100_000)
        .await
    {
        Err(error) if format!("{error:#}").contains("found on the peer's work") => {}
        other => bail!("{when}: reserving the peer's fanout returned {other:?}"),
    }
    ensure!(
        c.ledger.cpfp_package(&fanout.fanout_txid).await?.is_none(),
        "{when}: a refused reservation wrote a CPFP package"
    );
    Ok(())
}

/// A block found on the peer's work is the peer's to sponsor, whichever
/// node landed it: here node B lands and matures a block whose coinbase
/// carries an extranonce1 from node A's half. B's ledger refuses to reserve
/// CPFP funding for its fanouts until they are overdue: the block matured,
/// and A's newest work B holds was published, `SPONSOR_TAKEOVER_AFTER` ago.
/// The broadcaster then takes one over only from a silent finder: A's own
/// database (a second fixture database) shows A's newest work as old, or has
/// not answered for a minute. A takeover, once started, is finished by this
/// node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fanout_found_on_the_peers_work_is_funded_here_only_through_a_takeover() -> Result<()> {
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    // Node A's own database, which B's broadcaster reads live.
    let peer = FixtureDatabase::open(&raw, "adoption_sponsor_peer_").await?;
    let peer_url = peer.url.clone();
    let result = run(&raw, "adoption_sponsor_", |url| {
        funded_only_through_a_takeover(url, peer_url)
    })
    .await;
    peer.close(result).await
}

async fn funded_only_through_a_takeover(url: String, peer_url: String) -> Result<()> {
    let peer = Ledger::connect(&peer_url, "adoption-peer".into(), 2, true).await?;
    let f = Fixture::open(url, "adoption-sponsor", Settlement::Ctv).await?;
    let result = takeover_funding(&f, &peer, &peer_url).await;
    f.close().await;
    peer.pool.close().await;
    result
}

async fn takeover_funding(f: &Fixture, peer: &Ledger, peer_url: &str) -> Result<()> {
    let c = &f.coordinator;
    let pool = &f.pool;
    f.publish(&[legacy_tx(0x11, 1), segwit_tx(0x22)]).await?;
    let peer_extranonce1 = NodeIndex::A.extranonce1_range().start() + 41;
    let worker = c.authorize("peer.rig").await?;
    let job = f.issue(&worker, &format!("{peer_extranonce1:08x}")).await?;
    let proof = solve(&job)?;
    let hash = proof.block_hash_hex.clone();
    c.submit(&worker, &job, proof, false.into()).await?;
    let claim = c
        .ledger
        .claim_candidate(60)
        .await?
        .context("the found block was not enqueued")?;
    f.chain.accept_blocks();
    c.process_candidate(&claim).await?;
    ensure!(
        outbox_state(pool, &hash).await?.as_deref() == Some("submitted"),
        "the offer did not land: outbox {:?}",
        outbox_report(pool, &hash).await?
    );
    let landed = block_state(pool, &hash).await?;
    ensure!(
        landed.chain == "confirmed" && landed.origin == NODE.node.index(),
        "the landing is {landed:?}"
    );
    // Matured by this node's reconciler, its fanouts are due.
    c.ledger
        .reconcile_blocks_at_revision(
            &[BlockObservation {
                block_hash: hash.clone(),
                active: true,
            }],
            PARENT_HEIGHT + 1 + qbit_prism::QBIT_COINBASE_MATURITY_BLOCKS,
            revision(pool).await?,
        )
        .await?;
    let fanout = c
        .ledger
        .claim_fanout(60)
        .await?
        .context("no fanout of the matured block is due")?;
    let txid = fanout.fanout_txid.clone();
    ensure!(
        fanout.block_hash == hash && c.ledger.fanout_sponsor(&txid).await?.is_none(),
        "node B would sponsor fanout {txid} of a block found on node A's work"
    );
    let funding = "55".repeat(32);
    refused_reservation(c, &fanout, &funding, "just matured").await?;
    ensure!(
        !sponsors_fanout(c, &txid).await?,
        "the broadcaster would sponsor a fanout found on the peer's work"
    );
    // Node A's work, as the sync copied it: published a moment ago.
    sqlx::query(
        "INSERT INTO qbit_prism_jobs SELECT (jsonb_populate_record(NULL::qbit_prism_jobs,to_jsonb(j)||jsonb_build_object('job_id','prepared:peer-work','origin_node',0,'created_at',clock_timestamp()))).* \
         FROM qbit_prism_jobs j WHERE j.job_id LIKE 'prepared:%' ORDER BY j.created_at LIMIT 1",
    )
    .execute(pool)
    .await?;
    // Its block matured over half an hour ago, but A's newest work held
    // here is a moment old: not overdue, and the ledger refuses.
    sqlx::query("UPDATE qbit_pool_blocks SET matured_at=matured_at-interval '31 minutes' WHERE block_hash=$1")
        .bind(&hash)
        .execute(pool)
        .await?;
    ensure!(
        c.ledger.fanout_sponsor(&txid).await?.is_none(),
        "a fanout whose finder published work a moment ago is overdue"
    );
    refused_reservation(c, &fanout, &funding, "the finder publishing work").await?;
    // A's newest work held here is half an hour old as well: overdue.
    sqlx::query("UPDATE qbit_prism_jobs SET created_at=clock_timestamp()-interval '31 minutes' WHERE job_id='prepared:peer-work'")
        .execute(pool)
        .await?;
    ensure!(
        c.ledger.fanout_sponsor(&txid).await? == Some(FanoutSponsor::Overdue),
        "a fanout whose block matured, and whose finder last published work, half an hour ago is not overdue"
    );
    // A's own database answers, and shows work A published a moment ago,
    // though this node's copy is stale: A is alive, and keeps its fanout.
    sqlx::query(
        "INSERT INTO qbit_prism_jobs(job_id,instance_id,parent_hash,payout_revision,payload,expires_at,origin_node) \
         VALUES('prepared:live-work','adoption-peer',repeat('ab',32),0,'{}'::jsonb,clock_timestamp()+interval '1 hour',0)",
    )
    .execute(&peer.pool)
    .await?;
    let live_view = || PeerJournal::new(&[peer_url.to_owned()], Duration::from_secs(30));
    c.finder_liveness.use_journal(live_view()?).await;
    ensure!(
        !sponsors_fanout(c, &txid).await?,
        "a finder whose own database shows fresh work was taken over"
    );
    // A's own database shows that work half an hour old: A is silent (a
    // fresh view, as once the last verdict has expired).
    sqlx::query("UPDATE qbit_prism_jobs SET created_at=clock_timestamp()-interval '31 minutes' WHERE job_id='prepared:live-work'")
        .execute(&peer.pool)
        .await?;
    c.finder_liveness.use_journal(live_view()?).await;
    ensure!(
        sponsors_fanout(c, &txid).await?,
        "a finder whose own database shows no work for half an hour was not taken over"
    );
    // A's database does not answer at all. A first failed check may be a
    // blip: not silent yet. Once the checks have failed for a minute, the
    // stale copy here decides.
    c.finder_liveness
        .use_journal(PeerJournal::new(
            &["postgresql://prism@127.0.0.1:1/unreachable".to_owned()],
            Duration::from_millis(500),
        )?)
        .await;
    ensure!(
        !sponsors_fanout(c, &txid).await?,
        "one failed check of the finder's database handed its fanout over"
    );
    c.finder_liveness
        .assume_unreachable_since(std::time::Instant::now() - Duration::from_secs(61))
        .await;
    ensure!(
        sponsors_fanout(c, &txid).await?,
        "an overdue fanout whose finder's database has not answered for a minute was not taken over"
    );
    // So the broadcaster takes the fanout over and reserves its funding.
    ensure!(
        c.ledger
            .reserve_cpfp_funding(&fanout, "sponsor", &funding, 0, 100_000)
            .await?,
        "the takeover's reservation was not written"
    );
    let package = c
        .ledger
        .cpfp_package(&txid)
        .await?
        .context("the takeover's reservation wrote no CPFP package")?;
    ensure!(
        package["funding_txid"] == json!(funding) && package["wallet_name"] == "sponsor",
        "the takeover reserved {package}"
    );
    // A takeover this node started is finished by it, even once the peer
    // publishes again.
    sqlx::query(
        "UPDATE qbit_prism_jobs SET created_at=clock_timestamp() WHERE job_id='prepared:peer-work'",
    )
    .execute(pool)
    .await?;
    ensure!(
        c.ledger.fanout_sponsor(&txid).await? == Some(FanoutSponsor::Held)
            && sponsors_fanout(c, &txid).await?,
        "a takeover with its package held was not held"
    );
    Ok(())
}

/// S8 with no adoptable record: with the record the block was found on
/// gone, a held record on the same parent whose template holds other
/// transactions, and one with the block's transactions whose window is not
/// the one the block's coinbase commits to, are each refused for that
/// reason; with no record left on the parent, none is tried. Nothing lands,
/// and every pass reports the block again. Control: once its record is back,
/// the same block adopts from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_block_no_held_prepared_record_rebuilds_is_reported_and_lands_nothing() -> Result<()>
{
    let Some(raw) = gate::database_url(gate::site!())? else {
        return Ok(());
    };
    run(&raw, "adoption_refused_", refused_without_its_record).await
}

async fn refused_without_its_record(url: String) -> Result<()> {
    let f = Fixture::open(url, "adoption-refused", Settlement::Direct).await?;
    let result = refused(&f).await;
    f.close().await;
    result
}

async fn refused(f: &Fixture) -> Result<()> {
    let c = &f.coordinator;
    let pool = &f.pool;
    let transactions = [legacy_tx(0x11, 1), segwit_tx(0x22)];
    let found_on = f.publish(&transactions).await?;
    let session = c.new_session_id().await?;
    let worker = c.authorize("finder.rig").await?;
    let job = f
        .issue(&worker, &format!("{:08x}", session.value()))
        .await?;
    // Found by the peer and never offered here: this node only ever sees it
    // on the chain.
    let proof = solve(&job)?;
    let hash = proof.block_hash_hex.clone();
    let block = hex::decode(&proof.block_hex)?;

    // A settlement elsewhere bumps the payout revision: the work is rebuilt
    // on the same parent and template, on a newly anchored window.
    let mut tx = c.ledger.settlement_transaction().await?;
    bump_payout_revision(&mut tx).await?;
    tx.commit().await?;
    let reanchored = f.publish(&transactions).await?;
    ensure!(
        reanchored.storage_key != found_on.storage_key
            && reanchored.window != found_on.window
            && reanchored.template["previousblockhash"] == found_on.template["previousblockhash"],
        "the revision bump did not rebuild the work on another window on the same parent"
    );
    // And the template churns: the same window, other transactions.
    let churned = f.publish(&[legacy_tx(0x33, 2)]).await?;
    ensure!(
        churned.storage_key != reanchored.storage_key && churned.window == reanchored.window,
        "the churned template did not publish newer work on the same window"
    );

    serve_chain(&f.chain, &block, ADOPT_AFTER_CONFIRMATIONS)?;
    let revision_before = revision(pool).await?;
    let parent = parent_hash();
    // Kept aside to put back for the control.
    let kept: Value =
        sqlx::query_scalar("SELECT to_jsonb(j) FROM qbit_prism_jobs j WHERE job_id=$1")
            .bind(&found_on.storage_key)
            .fetch_one(pool)
            .await?;
    ensure!(
        sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id=$1")
            .bind(&found_on.storage_key)
            .execute(pool)
            .await?
            .rows_affected()
            == 1,
        "the block's own record was not held"
    );

    // Two records on the parent, neither the block's.
    let refused = c.adopt_block(&hash).await?;
    let Adoption::NoRecord { block_hash, reason } = &refused else {
        bail!("a block with no record of its own was adopted: {refused:?}");
    };
    ensure!(*block_hash == hash, "the refusal names {block_hash}");
    let root = hex::encode(coinbase_witness_reserved_value(
        codec::coinbase_from_block(&block)?,
    )?);
    for expected in [
        format!(
            "{}: its template does not hold the block's transactions",
            churned.storage_key
        ),
        format!("{}: rebuilt commitment root ", reanchored.storage_key),
        format!(" is not the block's {root}"),
    ] {
        ensure!(
            reason.contains(&expected),
            "the refusal does not say {expected:?}: {reason}"
        );
    }
    ensure!(
        held_rows(pool, &hash).await? == 0,
        "a refused adoption landed rows"
    );
    // Each pass reports it again and lands nothing (no retry interval here).
    let mut state = AdoptionState::retrying_every(std::time::Duration::ZERO);
    for pass in 1..=2 {
        let adoptions = c.adoption_pass(&mut state).await?;
        ensure!(
            adoptions == [refused.clone()],
            "pass {pass} reported {adoptions:?}, not the refusal"
        );
        ensure!(
            held_rows(pool, &hash).await? == 0,
            "pass {pass} landed rows"
        );
    }

    // No record at all on the parent: none is tried.
    sqlx::query("DELETE FROM qbit_prism_jobs WHERE job_id=ANY($1)")
        .bind(vec![
            reanchored.storage_key.clone(),
            churned.storage_key.clone(),
        ])
        .execute(pool)
        .await?;
    ensure!(
        c.adopt_block(&hash).await?
            == Adoption::NoRecord {
                block_hash: hash.clone(),
                reason: format!("no prepared record on parent {parent} is held here"),
            },
        "a block with no record on its parent was not refused as such"
    );
    let adoptions = c.adoption_pass(&mut state).await?;
    ensure!(
        matches!(adoptions.as_slice(), [Adoption::NoRecord { block_hash, .. }] if *block_hash == hash),
        "the pass reported {adoptions:?}"
    );
    ensure!(
        held_rows(pool, &hash).await? == 0 && revision(pool).await? == revision_before,
        "a refused adoption landed rows or moved the revision"
    );

    // Control: with the block's own record back, the next pass adopts it.
    sqlx::query(
        "INSERT INTO qbit_prism_jobs SELECT * FROM jsonb_populate_record(NULL::qbit_prism_jobs,$1)",
    )
    .bind(&kept)
    .execute(pool)
    .await?;
    let adoptions = c.adoption_pass(&mut state).await?;
    ensure!(
        adoptions == [landed(&hash, &found_on.storage_key)],
        "the block did not adopt once its record was back: {adoptions:?}"
    );
    let adopted = block_state(pool, &hash).await?;
    ensure!(
        adopted.chain == "prepared" && adopted.origin == NODE.node.index(),
        "the adopted block is {adopted:?}"
    );
    ensure!(
        c.adoption_pass(&mut state).await?.is_empty(),
        "the adopted block is still reported"
    );
    session.release().await
}

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
