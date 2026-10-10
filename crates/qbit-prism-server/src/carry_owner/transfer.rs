//! The `carry-owner` operator commands: `status`, `release` and `transfer`
//! (docs/prism-ledger-ops.md, "Carry owner").
//!
//! Moving the carry owner is deliberate and guarded, never automatic:
//!
//! 1. `release` on the owner appends a `release` row to its journal, with the
//!    tip read under the lock, and bumps its payout revision, in one
//!    transaction under `SETTLEMENT_LOCK`. Work built after it commits is
//!    carry-free at once, because every snapshot reads this node's journal
//!    under that lock; its guard closes the carry gate at its next check.
//! 2. `transfer` on the other node appends an `acquire` row, and only when:
//!    - the peer answers, and its latest row is not an ownership claim, so it
//!      cannot still act as owner;
//!    - the chain has moved at least the orphan-confirmation depth past the
//!      peer's release, so no block on the peer's carry-paying work can still
//!      land without a reorganisation that deep;
//!    - a scan of the active chain finds every pool block landed and
//!      confirmed in this node's ledger, so its balances include every carry
//!      the old owner paid. The scan starts no higher than the tip the
//!      peer's last claim of ownership (an `acquire`) recorded, and at 0 for
//!      a peer that owned since its seed, which records none, so it covers
//!      every block the peer may have found while it paid.
//!
//!    The `acquire` records the peer row it was checked against, which the
//!    guard requires of a claim facing a release, and the peer's latest row
//!    is copied into this node's journal with it, so a peer rebuilt from this
//!    node's database gets its release back.
//! 3. The operator then sets `PRISM_CARRY_OWNER` on both nodes to match and
//!    restarts their frontends; until then each guard reports
//!    `setting_pending` and builds carry-free work.
use super::*;
use crate::rpc::Rpc;
use anyhow::{bail, ensure};
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::future::Future;

/// The chain the scan reads: its tip, and each height's block and coinbase.
pub trait ChainSource: Sync {
    /// The active tip's height and hash.
    fn tip(&self) -> impl Future<Output = Result<(u64, String)>> + Send;
    /// The active chain's block hash at `height`.
    fn block_hash(&self, height: u64) -> impl Future<Output = Result<String>> + Send;
    /// The active chain's block at `height` and its coinbase.
    fn coinbase(&self, height: u64) -> impl Future<Output = Result<CoinbaseView>> + Send;
    /// [`ChainSource::coinbase`] with the block's parent hash, when the source
    /// learns it from the same read. A walk checks that the blocks it read
    /// link up, so a read that spans a reorganisation is never taken for a
    /// chain. A source that cannot tell (`None`) is trusted to be consistent.
    fn linked_coinbase(
        &self,
        height: u64,
    ) -> impl Future<Output = Result<(CoinbaseView, Option<String>)>> + Send {
        async move { Ok((self.coinbase(height).await?, None)) }
    }
}

/// One block a walk read: its hash, its parent's when known, and whether it
/// is a pool block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalkedBlock {
    pub hash: String,
    pub parent: Option<String>,
    pub pool: bool,
}

/// The lowest height above `from_height` whose block does not name the block
/// read below it as its parent: a read that spanned a reorganisation.
pub fn first_link_break(blocks: &BTreeMap<u64, WalkedBlock>, from_height: u64) -> Option<u64> {
    blocks
        .iter()
        .zip(blocks.iter().skip(1))
        .find(|((low_height, low), (high_height, high))| {
            **low_height >= from_height
                && **high_height == **low_height + 1
                && high
                    .parent
                    .as_ref()
                    .is_some_and(|parent| *parent != low.hash)
        })
        .map(|(_, (high_height, _))| *high_height)
}

/// What the scan needs of one block's coinbase.
#[derive(Clone, Debug)]
pub struct CoinbaseView {
    pub block_hash: String,
    pub script_sig: Vec<u8>,
    pub output_scripts: Vec<Vec<u8>>,
}

/// How the scan recognises the pool's blocks: the coinbase tag in the
/// scriptSig, or the pool-fee program among the outputs. Either is enough,
/// so a block is missed only if it carries neither: a false positive (a
/// foreign block with the same tag) refuses a transfer, a false negative
/// could double-pay.
#[derive(Clone, Debug)]
pub struct PoolRecognizer {
    /// Coinbase tags, the current one first.
    pub tags: Vec<Vec<u8>>,
    /// P2MR scriptPubKeys of pool-fee programs, `OP_2 <32 bytes>`.
    pub fee_scripts: Vec<Vec<u8>>,
}

impl PoolRecognizer {
    /// The pool's current markers: its coinbase tag and pool-fee program.
    pub fn new(tag: &str, fee_program_hex: Option<&str>) -> Result<Self> {
        Self {
            tags: Vec::new(),
            fee_scripts: Vec::new(),
        }
        .also(
            &[tag.to_owned()],
            fee_program_hex
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
                .as_slice(),
        )
    }

    /// Also the markers the pool used before (`carry-owner transfer
    /// --also-tag` and `--also-fee-program`): blocks found under an earlier
    /// tag or fee recipient are pool blocks too.
    pub fn also(mut self, tags: &[String], fee_programs_hex: &[String]) -> Result<Self> {
        for tag in tags {
            ensure!(!tag.is_empty(), "a coinbase tag is empty");
            self.tags.push(tag.as_bytes().to_vec());
        }
        for hex in fee_programs_hex.iter().filter(|hex| !hex.is_empty()) {
            let program = hex::decode(hex).context("a pool-fee program is not hex")?;
            ensure!(program.len() == 32, "a pool-fee program is not 32 bytes");
            self.fee_scripts
                .push([&[0x52, 0x20][..], &program].concat());
        }
        Ok(self)
    }

    pub fn is_pool_block(&self, coinbase: &CoinbaseView) -> bool {
        self.tags.iter().any(|tag| {
            coinbase
                .script_sig
                .windows(tag.len())
                .any(|window| window == tag.as_slice())
        }) || self
            .fee_scripts
            .iter()
            .any(|fee| coinbase.output_scripts.iter().any(|script| script == fee))
    }
}

/// A pool block's landing in this node's ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Landing {
    /// Landed and confirmed: its rows count in the balances.
    Confirmed,
    /// Landed, but the local reconciler has not confirmed it (yet).
    Unconfirmed,
    /// No landing rows at all.
    Missing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ScannedBlock {
    pub height: u64,
    pub block_hash: String,
    pub landing: Landing,
}

/// The result of one scan of the active chain.
#[derive(Clone, Debug, Serialize)]
pub struct ChainScan {
    pub from_height: u64,
    pub tip_height: u64,
    pub tip_hash: String,
    pub scanned_blocks: u64,
    pub pool_blocks: Vec<ScannedBlock>,
}

impl ChainScan {
    pub fn unresolved(&self) -> impl Iterator<Item = &ScannedBlock> {
        self.pool_blocks
            .iter()
            .filter(|block| block.landing != Landing::Confirmed)
    }
}

/// How many coinbases the scan reads at once.
const SCAN_CONCURRENCY: usize = 16;

/// The blocks one walk of the active chain read, by height, from the first
/// height scanned to the tip: a linked chain ending at `tip_hash`.
#[derive(Clone, Debug)]
pub struct ChainWalk {
    pub tip_height: u64,
    pub tip_hash: String,
    pub blocks: BTreeMap<u64, WalkedBlock>,
}

/// Read `heights` of the active chain, recognising pool blocks, a bounded
/// number at a time.
pub(crate) async fn read_blocks<C: ChainSource>(
    chain: &C,
    heights: std::ops::RangeInclusive<u64>,
    recognizer: &PoolRecognizer,
) -> Result<Vec<(u64, WalkedBlock)>> {
    stream::iter(heights)
        .map(|height| async move {
            let (coinbase, parent) = chain.linked_coinbase(height).await?;
            Ok::<_, anyhow::Error>((
                height,
                WalkedBlock {
                    pool: recognizer.is_pool_block(&coinbase),
                    hash: coinbase.block_hash,
                    parent,
                },
            ))
        })
        .buffered(SCAN_CONCURRENCY)
        .try_collect()
        .await
}

/// Walk the active chain from `from_height` to its tip, recognising pool
/// blocks. It ends only on a walk whose blocks link up, whose top block is
/// the tip it read, and whose tip did not move meanwhile: the walked range is
/// then exactly that tip's ancestry. A read that spans a reorganisation is
/// read again from below the break; after a tip move the walk keeps the
/// highest block read that is still active, and everything below it, which
/// the links prove to be its ancestry.
pub async fn walk_chain<C: ChainSource>(
    chain: &C,
    from_height: u64,
    recognizer: &PoolRecognizer,
) -> Result<ChainWalk> {
    let mut blocks: BTreeMap<u64, WalkedBlock> = BTreeMap::new();
    let (mut tip_height, mut tip_hash) = chain.tip().await?;
    let mut next = from_height;
    loop {
        if next <= tip_height {
            blocks.extend(read_blocks(chain, next..=tip_height, recognizer).await?);
        }
        let top_is_tip = tip_height < from_height
            || blocks
                .get(&tip_height)
                .is_some_and(|block| block.hash == tip_hash);
        if let Some(broken) = first_link_break(&blocks, from_height).or_else(|| {
            // A top block other than the tip read is a break at the top.
            (!top_is_tip).then_some(tip_height)
        }) {
            // Either side of the break may be stale: read both again.
            let again = broken.saturating_sub(1).max(from_height);
            blocks.retain(|height, _| *height < again);
            next = again;
            (tip_height, tip_hash) = chain.tip().await?;
            continue;
        }
        let (now_height, now_hash) = chain.tip().await?;
        if now_hash == tip_hash {
            break;
        }
        // The highest block read that is still on the active chain; the
        // links make everything below it its ancestry.
        let mut keep = None;
        let mut height = tip_height.min(now_height);
        while height >= from_height {
            if let Some(block) = blocks.get(&height) {
                if block.hash == chain.block_hash(height).await? {
                    keep = Some(height);
                    break;
                }
            }
            if height == 0 {
                break;
            }
            height -= 1;
        }
        blocks.retain(|height, _| keep.is_some_and(|keep| *height <= keep));
        next = keep.map_or(from_height, |keep| keep + 1);
        tip_height = now_height;
        tip_hash = now_hash;
    }
    Ok(ChainWalk {
        tip_height,
        tip_hash,
        blocks,
    })
}

/// Scan the active chain from `from_height` to its tip for pool blocks and
/// report each one's landing in `ledger`: confirmed (landed and counted),
/// unconfirmed, or missing (no `qbit_pool_blocks` row or no audit).
pub async fn scan_chain<C: ChainSource>(
    chain: &C,
    ledger: &Ledger,
    from_height: u64,
    recognizer: &PoolRecognizer,
) -> Result<ChainScan> {
    let walk = walk_chain(chain, from_height, recognizer).await?;
    let pool: Vec<(u64, String)> = walk
        .blocks
        .iter()
        .filter(|(_, block)| block.pool)
        .map(|(height, block)| (*height, block.hash.clone()))
        .collect();
    let hashes: Vec<&str> = pool.iter().map(|(_, hash)| hash.as_str()).collect();
    let rows: Vec<(String, String, bool)> = sqlx::query_as("SELECT block.block_hash,block.chain_state,EXISTS(SELECT 1 FROM qbit_pool_audit_bundles audit WHERE audit.block_hash=block.block_hash) FROM qbit_pool_blocks block WHERE block.block_hash=ANY($1::text[])")
        .bind(&hashes)
        .fetch_all(&mut *ledger.acquire().await?)
        .await?;
    let landed: BTreeMap<String, (String, bool)> = rows
        .into_iter()
        .map(|(hash, state, audited)| (hash, (state, audited)))
        .collect();
    let pool_blocks = pool
        .into_iter()
        .map(|(height, block_hash)| {
            let landing = match landed.get(&block_hash) {
                Some((state, true)) if state == "confirmed" => Landing::Confirmed,
                Some((_, true)) => Landing::Unconfirmed,
                _ => Landing::Missing,
            };
            ScannedBlock {
                height,
                block_hash,
                landing,
            }
        })
        .collect();
    Ok(ChainScan {
        from_height,
        tip_height: walk.tip_height,
        tip_hash: walk.tip_hash,
        scanned_blocks: walk.blocks.len() as u64,
        pool_blocks,
    })
}

/// The node's chain through its RPC.
pub struct RpcChain<'a>(pub &'a Rpc);

impl ChainSource for RpcChain<'_> {
    async fn tip(&self) -> Result<(u64, String)> {
        let hash = self.0.call("getbestblockhash", json!([])).await?;
        let hash = hash.as_str().context("getbestblockhash returned no hash")?;
        let header = self.0.call("getblockheader", json!([hash])).await?;
        let height = header["height"]
            .as_u64()
            .context("getblockheader returned no height")?;
        Ok((height, hash.to_owned()))
    }

    async fn block_hash(&self, height: u64) -> Result<String> {
        Ok(self
            .0
            .call("getblockhash", json!([height]))
            .await?
            .as_str()
            .context("getblockhash returned no hash")?
            .to_owned())
    }

    async fn coinbase(&self, height: u64) -> Result<CoinbaseView> {
        Ok(self.linked_coinbase(height).await?.0)
    }

    async fn linked_coinbase(&self, height: u64) -> Result<(CoinbaseView, Option<String>)> {
        let block_hash = self.block_hash(height).await?;
        let block = self.0.call("getblock", json!([block_hash, 1])).await?;
        let parent = block["previousblockhash"].as_str().map(str::to_owned);
        let txid = block["tx"][0]
            .as_str()
            .context("getblock returned no coinbase")?;
        let tx = match self
            .0
            .call("getrawtransaction", json!([txid, true, block_hash]))
            .await
        {
            Ok(tx) => tx,
            // A node that cannot look a transaction up by block still returns
            // the decoded block.
            Err(_) => self.0.call("getblock", json!([block_hash, 2])).await?["tx"][0].clone(),
        };
        Ok((coinbase_view(block_hash, &tx)?, parent))
    }
}

/// The scan's view of a decoded coinbase transaction.
pub fn coinbase_view(block_hash: String, tx: &Value) -> Result<CoinbaseView> {
    let script_sig = hex::decode(
        tx["vin"][0]["coinbase"]
            .as_str()
            .context("the first transaction is not a coinbase")?,
    )?;
    let output_scripts = tx["vout"]
        .as_array()
        .context("the coinbase has no outputs")?
        .iter()
        .map(|output| {
            Ok(hex::decode(
                output["scriptPubKey"]["hex"]
                    .as_str()
                    .context("a coinbase output has no script")?,
            )?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CoinbaseView {
        block_hash,
        script_sig,
        output_scripts,
    })
}

/// One named check of `transfer` or `release`.
#[derive(Clone, Debug, Serialize)]
pub struct Check {
    pub check: &'static str,
    pub ok: bool,
    pub detail: String,
}

fn check(check: &'static str, ok: bool, detail: impl Into<String>) -> Check {
    Check {
        check,
        ok,
        detail: detail.into(),
    }
}

/// Everything `transfer` decides from.
#[derive(Clone, Debug)]
pub struct TransferFacts {
    pub node_index: i16,
    pub database_node: Option<i16>,
    pub own: Option<RoleRow>,
    pub peer_read: PeerRead,
    /// The peer's latest row in this node's synced copy of its journal.
    pub peer_synced: Option<RoleRow>,
    /// The peer's latest claim of ownership, the newer of its live journal's
    /// and the synced copy's.
    pub peer_claim: Option<RoleRow>,
    /// Whether the peer's live journal answered for its claims too.
    pub peer_claim_read: bool,
    pub tip_height: u64,
    /// How far past the peer's release the tip must be: the orphan
    /// confirmation depth.
    pub release_depth: u64,
    pub scan: ChainScan,
}

/// `transfer`'s checks, every one evaluated. The transfer may proceed only
/// when all pass.
pub fn transfer_checks(facts: &TransferFacts) -> Vec<Check> {
    let mut checks = vec![check(
        "node_identity",
        facts.database_node == Some(facts.node_index),
        format!(
            "PRISM_NODE_INDEX is {}; the database's node identity is {}",
            facts.node_index,
            facts
                .database_node
                .map_or("unset".to_owned(), |node| node.to_string())
        ),
    )];
    checks.push(match &facts.own {
        Some(own) if own.carry_owner => check(
            "this_node_not_owner",
            false,
            format!(
                "this node already holds ownership (epoch {}, {})",
                own.epoch, own.action
            ),
        ),
        Some(own) => check(
            "this_node_not_owner",
            true,
            format!(
                "this node's latest row: epoch {}, {}",
                own.epoch, own.action
            ),
        ),
        None => check(
            "this_node_not_owner",
            true,
            "this node has no journal row yet",
        ),
    });
    match &facts.peer_read {
        PeerRead::Failed => checks.push(check(
            "peer_answered",
            false,
            "the peer's journal could not be read, so it may still act as owner",
        )),
        PeerRead::Answered { peer, own_at_peer } => {
            checks.push(check(
                "peer_answered",
                true,
                "the peer's journal was read live",
            ));
            // The newest of the peer's rows this node can see, live or
            // synced, as the guard decides: a peer restored behind the copy
            // this node holds must not pass on its older release. Its live
            // journal must not claim ownership either.
            let live = peer.as_ref();
            let peer = [live, facts.peer_synced.as_ref()]
                .into_iter()
                .flatten()
                .max_by_key(|row| row.epoch);
            // The live row and this node's copy of it at one epoch disagree:
            // a peer sync conflict, whichever of them claims ownership.
            let conflict = live
                .zip(facts.peer_synced.as_ref())
                .filter(|(live, synced)| live.epoch == synced.epoch && live != synced);
            checks.push(match peer {
                None => check(
                    "peer_not_owner",
                    false,
                    "the peer has no journal row: it has never run in dual-writer mode",
                ),
                Some(_) if conflict.is_some() => check(
                    "peer_not_owner",
                    false,
                    format!(
                        "the peer's live journal and this node's copy of it disagree at epoch {}: a peer sync conflict to investigate",
                        conflict.map_or(0, |(live, _)| live.epoch)
                    ),
                ),
                Some(peer) if peer.carry_owner => check(
                    "peer_not_owner",
                    false,
                    format!(
                        "the peer holds ownership (epoch {}, {}); run carry-owner release on it first",
                        peer.epoch, peer.action
                    ),
                ),
                Some(_) if live.is_some_and(|live| live.carry_owner) => check(
                    "peer_not_owner",
                    false,
                    format!(
                        "the peer's live journal claims ownership (epoch {}, {}) behind a newer row this node holds of it: it was restored from a backup; wait for its own-log recovery",
                        live.map_or(0, |live| live.epoch),
                        live.map_or("", |live| live.action.as_str())
                    ),
                ),
                Some(peer) => check(
                    "peer_not_owner",
                    true,
                    format!("the peer's latest row: epoch {}, {}", peer.epoch, peer.action),
                ),
            });
            let behind = own_at_peer.as_ref().is_some_and(|theirs| {
                facts
                    .own
                    .as_ref()
                    .is_none_or(|own| theirs.epoch > own.epoch)
            });
            checks.push(check(
                "own_journal_current",
                !behind,
                if behind {
                    "the peer holds a newer row of this node's than this node does; wait for own-log recovery"
                } else {
                    "this node's journal is at least as new as the peer's copy of it"
                },
            ));
            checks.push(scan_start(facts));
            if let Some(peer) = peer.filter(|peer| peer.action == "release") {
                checks.push(match peer.tip_height() {
                    Some(released) => {
                        let due = released.saturating_add(facts.release_depth);
                        check(
                            "release_depth",
                            facts.tip_height >= due,
                            format!(
                                "the peer released at height {released}; the tip is {} and must reach {due}",
                                facts.tip_height
                            ),
                        )
                    }
                    None => check(
                        "release_depth",
                        false,
                        "the peer's release row records no tip height",
                    ),
                });
            }
        }
    }
    let unresolved: Vec<String> = facts
        .scan
        .unresolved()
        .take(20)
        .map(|block| {
            format!(
                "{} at {} ({})",
                block.block_hash,
                block.height,
                match block.landing {
                    Landing::Missing => "no landing rows",
                    _ => "landed, not confirmed",
                }
            )
        })
        .collect();
    let count = facts.scan.unresolved().count();
    checks.push(check(
        "chain_scan",
        count == 0,
        if count == 0 {
            format!(
                "{} pool blocks between heights {} and {} are landed and confirmed here",
                facts.scan.pool_blocks.len(),
                facts.scan.from_height,
                facts.scan.tip_height
            )
        } else {
            format!(
                "{count} pool blocks on the active chain are not landed and confirmed here: {}",
                unresolved.join(", ")
            )
        },
    ));
    checks
}

/// The scan must start at or below the tip the peer's latest claim of
/// ownership recorded (an `acquire`'s): below it, nothing the peer found was
/// carry-paying. A claim that recorded none (a `seed`) bounds nothing, so the
/// scan must read the whole chain. A peer that never claimed found only
/// carry-free blocks, which can only underpay here if they are missing.
fn scan_start(facts: &TransferFacts) -> Check {
    let start = facts.scan.from_height;
    if !facts.peer_claim_read {
        return check(
            "scan_start",
            false,
            "the peer's claims of ownership could not be read, so nothing bounds the scan",
        );
    }
    let Some(claim) = &facts.peer_claim else {
        // A release follows a claim: without one in sight, nothing bounds the
        // scan.
        let released = [facts.peer_read.peer_live(), facts.peer_synced.as_ref()]
            .into_iter()
            .flatten()
            .any(|row| row.action == "release");
        return if released {
            check(
                "scan_start",
                false,
                "the peer released ownership, but no claim of it can be read, so nothing bounds the scan",
            )
        } else {
            check(
                "scan_start",
                true,
                "the peer never claimed ownership, so it found no carry-paying block",
            )
        };
    };
    match claim.tip_height() {
        Some(bound) => check(
            "scan_start",
            start <= bound,
            format!(
                "the scan starts at height {start}; the peer's last claim of ownership (epoch {}, {}) recorded height {bound}, and the scan must start at or below it",
                claim.epoch, claim.action
            ),
        ),
        None => check(
            "scan_start",
            start == 0,
            format!(
                "the scan starts at height {start}; the peer's last claim of ownership (epoch {}, {}) recorded no height, so the scan must read the whole chain (--from-height 0)",
                claim.epoch, claim.action
            ),
        ),
    }
}

/// What `status`, `release` and `transfer` print.
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub command: &'static str,
    pub node_index: i16,
    pub carry_owner_setting: bool,
    pub database_node: Option<i16>,
    pub local: LatestRoles,
    pub peer: Option<LatestRoles>,
    pub decision: CarryDecision,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<Check>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan: Option<ChainScan>,
    /// The journal row written, when one was.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub written: Option<Value>,
}

impl Report {
    fn new(command: &'static str, settings: &CarryOwnerSettings) -> Self {
        Self {
            schema: "qbit.prism.carry-owner.v1",
            command,
            node_index: settings.node_index,
            carry_owner_setting: settings.carry_owner,
            database_node: None,
            local: LatestRoles::default(),
            peer: None,
            decision: CarryDecision::CarryFree(CarryFreeReason::PeerUnconfirmed),
            checks: Vec::new(),
            scan: None,
            written: None,
        }
    }

    pub fn passed(&self) -> bool {
        self.checks.iter().all(|check| check.ok)
    }
}

/// Read everything the guard reads, once, and decide as a fresh guard would.
async fn observe(
    ledger: &Ledger,
    settings: &CarryOwnerSettings,
    peer: &PeerJournal,
    report: &mut Report,
) -> Result<PeerRead> {
    let peer_read = peer.read(settings.node_index).await;
    observe_with(ledger, settings, peer_read, report).await
}

/// [`observe`], with the peer's journal already read.
async fn observe_with(
    ledger: &Ledger,
    settings: &CarryOwnerSettings,
    peer_read: PeerRead,
    report: &mut Report,
) -> Result<PeerRead> {
    let mut connection = ledger.acquire().await?;
    report.database_node = read_node_identity(&mut connection).await?;
    report.local = read_latest_roles(&mut connection, settings.node_index).await?;
    drop(connection);
    report.peer = match &peer_read {
        PeerRead::Answered { peer, own_at_peer } => Some(LatestRoles {
            own: own_at_peer.clone(),
            peer: peer.clone(),
        }),
        PeerRead::Failed => None,
    };
    report.decision = Guard::default().decide(&GuardInputs {
        node_index: settings.node_index,
        database_node: report.database_node,
        env_owner: settings.carry_owner,
        own: report.local.own.clone(),
        peer_synced: report.local.peer.clone(),
        peer_read: peer_read.clone(),
    });
    Ok(peer_read)
}

/// `carry-owner status`: what the guard reads, and what a guard that has
/// just started would decide from it.
pub async fn status(ledger: &Ledger, settings: &CarryOwnerSettings) -> Result<Report> {
    let peer = PeerJournal::new(&settings.peer_urls, settings.peer_timeout)?;
    let mut report = Report::new("status", settings);
    observe(ledger, settings, &peer, &mut report).await?;
    Ok(report)
}

/// The bound on `release`'s tip read under `SETTLEMENT_LOCK`.
const RELEASE_TIP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The least release depth `transfer` waits for, whatever
/// `PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS` says: carry-paying work built just
/// before a release can still reach the tip after the one it recorded, and
/// the scan must see what it finds there.
pub const MIN_RELEASE_DEPTH: u64 = 2;

/// `carry-owner release`: this node gives up ownership. The `release` row
/// and a payout revision bump commit together, so work built before it is
/// superseded. Without `confirm` nothing is written and the report says what
/// would be.
pub async fn release<C: ChainSource>(
    ledger: &Ledger,
    settings: &CarryOwnerSettings,
    chain: &C,
    reason: &str,
    confirm: bool,
) -> Result<Report> {
    let peer = PeerJournal::new(&settings.peer_urls, settings.peer_timeout)?;
    let mut report = Report::new("release", settings);
    observe(ledger, settings, &peer, &mut report).await?;
    report.checks.push(check(
        "node_identity",
        report.database_node == Some(settings.node_index),
        format!("the database's node identity is {:?}", report.database_node),
    ));
    report.checks.push(match &report.local.own {
        Some(own) if own.carry_owner => check(
            "this_node_owner",
            true,
            format!(
                "this node holds ownership (epoch {}, {})",
                own.epoch, own.action
            ),
        ),
        _ => check(
            "this_node_owner",
            false,
            "this node does not hold ownership",
        ),
    });
    if !report.passed() || !confirm {
        return Ok(report);
    }
    let mut tx = ledger.settlement_transaction().await?;
    let latest = read_latest_roles(&mut tx, settings.node_index).await?;
    ensure!(
        latest.own == report.local.own,
        "this node's journal changed while release was checking; run it again"
    );
    // The release height is the tip when the release is fenced: read under
    // SETTLEMENT_LOCK, which every snapshot takes before it reads this node's
    // journal, so no carry-paying snapshot is taken after it. Work already
    // built on an earlier snapshot can still reach the next tip, which the
    // release depth (at least `MIN_RELEASE_DEPTH`) covers. The read is
    // bounded, since the lock holds back all work building meanwhile.
    let (tip_height, _) = tokio::time::timeout(RELEASE_TIP_TIMEOUT, chain.tip())
        .await
        .context("the node did not report its tip in time while the release held SETTLEMENT_LOCK; nothing was written")??;
    let detail = json!({"reason": reason, "tip_height": tip_height});
    let epoch = append_role(
        &mut tx,
        settings.node_index,
        false,
        "release",
        "carry-owner release",
        &detail,
    )
    .await?;
    let revision = crate::ledger::bump_payout_revision(&mut tx).await?;
    tx.commit().await?;
    report.written = Some(
        json!({"origin_node": settings.node_index, "epoch": epoch, "carry_owner": false, "action": "release", "detail": detail, "payout_revision": revision}),
    );
    Ok(report)
}

/// `carry-owner transfer`: this node takes ownership over from the peer,
/// after every check of [`transfer_checks`] passes. Without `confirm`
/// nothing is written.
#[allow(clippy::too_many_arguments)]
pub async fn transfer<C: ChainSource>(
    ledger: &Ledger,
    settings: &CarryOwnerSettings,
    chain: &C,
    recognizer: &PoolRecognizer,
    from_height: u64,
    release_depth: u64,
    reason: &str,
    confirm: bool,
) -> Result<Report> {
    let release_depth = release_depth.max(MIN_RELEASE_DEPTH);
    let peer = PeerJournal::new(&settings.peer_urls, settings.peer_timeout)?;
    let mut report = Report::new("transfer", settings);
    // The peer's latest rows and its latest claim, from one snapshot.
    let view = peer.view(settings.node_index).await;
    let peer_read = match &view {
        Some(view) => PeerRead::Answered {
            peer: view.latest.peer.clone(),
            own_at_peer: view.latest.own.clone(),
        },
        None => PeerRead::Failed,
    };
    let peer_read = observe_with(ledger, settings, peer_read, &mut report).await?;
    let peer_node = peer_index(settings.node_index);
    let synced_claim = {
        let mut connection = ledger.acquire().await?;
        read_latest_claim(&mut connection, peer_node).await?
    };
    let peer_claim_read = view.is_some();
    let live_claim = view.and_then(|view| view.peer_claim);
    let peer_claim = [live_claim, synced_claim]
        .into_iter()
        .flatten()
        .max_by_key(|row| row.epoch);
    let scan = scan_chain(chain, ledger, from_height, recognizer).await?;
    let facts = TransferFacts {
        node_index: settings.node_index,
        database_node: report.database_node,
        own: report.local.own.clone(),
        peer_read,
        peer_synced: report.local.peer.clone(),
        peer_claim,
        peer_claim_read,
        tip_height: scan.tip_height,
        release_depth,
        scan: scan.clone(),
    };
    report.checks = transfer_checks(&facts);
    report.scan = Some(scan);
    if !report.passed() || !confirm {
        return Ok(report);
    }
    // The peer's latest row once more, right before the write: the very row
    // the checks read live, not an ownership claim, or nothing is written.
    let checked_live = report.peer.as_ref().and_then(|peer| peer.peer.clone());
    let tail = match peer.latest_row_json(peer_node).await {
        Some(Some(tail)) => tail,
        _ => bail!("the peer's journal changed or could not be read again; nothing was written"),
    };
    let latest: RoleRow = serde_json::from_value(tail.clone())
        .context("the peer's latest journal row does not decode")?;
    ensure!(
        Some(&latest) == checked_live.as_ref(),
        "the peer's journal changed while transfer was checking: its latest row is now epoch {}, {}; nothing was written",
        latest.epoch,
        latest.action
    );
    ensure!(
        !latest.carry_owner,
        "the peer holds ownership (epoch {}, {}); nothing was written; run carry-owner release on it first",
        latest.epoch,
        latest.action
    );
    let unchanged = |local: &LatestRoles| local.own == report.local.own;
    let mut connection = ledger.acquire().await?;
    let local = read_latest_roles(&mut connection, settings.node_index).await?;
    drop(connection);
    ensure!(
        unchanged(&local),
        "this node's journal changed while transfer was checking; nothing was written; run it again"
    );
    // Copied into this node's journal as the peer sync copies it (D1's
    // journal tail), so a peer rebuilt from this node's database gets its
    // release back. It is the peer's own row, which the sync would copy
    // anyway, so the copy stands even if the acquire below is refused.
    let peer_index =
        crate::node_identity::NodeIndex::from_index(peer_node.into()).context("no peer node")?;
    let applied = ledger.apply_node_roles(&json!([tail]), peer_index).await?;
    ensure!(
        applied.conflicts.is_empty(),
        "this node's journal holds the peer's row at epoch {} with other content (recorded as a peer sync conflict); the acquire was not written",
        latest.epoch
    );
    let copied = applied.inserted.values().sum::<u64>() == 1;
    let mut tx = ledger.settlement_transaction().await?;
    let local = read_latest_roles(&mut tx, settings.node_index).await?;
    ensure!(
        unchanged(&local),
        "this node's journal changed while transfer was checking; the acquire was not written; run it again"
    );
    // The newest peer row the checks read, live or synced: what the guard
    // compares this claim with. A newer one synced meanwhile was not checked.
    let checked = [checked_live.as_ref(), report.local.peer.as_ref()]
        .into_iter()
        .flatten()
        .max_by_key(|row| row.epoch);
    ensure!(
        local.peer.as_ref() == checked,
        "the peer's journal changed while transfer was checking; the acquire was not written; run it again"
    );
    let detail = json!({
        "reason": reason,
        "tip_height": facts.tip_height,
        "peer_epoch": checked.map(|row| row.epoch),
        "scan_from_height": from_height,
        "pool_blocks": facts.scan.pool_blocks.len(),
    });
    let epoch = append_role(
        &mut tx,
        settings.node_index,
        true,
        "acquire",
        "carry-owner transfer",
        &detail,
    )
    .await?;
    tx.commit().await?;
    report.written = Some(
        json!({"origin_node": settings.node_index, "epoch": epoch, "carry_owner": true, "action": "acquire", "detail": detail, "peer_row": {"epoch": latest.epoch, "copied": copied}}),
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(
        origin_node: i16,
        epoch: i64,
        carry_owner: bool,
        action: &str,
        tip: Option<u64>,
    ) -> RoleRow {
        RoleRow {
            origin_node,
            epoch,
            carry_owner,
            action: action.into(),
            detail: tip.map_or(Value::Null, |tip| json!({"tip_height": tip})),
        }
    }

    fn scan(blocks: &[(u64, Landing)]) -> ChainScan {
        ChainScan {
            from_height: 1,
            tip_height: 200,
            tip_hash: "tip".into(),
            scanned_blocks: 200,
            pool_blocks: blocks
                .iter()
                .map(|(height, landing)| ScannedBlock {
                    height: *height,
                    block_hash: format!("block-{height}"),
                    landing: *landing,
                })
                .collect(),
        }
    }

    fn facts(peer: Option<RoleRow>) -> TransferFacts {
        TransferFacts {
            node_index: 1,
            database_node: Some(1),
            own: Some(role(1, 10, false, "seed", None)),
            peer_read: PeerRead::Answered {
                peer,
                own_at_peer: Some(role(1, 10, false, "seed", None)),
            },
            peer_synced: None,
            // The claim before the peer's release, recorded at height 100.
            peer_claim: Some(role(0, 5, true, "acquire", Some(100))),
            peer_claim_read: true,
            tip_height: 200,
            release_depth: 6,
            scan: scan(&[(150, Landing::Confirmed), (190, Landing::Confirmed)]),
        }
    }

    fn failed(checks: &[Check]) -> Vec<&'static str> {
        checks
            .iter()
            .filter(|check| !check.ok)
            .map(|check| check.check)
            .collect()
    }

    #[test]
    fn a_transfer_after_a_deep_enough_release_with_every_block_landed_passes() {
        let checks = transfer_checks(&facts(Some(role(0, 12, false, "release", Some(190)))));
        assert!(failed(&checks).is_empty(), "{checks:?}");
    }

    #[test]
    fn the_peer_must_answer_and_must_not_hold_ownership() {
        let mut unreachable = facts(None);
        unreachable.peer_read = PeerRead::Failed;
        assert_eq!(failed(&transfer_checks(&unreachable)), ["peer_answered"]);
        assert_eq!(
            failed(&transfer_checks(&facts(Some(role(
                0, 11, true, "seed", None
            ))))),
            ["peer_not_owner"]
        );
        assert_eq!(failed(&transfer_checks(&facts(None))), ["peer_not_owner"]);
    }

    #[test]
    fn a_peer_restored_behind_this_nodes_copy_of_its_journal_is_refused() {
        // The peer's live journal shows an old release, but this node holds
        // the peer's later acquire: the peer was restored from a backup.
        let mut restored = facts(Some(role(0, 12, false, "release", Some(150))));
        restored.peer_synced = Some(role(0, 14, true, "acquire", Some(170)));
        assert_eq!(failed(&transfer_checks(&restored)), ["peer_not_owner"]);
        // An older synced row changes nothing.
        restored.peer_synced = Some(role(0, 11, false, "seed", None));
        assert!(failed(&transfer_checks(&restored)).is_empty());
        // And the other way round: the peer's live journal claims ownership
        // behind the release this node holds a copy of.
        let mut behind = facts(Some(role(0, 12, true, "acquire", Some(150))));
        behind.peer_synced = Some(role(0, 14, false, "release", Some(170)));
        let detail = |facts: &TransferFacts| {
            let checks = transfer_checks(facts);
            assert_eq!(failed(&checks), ["peer_not_owner"]);
            checks
                .into_iter()
                .find(|check| check.check == "peer_not_owner")
                .map(|check| check.detail)
                .unwrap_or_default()
        };
        assert!(detail(&behind).contains("restored"));
        // At the same epoch, the copies disagree: a sync conflict, whichever
        // of them claims ownership.
        behind.peer_synced = Some(role(0, 12, false, "release", Some(150)));
        assert!(detail(&behind).contains("conflict"));
        let mut mirrored = facts(Some(role(0, 12, false, "release", Some(150))));
        mirrored.peer_synced = Some(role(0, 12, true, "acquire", Some(150)));
        assert!(detail(&mirrored).contains("conflict"));
    }

    #[test]
    fn the_release_must_be_buried_by_the_orphan_depth() {
        let shallow = facts(Some(role(0, 12, false, "release", Some(195))));
        assert_eq!(failed(&transfer_checks(&shallow)), ["release_depth"]);
        let unrecorded = facts(Some(role(0, 12, false, "release", None)));
        assert_eq!(failed(&transfer_checks(&unrecorded)), ["release_depth"]);
        // A peer that never held ownership has no release to wait for.
        assert!(failed(&transfer_checks(&facts(Some(role(
            0, 11, false, "seed", None
        )))))
        .is_empty());
    }

    #[test]
    fn every_pool_block_on_the_chain_must_be_landed_and_confirmed_here() {
        for landing in [Landing::Missing, Landing::Unconfirmed] {
            let mut missing = facts(Some(role(0, 12, false, "release", Some(150))));
            missing.scan = scan(&[(150, Landing::Confirmed), (199, landing)]);
            let checks = transfer_checks(&missing);
            assert_eq!(failed(&checks), ["chain_scan"]);
            assert!(checks
                .iter()
                .any(|check| check.check == "chain_scan" && check.detail.contains("block-199")));
        }
    }

    #[test]
    fn this_node_must_not_already_own_and_its_journal_must_be_current() {
        let mut owner = facts(Some(role(0, 12, false, "release", Some(150))));
        owner.own = Some(role(1, 13, true, "acquire", Some(190)));
        assert_eq!(failed(&transfer_checks(&owner)), ["this_node_not_owner"]);
        let mut behind = facts(Some(role(0, 12, false, "release", Some(150))));
        behind.peer_read = PeerRead::Answered {
            peer: Some(role(0, 12, false, "release", Some(150))),
            own_at_peer: Some(role(1, 11, false, "seed", None)),
        };
        assert_eq!(failed(&transfer_checks(&behind)), ["own_journal_current"]);
        let mut unidentified = facts(Some(role(0, 12, false, "release", Some(150))));
        unidentified.database_node = None;
        assert_eq!(failed(&transfer_checks(&unidentified)), ["node_identity"]);
    }

    #[test]
    fn the_scan_starts_no_higher_than_the_tip_of_the_peers_last_claim() {
        let starting_at = |from_height, claim: Option<RoleRow>, release_tip| {
            let mut facts = facts(Some(role(0, 12, false, "release", release_tip)));
            facts.scan.from_height = from_height;
            facts.peer_claim = claim;
            failed(&transfer_checks(&facts))
        };
        let acquired = role(0, 8, true, "acquire", Some(120));
        assert!(starting_at(120, Some(acquired.clone()), Some(190)).is_empty());
        assert_eq!(starting_at(121, Some(acquired), Some(190)), ["scan_start"]);
        // A seed records no height: the peer may have paid from any height
        // before its release, so only the whole chain will do.
        let seeded = role(0, 8, true, "seed", None);
        assert_eq!(
            starting_at(190, Some(seeded.clone()), Some(190)),
            ["scan_start"]
        );
        assert!(starting_at(0, Some(seeded.clone()), Some(190)).is_empty());
        assert_eq!(starting_at(0, Some(seeded), None), ["release_depth"]);
        // A peer that never claimed ownership, its only row a non-owner
        // seed, bounds nothing.
        let mut never = facts(Some(role(0, 11, false, "seed", None)));
        never.scan.from_height = 199;
        never.peer_claim = None;
        assert!(failed(&transfer_checks(&never)).is_empty());
        // Claims the peer's live journal did not answer for bound nothing
        // either way.
        let mut unread = facts(Some(role(0, 12, false, "release", Some(190))));
        unread.peer_claim_read = false;
        assert_eq!(failed(&transfer_checks(&unread)), ["scan_start"]);
    }

    #[test]
    fn earlier_tags_and_fee_programs_recognise_earlier_pool_blocks() {
        let old_program = "cd".repeat(32);
        let current = PoolRecognizer::new("/PRISM/", None).unwrap();
        let with_history = PoolRecognizer::new("/PRISM/", None)
            .unwrap()
            .also(
                &["/OLDPOOL/".to_owned()],
                std::slice::from_ref(&old_program),
            )
            .unwrap();
        let tagged = CoinbaseView {
            block_hash: "h".into(),
            script_sig: [&[0x03, 0x01, 0x02, 0x03][..], b"/OLDPOOL/", &[0; 12]].concat(),
            output_scripts: vec![vec![0x00, 0x14]],
        };
        assert!(!current.is_pool_block(&tagged));
        assert!(with_history.is_pool_block(&tagged));
        let paid = CoinbaseView {
            script_sig: vec![0x03, 0x01, 0x02, 0x03],
            output_scripts: vec![[&[0x52, 0x20][..], &hex::decode(&old_program).unwrap()].concat()],
            ..tagged
        };
        assert!(!current.is_pool_block(&paid));
        assert!(with_history.is_pool_block(&paid));
        assert!(PoolRecognizer::new("/PRISM/", None)
            .unwrap()
            .also(&[], &["ab".to_owned()])
            .is_err());
    }

    #[test]
    fn a_release_without_a_readable_claim_leaves_the_scan_unbounded() {
        // The peer's latest row is a release, but no claim of ownership of its
        // was read, live or synced (a lagging path answered): refused.
        let mut released = facts(Some(role(0, 12, false, "release", Some(190))));
        released.peer_claim = None;
        assert_eq!(failed(&transfer_checks(&released)), ["scan_start"]);
        // A peer that never claimed and never released bounds nothing.
        assert!(failed(&transfer_checks(&facts(Some(role(
            0, 11, false, "seed", None
        )))))
        .is_empty());
    }

    #[test]
    fn pool_blocks_are_recognised_by_tag_or_fee_program() {
        let program = "ab".repeat(32);
        let recognizer = PoolRecognizer::new("/PRISM/", Some(&program)).unwrap();
        let mut view = CoinbaseView {
            block_hash: "h".into(),
            script_sig: [&[0x03, 0x01, 0x02, 0x03][..], b"/PRISM/", &[0; 12]].concat(),
            output_scripts: vec![vec![0x00, 0x14]],
        };
        assert!(recognizer.is_pool_block(&view));
        view.script_sig = vec![0x03, 0x01, 0x02, 0x03, 0xff];
        assert!(!recognizer.is_pool_block(&view));
        view.output_scripts
            .push([&[0x52, 0x20][..], &hex::decode(&program).unwrap()].concat());
        assert!(recognizer.is_pool_block(&view));
    }

    /// A chain held in memory, whose branch can change between calls: after
    /// `switch_after` block reads, or at the first tip read when it is 0.
    struct FakeChain {
        blocks: std::sync::Mutex<Vec<String>>,
        then: std::sync::Mutex<Option<Vec<String>>>,
        switch_after: usize,
        reads: std::sync::atomic::AtomicUsize,
        pool: std::collections::BTreeSet<String>,
    }

    impl FakeChain {
        fn new(blocks: &[&str], then: Option<&[&str]>, switch_after: usize, pool: &[&str]) -> Self {
            Self {
                blocks: std::sync::Mutex::new(chain(blocks)),
                then: std::sync::Mutex::new(then.map(chain)),
                switch_after,
                reads: Default::default(),
                pool: pool.iter().map(|name| (*name).to_owned()).collect(),
            }
        }

        fn switch(&self) {
            if let Some(next) = self.then.lock().unwrap().take() {
                *self.blocks.lock().unwrap() = next;
            }
        }
    }

    impl ChainSource for FakeChain {
        async fn tip(&self) -> Result<(u64, String)> {
            let tip = {
                let blocks = self.blocks.lock().unwrap();
                (blocks.len() as u64 - 1, blocks.last().unwrap().clone())
            };
            if self.switch_after == 0 {
                self.switch();
            }
            Ok(tip)
        }
        async fn block_hash(&self, height: u64) -> Result<String> {
            Ok(self.blocks.lock().unwrap()[height as usize].clone())
        }
        async fn coinbase(&self, height: u64) -> Result<CoinbaseView> {
            Ok(self.linked_coinbase(height).await?.0)
        }
        async fn linked_coinbase(&self, height: u64) -> Result<(CoinbaseView, Option<String>)> {
            let (hash, parent) = {
                let blocks = self.blocks.lock().unwrap();
                let index = height as usize;
                (
                    blocks[index].clone(),
                    index.checked_sub(1).map(|below| blocks[below].clone()),
                )
            };
            let read = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if self.switch_after > 0 && read == self.switch_after {
                self.switch();
            }
            Ok((
                CoinbaseView {
                    script_sig: if self.pool.contains(&hash) {
                        b"/PRISM/".to_vec()
                    } else {
                        Vec::new()
                    },
                    block_hash: hash,
                    output_scripts: Vec::new(),
                },
                parent,
            ))
        }
    }

    fn chain(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn hashes(walk: &ChainWalk, pool_only: bool) -> Vec<&str> {
        walk.blocks
            .values()
            .filter(|block| !pool_only || block.pool)
            .map(|block| block.hash.as_str())
            .collect()
    }

    #[tokio::test]
    async fn the_walk_follows_a_tip_that_moves_and_a_reorganisation() {
        // After the first tip read the chain reorganises at height 2 and grows.
        let fake = FakeChain::new(
            &["g", "a1", "a2", "a3"],
            Some(&["g", "a1", "b2", "b3", "b4"]),
            0,
            &["a2", "b3"],
        );
        let recognizer = PoolRecognizer::new("/PRISM/", None).unwrap();
        let walk = walk_chain(&fake, 1, &recognizer).await.unwrap();
        assert_eq!((walk.tip_height, walk.tip_hash.as_str()), (4, "b4"));
        assert_eq!(hashes(&walk, false), ["a1", "b2", "b3", "b4"]);
        assert_eq!(
            hashes(&walk, true),
            ["b3"],
            "the orphaned a2 is gone and b3 was found"
        );
    }

    #[tokio::test]
    async fn a_read_that_spans_a_reorganisation_is_read_again() {
        // The reorganisation lands after the second block read: heights 1
        // and 2 come from the old branch, 3 and 4 from the new one, whose b3
        // names b2, not the a2 read below it.
        let fake = FakeChain::new(
            &["g", "a1", "a2", "a3", "a4"],
            Some(&["g", "a1", "b2", "b3", "b4"]),
            2,
            &["a2", "b2"],
        );
        let recognizer = PoolRecognizer::new("/PRISM/", None).unwrap();
        let walk = walk_chain(&fake, 1, &recognizer).await.unwrap();
        assert_eq!((walk.tip_height, walk.tip_hash.as_str()), (4, "b4"));
        assert_eq!(hashes(&walk, false), ["a1", "b2", "b3", "b4"]);
        assert_eq!(
            hashes(&walk, true),
            ["b2"],
            "the stale a2 must not survive beside the new branch"
        );
        let links = walk.blocks.values().zip(walk.blocks.values().skip(1));
        for (low, high) in links {
            assert_eq!(high.parent.as_deref(), Some(low.hash.as_str()));
        }
    }

    #[tokio::test]
    async fn a_walk_from_genesis_reads_every_height_once_on_a_still_chain() {
        let fake = FakeChain::new(&["g", "x1", "x2"], None, 0, &["g"]);
        let recognizer = PoolRecognizer::new("/PRISM/", None).unwrap();
        let walk = walk_chain(&fake, 0, &recognizer).await.unwrap();
        assert_eq!(walk.blocks.len(), 3);
        assert!(walk.blocks[&0].pool);
        assert_eq!(fake.reads.load(std::sync::atomic::Ordering::SeqCst), 3);
        // A start above the tip reads nothing.
        let walk = walk_chain(&fake, 9, &recognizer).await.unwrap();
        assert!(walk.blocks.is_empty());
    }

    #[test]
    fn a_link_break_is_the_first_block_that_does_not_name_the_one_below() {
        let block = |hash: &str, parent: Option<&str>| WalkedBlock {
            hash: hash.into(),
            parent: parent.map(str::to_owned),
            pool: false,
        };
        let mut blocks = BTreeMap::from([
            (5, block("a5", Some("a4"))),
            (6, block("a6", Some("a5"))),
            (7, block("b7", Some("b6"))),
            (8, block("b8", Some("b7"))),
        ]);
        assert_eq!(first_link_break(&blocks, 5), Some(7));
        // Unknown parents and gaps are not breaks.
        blocks.insert(7, block("b7", None));
        assert_eq!(first_link_break(&blocks, 5), None);
        blocks.remove(&7);
        assert_eq!(first_link_break(&blocks, 5), None);
    }
}
