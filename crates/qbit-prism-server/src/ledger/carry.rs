//! The carry gate: which prior balances work is built on (PRISM 3.1 dual
//! writer, CONTRACT.md §1 and §4).
//!
//! In dual-writer mode two nodes build work on their own ledgers, and only
//! the carry owner pays carried balances down. All other work is carry-free:
//! its prior balances are empty, so its coinbase pays each eligible miner the
//! window's gross and no carried balance, and every account's delta,
//! `gross - onchain`, is at least zero (invariant 2). A carry-free block can
//! therefore never create debt, whatever another writer paid meanwhile, and
//! two writers can never pay one carry twice.
//!
//! The gate is the one switch the job-build prior path reads
//! ([`Ledger::job_prior_balance_sql`]): the snapshot a job is built on, and
//! the refresh probe and `payout_state` that compare published work with the
//! ledger. A landing that rebuilds a window from current balances reads the
//! balances of the window's own mode ([`carry_free_block`]); landing,
//! confirmation and the #478 debt record otherwise read the canonical
//! balances, whatever the gate says.
//!
//! - **Single writer** (`PRISM_DUAL_WRITER` off): the gate is open for good,
//!   and every statement is 3.0's.
//! - **Dual writer**: the gate starts closed, carry-free, and only the owner
//!   guard (`crate::carry_owner`) opens it. Work is also carry-free whenever
//!   this node's journal no longer claims ownership, so a release takes
//!   effect at its commit, before the guard's next check closes the gate. A
//!   change is published to the gate first and then fenced by a payout
//!   revision bump. A snapshot reads the gate after it has read the revision
//!   under `SETTLEMENT_LOCK`, which the bump also takes, so work built under
//!   the old mode is always at a revision the bump superseded: its shares
//!   are no longer credited, and a block on it is offered only through the
//!   #478 capture bound.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// The prior balances of carry-free work: the canonical read's columns, no
/// rows. The planner folds `WHERE false` into a one-time filter, so the
/// balance function never runs.
pub(super) const CARRY_FREE_PRIOR_BALANCE_SQL: &str = "SELECT miner_id,payout_order_key,encode(p2mr_program,'hex') AS program,balance_sats::text AS balance FROM qbit_current_carry_forward_balances() WHERE false";

/// Whether this node's latest journal row claims ownership.
const OWN_CLAIM_SQL: &str = "SELECT carry_owner FROM qbit_prism_node_roles WHERE origin_node=$1 ORDER BY epoch DESC LIMIT 1";

/// The `prior_balances_digest` of carry-free work: the digest of no balances.
pub fn carry_free_prior_digest() -> [u8; 32] {
    static DIGEST: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();
    *DIGEST.get_or_init(|| qbit_prism::prior_balances_digest(&[]))
}

/// The switch itself, shared by every clone of one frontend's [`Ledger`].
/// It is consulted only in dual-writer mode, which the ledger's dual-writer
/// identity ([`Ledger::set_dual_writer_identity`]) turns on before any work is
/// built; it starts closed.
#[derive(Debug, Default)]
pub struct CarryGate {
    /// Whether work may pay carried balances now.
    paying: AtomicBool,
    /// A change whose revision bump has not committed yet. The guard retries
    /// the bump until it does.
    fence_pending: AtomicBool,
    /// The frontend's `extranonce2_size`, which locates extranonce1 in a
    /// coinbase ([`finder_node`]); set with the dual-writer identity.
    extranonce2_size: std::sync::OnceLock<usize>,
}

/// The node whose half of the extranonce1 space a coinbase's extranonce1 is
/// in (D1: node A `[1, 2^31-1]`, node B `[2^31, 2^32-1]`): the node that
/// issued the work the block was found on, whichever node landed its rows.
/// The scriptSig ends with extranonce1 (four bytes, big-endian, as Stratum
/// sends it) and extranonce2 (`extranonce2_size` bytes). `None` when the
/// coinbase does not parse or extranonce1 is 0, which no session is given.
pub fn finder_node(coinbase_tx: &[u8], extranonce2_size: usize) -> Option<i16> {
    let script_sig = crate::codec::coinbase_script_sig(coinbase_tx).ok()?;
    let end = script_sig.len().checked_sub(extranonce2_size)?;
    let start = end.checked_sub(4)?;
    let extranonce1 = u32::from_be_bytes(script_sig[start..end].try_into().ok()?);
    crate::node_identity::NodeIndex::ALL
        .into_iter()
        .find(|node| node.extranonce1_range().contains(&extranonce1))
        .map(|node| node.index())
}

impl Ledger {
    /// Whether this frontend runs in dual-writer mode: it was given a
    /// dual-writer identity.
    pub fn dual_writer(&self) -> bool {
        self.dual_writer_identity().is_some()
    }

    /// This node's `origin_node` in dual-writer mode.
    pub fn own_node(&self) -> Option<i16> {
        self.dual_writer_identity()
            .map(|identity| identity.node.index())
    }

    /// Record the frontend's `extranonce2_size`, so the ledger can tell which
    /// node's work a block was found on. Set once, with the dual-writer
    /// identity; a second, different value is refused.
    pub fn set_extranonce2_size(&self, size: usize) -> Result<()> {
        let recorded = *self.carry.extranonce2_size.get_or_init(|| size);
        ensure!(
            recorded == size,
            "this ledger already reads extranonce2 as {recorded} bytes"
        );
        Ok(())
    }

    /// Dual writer: whether this node issued the work a block with this
    /// coinbase was found on ([`finder_node`]). A block adopted from the
    /// peer's work (S8) carries this node as its origin, but was found by
    /// the peer. Without a recorded extranonce2 size, or for a coinbase that
    /// names no node, `origin_is_own` decides.
    pub fn found_here(&self, coinbase_tx: Option<&[u8]>, origin_is_own: bool) -> bool {
        let (Some(own), Some(size), Some(coinbase)) = (
            self.own_node(),
            self.carry.extranonce2_size.get(),
            coinbase_tx,
        ) else {
            return origin_is_own;
        };
        finder_node(coinbase, *size).map_or(origin_is_own, |finder| finder == own)
    }

    /// Whether work built now pays carried balances: always in single-writer
    /// mode, and in dual mode only while the owner guard holds the gate open.
    pub fn carry_paying(&self) -> bool {
        !self.dual_writer() || self.carry.paying.load(Ordering::SeqCst)
    }

    /// The prior-balance read the gate alone allows. Work reads it only
    /// through [`Ledger::job_prior_balance_sql_in`], which also reads the
    /// journal. Single-writer mode always runs 3.0's canonical statement.
    fn job_prior_balance_sql(&self) -> &'static str {
        if self.carry_paying() {
            window::PRIOR_BALANCE_SQL
        } else {
            CARRY_FREE_PRIOR_BALANCE_SQL
        }
    }

    /// [`Ledger::job_prior_balance_sql`], read in `tx`. In dual-writer mode
    /// work is also carry-free whenever this node's latest journal row does
    /// not claim ownership, so a `carry-owner release`, which commits its row
    /// under `SETTLEMENT_LOCK`, takes effect at its commit rather than at the
    /// guard's next check. Single-writer mode reads nothing.
    pub(super) async fn job_prior_balance_sql_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> sqlx::Result<&'static str> {
        let sql = self.job_prior_balance_sql();
        let Some(own) = self.own_node().filter(|_| sql == window::PRIOR_BALANCE_SQL) else {
            return Ok(sql);
        };
        let claims: Option<bool> = sqlx::query_scalar(OWN_CLAIM_SQL)
            .bind(own)
            .fetch_optional(&mut **tx)
            .await?;
        Ok(if claims == Some(true) {
            sql
        } else {
            CARRY_FREE_PRIOR_BALANCE_SQL
        })
    }

    /// Open (`paying`) or close the gate, then fence the change with a payout
    /// revision bump. Returns whether the gate changed. In single-writer mode
    /// the gate is open for good and this changes nothing.
    ///
    /// The gate moves first: from that instant a snapshot builds under the
    /// new mode, and the bump supersedes every revision a snapshot under the
    /// old mode could have read. If the bump fails the change stays in force
    /// and the fence stays pending; [`Ledger::fence_carry_change`] retries it.
    pub async fn set_carry_paying(&self, paying: bool) -> Result<bool> {
        if !self.dual_writer() {
            return Ok(false);
        }
        let changed = self.carry.paying.swap(paying, Ordering::SeqCst) != paying;
        if changed {
            self.carry.fence_pending.store(true, Ordering::SeqCst);
        }
        self.fence_carry_change().await?;
        Ok(changed)
    }

    /// A transaction holding `SETTLEMENT_LOCK` on a writable ledger: the
    /// lock every payout revision writer, and every node-role journal
    /// writer, takes.
    pub async fn settlement_transaction(&self) -> Result<Transaction<'static, Postgres>> {
        let mut tx = self.begin().await?;
        self.lock(&mut tx, SETTLEMENT_LOCK).await?;
        writable(&mut tx).await?;
        Ok(tx)
    }

    /// Commit a pending carry change's payout revision bump, if one is
    /// pending. Returns the revision it bumped to.
    pub async fn fence_carry_change(&self) -> Result<Option<i64>> {
        if !self.carry.fence_pending.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let mut tx = self.settlement_transaction().await?;
        let revision = bump_payout_revision(&mut tx).await?;
        // Cleared inside the commit window: a change that lands while the
        // bump is in flight sets it again, and its own bump follows.
        self.carry.fence_pending.store(false, Ordering::SeqCst);
        if let Err(error) = tx.commit().await {
            self.carry.fence_pending.store(true, Ordering::SeqCst);
            return Err(error.into());
        }
        Ok(Some(revision))
    }

    /// Whether a carry change still waits for its revision bump.
    pub fn carry_fence_pending(&self) -> bool {
        self.carry.fence_pending.load(Ordering::SeqCst)
    }
}

/// Bump the payout revision in a transaction that holds `SETTLEMENT_LOCK`
/// ([`Ledger::settlement_transaction`]), superseding all work built at the
/// revisions before it. Returns the new revision.
pub async fn bump_payout_revision(tx: &mut Transaction<'_, Postgres>) -> Result<i64> {
    connect::lock_cluster_authority(tx).await?;
    Ok(sqlx::query_scalar("UPDATE qbit_prism_cluster SET payout_revision=payout_revision+1,updated_at=clock_timestamp() WHERE singleton RETURNING payout_revision")
        .fetch_one(&mut **tx)
        .await?)
}

/// Whether a block built on `prior_balances_digest` is carry-free work in
/// dual-writer mode: its coinbase paid no carried balance, so no landing,
/// confirmation or capture of it can create debt (module comment).
pub(super) fn carry_free_block(dual_writer: bool, prior_balances_digest: &[u8; 32]) -> bool {
    dual_writer && *prior_balances_digest == carry_free_prior_digest()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_identity::{NodeIdentity, NodeIndex};

    fn ledger() -> Ledger {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/never-connected")
            .unwrap();
        Ledger::offline_for_tests(pool, "carry-gate".into())
    }

    #[tokio::test]
    async fn a_single_writer_ledger_always_pays_and_runs_the_canonical_statement() {
        let ledger = ledger();
        assert!(!ledger.dual_writer());
        assert!(ledger.carry_paying());
        assert_eq!(ledger.job_prior_balance_sql(), window::PRIOR_BALANCE_SQL);
        // The gate is never consulted, so moving it changes nothing and bumps nothing.
        assert!(!ledger.set_carry_paying(false).await.unwrap());
        assert!(ledger.carry_paying());
        assert!(!ledger.carry_fence_pending());
    }

    #[tokio::test]
    async fn a_dual_writer_ledger_starts_carry_free() {
        let ledger = ledger();
        ledger
            .set_dual_writer_identity(NodeIdentity {
                node: NodeIndex::B,
                carry_owner: false,
            })
            .unwrap();
        assert!(ledger.dual_writer());
        assert_eq!(ledger.own_node(), Some(1));
        assert!(!ledger.carry_paying());
        assert_eq!(ledger.job_prior_balance_sql(), CARRY_FREE_PRIOR_BALANCE_SQL);
        // Every clone shares the gate.
        ledger.carry.paying.store(true, Ordering::SeqCst);
        assert!(ledger.clone().carry_paying());
        assert_eq!(ledger.job_prior_balance_sql(), window::PRIOR_BALANCE_SQL);
    }

    /// A coinbase whose scriptSig is a height push, the tag, extranonce1 and
    /// eight bytes of extranonce2.
    fn coinbase(extranonce1: u32) -> Vec<u8> {
        let script_sig = [
            &[0x03, 0x01, 0x02, 0x03][..],
            b"/PRISM/",
            &extranonce1.to_be_bytes(),
            &[0xee; 8],
        ]
        .concat();
        [
            &[2, 0, 0, 0, 1][..],
            &[0; 32],
            &[0xff; 4],
            &[script_sig.len() as u8],
            &script_sig,
            &[0xff; 4],
            &[0],
            &[0; 4],
        ]
        .concat()
    }

    #[test]
    fn the_finder_is_the_node_whose_extranonce1_half_the_coinbase_carries() {
        assert_eq!(finder_node(&coinbase(1), 8), Some(0));
        assert_eq!(finder_node(&coinbase(0x7fff_ffff), 8), Some(0));
        assert_eq!(finder_node(&coinbase(0x8000_0000), 8), Some(1));
        assert_eq!(finder_node(&coinbase(u32::MAX), 8), Some(1));
        // No session is given extranonce1 0; a wrong layout names nobody.
        assert_eq!(finder_node(&coinbase(0), 8), None);
        assert_eq!(finder_node(&[1, 2, 3], 8), None);
        assert_eq!(finder_node(&coinbase(5), 64), None);
    }

    #[tokio::test]
    async fn found_here_follows_the_finder_and_falls_back_to_the_origin() {
        let ledger = ledger();
        // Single writer: the origin decides, whatever the coinbase says.
        assert!(ledger.found_here(Some(&coinbase(u32::MAX)), true));
        ledger
            .set_dual_writer_identity(NodeIdentity {
                node: NodeIndex::A,
                carry_owner: true,
            })
            .unwrap();
        assert!(ledger.found_here(Some(&coinbase(u32::MAX)), true));
        ledger.set_extranonce2_size(8).unwrap();
        assert!(ledger.set_extranonce2_size(4).is_err());
        // Adopted from B's work: this node's rows, B's block.
        assert!(!ledger.found_here(Some(&coinbase(u32::MAX)), true));
        assert!(ledger.found_here(Some(&coinbase(7)), false));
        assert!(!ledger.found_here(None, false));
        assert!(ledger.found_here(Some(&[0]), true));
    }

    #[test]
    fn the_carry_free_statement_is_the_canonical_one_with_no_rows() {
        assert_eq!(
            CARRY_FREE_PRIOR_BALANCE_SQL,
            format!("{} WHERE false", window::PRIOR_BALANCE_SQL)
        );
    }

    #[test]
    fn only_dual_writer_empty_priors_are_carry_free_blocks() {
        let empty = carry_free_prior_digest();
        assert_eq!(empty, qbit_prism::prior_balances_digest(&[]));
        assert!(carry_free_block(true, &empty));
        assert!(!carry_free_block(false, &empty));
        let paying = qbit_prism::prior_balances_digest(&[CarryForwardBalance {
            recipient_id: "m".into(),
            order_key: "m".into(),
            p2mr_program_hex: "aa".repeat(32),
            balance_sats: 1,
        }]);
        assert!(!carry_free_block(true, &paying));
    }
}
