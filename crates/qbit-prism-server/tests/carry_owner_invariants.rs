//! PRISM 3.1 dual writer, "two writers, one carry owner": a model check of
//! the payout invariants of CONTRACT.md §4 against the real payout policy.
//! Pure Rust: no database and no environment input, so it is not gated.
//!
//! The model runs one chain and two nodes. Every pool block's coinbase is
//! `qbit_prism::apply_payout_policy(window, prior balances, policy)`, and the
//! nodes differ only in the prior balances they pass:
//!
//! - The **carry owner** builds on its view: the initial balances plus the
//!   deltas of the blocks it has landed and confirmed on the active chain. Its
//!   own block is in the view before its next job; a peer block joins only
//!   once peer sync has delivered it and it is on the active chain. On a reorg
//!   the owner deactivates the disconnected blocks before it builds on the new
//!   tip.
//! - The **non-owner** builds carry-free work: no prior balances at all.
//!
//! A block's delta for an account is `gross - onchain` (settlement fees are
//! separate). The canonical balance, the truth, is the initial balance plus
//! the deltas of every pool block on the active chain; debt is
//! `max(0, -balance)` (`ledger/divergence.rs`, #478); and a block's overpay is,
//! per account, how much it raises that debt over its ancestry's.
//!
//! Every appended pool block is checked against why the invariants hold:
//!
//! 1. The policy pays an account on chain only out of its candidate balance,
//!    `onchain <= max(0, issued + gross)`, so a block overpays an account by at
//!    most `max(0, issued - truth)`, and never by more than `max(0, issued)`:
//!    summed, the #478 bound `F`.
//! 2. `issued - truth` is the deltas of the blocks the view holds outside the
//!    ancestry, minus those of the ancestry blocks it lacks. Carry-free deltas
//!    are never negative (invariant 2), so a missing peer block only lowers the
//!    prior: sync lag delays carry and never overpays. Only a missing own block
//!    (the #478 race) or a block the view kept across a reorg can lift the
//!    prior above the truth.
//! 3. So under the contract's rules, where the view stays inside the ancestry
//!    and holds every own block of it, no block overpays (invariant 1).
//!
//! The properties run random event sequences (owner, non-owner and foreign
//! blocks, peer sync, reorgs within the maturity depth) under the contract's
//! rules and under the #478 race. Fixed-seed corpora of the same inputs show
//! that they reach every policy branch, that the #478 race does overpay
//! within `F`, and that skipping reconcile-before-build overpays.
use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{TestCaseError, TestRunner};
use qbit_pool_builder::WeightedEntitlement;
use qbit_prism::{
    apply_payout_policy, prior_balances_digest, CarryForwardBalance, PayoutPolicy,
    PayoutPolicyAccount, PayoutPolicyAccountType, PayoutPolicyAction, PayoutPolicyManifest,
    PoolFeePolicy, PrismError, PrismRewardManifest,
};
use qbit_prism_server::ledger::{carry_free_prior_digest, landing_divergence, overpay_bound};

/// The miners' payout programs. The policy, the canonical balances and the
/// #478 record all key balances by program, so each program is one account.
const PROGRAMS: [u8; 4] = [0xa1, 0xb2, 0xc3, 0xd4];
const POOL_FEE_PROGRAM: u8 = 0x99;
const A: usize = 0;
const B: usize = 1;
/// The model's first block height; everything below it is mature and is in
/// every balance already.
const START_HEIGHT: u64 = 1_000;

/// Per-program amounts, indexed like `PROGRAMS`.
type Amounts = [i128; 4];

/// One payout-window row: who earned it and which program pays it.
struct Row {
    recipient_id: &'static str,
    order_key: &'static str,
    program: usize,
}

/// The window rows. The first four are the programs' own rows; the last is a
/// second worker paid to miner-d's program under its own recipient id, which
/// the policy aggregates into miner-d's account.
const ROWS: [Row; 5] = [
    Row {
        recipient_id: "miner-a",
        order_key: "01",
        program: 0,
    },
    Row {
        recipient_id: "miner-b",
        order_key: "02",
        program: 1,
    },
    Row {
        recipient_id: "miner-c",
        order_key: "03",
        program: 2,
    },
    Row {
        recipient_id: "miner-d",
        order_key: "04",
        program: 3,
    },
    Row {
        recipient_id: "miner-d.rig2",
        order_key: "05",
        program: 3,
    },
];

fn program_hex(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn program_index(hex: &str) -> usize {
    let hex = hex.to_ascii_lowercase();
    PROGRAMS
        .iter()
        .position(|byte| program_hex(*byte) == hex)
        .expect("every miner account is one of the model's programs")
}

/// `max(0, -balance)`: PRISM's debt.
fn debt(balance: i128) -> i128 {
    (-balance).max(0)
}

fn add(total: &mut Amounts, delta: &Amounts) {
    for (total, delta) in total.iter_mut().zip(delta) {
        *total += delta;
    }
}

/// The non-zero amounts as balances under each program's own row: the shape
/// of `qbit_current_carry_forward_balances()`, which omits zero balances.
fn as_balances(amounts: &Amounts) -> Vec<CarryForwardBalance> {
    (0..PROGRAMS.len())
        .filter(|program| amounts[*program] != 0)
        .map(|program| CarryForwardBalance {
            recipient_id: ROWS[program].recipient_id.to_string(),
            order_key: ROWS[program].order_key.to_string(),
            p2mr_program_hex: program_hex(PROGRAMS[program]),
            balance_sats: amounts[program],
        })
        .collect()
}

fn miner_accounts(manifest: &PayoutPolicyManifest) -> impl Iterator<Item = &PayoutPolicyAccount> {
    manifest
        .accounts
        .iter()
        .filter(|account| account.account_type == PayoutPolicyAccountType::Miner)
}

/// The policy settings a case varies. A pool fee is always configured, as
/// `Config::ensure_pool_fee_settles_dust` requires: it is where sub-floor
/// dust goes. At 0 bps it earns nothing and takes only that dust.
#[derive(Clone, Copy, Debug)]
struct PolicyParams {
    floor_sats: u64,
    fee_bps: u16,
}

fn payout_policy(params: PolicyParams) -> PayoutPolicy {
    PayoutPolicy {
        min_output_sats: Some(params.floor_sats),
        pool_fee_policy: Some(PoolFeePolicy {
            fee_bps: params.fee_bps,
            recipient_id: "pool-fee".to_string(),
            order_key: "99".to_string(),
            p2mr_program_hex: program_hex(POOL_FEE_PROGRAM),
        }),
        ..PayoutPolicy::day_one_default()
    }
}

/// One block's payout window: its coinbase value and each row's weight.
#[derive(Clone, Debug)]
struct Window {
    coinbase_value_sats: u64,
    weights: [u16; 5],
}

/// The reward manifest of `window`. The policy reads only the height, the
/// coinbase value and the entitlements; the share-slice fields are inert.
fn reward_manifest(block_height: u64, window: &Window) -> PrismRewardManifest {
    let entitlements = ROWS
        .iter()
        .zip(window.weights)
        .filter(|(_, weight)| *weight > 0)
        .map(|(row, weight)| WeightedEntitlement {
            recipient_id: row.recipient_id.to_string(),
            order_key: row.order_key.to_string(),
            p2mr_program_hex: program_hex(PROGRAMS[row.program]),
            weight: u128::from(weight),
        })
        .collect::<Vec<_>>();
    PrismRewardManifest {
        schema: "qbit.prism.reward-manifest.v1".to_string(),
        block_height,
        coinbase_value_sats: window.coinbase_value_sats,
        network_difficulty: 16,
        window_multiplier: 8,
        requested_window_weight: 128,
        counted_window_weight: entitlements
            .iter()
            .map(|entitlement| entitlement.weight)
            .sum(),
        anchor_job_issued_at_ms: 1,
        anchor_share_seq: 1,
        newest_share_seq: 1,
        oldest_share_seq: 1,
        included_share_count: entitlements.len(),
        share_slice_digest_hex: "00".repeat(32),
        shares: Vec::new(),
        entitlements,
        cut: None,
    }
}

/// Who found a block: the carry owner, the non-owner, or a miner outside the
/// pool (no payout rows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Finder {
    Owner,
    Peer,
    Foreign,
}

/// How the owner keeps its view.
#[derive(Clone, Copy, Debug)]
struct Rules {
    /// The owner's own block is in its view before its next job. When false
    /// it joins only at a later `LandOwn` event: the #478 race.
    own_blocks_land_at_once: bool,
    /// On a reorg the owner deactivates the disconnected blocks before it
    /// builds on the new tip. False only in the negative control.
    reconcile_on_reorg: bool,
}

impl Rules {
    /// The 3.1 contract.
    const CONTRACT: Self = Self {
        own_blocks_land_at_once: true,
        reconcile_on_reorg: true,
    };
    /// The pre-existing #478 race on the owner.
    const OWN_LANDING_LAG: Self = Self {
        own_blocks_land_at_once: false,
        reconcile_on_reorg: true,
    };
    /// Negative control: the owner builds on a new tip without reconciling.
    const NO_RECONCILE: Self = Self {
        own_blocks_land_at_once: true,
        reconcile_on_reorg: false,
    };
}

#[derive(Clone, Debug)]
enum Event {
    /// A block is found on the active tip.
    Find(Finder, Window),
    /// Peer sync delivers one of the non-owner's blocks to the owner: `pick`
    /// modulo the undelivered count, so delivery can be out of order.
    Sync(u8),
    /// One of the owner's own blocks lands in its view (#478 race only).
    LandOwn(u8),
    /// The top `depth` blocks are disconnected, never deeper than the
    /// maturity depth, and `branch`, longer than what it replaces, becomes
    /// active.
    Reorg {
        depth: usize,
        branch: Vec<(Finder, Window)>,
    },
}

#[derive(Clone, Debug)]
struct Case {
    params: PolicyParams,
    /// Blocks deeper than this are mature and never disconnected.
    maturity_depth: usize,
    /// Each program's balance before the first event, in every view.
    initial: [i64; 4],
    events: Vec<Event>,
}

/// A block the model has seen, on the active chain or disconnected.
struct Block {
    finder: Finder,
    height: u64,
    /// `gross - onchain` per program; zero for a foreign block.
    delta: Amounts,
    /// The debt it raised on its ancestry when it was appended.
    overpay_sats: i128,
}

/// How the owner's view differs from the ancestry of the block it builds.
#[derive(Clone, Debug, Default)]
struct ViewGaps {
    /// Own blocks of the ancestry the view lacks, and their summed deltas.
    missing_own: usize,
    missing_own_delta: Amounts,
    /// Peer blocks of the ancestry the view lacks, and their summed deltas.
    missing_peer: usize,
    missing_peer_delta: Amounts,
    /// Blocks the view holds that are not in the ancestry, and their deltas.
    stale: usize,
    stale_delta: Amounts,
}

/// One appended pool block: what it paid and what it did to the debt.
#[derive(Debug)]
struct Landing {
    manifest: PayoutPolicyManifest,
    /// The as-issued prior per program.
    issued: Amounts,
    /// The canonical balances of its ancestry.
    truth_before: Amounts,
    delta: Amounts,
    /// Per program, how much it raised the debt.
    overpay: Amounts,
    /// Owner blocks only; default for carry-free work.
    gaps: ViewGaps,
    /// `F` of its as-issued prior.
    bound_sats: i128,
}

impl Landing {
    fn overpay_sats(&self) -> i128 {
        self.overpay.iter().sum()
    }

    fn account(&self, recipient_id: &str) -> &PayoutPolicyAccount {
        self.manifest
            .accounts
            .iter()
            .find(|account| account.recipient_id == recipient_id)
            .unwrap_or_else(|| panic!("no account for {recipient_id}"))
    }
}

/// How often each policy branch and model path occurred.
#[derive(Debug, Default)]
struct Stats(BTreeMap<&'static str, u64>);

impl Stats {
    fn add(&mut self, what: &'static str, count: u64) {
        *self.0.entry(what).or_default() += count;
    }

    fn bump(&mut self, what: &'static str) {
        self.add(what, 1);
    }

    fn get(&self, what: &str) -> u64 {
        self.0.get(what).copied().unwrap_or(0)
    }

    fn merge(&mut self, other: &Stats) {
        for (what, count) in &other.0 {
            self.add(what, *count);
        }
    }
}

struct Model {
    rules: Rules,
    maturity_depth: usize,
    policy: PayoutPolicy,
    initial: Amounts,
    /// Every block seen, by id.
    blocks: Vec<Block>,
    /// The active chain above `START_HEIGHT`, oldest first.
    chain: Vec<usize>,
    /// The blocks whose deltas the owner's balances include.
    view: BTreeSet<usize>,
    /// The non-owner's blocks peer sync has not delivered yet.
    unsynced: Vec<usize>,
    /// The owner's blocks not yet in its view (#478 race only).
    unlanded: Vec<usize>,
    stats: Stats,
}

impl Model {
    fn new(params: PolicyParams, rules: Rules, maturity_depth: usize, initial: [i64; 4]) -> Self {
        Self {
            rules,
            maturity_depth,
            policy: payout_policy(params),
            initial: initial.map(i128::from),
            blocks: Vec::new(),
            chain: Vec::new(),
            view: BTreeSet::new(),
            unsynced: Vec::new(),
            unlanded: Vec::new(),
            stats: Stats::default(),
        }
    }

    fn tip_height(&self) -> u64 {
        START_HEIGHT + self.chain.len() as u64
    }

    fn balances<'a>(&self, ids: impl IntoIterator<Item = &'a usize>) -> Amounts {
        let mut balances = self.initial;
        for id in ids {
            add(&mut balances, &self.blocks[*id].delta);
        }
        balances
    }

    /// The canonical balances: every pool block on the active chain.
    fn truth(&self) -> Amounts {
        self.balances(&self.chain)
    }

    /// The owner's balances: the blocks in its view.
    fn view_balances(&self) -> Amounts {
        self.balances(&self.view)
    }

    fn view_gaps(&self) -> ViewGaps {
        let mut gaps = ViewGaps::default();
        for id in self.chain.iter().filter(|id| !self.view.contains(id)) {
            let block = &self.blocks[*id];
            match block.finder {
                Finder::Owner => {
                    gaps.missing_own += 1;
                    add(&mut gaps.missing_own_delta, &block.delta);
                }
                Finder::Peer => {
                    gaps.missing_peer += 1;
                    add(&mut gaps.missing_peer_delta, &block.delta);
                }
                Finder::Foreign => {}
            }
        }
        for id in self.view.iter().filter(|id| !self.chain.contains(id)) {
            gaps.stale += 1;
            add(&mut gaps.stale_delta, &self.blocks[*id].delta);
        }
        gaps
    }

    fn append(&mut self, block: Block) -> usize {
        let id = self.blocks.len();
        self.blocks.push(block);
        self.chain.push(id);
        id
    }

    fn apply(&mut self, event: &Event) -> Result<(), TestCaseError> {
        match event {
            Event::Find(finder, window) => {
                self.find(*finder, window)?;
            }
            Event::Sync(pick) => self.sync(*pick),
            Event::LandOwn(pick) => self.land_own(*pick),
            Event::Reorg { depth, branch } => {
                self.reorg(*depth, branch)?;
            }
        }
        Ok(())
    }

    /// `finder` builds work on the active tip and finds a block with it.
    /// Returns the pool block appended, or `None` for a foreign block or work
    /// the policy refused to build.
    fn find(&mut self, finder: Finder, window: &Window) -> Result<Option<Landing>, TestCaseError> {
        let height = self.tip_height() + 1;
        if finder == Finder::Foreign {
            self.append(Block {
                finder,
                height,
                delta: [0; 4],
                overpay_sats: 0,
            });
            self.stats.bump("foreign block");
            return Ok(None);
        }
        // The carry gate: the owner builds on its view, the non-owner on
        // nothing.
        let issued_balances = match finder {
            Finder::Owner => as_balances(&self.view_balances()),
            _ => Vec::new(),
        };
        let manifest = match apply_payout_policy(
            &reward_manifest(height, window),
            &issued_balances,
            &self.policy,
        ) {
            Ok(manifest) => manifest,
            Err(error) => {
                self.refused(finder, error)?;
                return Ok(None);
            }
        };

        let mut issued = [0; 4];
        let mut delta = [0; 4];
        for account in miner_accounts(&manifest) {
            let program = program_index(&account.p2mr_program_hex);
            let account_delta =
                i128::from(account.gross_amount_sats) - i128::from(account.onchain_amount_sats);
            // The carry row is the prior plus this delta.
            prop_assert_eq!(
                account.carry_forward_balance_sats - account.prior_balance_sats,
                account_delta
            );
            // On chain, the policy pays only out of the candidate balance.
            prop_assert!(
                i128::from(account.onchain_amount_sats) <= account.candidate_balance_sats.max(0)
            );
            issued[program] += account.prior_balance_sats;
            delta[program] += account_delta;
        }
        let expected_issued = match finder {
            Finder::Owner => self.view_balances(),
            _ => [0; 4],
        };
        prop_assert_eq!(
            issued,
            expected_issued,
            "the manifest's priors are the job's"
        );

        if finder == Finder::Peer {
            // Invariant 2: the carry gate's empty prior set, and no miner's
            // delta is negative.
            prop_assert_eq!(
                prior_balances_digest(&issued_balances),
                carry_free_prior_digest()
            );
            for account in miner_accounts(&manifest) {
                prop_assert_eq!(account.prior_balance_sats, 0);
                prop_assert!(account.onchain_amount_sats <= account.gross_amount_sats);
                prop_assert!(account.carry_forward_balance_sats >= 0);
            }
        }

        let truth_before = self.truth();
        let mut overpay = [0; 4];
        for program in 0..PROGRAMS.len() {
            let before = truth_before[program];
            overpay[program] = (debt(before + delta[program]) - debt(before)).max(0);
        }
        let overpay_sats: i128 = overpay.iter().sum();
        // The model measures overpay exactly as the #478 record does.
        prop_assert_eq!(
            landing_divergence(&manifest.accounts, &as_balances(&truth_before)).overpay_sats,
            overpay_sats
        );
        let bound_sats = overpay_bound(&issued_balances);

        let gaps = if finder == Finder::Owner {
            let gaps = self.view_gaps();
            for program in 0..PROGRAMS.len() {
                let excess = issued[program] - truth_before[program];
                // The prior differs from the truth by exactly the view's gaps.
                prop_assert_eq!(
                    excess,
                    gaps.stale_delta[program]
                        - gaps.missing_own_delta[program]
                        - gaps.missing_peer_delta[program]
                );
                // A block overpays by at most what its prior exceeds the
                // truth, ...
                prop_assert!(overpay[program] <= excess.max(0));
                // ... so a missing peer block (delta >= 0) never causes it:
                // only a missing own block or a stale one can.
                prop_assert!(
                    overpay[program]
                        <= (gaps.stale_delta[program] - gaps.missing_own_delta[program]).max(0),
                    "overpay {} beyond the missing own and stale blocks' deltas: {:?}",
                    overpay[program],
                    gaps
                );
                // #478: at most the account's positive as-issued prior.
                prop_assert!(overpay[program] <= issued[program].max(0));
            }
            // #478: at most F, the block's positive as-issued float.
            prop_assert!(overpay_sats <= bound_sats);
            if self.rules.reconcile_on_reorg {
                prop_assert_eq!(gaps.stale, 0, "the view outlived a reorg");
            }
            if self.rules.own_blocks_land_at_once {
                prop_assert_eq!(gaps.missing_own, 0, "the view lacks an own block");
            }
            // Invariant 1: a view inside the ancestry that holds every own
            // block of it never overpays, whatever peer blocks it lacks.
            if gaps.stale == 0 && gaps.missing_own == 0 {
                prop_assert_eq!(overpay_sats, 0, "invariant 1: {:?}", overpay);
            }
            gaps
        } else {
            // Invariant 1 for carry-free work: deltas >= 0 never raise a debt.
            prop_assert_eq!(overpay_sats, 0, "carry-free work overpaid: {:?}", overpay);
            ViewGaps::default()
        };

        let id = self.append(Block {
            finder,
            height,
            delta,
            overpay_sats,
        });
        match finder {
            Finder::Owner if self.rules.own_blocks_land_at_once => {
                self.view.insert(id);
            }
            Finder::Owner => self.unlanded.push(id),
            _ => self.unsynced.push(id),
        }

        let landing = Landing {
            manifest,
            issued,
            truth_before,
            delta,
            overpay,
            gaps,
            bound_sats,
        };
        self.count(finder, window, &landing);
        Ok(Some(landing))
    }

    /// The policy refused to build: no work, so no block. Carry-free work
    /// always builds (its eligible accounts are paid exactly their gross and
    /// the pool fee takes the rest), so only the owner's paying work may be
    /// refused, and only by the floor pass of the proportional allocation.
    fn refused(&mut self, finder: Finder, error: PrismError) -> Result<(), TestCaseError> {
        prop_assert!(
            finder == Finder::Owner,
            "carry-free work must always build: {}",
            error
        );
        match error {
            PrismError::PayoutExceedsCandidateBalance { .. } => self
                .stats
                .bump("owner build refused: the floor pass left the reward uncovered"),
            PrismError::NoOnchainRecipients => self
                .stats
                .bump("owner build refused: no allocation clears the floor"),
            other => {
                return Err(TestCaseError::fail(format!(
                    "unexpected policy refusal: {other}"
                )))
            }
        }
        Ok(())
    }

    fn count(&mut self, finder: Finder, window: &Window, landing: &Landing) {
        let stats = &mut self.stats;
        let owner = finder == Finder::Owner;
        stats.bump(if owner { "owner block" } else { "peer block" });
        if window.weights[3] > 0 && window.weights[4] > 0 {
            stats.bump("two rows aggregated into one program");
        }
        if landing
            .manifest
            .pool_fee
            .as_ref()
            .is_some_and(|fee| fee.swept_dust_liability_sats > 0)
        {
            stats.bump("dust swept to the pool fee");
        }
        for account in miner_accounts(&landing.manifest) {
            let program = program_index(&account.p2mr_program_hex);
            if account.action == PayoutPolicyAction::Accrued && account.gross_amount_sats > 0 {
                stats.bump("gross accrued below the floor");
            }
            if account.onchain_amount_sats > account.gross_amount_sats {
                stats.bump("carried balance paid");
            }
            if account.action == PayoutPolicyAction::Onchain
                && i128::from(account.onchain_amount_sats) < account.candidate_balance_sats
            {
                stats.bump("payout scaled to the reward");
            }
            if !owner
                && landing.truth_before[program] < 0
                && account.gross_amount_sats > 0
                && account.onchain_amount_sats == account.gross_amount_sats
            {
                stats.bump("debtor paid its gross by carry-free work");
            }
            if owner && landing.truth_before[program] < 0 && landing.delta[program] > 0 {
                stats.bump("owner recovered debt");
            }
        }
        if owner {
            if landing
                .gaps
                .missing_peer_delta
                .iter()
                .any(|delta| *delta > 0)
            {
                stats.bump("owner built under peer sync lag");
            }
            if landing.gaps.missing_own > 0 {
                stats.bump("owner built without an own block");
            }
            if landing.gaps.stale > 0 {
                stats.bump("owner built on a stale view");
            }
        }
        let overpay_sats = landing.overpay_sats();
        if overpay_sats > 0 {
            stats.bump("overpaid block");
            stats.add(
                "overpaid sats",
                u64::try_from(overpay_sats).expect("a positive overpay"),
            );
            if landing.gaps.missing_own > 0 {
                stats.bump("overpaid block missing an own block");
            }
            if landing.gaps.stale > 0 {
                stats.bump("overpaid block on a stale view");
            }
        }
    }

    /// Peer sync delivers one of the non-owner's blocks; it joins the owner's
    /// view only if it is on the owner's active chain.
    fn sync(&mut self, pick: u8) {
        if self.unsynced.len() > 1 && usize::from(pick) % self.unsynced.len() != 0 {
            self.stats.bump("peer block synced out of order");
        }
        if let Some(id) = take(&mut self.unsynced, pick) {
            self.join_view(id, "peer block synced", "stale peer block dropped at sync");
        }
    }

    /// One of the owner's own blocks lands; it joins the view only if it is
    /// still on the active chain.
    fn land_own(&mut self, pick: u8) {
        if let Some(id) = take(&mut self.unlanded, pick) {
            self.join_view(id, "own block landed late", "stale own block dropped");
        }
    }

    fn join_view(&mut self, id: usize, joined: &'static str, dropped: &'static str) {
        if self.chain.contains(&id) {
            self.view.insert(id);
            self.stats.bump(joined);
        } else {
            self.stats.bump(dropped);
        }
    }

    /// Disconnect the top `depth` blocks, but never a mature one, and make
    /// `branch` active on the new tip. Returns its pool blocks.
    fn reorg(
        &mut self,
        depth: usize,
        branch: &[(Finder, Window)],
    ) -> Result<Vec<Landing>, TestCaseError> {
        // A reorg deeper than the maturity depth is not generated.
        let disconnect = depth.min(self.maturity_depth).min(self.chain.len());
        if disconnect == 0 {
            return Ok(Vec::new());
        }
        if depth > self.maturity_depth {
            self.stats.bump("reorg held at the maturity depth");
        }
        self.stats.bump("reorg");
        let old_tip = self.tip_height();
        let disconnected = self.chain.split_off(self.chain.len() - disconnect);
        for id in &disconnected {
            let block = &self.blocks[*id];
            // Only an immature block is ever disconnected.
            prop_assert!(old_tip - block.height < self.maturity_depth as u64);
            if block.finder != Finder::Foreign {
                self.stats.bump("pool block disconnected");
            }
            if self.rules.reconcile_on_reorg {
                self.view.remove(id);
            }
        }
        // The new branch has more work: it is longer than what it replaces.
        prop_assert!(branch.len() > disconnect);
        let mut landings = Vec::new();
        for (finder, window) in branch {
            let length = self.chain.len();
            if let Some(landing) = self.find(*finder, window)? {
                landings.push(landing);
            }
            if self.chain.len() == length {
                // The owner could not build here; another miner's block
                // takes the height.
                self.find(Finder::Foreign, window)?;
            }
        }
        // So block depths only grow: a mature block stays mature.
        prop_assert!(self.tip_height() > old_tip);
        Ok(landings)
    }

    /// Deliver everything in flight, then recheck the final active chain:
    /// each block raised the debt of exactly its own chain prefix by what was
    /// recorded when it was appended, and the owner's caught-up view is the
    /// truth.
    fn settle(&mut self) -> Result<(), TestCaseError> {
        while !self.unsynced.is_empty() {
            self.sync(0);
        }
        while !self.unlanded.is_empty() {
            self.land_own(0);
        }
        let mut balances = self.initial;
        for id in &self.chain {
            let block = &self.blocks[*id];
            let mut raised = 0;
            for (balance, delta) in balances.iter_mut().zip(block.delta) {
                raised += (debt(*balance + delta) - debt(*balance)).max(0);
                *balance += delta;
            }
            prop_assert_eq!(raised, block.overpay_sats);
        }
        prop_assert_eq!(balances, self.truth());
        if self.rules.reconcile_on_reorg {
            prop_assert_eq!(self.view_balances(), self.truth());
        }
        Ok(())
    }
}

fn take(queue: &mut Vec<usize>, pick: u8) -> Option<usize> {
    if queue.is_empty() {
        return None;
    }
    let index = usize::from(pick) % queue.len();
    Some(queue.remove(index))
}

fn run_case(case: &Case, rules: Rules) -> Result<Stats, TestCaseError> {
    let mut model = Model::new(case.params, rules, case.maturity_depth, case.initial);
    for event in &case.events {
        model.apply(event)?;
    }
    model.settle()?;
    Ok(model.stats)
}

fn any_window() -> impl Strategy<Value = Window> {
    (
        10_000u64..=90_000,
        proptest::array::uniform5(prop_oneof![2 => Just(0u16), 5 => 1u16..=16]),
    )
        .prop_map(|(coinbase_value_sats, mut weights)| {
            // Every block is found on someone's shares.
            if weights.iter().all(|weight| *weight == 0) {
                weights[0] = 1;
            }
            Window {
                coinbase_value_sats,
                weights,
            }
        })
}

fn any_finder() -> impl Strategy<Value = Finder> {
    prop_oneof![
        4 => Just(Finder::Owner),
        3 => Just(Finder::Peer),
        1 => Just(Finder::Foreign),
    ]
}

fn any_event(own_landing_lag: bool) -> BoxedStrategy<Event> {
    let find =
        (any_finder(), any_window()).prop_map(|(finder, window)| Event::Find(finder, window));
    let sync = any::<u8>().prop_map(Event::Sync);
    let reorg = (1usize..=3).prop_flat_map(|depth| {
        prop::collection::vec((any_finder(), any_window()), depth + 1)
            .prop_map(move |branch| Event::Reorg { depth, branch })
    });
    if own_landing_lag {
        prop_oneof![
            8 => find,
            3 => sync,
            3 => any::<u8>().prop_map(Event::LandOwn),
            2 => reorg,
        ]
        .boxed()
    } else {
        prop_oneof![8 => find, 3 => sync, 2 => reorg].boxed()
    }
}

fn any_case(own_landing_lag: bool) -> impl Strategy<Value = Case> {
    (
        prop_oneof![Just(1_500u64), Just(4_000u64), Just(9_000u64)],
        prop_oneof![Just(0u16), Just(125u16), Just(250u16)],
        1usize..=3,
        proptest::array::uniform4(prop_oneof![Just(0i64), -20_000i64..=-1, 1i64..=20_000]),
        prop::collection::vec(any_event(own_landing_lag), 10..=40),
    )
        .prop_map(
            |(floor_sats, fee_bps, maturity_depth, initial, events)| Case {
                params: PolicyParams {
                    floor_sats,
                    fee_bps,
                },
                maturity_depth,
                initial,
                events,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

    /// Invariants 1 and 2 under the contract's rules: owner, non-owner and
    /// foreign blocks, peer sync lagging and out of order, and reorgs within
    /// the maturity depth. No block raises any account's debt, and every
    /// non-owner coinbase is carry-free.
    #[test]
    fn no_block_overpays_and_non_owner_work_is_carry_free(case in any_case(false)) {
        run_case(&case, Rules::CONTRACT)?;
    }

    /// The #478 race: the owner's own blocks may still be landing when it
    /// builds its next job. An owner block then overpays only an account a
    /// missing own block paid down, by no more than that, and in total by no
    /// more than `F` of its as-issued prior; missing peer blocks never add
    /// to it.
    #[test]
    fn an_owner_overpay_needs_a_missing_own_block_and_stays_within_f(case in any_case(true)) {
        run_case(&case, Rules::OWN_LANDING_LAG)?;
    }
}

/// The first `cases` inputs of the properties' strategy under a fixed seed.
fn corpus(own_landing_lag: bool, cases: usize) -> Vec<Case> {
    let mut runner = TestRunner::deterministic();
    let strategy = any_case(own_landing_lag);
    (0..cases)
        .map(|_| {
            strategy
                .new_tree(&mut runner)
                .expect("a generated case")
                .current()
        })
        .collect()
}

fn run_corpus(cases: &[Case], rules: Rules) -> Stats {
    let mut stats = Stats::default();
    for case in cases {
        match run_case(case, rules) {
            Ok(case_stats) => stats.merge(&case_stats),
            Err(error) => panic!("{error}\n{case:#?}"),
        }
    }
    stats
}

/// The properties are not vacuous: their inputs reach every policy branch
/// (floors, sweeps, proportional scaling, carry and debt paydown) and every
/// model path (sync lag, out-of-order and stale syncs, reorgs held at the
/// maturity depth), and still no block overpays.
#[test]
fn the_property_inputs_reach_every_policy_branch_and_model_path() {
    let stats = run_corpus(&corpus(false, 256), Rules::CONTRACT);
    println!("{stats:#?}");
    for path in [
        "owner block",
        "peer block",
        "foreign block",
        "two rows aggregated into one program",
        "gross accrued below the floor",
        "dust swept to the pool fee",
        "payout scaled to the reward",
        "carried balance paid",
        "owner recovered debt",
        "debtor paid its gross by carry-free work",
        "owner built under peer sync lag",
        "peer block synced",
        "peer block synced out of order",
        "stale peer block dropped at sync",
        "reorg",
        "reorg held at the maturity depth",
        "pool block disconnected",
    ] {
        assert!(stats.get(path) > 0, "no input reached {path}: {stats:#?}");
    }
    assert_eq!(stats.get("overpaid block"), 0, "{stats:#?}");
}

/// The #478 race is real, and the property's bound is not vacuous: with own
/// landings lagging, owner blocks do overpay, each with an own block missing
/// from its view (the per-block checks hold each to `F`).
#[test]
fn the_478_race_overpays_only_with_an_own_block_missing() {
    let stats = run_corpus(&corpus(true, 256), Rules::OWN_LANDING_LAG);
    println!("{stats:#?}");
    assert!(stats.get("overpaid block") > 0, "{stats:#?}");
    assert_eq!(
        stats.get("overpaid block"),
        stats.get("overpaid block missing an own block")
    );
    assert_eq!(stats.get("owner built on a stale view"), 0);
}

/// Negative control: the same inputs, but the owner builds on a new tip
/// before deactivating the blocks a reorg disconnected. Some blocks overpay,
/// every one of them on a stale view; under the contract's rules none does.
#[test]
fn skipping_reconcile_before_build_overpays() {
    let cases = corpus(false, 256);
    let stats = run_corpus(&cases, Rules::NO_RECONCILE);
    println!("{stats:#?}");
    assert!(stats.get("overpaid block") > 0, "{stats:#?}");
    assert_eq!(
        stats.get("overpaid block"),
        stats.get("overpaid block on a stale view")
    );
    assert_eq!(run_corpus(&cases, Rules::CONTRACT).get("overpaid block"), 0);
}

/// Floor 4,000 sats and a 200 bps pool fee: a 50,000-sat coinbase leaves a
/// 49,000-sat miner reward.
fn unit_model(rules: Rules, initial: [i64; 4]) -> Model {
    Model::new(
        PolicyParams {
            floor_sats: 4_000,
            fee_bps: 200,
        },
        rules,
        3,
        initial,
    )
}

fn window(coinbase_value_sats: u64, weights: [u16; 5]) -> Window {
    Window {
        coinbase_value_sats,
        weights,
    }
}

fn mine(model: &mut Model, finder: Finder, window: &Window) -> Landing {
    model
        .find(finder, window)
        .unwrap_or_else(|error| panic!("{error}"))
        .expect("a pool block")
}

fn amounts(account: &PayoutPolicyAccount) -> (i128, u64, u64, i128) {
    (
        account.prior_balance_sats,
        account.gross_amount_sats,
        account.onchain_amount_sats,
        account.carry_forward_balance_sats,
    )
}

/// A debtor's gross at or above the floor is paid in full by carry-free
/// work: no debt is recovered and none is added.
#[test]
fn a_non_owner_block_pays_a_debtor_its_gross_and_leaves_the_debt() {
    let mut model = unit_model(Rules::CONTRACT, [-5_000, 0, 0, 0]);
    let landing = mine(&mut model, Finder::Peer, &window(50_000, [10, 10, 0, 0, 0]));
    // (prior, gross, onchain, carry)
    assert_eq!(amounts(landing.account("miner-a")), (0, 24_500, 24_500, 0));
    assert_eq!(landing.delta[A], 0);
    assert_eq!(landing.overpay_sats(), 0);
    assert_eq!(model.truth()[A], -5_000);
}

/// The owner recovers a debt from the debtor's gross once the peer blocks
/// have reached it. A peer block it has not seen yet only makes it withhold
/// more, which the debtor is owed afterwards.
#[test]
fn an_owner_block_recovers_a_debt_after_peer_blocks() {
    let paid = window(50_000, [10, 10, 0, 0, 0]);
    let sub_floor = window(50_000, [1, 19, 0, 0, 0]);
    for synced in [2, 1] {
        let mut model = unit_model(Rules::CONTRACT, [-5_000, 0, 0, 0]);
        // Paid its gross: the debt stays.
        mine(&mut model, Finder::Peer, &paid);
        // 2,450 sats below the floor accrue against the debt.
        let accrued = mine(&mut model, Finder::Peer, &sub_floor);
        assert_eq!(amounts(accrued.account("miner-a")), (0, 2_450, 0, 2_450));
        assert_eq!(model.truth()[A], -2_550);
        for _ in 0..synced {
            model.sync(0);
        }

        let owner = mine(&mut model, Finder::Owner, &paid);
        assert_eq!(owner.overpay_sats(), 0);
        let swept = owner.manifest.pool_fee.as_ref().unwrap();
        if synced == 2 {
            // The debt is recovered exactly; the pool fee takes it.
            assert_eq!(
                amounts(owner.account("miner-a")),
                (-2_550, 24_500, 21_950, 0)
            );
            assert_eq!(swept.swept_dust_liability_sats, 2_550);
            assert_eq!(model.truth()[A], 0);
        } else {
            // The unsynced accrual is withheld once more and owed back.
            assert_eq!(owner.gaps.missing_peer, 1);
            assert_eq!(
                amounts(owner.account("miner-a")),
                (-5_000, 24_500, 19_500, 0)
            );
            assert_eq!(swept.swept_dust_liability_sats, 5_000);
            assert_eq!(model.truth()[A], 2_450);
            model.sync(0);
            assert_eq!(model.view_balances(), model.truth());
        }
    }
}

/// Carry-free work pays each eligible miner exactly its gross; a gross below
/// the floor accrues as carry and its satoshis go to the pool fee, which
/// takes the whole reward when no miner clears the floor.
#[test]
fn a_carry_free_block_sweeps_sub_floor_gross_to_the_pool_fee() {
    let mut model = unit_model(Rules::CONTRACT, [0; 4]);
    let landing = mine(&mut model, Finder::Peer, &window(50_000, [1, 19, 0, 0, 0]));
    assert_eq!(amounts(landing.account("miner-a")), (0, 2_450, 0, 2_450));
    assert_eq!(amounts(landing.account("miner-b")), (0, 46_550, 46_550, 0));
    let fee = landing.manifest.pool_fee.as_ref().unwrap();
    assert_eq!(
        (
            fee.earned_pool_fee_sats,
            fee.swept_dust_liability_sats,
            fee.amount_sats
        ),
        (1_000, 2_450, 3_450)
    );
    assert_eq!(landing.delta, [2_450, 0, 0, 0]);

    // 9,800 sats split 3,267 / 3,267 / 3,266: all below the floor.
    let landing = mine(&mut model, Finder::Peer, &window(10_000, [1, 1, 1, 0, 0]));
    let fee = landing.manifest.pool_fee.as_ref().unwrap();
    assert_eq!(
        (
            fee.earned_pool_fee_sats,
            fee.swept_dust_liability_sats,
            fee.amount_sats
        ),
        (200, 9_800, 10_000)
    );
    assert_eq!(landing.delta, [3_267, 3_267, 3_266, 0]);
    assert!(miner_accounts(&landing.manifest).all(|account| account.onchain_amount_sats == 0));
    assert_eq!(model.truth(), [5_717, 3_267, 3_266, 0]);
}

/// #478: an owner job built before its previous block landed pays a carried
/// balance that block already paid down. The debt is bounded by `F` of the
/// job's prior. The same job missing a peer block instead overpays nothing.
#[test]
fn a_missing_own_block_overpays_within_f_and_a_missing_peer_block_does_not() {
    let job = window(50_000, [1, 19, 0, 0, 0]);
    let mut model = unit_model(Rules::OWN_LANDING_LAG, [3_000, 0, 0, 0]);
    // 49,000 sats over candidates 5,450 and 46,550: miner-a gets 5,136.
    let first = mine(&mut model, Finder::Owner, &job);
    assert_eq!(
        amounts(first.account("miner-a")),
        (3_000, 2_450, 5_136, 314)
    );
    assert_eq!(model.truth()[A], 314);
    let second = mine(&mut model, Finder::Owner, &job);
    assert_eq!(second.gaps.missing_own, 1);
    assert_eq!(second.issued[A], 3_000);
    assert_eq!(second.delta[A], -2_686);
    assert_eq!(second.overpay, [2_372, 0, 0, 0]);
    assert_eq!(second.bound_sats, 3_000);
    assert_eq!(model.truth()[A], -2_372);
    model.land_own(0);
    model.land_own(0);
    assert_eq!(model.view_balances(), model.truth());

    let mut model = unit_model(Rules::CONTRACT, [3_000, 0, 0, 0]);
    mine(&mut model, Finder::Peer, &job);
    let owner = mine(&mut model, Finder::Owner, &job);
    assert_eq!(owner.gaps.missing_peer, 1);
    assert_eq!(owner.issued[A], 3_000);
    assert_eq!(owner.truth_before[A], 5_450);
    assert_eq!(owner.delta[A], -2_686);
    assert_eq!(owner.overpay_sats(), 0);
    assert_eq!(model.truth()[A], 2_764);
}

/// Negative control, the minimal case: a peer block accrued 2,450 sats to
/// miner-a and reached the owner, then a reorg disconnected it. An owner that
/// builds on the new tip before deactivating it pays those 2,450 sats on
/// chain: 4,667 sats against a canonical balance of zero, a 2,217-sat debt.
/// Reconciling first, the same block accrues them instead.
#[test]
fn an_unreconciled_reorg_lets_a_stale_peer_block_overpay() {
    let job = window(50_000, [1, 19, 0, 0, 0]);
    let branch = [(Finder::Foreign, job.clone()), (Finder::Owner, job.clone())];
    for rules in [Rules::NO_RECONCILE, Rules::CONTRACT] {
        let mut model = unit_model(rules, [0; 4]);
        mine(&mut model, Finder::Peer, &job);
        model.sync(0);
        assert_eq!(model.view_balances()[A], 2_450);
        let landings = model
            .reorg(1, &branch)
            .unwrap_or_else(|error| panic!("{error}"));
        let [owner] = landings.as_slice() else {
            panic!("one pool block on the new branch: {landings:#?}");
        };
        assert_eq!(owner.truth_before[A], 0);
        if rules.reconcile_on_reorg {
            assert_eq!(owner.gaps.stale, 0);
            assert_eq!(amounts(owner.account("miner-a")), (0, 2_450, 0, 2_450));
            assert_eq!(owner.overpay_sats(), 0);
            assert_eq!(model.truth()[A], 2_450);
        } else {
            assert_eq!(owner.gaps.stale, 1);
            // The stale view still sees 233 sats owed after the payout.
            assert_eq!(
                amounts(owner.account("miner-a")),
                (2_450, 2_450, 4_667, 233)
            );
            assert_eq!(owner.overpay, [2_217, 0, 0, 0]);
            assert!(owner.overpay_sats() <= owner.bound_sats);
            assert_eq!(model.truth()[A], -2_217);
        }
    }
}

/// Maturity: a reorg never reaches below the maturity depth, and the longer
/// branch keeps every deeper block's depth growing.
#[test]
fn a_reorg_stops_at_the_maturity_depth() {
    let job = window(50_000, [5, 5, 0, 0, 0]);
    let mut model = Model::new(
        PolicyParams {
            floor_sats: 4_000,
            fee_bps: 200,
        },
        Rules::CONTRACT,
        1,
        [0; 4],
    );
    mine(&mut model, Finder::Owner, &job);
    mine(&mut model, Finder::Peer, &job);
    let kept = model.chain[0];
    let branch = vec![(Finder::Foreign, job.clone()); 4];
    model
        .reorg(3, &branch)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(model.chain[0], kept);
    assert_eq!(model.chain.len(), 1 + 4);
    assert_eq!(model.stats.get("reorg held at the maturity depth"), 1);
    assert_eq!(model.stats.get("pool block disconnected"), 1);
}

/// The non-owner's empty prior set is the carry gate's: the digest every
/// carry-free block commits to.
#[test]
fn the_non_owner_prior_set_is_the_carry_gates() {
    let mut model = unit_model(Rules::CONTRACT, [7_000, -3_000, 0, 0]);
    let landing = mine(&mut model, Finder::Peer, &window(50_000, [3, 2, 1, 1, 1]));
    assert_eq!(landing.issued, [0; 4]);
    assert!(miner_accounts(&landing.manifest).all(|account| account.prior_balance_sats == 0));
    assert_eq!(prior_balances_digest(&[]), carry_free_prior_digest());
    // A peer block never lists a prior-only account: the carried balances are
    // untouched.
    assert_eq!(miner_accounts(&landing.manifest).count(), 4);
    assert_eq!(model.truth()[B], -3_000 + landing.delta[B]);
}
