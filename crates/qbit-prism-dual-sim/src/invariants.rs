//! The payout-invariant checker: CONTRACT.md §4, read from both databases,
//! the chain and the share records, after a scenario has settled.
//!
//! **The census comes from the chain, not from a database.** Every block of
//! the active chain since the scenario started is read from C (the rest of
//! the network), and a block whose coinbase carries the pool's tag
//! (`/PRISM/`) is a pool block. A database's `chain_state` never decides
//! what is on the chain, so a pool block on chain that no database knows is
//! found too (it has no landing rows anywhere: invariant 3). Each block's
//! origin is the node whose frontend issued the job it was solved on: the
//! job id names its frontend (`<instance>-<uuid>`).
//!
//! **Checks**, each a [`Check`] with its own verdict:
//!
//! 1. `inv1-no-overpay`: per account (P2MR program), the balance is the sum
//!    of `gross - onchain` of the as-issued carry rows of every census block,
//!    in height order (PRISM's own definition, `ledger/divergence.rs`). Debt
//!    is `max(0, -balance)`. A block that raises an account's debt fails,
//!    unless the owner (in the 3.0 pair, the one database) recorded that
//!    block and account as a #478 divergence whose `overpay_sats` covers the
//!    increase. `owner-balances-match-chain` then holds the owner's own
//!    balance view to the same sums.
//! 2. `inv2-non-owner-carry-free`: every block the non-owner issued has an
//!    empty prior set, the empty prior digest, and `onchain <= gross` for
//!    every miner, in its audit and in its carry rows.
//! 3. `inv3-landing-rows`: every census block has its landing rows in every
//!    checked database (block, audit bundle and snapshot, one payout entry
//!    per manifest account, one carry row per miner account, its CTV fanout
//!    set and artifacts), identical across databases.
//!    `inv3-audits-verify`: each node's frontend serves the block's bundle,
//!    `qbit-prism-audit-verify` accepts it against the coinbase from the
//!    chain with the pinned ledger key, its manifest key is the pinned one,
//!    and both nodes serve the same bundle. `inv3-fanouts-identical`: each
//!    database holds the bundle's fanout transactions byte for byte.
//! 4. `inv4-acked-shares-present`: every share a miner saw accepted is in
//!    every checked database, except shares a scenario excuses as a dead
//!    node's documented tail (reported with the reason).
//!    `inv4-no-double-credit`: one ledger row per share id and per header in
//!    each database, every row mapped to its header, and a header present in
//!    both databases has the same row in both. `inv4-windows-unchanged`: every
//!    window a landing recorded still has exactly its shares, with no newer
//!    eligible share. `inv4-windows-reproducible`: each recorded window,
//!    recomputed from every database, gives the recorded digest.
//!
//! Plus `ledger-integrity` (`qbit_carry_forward_integrity_report()` clean)
//! and `candidates-settled` (no candidate left unfinished).

use crate::{
    frontend::Node,
    load::ShareRecord,
    sim::{Sim, Topology},
};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// `hex("/PRISM/")`, the default `PRISM_COINBASE_TAG`.
pub const POOL_TAG_HEX: &str = "2f505249534d2f";
/// The empty prior set's digest (`qbit_prism::prior_balances_digest` of []).
pub const EMPTY_PRIOR_DIGEST: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// At most this many problems are listed per check; the count is exact.
const PROBLEM_LIMIT: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pass,
    Fail,
    /// Not applicable to this topology, with the reason in `summary`.
    Skip,
}

#[derive(Clone, Debug, Serialize)]
pub struct Check {
    pub id: String,
    pub status: Status,
    pub summary: String,
    pub problem_count: usize,
    pub problems: Vec<String>,
    pub data: Value,
}

impl Check {
    fn skip(id: &str, reason: &str) -> Self {
        Self {
            id: id.to_owned(),
            status: Status::Skip,
            summary: reason.to_owned(),
            problem_count: 0,
            problems: Vec::new(),
            data: Value::Null,
        }
    }
}

struct CheckBuilder {
    id: String,
    problems: Vec<String>,
    data: Value,
}

impl CheckBuilder {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            problems: Vec::new(),
            data: json!({}),
        }
    }

    fn problem(&mut self, problem: String) {
        self.problems.push(problem);
    }

    fn data(&mut self, key: &str, value: Value) {
        self.data[key] = value;
    }

    fn finish(self, summary: String) -> Check {
        let count = self.problems.len();
        Check {
            status: if count == 0 {
                Status::Pass
            } else {
                Status::Fail
            },
            summary,
            problem_count: count,
            problems: self.problems.into_iter().take(PROBLEM_LIMIT).collect(),
            id: self.id,
            data: self.data,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct InvariantReport {
    pub checks: Vec<Check>,
    pub census: Vec<CensusBlock>,
}

impl InvariantReport {
    pub fn failures(&self) -> Vec<&Check> {
        self.checks
            .iter()
            .filter(|check| check.status == Status::Fail)
            .collect()
    }

    pub fn passed(&self) -> bool {
        self.failures().is_empty()
    }

    pub fn get(&self, id: &str) -> Option<&Check> {
        self.checks.iter().find(|check| check.id == id)
    }
}

/// What a scenario tells the checker beyond the run itself.
#[derive(Clone, Debug, Default)]
pub struct CheckOptions {
    /// Accepted shares that may be missing, by share id, with why: a dead
    /// node's documented unsynced tail.
    pub excused_missing: BTreeMap<String, String>,
    /// Databases whose landing rows are not expected yet (a node still
    /// down), with why. Its other checks still run where they can.
    pub absent_ledgers: BTreeMap<Node, String>,
    /// Who owned the carry from which height, oldest first, when it is not
    /// the dual-writer pair's A from the start: the 3.0 epoch before a
    /// cutover has no owner (S9), and a transfer moves it (S11). Empty means
    /// the topology's default.
    pub ownership: Vec<(u64, Option<Node>)>,
}

impl CheckOptions {
    /// The carry owner when a block at `height` was built: `None` in a
    /// single-writer epoch.
    pub fn owner_at(&self, topology: Topology, height: u64) -> Option<Node> {
        if self.ownership.is_empty() {
            return (topology == Topology::DualWriter).then_some(Node::A);
        }
        self.ownership
            .iter()
            .take_while(|(from, _)| *from <= height)
            .last()
            .and_then(|(_, owner)| *owner)
    }
}

/// One pool block of the active chain.
#[derive(Clone, Debug, Serialize)]
pub struct CensusBlock {
    pub hash: String,
    pub height: u64,
    pub origin: Option<Node>,
    pub coinbase_txid: String,
    /// Which checked databases hold its `qbit_pool_blocks` row.
    pub landed_in: Vec<Node>,
    #[serde(skip)]
    pub coinbase_hex: String,
}

/// The databases the checker holds to the invariants: in the 3.0 pair the
/// one writer, otherwise both.
pub fn ledger_nodes(sim: &Sim) -> Vec<Node> {
    match sim.config.topology {
        Topology::SingleWriter => vec![Node::A],
        Topology::DualWriter | Topology::Unsynced => Node::BOTH.to_vec(),
    }
}

/// The owner at the census's newest block, whose balance view the checker
/// holds to the chain: the dual-writer owner, or in the 3.0 pair the one
/// database (A's).
fn current_owner(sim: &Sim, options: &CheckOptions, census: &[CensusBlock]) -> Node {
    let tip = census.last().map_or(sim.start_height, |block| block.height);
    options
        .owner_at(sim.config.topology, tip)
        .unwrap_or(Node::A)
}

/// Run every check.
pub async fn check(
    sim: &Sim,
    shares: &[ShareRecord],
    options: &CheckOptions,
) -> Result<InvariantReport> {
    let mut pools = BTreeMap::new();
    for node in ledger_nodes(sim) {
        if options.absent_ledgers.contains_key(&node) {
            continue;
        }
        pools.insert(node, sim.pool(node).await?);
    }
    let census = census(sim, &pools, shares).await?;
    let mut checks = Vec::new();
    checks.push(no_overpay(sim, &pools, &census, options).await?);
    checks.push(owner_balances_match_chain(sim, &pools, &census, options).await?);
    checks.push(non_owner_carry_free(sim, &pools, &census, options).await?);
    checks.push(landing_rows(&pools, &census, options).await?);
    checks.push(audits_verify(sim, &census, options).await?);
    checks.push(fanouts_identical(&pools, &census).await?);
    checks.push(acked_shares_present(&pools, shares, options).await?);
    checks.push(no_double_credit(&pools).await?);
    checks.push(windows_unchanged(&pools).await?);
    checks.push(windows_reproducible(&pools).await?);
    checks.push(ledger_integrity(&pools).await?);
    checks.push(candidates_settled(&pools).await?);
    for pool in pools.values() {
        pool.close().await;
    }
    Ok(InvariantReport { checks, census })
}

// --- census ----------------------------------------------------------------

async fn census(
    sim: &Sim,
    pools: &BTreeMap<Node, PgPool>,
    shares: &[ShareRecord],
) -> Result<Vec<CensusBlock>> {
    let network = &sim.chain.c;
    let (tip, _, _) = network.tip().await?;
    let mut landed: BTreeMap<String, Vec<Node>> = BTreeMap::new();
    for (node, pool) in pools {
        let hashes: Vec<String> = sqlx::query_scalar("SELECT block_hash FROM qbit_pool_blocks")
            .fetch_all(pool)
            .await?;
        for hash in hashes {
            landed.entry(hash).or_default().push(*node);
        }
    }
    let issued_by: BTreeMap<String, Node> = shares
        .iter()
        .filter(|record| record.scheduled_block && record.accepted())
        .filter_map(|record| Some((record.header_hash().to_owned(), record.issuer?)))
        .collect();
    let mut blocks = Vec::new();
    for height in (sim.start_height + 1)..=tip {
        let hash = network
            .rpc("getblockhash", json!([height]))
            .await?
            .as_str()
            .context("block hash")?
            .to_owned();
        let block = network.rpc("getblock", json!([hash, 2])).await?;
        let coinbase = &block["tx"][0];
        let script_sig = coinbase["vin"][0]["coinbase"].as_str().unwrap_or_default();
        if !script_sig.contains(POOL_TAG_HEX) {
            continue;
        }
        let mut origin = issued_by.get(&hash).copied();
        if origin.is_none() {
            for pool in pools.values() {
                if let Some(job) = solver_job(pool, &hash).await? {
                    origin = crate::load::issuer_of(&job);
                    if origin.is_some() {
                        break;
                    }
                }
            }
        }
        blocks.push(CensusBlock {
            landed_in: landed.remove(&hash).unwrap_or_default(),
            hash,
            height,
            origin,
            coinbase_txid: coinbase["txid"].as_str().unwrap_or_default().to_owned(),
            coinbase_hex: coinbase["hex"].as_str().unwrap_or_default().to_owned(),
        });
    }
    Ok(blocks)
}

/// The job id of the share that solved `hash`, credited or deferred.
async fn solver_job(pool: &PgPool, hash: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT job_id FROM qbit_share_ledger WHERE lower(right(share_id, 64)) = $1 \
         UNION ALL SELECT share->>'job_id' FROM qbit_prism_deferred_shares WHERE block_hash = $1 \
         LIMIT 1",
    )
    .bind(hash)
    .fetch_optional(pool)
    .await?)
}

/// One block's as-issued carry rows, per program: `(gross, onchain, prior)`.
async fn carry_rows(pool: &PgPool, hash: &str) -> Result<BTreeMap<String, (i128, i128, i128)>> {
    let rows = sqlx::query(
        "SELECT encode(p2mr_program, 'hex') AS program, \
                sum(gross_amount_sats)::text AS gross, sum(onchain_amount_sats)::text AS onchain, \
                sum(prior_balance_sats)::text AS prior \
         FROM qbit_payout_carry_forward \
         WHERE block_hash = $1 AND maturity_state <> 'reversed' GROUP BY p2mr_program",
    )
    .bind(hash)
    .fetch_all(pool)
    .await?;
    let mut carry = BTreeMap::new();
    for row in rows {
        carry.insert(
            row.try_get::<String, _>("program")?,
            (
                parse_i128(&row.try_get::<String, _>("gross")?)?,
                parse_i128(&row.try_get::<String, _>("onchain")?)?,
                parse_i128(&row.try_get::<String, _>("prior")?)?,
            ),
        );
    }
    Ok(carry)
}

fn parse_i128(text: &str) -> Result<i128> {
    text.parse()
        .with_context(|| format!("{text:?} is not an integer"))
}

/// The database a block's landing rows are read from: its origin's when
/// that database has them, else any that does.
fn source_of<'a>(
    block: &CensusBlock,
    pools: &'a BTreeMap<Node, PgPool>,
) -> Option<(Node, &'a PgPool)> {
    block
        .origin
        .and_then(|origin| {
            block
                .landed_in
                .contains(&origin)
                .then(|| pools.get(&origin).map(|pool| (origin, pool)))
                .flatten()
        })
        .or_else(|| {
            block
                .landed_in
                .iter()
                .find_map(|node| pools.get(node).map(|pool| (*node, pool)))
        })
}

// --- invariant 1 -------------------------------------------------------------

/// One census block's as-issued carry rows, per program: `(gross, onchain,
/// prior)`, and the #478 overpay its confirming database recorded per
/// program, when it may carry one.
#[derive(Clone, Debug)]
pub struct BlockRows {
    pub hash: String,
    pub height: u64,
    pub origin: Option<Node>,
    pub carry: BTreeMap<String, (i128, i128, i128)>,
    pub recorded_overpay: BTreeMap<String, i128>,
}

/// A block that raised an account's debt with no record to cover it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DebtIncrease {
    pub block: String,
    pub program: String,
    pub increase: i128,
    pub debt_after: i128,
    pub recorded: Option<i128>,
}

/// Walk `blocks` in order: per program, `balance += gross - onchain`, and
/// every increase of `max(0, -balance)` not covered by a recorded #478
/// overpay is returned. Also returns the final balances and how many
/// increases records covered.
pub fn debt_walk(blocks: &[BlockRows]) -> (Vec<DebtIncrease>, BTreeMap<String, i128>, usize) {
    let mut balances: BTreeMap<String, i128> = BTreeMap::new();
    let mut increases = Vec::new();
    let mut excused = 0;
    for block in blocks {
        for (program, (gross, onchain, _)) in &block.carry {
            let before = *balances.get(program).unwrap_or(&0);
            let after = before + gross - onchain;
            balances.insert(program.clone(), after);
            let increase = (-after).max(0) - (-before).max(0);
            if increase <= 0 {
                continue;
            }
            let recorded = block.recorded_overpay.get(program).copied();
            if recorded.is_some_and(|recorded| recorded >= increase) {
                excused += 1;
                continue;
            }
            increases.push(DebtIncrease {
                block: block.hash.clone(),
                program: program.clone(),
                increase,
                debt_after: (-after).max(0),
                recorded,
            });
        }
    }
    (increases, balances, excused)
}

async fn no_overpay(
    sim: &Sim,
    pools: &BTreeMap<Node, PgPool>,
    census: &[CensusBlock],
    options: &CheckOptions,
) -> Result<Check> {
    let mut check = CheckBuilder::new("inv1-no-overpay");
    let mut blocks = Vec::new();
    for block in census {
        let Some((_, pool)) = source_of(block, pools) else {
            check.problem(format!(
                "pool block {} at {} has no carry rows in any database, so its payouts cannot be \
                 accounted",
                block.hash, block.height
            ));
            continue;
        };
        let carry = carry_rows(pool, &block.hash).await?;
        // The #478 race is recorded by the database that confirmed the
        // block; in a dual-writer epoch only the owner's own blocks may
        // carry it.
        let recorder = match options.owner_at(sim.config.topology, block.height) {
            Some(owner) => (block.origin == Some(owner))
                .then(|| pools.get(&owner))
                .flatten(),
            None => Some(pool),
        };
        let mut recorded_overpay = BTreeMap::new();
        if let Some(recorder) = recorder {
            for program in carry.keys() {
                if let Some(recorded) = recorded_overpay_of(recorder, &block.hash, program).await? {
                    recorded_overpay.insert(program.clone(), recorded);
                }
            }
        }
        blocks.push(BlockRows {
            hash: block.hash.clone(),
            height: block.height,
            origin: block.origin,
            carry,
            recorded_overpay,
        });
    }
    let (increases, balances, excused) = debt_walk(&blocks);
    for increase in &increases {
        let block = blocks.iter().find(|b| b.hash == increase.block);
        check.problem(format!(
            "pool block {} at {} (origin {:?}) raised the debt of {} by {} sats to {} (recorded \
             #478 overpay: {})",
            increase.block,
            block.map_or(0, |b| b.height),
            block.and_then(|b| b.origin),
            short(&increase.program),
            increase.increase,
            increase.debt_after,
            increase
                .recorded
                .map_or_else(|| "none".into(), |recorded| recorded.to_string())
        ));
    }
    let debt: i128 = balances.values().map(|balance| (-balance).max(0)).sum();
    check.data(
        "blocks",
        json!(blocks
            .iter()
            .map(|block| json!({
                "block": block.hash, "height": block.height, "origin": block.origin,
                "accounts": block.carry.iter().map(|(program, (gross, onchain, prior))| json!({
                    "program": short(program), "prior": prior.to_string(),
                    "gross": gross.to_string(), "onchain": onchain.to_string(),
                })).collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>()),
    );
    check.data(
        "final_balances",
        json!(balances
            .iter()
            .map(|(p, b)| (short(p), b.to_string()))
            .collect::<BTreeMap<_, _>>()),
    );
    check.data("final_pool_debt_sats", json!(debt.to_string()));
    check.data("excused_478_increases", json!(excused));
    Ok(check.finish(format!(
        "{} pool blocks walked over {} accounts; final pool debt {debt} sats; {excused} recorded \
         #478 increases",
        census.len(),
        balances.len()
    )))
}

async fn recorded_overpay_of(pool: &PgPool, block: &str, program: &str) -> Result<Option<i128>> {
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT a.overpay_sats::text FROM qbit_prism_payout_divergence_accounts a \
         JOIN qbit_prism_payout_divergences d ON d.block_hash = a.block_hash \
         WHERE a.block_hash = $1 AND a.p2mr_program = decode($2, 'hex') \
           AND d.confirmed_at IS NOT NULL AND d.divergent_accounts > 0",
    )
    .bind(block)
    .bind(program)
    .fetch_optional(pool)
    .await?;
    recorded.as_deref().map(parse_i128).transpose()
}

async fn owner_balances_match_chain(
    sim: &Sim,
    pools: &BTreeMap<Node, PgPool>,
    census: &[CensusBlock],
    options: &CheckOptions,
) -> Result<Check> {
    let mut check = CheckBuilder::new("owner-balances-match-chain");
    let owner = current_owner(sim, options, census);
    let Some(pool) = pools.get(&owner) else {
        return Ok(Check::skip(
            "owner-balances-match-chain",
            "the owner's database is not checked in this run",
        ));
    };
    let mut truth: BTreeMap<String, i128> = BTreeMap::new();
    for block in census {
        let Some((_, source)) = source_of(block, pools) else {
            continue;
        };
        for (program, (gross, onchain, _)) in carry_rows(source, &block.hash).await? {
            *truth.entry(program).or_default() += gross - onchain;
        }
    }
    truth.retain(|_, balance| *balance != 0);
    let rows = sqlx::query(
        "SELECT encode(p2mr_program, 'hex') AS program, balance_sats::text AS balance \
         FROM qbit_current_carry_forward_balances()",
    )
    .fetch_all(pool)
    .await?;
    let mut view = BTreeMap::new();
    for row in rows {
        view.insert(
            row.try_get::<String, _>("program")?,
            parse_i128(&row.try_get::<String, _>("balance")?)?,
        );
    }
    for program in truth.keys().chain(view.keys()).collect::<BTreeSet<_>>() {
        let (want, have) = (truth.get(program), view.get(program));
        if want != have {
            check.problem(format!(
                "{}: the chain's as-issued rows sum to {}, the owner's balances say {}",
                short(program),
                want.map_or_else(|| "0".into(), i128::to_string),
                have.map_or_else(|| "0".into(), i128::to_string)
            ));
        }
    }
    check.data("accounts", json!(truth.len()));
    Ok(check.finish(format!(
        "node {owner:?}'s current balances against the sums over {} pool blocks",
        census.len()
    )))
}

// --- invariant 2 -------------------------------------------------------------

async fn non_owner_carry_free(
    sim: &Sim,
    pools: &BTreeMap<Node, PgPool>,
    census: &[CensusBlock],
    options: &CheckOptions,
) -> Result<Check> {
    // A block is the non-owner's when its origin is not the owner of its
    // height's epoch; a single-writer epoch has none.
    let blocks: Vec<&CensusBlock> = census
        .iter()
        .filter(|block| {
            options
                .owner_at(sim.config.topology, block.height)
                .is_some_and(|owner| block.origin.is_some_and(|origin| origin != owner))
        })
        .collect();
    if sim.config.topology != Topology::DualWriter && options.ownership.is_empty() {
        return Ok(Check::skip(
            "inv2-non-owner-carry-free",
            "single-writer topology: there is no non-owner",
        ));
    }
    let mut check = CheckBuilder::new("inv2-non-owner-carry-free");
    for block in &blocks {
        for node in &block.landed_in {
            let Some(pool) = pools.get(node) else {
                continue;
            };
            let row = sqlx::query(
                "SELECT jsonb_array_length(COALESCE(a.audit_bundle->'prior_balances', '[]'::jsonb)) AS prior_entries, \
                        a.audit_bundle #>> '{ledger_window_attestation,prior_balances_digest_hex}' AS prior_digest, \
                        (SELECT count(*) FROM jsonb_array_elements(a.audit_bundle #> '{payout_policy_manifest,accounts}') acc \
                          WHERE COALESCE(acc->>'account_type', 'miner') = 'miner' \
                            AND ((acc->>'prior_balance_sats')::numeric <> 0 \
                                 OR (acc->>'gross_amount_sats')::numeric < (acc->>'onchain_amount_sats')::numeric)) AS bad_accounts, \
                        (SELECT count(*) FROM qbit_payout_carry_forward c \
                          WHERE c.block_hash = a.block_hash \
                            AND (c.prior_balance_sats <> 0 OR c.gross_amount_sats < c.onchain_amount_sats)) AS bad_rows \
                 FROM qbit_pool_audit_bundles a WHERE a.block_hash = $1",
            )
            .bind(&block.hash)
            .fetch_optional(pool)
            .await?;
            let Some(row) = row else {
                check.problem(format!(
                    "{} has no audit bundle on node {node:?}",
                    block.hash
                ));
                continue;
            };
            let prior_entries: i32 = row.try_get("prior_entries")?;
            let digest: Option<String> = row.try_get("prior_digest")?;
            let bad_accounts: i64 = row.try_get("bad_accounts")?;
            let bad_rows: i64 = row.try_get("bad_rows")?;
            if prior_entries != 0
                || digest.as_deref() != Some(EMPTY_PRIOR_DIGEST)
                || bad_accounts != 0
                || bad_rows != 0
            {
                check.problem(format!(
                    "non-owner block {} on node {node:?}: {prior_entries} prior entries, prior \
                     digest {digest:?}, {bad_accounts} manifest accounts and {bad_rows} carry rows \
                     with a prior or onchain above gross",
                    block.hash
                ));
            }
        }
    }
    Ok(check.finish(format!(
        "{} blocks issued by a non-owner of their epoch",
        blocks.len()
    )))
}

// --- invariant 3 -------------------------------------------------------------

async fn landing_rows(
    pools: &BTreeMap<Node, PgPool>,
    census: &[CensusBlock],
    options: &CheckOptions,
) -> Result<Check> {
    let mut check = CheckBuilder::new("inv3-landing-rows");
    let hashes: Vec<String> = census.iter().map(|block| block.hash.clone()).collect();
    let mut fingerprints: BTreeMap<String, BTreeMap<Node, Value>> = BTreeMap::new();
    for (node, pool) in pools {
        let rows = sqlx::query(
            "SELECT h.block_hash, b.block_hash IS NOT NULL AS has_block, b.chain_state, \
                    a.block_hash IS NOT NULL AS has_bundle, \
                    COALESCE(b.as_issued_audit_sha256 = a.audit_bundle_sha256, false) AS marker_matches, \
                    s.snapshot_sha256 IS NOT NULL AS has_snapshot, \
                    (SELECT count(*) FROM qbit_pool_payout_entries p WHERE p.block_hash = h.block_hash) AS payout_rows, \
                    COALESCE(jsonb_array_length(a.audit_bundle #> '{payout_policy_manifest,accounts}'), -1) AS manifest_accounts, \
                    (SELECT count(*) FROM qbit_payout_carry_forward c WHERE c.block_hash = h.block_hash) AS carry_rows, \
                    (SELECT count(*) FROM jsonb_array_elements(COALESCE(a.audit_bundle #> '{payout_policy_manifest,accounts}', '[]'::jsonb)) acc \
                      WHERE COALESCE(acc->>'account_type', 'miner') = 'miner') AS manifest_miners, \
                    COALESCE(jsonb_array_length(a.audit_bundle #> '{ctv_fanout_manifest_set,manifests}'), 0) AS expected_fanouts, \
                    f.block_hash IS NOT NULL AS has_fanout_set, \
                    (SELECT count(*) FROM qbit_ctv_fanout_artifacts fa WHERE fa.block_hash = h.block_hash) AS fanout_artifacts, \
                    a.audit_bundle_sha256, a.share_snapshot_sha256, \
                    b.block_height, b.parent_hash, b.coinbase_txid, b.payout_manifest_sha256, \
                    b.as_issued_audit_sha256, \
                    (SELECT md5(string_agg(concat_ws('|', p.miner_id, p.payout_order_key, encode(p.p2mr_program, 'hex'), \
                         p.onchain_amount_sats, p.carry_forward_balance_sats, p.action), ',' \
                         ORDER BY p.payout_order_key, p.miner_id, p.p2mr_program)) \
                       FROM qbit_pool_payout_entries p WHERE p.block_hash = h.block_hash) AS payout_fp, \
                    (SELECT md5(string_agg(concat_ws('|', c.miner_id, c.payout_order_key, encode(c.p2mr_program, 'hex'), \
                         c.gross_amount_sats, c.prior_balance_sats, c.candidate_balance_sats, c.onchain_amount_sats, \
                         c.settlement_fee_sats, c.carry_forward_balance_sats, c.action), ',' \
                         ORDER BY c.payout_order_key, c.miner_id, c.p2mr_program)) \
                       FROM qbit_payout_carry_forward c WHERE c.block_hash = h.block_hash) AS carry_fp \
             FROM unnest($1::text[]) AS h(block_hash) \
             LEFT JOIN qbit_pool_blocks b ON b.block_hash = h.block_hash \
             LEFT JOIN qbit_pool_audit_bundles a ON a.block_hash = h.block_hash \
             LEFT JOIN qbit_prism_audit_snapshots s ON s.snapshot_sha256 = a.share_snapshot_sha256 \
             LEFT JOIN qbit_ctv_fanout_sets f ON f.block_hash = h.block_hash",
        )
        .bind(&hashes)
        .fetch_all(pool)
        .await?;
        for row in rows {
            let hash: String = row.try_get("block_hash")?;
            if !row.try_get::<bool, _>("has_block")? {
                if !options.absent_ledgers.contains_key(node) {
                    check.problem(format!(
                        "pool block {hash} has no landing rows on node {node:?}"
                    ));
                }
                continue;
            }
            let state: Option<String> = row.try_get("chain_state")?;
            let mut faults = Vec::new();
            if state.as_deref() != Some("confirmed") {
                faults.push(format!("chain_state {state:?}, not confirmed"));
            }
            if !row.try_get::<bool, _>("has_bundle")? {
                faults.push("no audit bundle".into());
            }
            if !row.try_get::<bool, _>("marker_matches")? {
                faults.push("as-issued audit marker differs from the bundle".into());
            }
            if !row.try_get::<bool, _>("has_snapshot")? {
                faults.push("no audit snapshot".into());
            }
            let payout_rows: i64 = row.try_get("payout_rows")?;
            let manifest_accounts: i32 = row.try_get("manifest_accounts")?;
            if payout_rows != i64::from(manifest_accounts) {
                faults.push(format!(
                    "{payout_rows} payout entries for {manifest_accounts} manifest accounts"
                ));
            }
            let carry: i64 = row.try_get("carry_rows")?;
            let miners: i64 = row.try_get("manifest_miners")?;
            if carry != miners {
                faults.push(format!("{carry} carry rows for {miners} miner accounts"));
            }
            let expected: i32 = row.try_get("expected_fanouts")?;
            let artifacts: i64 = row.try_get("fanout_artifacts")?;
            if row.try_get::<bool, _>("has_fanout_set")? != (expected > 0)
                || artifacts != i64::from(expected)
            {
                faults.push(format!(
                    "{artifacts} fanout artifacts for {expected} manifests"
                ));
            }
            // The landed coinbase must be the chain's: the census read it
            // from C, never from a database.
            let coinbase_txid: Option<String> = row.try_get("coinbase_txid")?;
            if let Some(block) = census.iter().find(|block| block.hash == hash) {
                if coinbase_txid.as_deref() != Some(block.coinbase_txid.as_str()) {
                    faults.push(format!(
                        "coinbase txid {coinbase_txid:?}, the chain's is {}",
                        block.coinbase_txid
                    ));
                }
            }
            if !faults.is_empty() {
                check.problem(format!(
                    "pool block {hash} on node {node:?}: {}",
                    faults.join("; ")
                ));
            }
            // CONTRACT.md D-1 and D-10: every immutable landing fact is the
            // same on both nodes; only local columns (chain and maturity
            // state, publication order, claims) may differ.
            fingerprints.entry(hash).or_default().insert(
                *node,
                json!({
                    "height": row.try_get::<Option<i64>, _>("block_height")?,
                    "parent": row.try_get::<Option<String>, _>("parent_hash")?,
                    "coinbase_txid": coinbase_txid,
                    "payout_manifest": row.try_get::<Option<String>, _>("payout_manifest_sha256")?,
                    "as_issued_audit": row.try_get::<Option<String>, _>("as_issued_audit_sha256")?,
                    "audit": row.try_get::<Option<String>, _>("audit_bundle_sha256")?,
                    "snapshot": row.try_get::<Option<String>, _>("share_snapshot_sha256")?,
                    "payouts": row.try_get::<Option<String>, _>("payout_fp")?,
                    "carry": row.try_get::<Option<String>, _>("carry_fp")?,
                }),
            );
        }
    }
    for (hash, by_node) in &fingerprints {
        let mut values = by_node.values();
        if let Some(first) = values.next() {
            if values.any(|other| other != first) {
                check.problem(format!(
                    "pool block {hash}'s landing rows differ between nodes: {}",
                    json!(by_node)
                ));
            }
        }
    }
    Ok(check.finish(format!(
        "{} pool blocks in {} databases",
        census.len(),
        pools.len()
    )))
}

/// The keys the test signing seeds give (`PRISM_ALLOW_TEST_SIGNING_SEEDS`):
/// `(ledger writer, coinbase manifest)`.
pub fn pinned_keys() -> Result<(String, String)> {
    let key = |seed: &str| -> Result<String> {
        Ok(
            qbit_pool_builder::ManifestSigningKey::from_seed_hex(&seed.repeat(32))
                .map_err(|error| anyhow::anyhow!("{error:?}"))?
                .public_key_hex(),
        )
    };
    Ok((key("22")?, key("11")?))
}

async fn audits_verify(sim: &Sim, census: &[CensusBlock], options: &CheckOptions) -> Result<Check> {
    let mut check = CheckBuilder::new("inv3-audits-verify");
    let (ledger_key, manifest_key) = pinned_keys()?;
    let verifier = sim.inputs.verifier_dir.join("qbit-prism-audit-verify");
    let dir = sim.report_dir.join("audits");
    std::fs::create_dir_all(&dir)?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let mut verified = 0;
    for block in census {
        let mut digests = BTreeMap::new();
        for node in Node::BOTH {
            if options.absent_ledgers.contains_key(&node) || !sim.frontend(node).running() {
                continue;
            }
            if sim.config.topology == Topology::Unsynced && !block.landed_in.contains(&node) {
                // The negative control: the other node cannot know it.
                continue;
            }
            let url = format!(
                "http://127.0.0.1:{}/audit/blocks/{}/bundle",
                sim.frontend(node).spec.api_port,
                block.hash
            );
            let response = client.get(&url).send().await?;
            if !response.status().is_success() {
                check.problem(format!(
                    "node {node:?} does not serve {}'s audit bundle: HTTP {}",
                    block.hash,
                    response.status()
                ));
                continue;
            }
            let body: Value = response.json().await?;
            let bundle = &body["audit_bundle"];
            let file = dir.join(format!("{}-{}.json", block.hash, node.label()));
            std::fs::write(&file, serde_json::to_vec(bundle)?)?;
            match run_verifier(&verifier, &file, &block.coinbase_hex, &ledger_key) {
                Ok(report) => {
                    let mut faults = Vec::new();
                    if report["coinbase_txid"] != block.coinbase_txid.as_str() {
                        faults.push(format!("coinbase txid {}", report["coinbase_txid"]));
                    }
                    if report["block_height"].as_u64() != Some(block.height)
                        && report["block_height"] != Value::Null
                    {
                        faults.push(format!("height {}", report["block_height"]));
                    }
                    let signer = &bundle["signed_coinbase_manifest"]["signature"]["public_key_hex"];
                    if signer != manifest_key.as_str() {
                        faults.push(format!("manifest signed by {signer}, not the pinned key"));
                    }
                    if faults.is_empty() {
                        verified += 1;
                    } else {
                        check.problem(format!(
                            "{}'s audit from node {node:?} verified but disagrees: {}",
                            block.hash,
                            faults.join("; ")
                        ));
                    }
                    digests.insert(node, report["audit_bundle_sha256_hex"].clone());
                }
                Err(error) => check.problem(format!(
                    "qbit-prism-audit-verify refused {}'s audit from node {node:?}: {error:#}",
                    block.hash
                )),
            }
        }
        let mut values = digests.values();
        if let Some(first) = values.next() {
            if values.any(|other| other != first) {
                check.problem(format!(
                    "the nodes serve different audits for {}: {}",
                    block.hash,
                    json!(digests)
                ));
            }
        }
    }
    check.data("verified", json!(verified));
    Ok(check.finish(format!(
        "{verified} audit bundles verified by the binary against the chain's coinbases"
    )))
}

fn run_verifier(verifier: &Path, bundle: &Path, coinbase_hex: &str, key: &str) -> Result<Value> {
    let output = std::process::Command::new(verifier)
        .arg(bundle)
        .args(["--coinbase-tx-hex", coinbase_hex])
        .args(["--ledger-writer-public-key-hex", key])
        .output()
        .with_context(|| format!("running {}", verifier.display()))?;
    if !output.status.success() {
        bail!(
            "exit {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("the verifier's report is not JSON")
}

async fn fanouts_identical(
    pools: &BTreeMap<Node, PgPool>,
    census: &[CensusBlock],
) -> Result<Check> {
    let mut check = CheckBuilder::new("inv3-fanouts-identical");
    let hashes: Vec<String> = census.iter().map(|block| block.hash.clone()).collect();
    let mut by_node = BTreeMap::new();
    for (node, pool) in pools {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT block_hash, fanout_txid, fanout_tx_hex, manifest_sha256 \
             FROM qbit_ctv_fanout_artifacts WHERE block_hash = ANY($1) ORDER BY fanout_txid",
        )
        .bind(&hashes)
        .fetch_all(pool)
        .await?;
        // Each artifact must be the bundle's own manifest transaction.
        let mismatched: Vec<String> = sqlx::query_scalar(
            "SELECT fa.fanout_txid FROM qbit_ctv_fanout_artifacts fa \
             JOIN qbit_pool_audit_bundles a ON a.block_hash = fa.block_hash \
             WHERE fa.block_hash = ANY($1) AND NOT EXISTS ( \
               SELECT 1 FROM jsonb_array_elements(a.audit_bundle #> '{ctv_fanout_manifest_set,manifests}') m \
               WHERE m->>'fanout_tx_hex' = fa.fanout_tx_hex)",
        )
        .bind(&hashes)
        .fetch_all(pool)
        .await?;
        for txid in mismatched {
            check.problem(format!(
                "fanout {txid} on node {node:?} is not one of its bundle's manifests"
            ));
        }
        by_node.insert(*node, rows);
    }
    let mut views = by_node.iter();
    if let Some((first_node, first)) = views.next() {
        for (node, rows) in views {
            let a: BTreeSet<_> = first.iter().collect();
            let b: BTreeSet<_> = rows.iter().collect();
            for only in a.difference(&b) {
                check.problem(format!(
                    "fanout {} of {} is on node {first_node:?} only, or differs on node {node:?}",
                    only.1, only.0
                ));
            }
            for only in b.difference(&a) {
                check.problem(format!(
                    "fanout {} of {} is on node {node:?} only, or differs on node {first_node:?}",
                    only.1, only.0
                ));
            }
        }
    }
    let count = by_node.values().map(Vec::len).max().unwrap_or(0);
    check.data("artifacts", json!(count));
    Ok(check.finish(format!("{count} CTV fanout transactions compared")))
}

// --- invariant 4 -------------------------------------------------------------

async fn acked_shares_present(
    pools: &BTreeMap<Node, PgPool>,
    shares: &[ShareRecord],
    options: &CheckOptions,
) -> Result<Check> {
    let mut check = CheckBuilder::new("inv4-acked-shares-present");
    let acked: Vec<&ShareRecord> = shares.iter().filter(|record| record.accepted()).collect();
    let headers: Vec<String> = acked
        .iter()
        .map(|record| record.header_hash().to_ascii_lowercase())
        .collect();
    let mut excused_seen = BTreeMap::new();
    for (node, pool) in pools {
        if options.absent_ledgers.contains_key(node) {
            continue;
        }
        let missing: Vec<String> = sqlx::query_scalar(
            "SELECT h.header_hash FROM unnest($1::text[]) AS h(header_hash) \
             WHERE NOT EXISTS (SELECT 1 FROM qbit_prism_share_hashes m \
                 JOIN qbit_share_ledger l ON l.share_id = m.share_id AND l.accepted \
                 WHERE m.header_hash = h.header_hash)",
        )
        .bind(&headers)
        .fetch_all(pool)
        .await?;
        let missing: BTreeSet<String> = missing.into_iter().collect();
        for record in &acked {
            if !missing.contains(&record.header_hash().to_ascii_lowercase()) {
                continue;
            }
            match options.excused_missing.get(&record.share_id) {
                Some(reason) => {
                    *excused_seen.entry(reason.clone()).or_insert(0u64) += 1;
                }
                None => check.problem(format!(
                    "acknowledged share {} (issued by {:?}, answered at {:?} ms) is missing on \
                     node {node:?}",
                    record.share_id, record.issuer, record.answered_ms
                )),
            }
        }
    }
    check.data("acknowledged", json!(acked.len()));
    check.data("excused_missing", json!(excused_seen));
    Ok(check.finish(format!(
        "{} acknowledged shares checked in {} databases",
        acked.len(),
        pools.len()
    )))
}

async fn no_double_credit(pools: &BTreeMap<Node, PgPool>) -> Result<Check> {
    let mut check = CheckBuilder::new("inv4-no-double-credit");
    let mut rows_by_node: BTreeMap<Node, BTreeMap<String, String>> = BTreeMap::new();
    for (node, pool) in pools {
        let problems: Vec<(String, String)> = sqlx::query_as(
            "SELECT 'duplicate share id', share_id FROM qbit_share_ledger GROUP BY share_id HAVING count(*) > 1 \
             UNION ALL \
             SELECT 'header credited twice', h FROM ( \
                 SELECT CASE WHEN share_id ~ '[0-9A-Fa-f]{64}$' THEN lower(right(share_id, 64)) \
                             ELSE encode(sha256(convert_to(share_id, 'UTF8')), 'hex') END AS h \
                 FROM qbit_share_ledger WHERE accepted) x GROUP BY h HAVING count(*) > 1 \
             UNION ALL \
             SELECT 'ledger row without its header', l.share_id FROM qbit_share_ledger l \
              WHERE l.accepted AND NOT EXISTS (SELECT 1 FROM qbit_prism_share_hashes h \
                WHERE h.share_id = l.share_id) \
             UNION ALL \
             SELECT 'header without its ledger row', h.header_hash FROM qbit_prism_share_hashes h \
              WHERE NOT EXISTS (SELECT 1 FROM qbit_share_ledger l WHERE l.share_id = h.share_id AND l.accepted) \
             UNION ALL \
             SELECT 'job issued after acceptance', share_seq::text FROM qbit_share_ledger \
              WHERE accepted AND job_issued_at > accepted_at",
        )
        .fetch_all(pool)
        .await?;
        for (kind, key) in problems {
            check.problem(format!("node {node:?}: {kind}: {key}"));
        }
        // What a header's row says, for the cross-node comparison: every
        // copied row must keep the origin's identity and content.
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT lower(right(share_id, 64)), \
                    concat_ws('|', share_id, share_seq, miner_id, encode(p2mr_program, 'hex'), \
                              share_difficulty, network_difficulty, job_id, \
                              floor(extract(epoch FROM job_issued_at) * 1000)::bigint, \
                              floor(extract(epoch FROM accepted_at) * 1000)::bigint, ntime) \
             FROM qbit_share_ledger WHERE accepted AND share_id ~ '[0-9A-Fa-f]{64}$'",
        )
        .fetch_all(pool)
        .await?;
        rows_by_node.insert(*node, rows.into_iter().collect());
    }
    let mut shared = 0;
    if let (Some(a), Some(b)) = (rows_by_node.get(&Node::A), rows_by_node.get(&Node::B)) {
        for (header, row) in a {
            if let Some(other) = b.get(header) {
                shared += 1;
                if other != row {
                    check.problem(format!(
                        "header {header} has different rows: node A {row}, node B {other}"
                    ));
                }
            }
        }
    }
    check.data("headers_on_both_nodes", json!(shared));
    Ok(check.finish(format!(
        "{} databases; {shared} headers held by both compared",
        pools.len()
    )))
}

/// Which shape of window a database records: 3.0's anchored range, or the
/// 3.1 range with a per-node cut (CONTRACT.md D-13, migration 028).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowShape {
    Anchored,
    Cut,
}

/// Whether the database's audit snapshots carry D-13's cut columns.
pub async fn window_shape(pool: &PgPool) -> Result<WindowShape> {
    let cut: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'qbit_prism_audit_snapshots' \
           AND column_name = 'cut_seq_0')",
    )
    .fetch_one(pool)
    .await?;
    Ok(if cut {
        WindowShape::Cut
    } else {
        WindowShape::Anchored
    })
}

/// One recorded window: its range, anchor and cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedWindow {
    pub snapshot: String,
    pub first: i64,
    pub last: i64,
    pub anchor_ms: i64,
    pub count: i64,
    /// `(cut[0], cut[1])`, `None` when the window has no cut.
    pub cut: Option<(Option<i64>, Option<i64>)>,
}

/// One `qbit_prism_audit_snapshots` row as `recorded_windows` reads it:
/// digest, first and last share_seq, anchor, count, and the cut.
type SnapshotRow = (String, i64, i64, i64, i64, Option<i64>, Option<i64>);

/// Every non-bootstrap window a database recorded.
pub async fn recorded_windows(pool: &PgPool) -> Result<Vec<RecordedWindow>> {
    let shape = window_shape(pool).await?;
    let cuts = match shape {
        WindowShape::Cut => "cut_seq_0, cut_seq_1",
        WindowShape::Anchored => "NULL::bigint, NULL::bigint",
    };
    let rows: Vec<SnapshotRow> = sqlx::query_as(&format!(
        "SELECT snapshot_sha256, first_share_seq, last_share_seq, anchor_ms, \
                    share_count::bigint, {cuts} \
             FROM qbit_prism_audit_snapshots WHERE inline_shares IS NULL"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(snapshot, first, last, anchor_ms, count, cut0, cut1)| RecordedWindow {
                snapshot,
                first,
                last,
                anchor_ms,
                count,
                cut: (cut0.is_some() || cut1.is_some()).then_some((cut0, cut1)),
            },
        )
        .collect())
}

/// D-13's eligibility predicate over ledger rows `l`, with `$1` the anchor
/// in milliseconds and `$2`, `$3` the cut: today's anchored predicate when
/// the window has no cut, otherwise also each node's rows up to its cut (a
/// null entry admits none of that node's rows).
fn eligible_sql(window: &RecordedWindow) -> &'static str {
    match window.cut {
        // $2 and $3 are bound as NULL here; naming them with their type keeps
        // PostgreSQL from refusing parameters it cannot type.
        None => {
            "l.accepted \
             AND l.accepted_at <= to_timestamp($1::double precision / 1000) \
             AND l.job_issued_at <= to_timestamp($1::double precision / 1000) \
             AND $2::bigint IS NULL AND $3::bigint IS NULL"
        }
        Some(_) => {
            "l.accepted \
             AND l.accepted_at <= to_timestamp($1::double precision / 1000) \
             AND l.job_issued_at <= to_timestamp($1::double precision / 1000) \
             AND ((l.origin_node = 0 AND l.share_seq <= $2) \
                  OR (l.origin_node = 1 AND l.share_seq <= $3))"
        }
    }
}

/// How a recorded window reads in a ledger now: the eligible rows in its
/// range, and the lowest eligible row above it, if any.
pub async fn window_now(pool: &PgPool, window: &RecordedWindow) -> Result<(i64, Option<i64>)> {
    let eligible = eligible_sql(window);
    let (cut0, cut1) = window.cut.unwrap_or((None, None));
    Ok(sqlx::query_as(&format!(
        "SELECT (SELECT count(*) FROM qbit_share_ledger l \
                  WHERE {eligible} AND l.share_seq BETWEEN $4 AND $5), \
                (SELECT min(l.share_seq) FROM qbit_share_ledger l \
                  WHERE {eligible} AND l.share_seq > $5)"
    ))
    .bind(window.anchor_ms)
    .bind(cut0)
    .bind(cut1)
    .bind(window.first)
    .bind(window.last)
    .fetch_one(pool)
    .await?)
}

/// CONTRACT.md §4.4: a window, once built, never changes. Every recorded
/// window must still hold exactly its shares, and no row above it may be
/// eligible for it (a late row that would have joined it).
pub async fn windows_unchanged(pools: &BTreeMap<Node, PgPool>) -> Result<Check> {
    let mut check = CheckBuilder::new("inv4-windows-unchanged");
    let mut windows = 0;
    for (node, pool) in pools {
        for window in recorded_windows(pool).await? {
            windows += 1;
            let (durable, newer) = window_now(pool, &window).await?;
            if durable != window.count || newer.is_some() {
                check.problem(format!(
                    "window {} on node {node:?} recorded {} shares; the ledger now holds \
                     {durable} in its range, and a newer eligible share at {newer:?}",
                    window.snapshot, window.count
                ));
            }
        }
    }
    Ok(check.finish(format!("{windows} recorded windows re-read")))
}

async fn windows_reproducible(pools: &BTreeMap<Node, PgPool>) -> Result<Check> {
    if pools.len() < 2 {
        return Ok(Check::skip(
            "inv4-windows-reproducible",
            "one database: every window is read where it was built",
        ));
    }
    let mut check = CheckBuilder::new("inv4-windows-reproducible");
    let mut compared = 0;
    for (node, pool) in pools {
        for window in recorded_windows(pool).await? {
            for (other, other_pool) in pools {
                if other == node {
                    continue;
                }
                compared += 1;
                let digest = window_digest(other_pool, &window).await?;
                if digest.as_deref() != Some(window.snapshot.as_str()) {
                    check.problem(format!(
                        "window {} built on node {node:?} recomputes on node {other:?} as \
                         {digest:?}",
                        window.snapshot
                    ));
                }
            }
        }
    }
    Ok(check.finish(format!("{compared} cross-node window recomputations")))
}

/// The snapshot digest of a recorded window, recomputed from a ledger
/// (`window.rs`'s `snapshot_sha256`: sha256 of the serde_json share array).
pub async fn window_digest(pool: &PgPool, window: &RecordedWindow) -> Result<Option<String>> {
    let eligible = eligible_sql(window);
    let (cut0, cut1) = window.cut.unwrap_or((None, None));
    Ok(sqlx::query_scalar(&format!(
        "SELECT encode(sha256(convert_to('[' || string_agg( \
             '{{\"share_seq\":' || l.share_seq \
             || ',\"share_id\":' || to_json(l.share_id)::text \
             || ',\"miner_id\":' || to_json(l.miner_id)::text \
             || ',\"order_key\":' || to_json(l.payout_order_key)::text \
             || ',\"p2mr_program_hex\":\"' || encode(l.p2mr_program, 'hex') || '\"' \
             || ',\"share_difficulty\":' || l.share_difficulty::text \
             || ',\"network_difficulty\":' || l.network_difficulty::text \
             || ',\"template_height\":' || l.template_height \
             || ',\"job_id\":' || to_json(l.job_id)::text \
             || ',\"job_issued_at_ms\":' || floor(extract(epoch FROM l.job_issued_at) * 1000)::bigint \
             || ',\"accepted_at_ms\":' || floor(extract(epoch FROM l.accepted_at) * 1000)::bigint \
             || ',\"ntime\":' || l.ntime \
             || COALESCE(',\"credit_policy\":' || to_json(l.credit_policy)::text, '') \
             || '}}', ',' ORDER BY l.share_seq) || ']', 'UTF8')), 'hex') \
         FROM qbit_share_ledger l \
         WHERE {eligible} AND l.share_seq BETWEEN $4 AND $5"
    ))
    .bind(window.anchor_ms)
    .bind(cut0)
    .bind(cut1)
    .bind(window.first)
    .bind(window.last)
    .fetch_one(pool)
    .await?)
}

// --- integrity and settlement ----------------------------------------------

async fn ledger_integrity(pools: &BTreeMap<Node, PgPool>) -> Result<Check> {
    let mut check = CheckBuilder::new("ledger-integrity");
    let mut reports = BTreeMap::new();
    for (node, pool) in pools {
        let report: Value = sqlx::query_scalar("SELECT qbit_carry_forward_integrity_report()")
            .fetch_one(pool)
            .await?;
        if report["mismatch_count"] != 0 || report["current_drift_count"] != 0 {
            check.problem(format!("node {node:?}'s carry integrity report: {report}"));
        }
        let divergence: Value = sqlx::query_scalar("SELECT qbit_prism_payout_divergence_report()")
            .fetch_one(pool)
            .await?;
        reports.insert(
            node.label(),
            json!({"integrity": report, "divergence": divergence}),
        );
    }
    check.data("reports", json!(reports));
    Ok(check.finish(format!("{} databases' integrity reports", pools.len())))
}

/// No block candidate is still on its way, and none waits for a landing
/// that never came. A `reconciliation` row whose block is landed and
/// confirmed is bookkeeping its next attempt finishes (a post-offer step
/// that lost a race with a payout-revision bump retries with a backoff): it
/// is listed, not failed. Invariant 3 separately holds every pool block on
/// the chain to its landing rows.
async fn candidates_settled(pools: &BTreeMap<Node, PgPool>) -> Result<Check> {
    let mut check = CheckBuilder::new("candidates-settled");
    let mut pending_bookkeeping = Vec::new();
    for (node, pool) in pools {
        let rows: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT o.block_hash, o.state, o.last_error, b.chain_state \
             FROM qbit_block_candidate_outbox o \
             LEFT JOIN qbit_pool_blocks b ON b.block_hash = o.block_hash \
             WHERE o.state IN ('pending', 'offer_reserved', 'offered', 'reconciliation')",
        )
        .fetch_all(pool)
        .await?;
        for (hash, state, error, landed) in rows {
            if state == "reconciliation" && landed.as_deref() == Some("confirmed") {
                pending_bookkeeping.push(json!({
                    "node": node, "block": hash, "last_error": error,
                }));
                continue;
            }
            check.problem(format!(
                "candidate {hash} on node {node:?} is still {state}, its block {} ({error:?})",
                landed.unwrap_or_else(|| "not landed".to_owned())
            ));
        }
    }
    let note = pending_bookkeeping.len();
    check.data("landed_awaiting_bookkeeping", json!(pending_bookkeeping));
    Ok(check.finish(format!(
        "no candidate in flight or unlanded; {note} landed blocks await their candidate's next attempt"
    )))
}

fn short(program: &str) -> String {
    program.chars().take(12).collect()
}

/// Write the report as JSON next to the scenario's logs.
pub fn write(report: &InvariantReport, dir: &Path) -> Result<PathBuf> {
    let path = dir.join("invariants.json");
    std::fs::write(&path, serde_json::to_vec_pretty(report)?)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pinned_keys_are_the_test_seeds_keys() -> Result<()> {
        let (ledger, manifest) = pinned_keys()?;
        assert_eq!(
            ledger,
            "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0"
        );
        assert_eq!(
            manifest,
            "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737"
        );
        Ok(())
    }

    fn rows(hash: &str, carry: &[(&str, i128, i128)], recorded: &[(&str, i128)]) -> BlockRows {
        BlockRows {
            hash: hash.into(),
            height: 0,
            origin: Some(Node::A),
            carry: carry
                .iter()
                .map(|(program, gross, onchain)| ((*program).to_owned(), (*gross, *onchain, 0)))
                .collect(),
            recorded_overpay: recorded
                .iter()
                .map(|(program, sats)| ((*program).to_owned(), *sats))
                .collect(),
        }
    }

    #[test]
    fn ownership_epochs_name_the_owner_by_height() {
        let default = CheckOptions::default();
        assert_eq!(default.owner_at(Topology::DualWriter, 5), Some(Node::A));
        assert_eq!(default.owner_at(Topology::SingleWriter, 5), None);
        let cutover = CheckOptions {
            ownership: vec![(100, Some(Node::A))],
            ..CheckOptions::default()
        };
        assert_eq!(cutover.owner_at(Topology::DualWriter, 99), None);
        assert_eq!(cutover.owner_at(Topology::DualWriter, 100), Some(Node::A));
        let transfer = CheckOptions {
            ownership: vec![(0, Some(Node::A)), (200, Some(Node::B))],
            ..CheckOptions::default()
        };
        assert_eq!(transfer.owner_at(Topology::DualWriter, 199), Some(Node::A));
        assert_eq!(transfer.owner_at(Topology::DualWriter, 200), Some(Node::B));
    }

    #[test]
    fn paying_one_carry_twice_raises_debt_and_is_caught() {
        // Block 1 accrues 100 for m (sub-floor). Block 2 pays it down (prior
        // 100, gross 10, onchain 110). Block 3 pays the same 100 again: m's
        // balance goes to -100.
        let blocks = [
            rows("b1", &[("m", 100, 0)], &[]),
            rows("b2", &[("m", 10, 110)], &[]),
            rows("b3", &[("m", 10, 110)], &[]),
        ];
        let (increases, balances, excused) = debt_walk(&blocks);
        assert_eq!(balances["m"], -100);
        assert_eq!(excused, 0);
        assert_eq!(
            increases,
            vec![DebtIncrease {
                block: "b3".into(),
                program: "m".into(),
                increase: 100,
                debt_after: 100,
                recorded: None,
            }]
        );
    }

    #[test]
    fn a_recorded_478_overpay_covers_exactly_the_debt_it_records() {
        let blocks = [
            rows("b1", &[("m", 100, 0)], &[]),
            rows("b2", &[("m", 10, 110)], &[]),
            rows("b3", &[("m", 10, 110)], &[("m", 100)]),
        ];
        let (increases, _, excused) = debt_walk(&blocks);
        assert!(increases.is_empty());
        assert_eq!(excused, 1);
        // A record smaller than the increase does not cover it.
        let short = [
            rows("b1", &[("m", 100, 0)], &[]),
            rows("b2", &[("m", 10, 110)], &[]),
            rows("b3", &[("m", 10, 110)], &[("m", 99)]),
        ];
        assert_eq!(debt_walk(&short).0.len(), 1);
    }

    #[test]
    fn existing_debt_that_shrinks_or_holds_is_not_an_increase() {
        let blocks = [
            rows("b1", &[("m", 0, 50)], &[("m", 50)]),
            rows("b2", &[("m", 30, 0)], &[]),
            rows("b3", &[("m", 0, 0)], &[]),
        ];
        let (increases, balances, _) = debt_walk(&blocks);
        assert!(increases.is_empty());
        assert_eq!(balances["m"], -20);
    }

    #[test]
    fn a_check_with_problems_fails_and_lists_at_most_the_limit() {
        let mut check = CheckBuilder::new("x");
        for index in 0..(PROBLEM_LIMIT + 5) {
            check.problem(format!("problem {index}"));
        }
        let check = check.finish("summary".into());
        assert_eq!(check.status, Status::Fail);
        assert_eq!(check.problem_count, PROBLEM_LIMIT + 5);
        assert_eq!(check.problems.len(), PROBLEM_LIMIT);
        assert_eq!(
            CheckBuilder::new("y").finish("ok".into()).status,
            Status::Pass
        );
    }
}
