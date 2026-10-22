//! The drained 2.x.x ledgers the migration tests start from.
//!
//! Both seeds write through the frozen `schema_2x` release SQL, never the live
//! in-tree files, and both leave the database as a drained 2.x.x pool leaves
//! it: every candidate terminal, the writer lease expired.
//!
//! - [`seed_s20`] is #545's weekly lifecycle ledger: 60,000 rows for 250
//!   recipients over three days and 36 blocks. It was written inline in
//!   `live_migration_lifecycle.rs` and moved here unchanged, so the rows it
//!   writes are the ones it wrote there.
//! - [`MainnetPlan`] is #575's cutover rehearsal ledger: the shape of mainnet
//!   2.x.x between 2026-07-15 and 2026-09-28 (#521's aggregates), at a
//!   parameterised scale and share density. See [`MainnetShape`].
//!
//! Everything a seed writes is a function of its inputs: the seed number, the
//! chain it is given and the cutover instant. Nothing reads the wall clock.
use anyhow::{ensure, Context, Result};
use qbit_pool_builder::ManifestSigningKey;
use qbit_prism::{AcceptedShare, FoundBlock, PayoutPolicy};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::File,
    io::Write,
    path::Path,
};

/// A found block's place on the chain the seed is written over.
#[derive(Clone, Debug)]
pub struct ChainBlock {
    pub height: u64,
    pub hash: String,
    pub parent: String,
}

/// A historical audit and the exact bytes its public artifact must serve.
#[derive(Clone, Debug)]
pub struct SeededArtifact {
    pub block_hash: String,
    pub digest: String,
    pub canonical: Vec<u8>,
}

/// What a seed wrote that later phases check against.
#[derive(Debug)]
pub struct Seeded {
    pub accepted_shares: i64,
    pub last_share_seq: i64,
    /// How far past the last row 2.x.x's share sequence ends: allocations
    /// that rolled back still advance a PostgreSQL sequence.
    pub sequence_headroom: i64,
    pub last_publication_ordinal: i64,
    pub last_carry_seq: i64,
    /// Every pool block the seed wrote, in the order it wrote them.
    pub hashes: Vec<String>,
    pub artifacts: Vec<SeededArtifact>,
}

/// The frozen release files, as a 2.x.x ledger applied them: 001 carries its
/// own BEGIN/COMMIT, and a #258 (v2.0.2) ledger applied 002 in a second call.
async fn apply_release_schema(pool: &PgPool, with_258: bool) -> Result<()> {
    sqlx::raw_sql(include_str!("../fixtures/schema_2x/001_share_ledger.sql"))
        .execute(pool)
        .await?;
    if with_258 {
        sqlx::raw_sql(include_str!(
            "../fixtures/schema_2x/002_candidate_bodies.sql"
        ))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// The three historical audit body layouts, in turn: a canonical sidecar
/// only, an external body file, and an inline body. Returns the inline body
/// and the recorded `body_uri`.
fn write_audit_layout(
    root: &Path,
    hash: &str,
    layout: usize,
    bundle: &qbit_prism::AuditBundle,
    digest: &str,
    canonical: &[u8],
) -> Result<(Option<Value>, Option<String>)> {
    let body_path = root.join(format!("legacy-audit-{hash}.json"));
    Ok(match layout % 3 {
        0 => {
            let path = root.join(format!(
                "prism-audit-bundle-canonical-{hash}-{digest}.json.gz"
            ));
            let mut encoder = flate2::GzBuilder::new()
                .mtime(0)
                .write(File::create(path)?, flate2::Compression::best());
            encoder.write_all(canonical)?;
            encoder.finish()?;
            (None, Some(body_path.to_string_lossy().into_owned()))
        }
        1 => {
            std::fs::write(&body_path, serde_json::to_vec(bundle)?)?;
            (None, Some(body_path.to_string_lossy().into_owned()))
        }
        _ => (Some(serde_json::to_value(bundle)?), None),
    })
}

// ---------------------------------------------------------------------------
// #545: the weekly lifecycle ledger.
// ---------------------------------------------------------------------------

/// The legacy history: distinct payout recipients, ledger rows (a few of
/// them rejected, as 2.x.x recorded rejects in the same table) and found
/// blocks, each with its own signed audit bundle and carry-forward rows.
pub const S20_RECIPIENTS: usize = 250;
pub const S20_SHARES: usize = 60_000;
pub const S20_BLOCKS: usize = 36;
/// Every this-many ledger rows is a 2.x.x reject.
const S20_REJECT_EVERY: usize = 97;
/// The 2.x.x network difficulty the legacy shares and blocks record.
const S20_NETWORK_DIFFICULTY: u128 = 100;
/// A candidate balance at or above this pays on chain; below it accrues.
const S20_FLOOR_SATS: u64 = 40_000;
/// Sats credited per unit of legacy share difficulty in a block.
const S20_SATS_PER_DIFFICULTY: u64 = 500;
/// The high-water mark 2.x.x's share sequence ends at, above the last row:
/// allocations that rolled back still advance a PostgreSQL sequence.
pub const S20_SEQUENCE_HEADROOM: i64 = 11;

/// #545's drained 2.x.x ledger over `blocks`, whose history ends an hour
/// before `now_ms`: the frozen release DDL, ordered shares from a skewed
/// recipient set with rejects among them, a signed audit bundle per found
/// block in the three historical body layouts, chained carry-forward rows,
/// and terminal candidate rows. The history is signed with `coinbase_key`
/// and `ledger_key`, as a cutover keeps them.
pub async fn seed_s20(
    pool: &PgPool,
    root: &Path,
    blocks: &[ChainBlock],
    now_ms: i64,
    coinbase_key: &ManifestSigningKey,
    ledger_key: &ManifestSigningKey,
) -> Result<Seeded> {
    ensure!(
        blocks.len() == S20_BLOCKS,
        "the #545 seed needs {S20_BLOCKS} chain blocks, not {}",
        blocks.len()
    );
    apply_release_schema(pool, true).await?;

    let recipients: Vec<(String, String)> = (0..S20_RECIPIENTS)
        .map(|index| {
            let program = hex::encode(Sha256::digest(format!("s20-legacy-recipient-{index}")));
            (format!("legacy-{index:03}"), program)
        })
        .collect();
    // Zipf-like weights: a few recipients hold most of the work.
    let weights: Vec<f64> = (1..=S20_RECIPIENTS).map(|rank| 1.0 / rank as f64).collect();
    let total_weight: f64 = weights.iter().sum();
    let mut rng = StdRng::seed_from_u64(0x0545_0020);
    // Three days of history, ending an hour before the cutover.
    let start_ms = now_ms - 3 * 86_400_000;
    let step_ms = (3 * 86_400_000 - 3_600_000) / S20_SHARES as i64;
    // The first `S20_SHARES % S20_BLOCKS` blocks take one more row, so
    // every configured row is written.
    let per_block = S20_SHARES / S20_BLOCKS;
    let longer_blocks = S20_SHARES % S20_BLOCKS;

    let mut balances: BTreeMap<usize, u64> = BTreeMap::new();
    let mut artifacts = Vec::with_capacity(S20_BLOCKS);
    let mut accepted_shares = 0i64;
    let mut seq = 0u64;
    for (block_index, block) in blocks.iter().enumerate() {
        let (height, hash, parent) = (&block.height, &block.hash, &block.parent);
        let block_shares = per_block + usize::from(block_index < longer_blocks);
        let mut window: Vec<AcceptedShare> = Vec::with_capacity(block_shares);
        let mut credited: BTreeMap<usize, u64> = BTreeMap::new();
        let mut transaction = pool.begin().await?;
        for _ in 0..block_shares {
            seq += 1;
            let mut pick = rng.gen::<f64>() * total_weight;
            let recipient = weights
                .iter()
                .position(|weight| {
                    pick -= weight;
                    pick <= 0.0
                })
                .unwrap_or(S20_RECIPIENTS - 1);
            let (miner, program) = &recipients[recipient];
            let difficulty: u128 = rng.gen_range(1..=4);
            let accepted_at_ms = start_ms + seq as i64 * step_ms;
            let share = AcceptedShare {
                share_seq: seq,
                share_id: format!("legacy:{seq:064x}"),
                miner_id: miner.clone(),
                order_key: miner.clone(),
                p2mr_program_hex: program.clone(),
                share_difficulty: difficulty,
                network_difficulty: S20_NETWORK_DIFFICULTY,
                template_height: *height,
                job_id: format!("legacy-job-{block_index}"),
                job_issued_at_ms: accepted_at_ms - 1_000,
                accepted_at_ms,
                ntime: u32::try_from(accepted_at_ms / 1_000)?,
                credit_policy: None,
            };
            let rejected = (seq as usize).is_multiple_of(S20_REJECT_EVERY);
            sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,accepted,reject_reason,writer_id,writer_epoch) VALUES($1,$2,$3,$3,decode($4,'hex'),$5::numeric,$6::numeric,$7,$8,to_timestamp($9::double precision/1000),to_timestamp($10::double precision/1000),$11,$12,$13,'python',7)")
                .bind(seq as i64)
                .bind(&share.share_id)
                .bind(miner)
                .bind(program)
                .bind(difficulty.to_string())
                .bind(S20_NETWORK_DIFFICULTY.to_string())
                .bind(*height as i64)
                .bind(&share.job_id)
                .bind(share.job_issued_at_ms)
                .bind(share.accepted_at_ms)
                .bind(i64::from(share.ntime))
                .bind(!rejected)
                .bind(rejected.then_some("stale-job"))
                .execute(&mut *transaction)
                .await?;
            if !rejected {
                accepted_shares += 1;
                *credited.entry(recipient).or_default() +=
                    difficulty as u64 * S20_SATS_PER_DIFFICULTY;
                window.push(share);
            }
        }
        let found_share = window
            .last()
            .context("a legacy block without shares")?
            .clone();
        let bundle = qbit_prism::build_audit_bundle(
            window,
            FoundBlock {
                block_height: *height,
                coinbase_value_sats: 5_000_000_000,
                network_difficulty: S20_NETWORK_DIFFICULTY,
                anchor_job_issued_at_ms: found_share.job_issued_at_ms,
            },
            vec![],
            PayoutPolicy::day_one_default(),
            coinbase_key,
            ledger_key,
        )?;
        let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
            &bundle,
            &ledger_key.public_key_hex(),
        )?;
        let canonical = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
        let (inline, body_uri) = write_audit_layout(
            root,
            hash,
            block_index,
            &bundle,
            &report.audit_bundle_sha256_hex,
            &canonical,
        )?;
        let found_at_ms = found_share.accepted_at_ms;
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,found_at,chain_state,maturity_state,matured_at) VALUES($1,$2,$3,$4,$5,to_timestamp($6::double precision/1000),'confirmed','mature',clock_timestamp())")
            .bind(hash)
            .bind(*height as i64)
            .bind(parent)
            .bind(&report.coinbase_txid)
            .bind(&report.coinbase_manifest_sha256_hex)
            .bind(found_at_ms)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,body_uri) VALUES($1,$2,$3,$4,$5)")
            .bind(hash)
            .bind(inline)
            .bind(&report.audit_bundle_sha256_hex)
            .bind(&report.coinbase_tx_hex)
            .bind(body_uri)
            .execute(&mut *transaction)
            .await?;
        // Each credited recipient's carry chain: the prior balance is the
        // last carry, and a candidate at the floor pays out whole.
        for (recipient, gross) in credited {
            let (miner, program) = &recipients[recipient];
            let prior = balances.get(&recipient).copied().unwrap_or(0);
            let candidate = prior + gross;
            let onchain = if candidate >= S20_FLOOR_SATS {
                candidate
            } else {
                0
            };
            let carry = candidate - onchain;
            balances.insert(recipient, carry);
            sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state) VALUES($1,$2,$3,$3,decode($4,'hex'),$5,$6::numeric,$7::numeric,$8,$9::numeric,$10,'mature')")
                .bind(*height as i64)
                .bind(hash)
                .bind(miner)
                .bind(program)
                .bind(i64::try_from(gross)?)
                .bind(prior.to_string())
                .bind(candidate.to_string())
                .bind(i64::try_from(onchain)?)
                .bind(carry.to_string())
                .bind(if onchain > 0 { "onchain" } else { "accrued" })
                .execute(&mut *transaction)
                .await?;
        }
        // The found block's candidate, drained by the 2.x.x submitter.
        sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256,state,attempt_count,completed_at) VALUES($1,$2,NULL,$3,'submitted',1,clock_timestamp())")
            .bind(hash)
            .bind(&found_share.share_id)
            .bind(hex::encode(Sha256::digest(&canonical)))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        artifacts.push(SeededArtifact {
            block_hash: hash.clone(),
            digest: report.audit_bundle_sha256_hex,
            canonical,
        });
    }
    // One candidate 2.x.x abandoned, whose block never reached the chain.
    sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,last_error,completed_at) VALUES($1,NULL,$2,'abandoned','stale before submission',clock_timestamp())")
        .bind(hex::encode(Sha256::digest("s20-abandoned-candidate")))
        .bind("88".repeat(32))
        .execute(pool)
        .await?;
    ensure!(
        seq as usize == S20_SHARES,
        "seeded {seq} ledger rows, not {S20_SHARES}"
    );
    let last_share_seq = seq as i64;
    sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
        .bind(last_share_seq + S20_SEQUENCE_HEADROOM)
        .execute(pool)
        .await?;
    finish(
        pool,
        accepted_shares,
        last_share_seq,
        S20_SEQUENCE_HEADROOM,
        blocks.iter().map(|block| block.hash.clone()).collect(),
        artifacts,
    )
    .await
}

async fn finish(
    pool: &PgPool,
    accepted_shares: i64,
    last_share_seq: i64,
    sequence_headroom: i64,
    hashes: Vec<String>,
    artifacts: Vec<SeededArtifact>,
) -> Result<Seeded> {
    let last_publication_ordinal: i64 =
        sqlx::query_scalar("SELECT max(audit_publication_sequence) FROM qbit_pool_blocks")
            .fetch_one(pool)
            .await?;
    let last_carry_seq: i64 =
        sqlx::query_scalar("SELECT max(carry_forward_seq) FROM qbit_payout_carry_forward")
            .fetch_one(pool)
            .await?;
    Ok(Seeded {
        accepted_shares,
        last_share_seq,
        sequence_headroom,
        last_publication_ordinal,
        last_carry_seq,
        hashes,
        artifacts,
    })
}

// ---------------------------------------------------------------------------
// #575: the mainnet-shaped cutover rehearsal ledger.
// ---------------------------------------------------------------------------

/// Payout identities mainnet 2.x.x saw between 2026-07-15 and 2026-09-28.
pub const MAINNET_IDENTITIES: usize = 257;
/// Pool blocks mainnet 2.x.x found in the same period.
pub const MAINNET_BLOCKS: usize = 13_566;
/// Where the mainnet-shaped ledger's share sequence ends past its last row.
pub const MAINNET_SEQUENCE_HEADROOM: i64 = 7;
/// 2.x.x marks a pool block mature once `tip >= height + 1000`
/// (`qbit_mark_mature_pool_payouts`), and the native reconciler calls the
/// same function, so a seeded block is immature exactly when it sits within
/// this many heights of the tip.
pub const MATURITY_HEIGHTS: u64 = 1_000;
const BUCKET_MS: i64 = 300_000;
const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;
/// 2.x.x reissued work about every 30 seconds.
const JOB_MS: i64 = 30_000;
/// Rows per server-side `INSERT ... SELECT FROM unnest(...)`.
const BATCH_ROWS: usize = 20_000;
/// 2.x.x reject reasons with their share of rejects.
const REJECTS: [(&str, f64); 5] = [
    ("stale-job", 0.60),
    ("low-difficulty", 0.20),
    ("duplicate-share", 0.10),
    ("unknown-job", 0.05),
    ("invalid-ntime-or-nonce", 0.05),
];

/// One of #521's three eras, with its 5-minute pool share rate.
#[derive(Clone, Debug)]
pub struct Phase {
    pub days: u32,
    /// Mean, in shares per second at 1x and full density.
    pub mean_rate: f64,
    /// The highest 5-minute rate; the lognormal draw is capped here.
    pub max_rate: f64,
    /// Coefficient of variation of the 5-minute rate.
    pub cv: f64,
    /// Share of the non-whale identities that first mine in this era.
    pub arrival_share: f64,
}

/// What a mainnet-shaped 2.x.x ledger looks like, and how much of it to
/// write.
///
/// `scale` multiplies the rate, the non-whale identities and the blocks
/// (3 is #521's growth shape). `density` thins the share *rows* only: each
/// row carries `1 / density` times the difficulty a mainnet share did, so
/// the work per identity per 5-minute bucket, the window weight and every
/// balance keep the mainnet shape while the ledger holds `density` of the
/// rows. The migration cost of the thinned ledger scales with its rows, and
/// the rehearsal report states both.
#[derive(Clone, Debug)]
pub struct MainnetShape {
    pub seed: u64,
    pub scale: u32,
    pub density: f64,
    pub identities: usize,
    pub blocks: usize,
    pub phases: Vec<Phase>,
    /// Share of the first era's arrivals in its first 24 hours (launch).
    /// With the era shares, this puts about 126 identities in the second
    /// day, about 41, 15 and 9 in an average hour of the three eras, and 257
    /// over all time; #521 measured 126, 47, 16 and 11.
    pub launch_burst: f64,
    /// Identities that at times hold most of the work, each for one span of
    /// days (`whale_spans`).
    pub whale_spans: Vec<(u32, u32)>,
    /// Chance a day has one dominant whale, and the share of the day's work
    /// it then holds.
    pub whale_day_probability: f64,
    pub whale_fraction: (f64, f64),
    /// Zipf exponent of the non-dominant work.
    pub zipf: f64,
    /// Worker difficulty spans `2^0 ..= 2^difficulty_bits`, three orders of
    /// magnitude at 10.
    pub difficulty_bits: f64,
    /// Share of the non-whale identities that are hobby rigs beside the
    /// ASICs: their work weight is scaled by `dust_weight` and their workers
    /// sit at the bottom of the difficulty range. Their block credit falls
    /// under the payout floor, so their carry accrues across blocks, as a
    /// small miner's does. #521 gives no per-address payout distribution;
    /// this is the modelling choice that makes owed balances non-trivial.
    pub dust_share: f64,
    pub dust_weight: f64,
    /// Lifetimes are lognormal with this median and 90th percentile, hours.
    pub lifetime_median_hours: f64,
    pub lifetime_p90_hours: f64,
    pub reject_rate: f64,
    pub stale_grace_rate: f64,
    /// Every n-th settled block is inactive, reversed or rejected instead.
    pub inactive_every: usize,
    pub reversed_every: usize,
    pub rejected_every: usize,
    /// At least this many of the newest blocks stay immature (pending
    /// payouts), on top of those found within `MATURITY_HEIGHTS` minutes of
    /// the end of history.
    pub min_immature: usize,
    pub abandoned_candidates: usize,
    /// Blocks found in the last this-many days carry CTV fanout rows.
    pub ctv_days: u32,
    /// The day the pool moved to v2.0.2 (#258): later blocks' candidates are
    /// terminal version 2 rows. `None` seeds a pre-#258 (v2.0.1) ledger.
    pub upgrade_258_day: Option<u32>,
    /// Leave `qbit_share_ledger_credit_policy_check` NOT VALID, as 001 does
    /// on a ledger upgraded from before the column; 017 then validates it.
    pub credit_policy_not_valid: bool,
    /// Shares in each block's signed audit bundle: the newest of its window.
    pub audit_window_cap: usize,
    pub coinbase_sats: u64,
    pub pool_fee_bps: u64,
    pub payout_floor_sats: u64,
    /// Heights between the last seeded block and the chain tip.
    pub tip_margin: u64,
}

impl MainnetShape {
    /// Mainnet 2.x.x at `scale` times its size and `density` of its rows.
    pub fn mainnet(scale: u32, density: f64) -> Result<Self> {
        ensure!(
            (1..=10).contains(&scale),
            "scale must be 1 to 10, not {scale}"
        );
        ensure!(
            density.is_finite() && density > 0.0 && density <= 1.0,
            "share density must be above 0 and at most 1, not {density}"
        );
        let scale_count = |count: usize| count * scale as usize;
        Ok(Self {
            seed: 0x0575_0001,
            scale,
            density,
            identities: scale_count(MAINNET_IDENTITIES),
            blocks: scale_count(MAINNET_BLOCKS),
            phases: vec![
                Phase {
                    days: 11,
                    mean_rate: 8.0,
                    max_rate: 92.0,
                    cv: 1.24,
                    arrival_share: 0.60,
                },
                Phase {
                    days: 25,
                    mean_rate: 25.0,
                    max_rate: 394.0,
                    cv: 0.65,
                    arrival_share: 0.18,
                },
                Phase {
                    days: 40,
                    mean_rate: 1.3,
                    max_rate: 17.0,
                    cv: 0.77,
                    arrival_share: 0.22,
                },
            ],
            launch_burst: 0.8,
            whale_spans: vec![(0, 36), (24, 62), (55, 76)],
            whale_day_probability: 0.8,
            whale_fraction: (0.82, 0.98),
            zipf: 1.1,
            difficulty_bits: 10.0,
            dust_share: 0.3,
            dust_weight: 1e-4,
            lifetime_median_hours: 37.0,
            lifetime_p90_hours: 245.0,
            reject_rate: 0.015,
            stale_grace_rate: 0.003,
            inactive_every: 250,
            reversed_every: 1_000,
            rejected_every: 500,
            min_immature: 3,
            abandoned_candidates: scale_count(12),
            ctv_days: 40,
            upgrade_258_day: Some(60),
            credit_policy_not_valid: true,
            audit_window_cap: 16,
            coinbase_sats: 5_000_000_000,
            pool_fee_bps: 200,
            payout_floor_sats: 14_720,
            tip_margin: 1,
        })
    }

    /// The per-PR rehearsal: every mainnet feature at about 25,000 rows and
    /// 40 blocks, each block state present, over the same 76 days. At 40
    /// blocks the payout window (eight blocks' work) spans more than one
    /// 4,096-row page of both window readers.
    pub fn pull_request() -> Self {
        let mut shape = Self::mainnet(1, 1.0 / 2_600.0).expect("static shape");
        shape.blocks = 40;
        shape.inactive_every = 20;
        shape.reversed_every = 29;
        shape.rejected_every = 23;
        shape.abandoned_candidates = 3;
        shape.audit_window_cap = 4;
        shape
    }

    fn days(&self) -> u32 {
        self.phases.iter().map(|phase| phase.days).sum()
    }

    fn validate(&self) -> Result<()> {
        ensure!(!self.phases.is_empty(), "a shape needs at least one era");
        ensure!(
            self.identities > self.whale_spans.len(),
            "a shape needs identities beyond its whales"
        );
        ensure!(self.blocks > 0, "a shape needs blocks");
        ensure!(
            self.audit_window_cap > 0,
            "each audit bundle needs at least one share"
        );
        ensure!(
            self.min_immature < MATURITY_HEIGHTS as usize,
            "immature blocks must fit under the maturity depth"
        );
        let arrivals: f64 = self.phases.iter().map(|phase| phase.arrival_share).sum();
        ensure!(
            (arrivals - 1.0).abs() < 1e-9,
            "era arrival shares must sum to 1, not {arrivals}"
        );
        for phase in &self.phases {
            ensure!(
                phase.days > 0
                    && phase.mean_rate > 0.0
                    && phase.max_rate >= phase.mean_rate
                    && phase.cv >= 0.0,
                "invalid era {phase:?}"
            );
        }
        for (from, to) in &self.whale_spans {
            ensure!(
                from < to && *to <= self.days(),
                "whale span {from}..{to} is outside the {} days",
                self.days()
            );
        }
        Ok(())
    }
}

/// A block's final 2.x.x state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockState {
    Mature,
    Immature,
    /// Disconnected while immature; its rows no longer count.
    Inactive,
    /// Disconnected after confirmation and reversed.
    Reversed,
    /// `submitblock` refused it; it never confirmed.
    Rejected,
}

impl BlockState {
    fn on_chain(self) -> bool {
        matches!(self, Self::Mature | Self::Immature)
    }
    fn chain_state(self) -> &'static str {
        match self {
            Self::Mature | Self::Immature => "confirmed",
            Self::Inactive => "inactive",
            Self::Reversed => "reversed",
            Self::Rejected => "rejected",
        }
    }
    fn maturity_state(self) -> &'static str {
        match self {
            Self::Mature => "mature",
            Self::Immature | Self::Inactive => "immature",
            Self::Reversed | Self::Rejected => "reversed",
        }
    }
}

#[derive(Clone, Debug)]
struct Identity {
    address: String,
    program: Vec<u8>,
    start_ms: i64,
    end_ms: i64,
    /// Difficulty of the identity's workers, before the per-day jitter.
    difficulty: f64,
    /// Zipf weight of its rank among the non-dominant work.
    zipf: f64,
}

#[derive(Clone, Copy, Debug)]
struct RowPlan {
    at_ms: i64,
    identity: usize,
    /// Row difficulty: the represented difficulty over the density.
    difficulty: u128,
    reject: Option<&'static str>,
    stale_grace: bool,
}

#[derive(Clone, Debug)]
struct BlockPlan {
    bucket: usize,
    row: usize,
    found_ms: i64,
    state: BlockState,
    /// Height relative to the first on-chain block.
    offset: u64,
}

/// A mainnet-shaped ledger, planned: identities, rates and every block's
/// place and state are fixed before a row is written, so a caller can build
/// the chain the blocks sit on first.
pub struct MainnetPlan {
    shape: MainnetShape,
    start_ms: i64,
    end_ms: i64,
    identities: Vec<Identity>,
    rates: Vec<f64>,
    /// Per day: the dominant whale and the work share it holds.
    dominant: Vec<Option<(usize, f64)>>,
    blocks: Vec<BlockPlan>,
    block_at: HashMap<(usize, usize), usize>,
    network_difficulty: u128,
    tip_offset: u64,
    summary: Value,
}

/// SplitMix64: an independent, reproducible stream per (purpose, index), so
/// a bucket's rows are the same whichever pass generates them.
fn stream(seed: u64, purpose: u64, index: u64) -> StdRng {
    let mut z = seed
        ^ purpose.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ index.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    StdRng::seed_from_u64(z ^ (z >> 31))
}

fn normal(rng: &mut StdRng) -> f64 {
    // Box-Muller; 1 - u keeps the logarithm finite.
    let u: f64 = 1.0 - rng.gen::<f64>();
    let v: f64 = rng.gen::<f64>();
    (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
}

fn stochastic_round(value: f64, rng: &mut StdRng) -> usize {
    let floor = value.floor();
    floor as usize + usize::from(rng.gen::<f64>() < value - floor)
}

const BECH32: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";

/// A mainnet-looking payout address: 62 characters, as 2.x.x recorded them.
/// The program is the SHA-256 of the address, which is what the rehearsal's
/// node answers `validateaddress` with.
fn address(seed: u64, index: usize) -> String {
    synthetic_address(&format!("prism-rehearsal-identity:{seed}:{index}"))
}

/// A mainnet-looking payout address derived from `label`.
pub fn synthetic_address(label: &str) -> String {
    let mut digest = Sha256::digest(label).to_vec();
    digest.extend(Sha256::digest(&digest));
    let characters: String = digest
        .iter()
        .take(58)
        .map(|byte| BECH32[usize::from(byte % 32)] as char)
        .collect();
    format!("qb1z{characters}")
}

/// The block hash a synthetic chain has at `height`.
pub fn synthetic_chain_hash(tag: &str, height: u64) -> String {
    hex::encode(Sha256::digest(format!(
        "prism-rehearsal-chain:{tag}:{height}"
    )))
}

fn orphan_hash(seed: u64, index: usize) -> String {
    hex::encode(Sha256::digest(format!(
        "prism-rehearsal-orphan:{seed}:{index}"
    )))
}

impl MainnetPlan {
    /// Plans the ledger whose history ends an hour before `cutover_ms`.
    pub fn new(shape: MainnetShape, cutover_ms: i64) -> Result<Self> {
        shape.validate()?;
        let days = i64::from(shape.days());
        let end_ms = cutover_ms - HOUR_MS;
        let start_ms = end_ms - days * DAY_MS;
        let buckets = usize::try_from(days * DAY_MS / BUCKET_MS)?;
        let identities = Self::identities(&shape, start_ms, end_ms)?;
        let mut rates = Vec::with_capacity(buckets);
        let mut phase_of_bucket = Vec::with_capacity(buckets);
        let mut first_bucket = 0usize;
        for (index, phase) in shape.phases.iter().enumerate() {
            let count = usize::try_from(i64::from(phase.days) * DAY_MS / BUCKET_MS)?;
            let sigma = (1.0 + phase.cv * phase.cv).ln().sqrt();
            for bucket in first_bucket..first_bucket + count {
                let z = normal(&mut stream(shape.seed, 1, bucket as u64));
                let factor = (sigma * z - sigma * sigma / 2.0).exp();
                rates.push((phase.mean_rate * factor).min(phase.max_rate));
                phase_of_bucket.push(index);
            }
            first_bucket += count;
        }
        let dominant = (0..shape.days())
            .map(|day| {
                let mut rng = stream(shape.seed, 2, u64::from(day));
                let alive: Vec<usize> = shape
                    .whale_spans
                    .iter()
                    .enumerate()
                    .filter(|(_, (from, to))| (*from..*to).contains(&day))
                    .map(|(index, _)| index)
                    .collect();
                let roll: f64 = rng.gen();
                let (low, high) = shape.whale_fraction;
                let fraction = rng.gen_range(low..=high);
                (roll < shape.whale_day_probability)
                    .then(|| alive.last().copied())
                    .flatten()
                    .map(|whale| (whale, fraction))
            })
            .collect();
        let mut plan = Self {
            shape,
            start_ms,
            end_ms,
            identities,
            rates,
            dominant,
            blocks: Vec::new(),
            block_at: HashMap::new(),
            network_difficulty: 1,
            tip_offset: 0,
            summary: Value::Null,
        };
        plan.place_blocks(&phase_of_bucket)?;
        Ok(plan)
    }

    fn identities(shape: &MainnetShape, start_ms: i64, end_ms: i64) -> Result<Vec<Identity>> {
        let whales = shape.whale_spans.len();
        let tail = shape.identities - whales;
        // Largest-remainder apportionment of the tail over the eras.
        let exact: Vec<f64> = shape
            .phases
            .iter()
            .map(|phase| phase.arrival_share * tail as f64)
            .collect();
        let mut counts: Vec<usize> = exact.iter().map(|value| value.floor() as usize).collect();
        let mut order: Vec<usize> = (0..counts.len()).collect();
        order.sort_by(|a, b| {
            (exact[*b] - exact[*b].floor())
                .total_cmp(&(exact[*a] - exact[*a].floor()))
                .then(a.cmp(b))
        });
        for index in order.into_iter().take(tail - counts.iter().sum::<usize>()) {
            counts[index] += 1;
        }
        let sigma = (shape.lifetime_p90_hours / shape.lifetime_median_hours).ln() / 1.281_551_565_5;
        let mu = shape.lifetime_median_hours.ln();
        let mut identities = Vec::with_capacity(shape.identities);
        for (index, (from, to)) in shape.whale_spans.iter().enumerate() {
            let mut rng = stream(shape.seed, 3, index as u64);
            let address = address(shape.seed, index);
            identities.push(Identity {
                program: Sha256::digest(address.as_bytes()).to_vec(),
                address,
                start_ms: start_ms + i64::from(*from) * DAY_MS,
                end_ms: (start_ms + i64::from(*to) * DAY_MS).min(end_ms),
                difficulty: 2f64.powf(
                    rng.gen_range((shape.difficulty_bits - 2.0).max(0.0)..=shape.difficulty_bits),
                ),
                zipf: 1.0,
            });
        }
        let mut era_start = start_ms;
        let mut index = whales;
        for (phase, count) in shape.phases.iter().zip(counts) {
            let era_ms = i64::from(phase.days) * DAY_MS;
            for arrival in 0..count {
                let mut rng = stream(shape.seed, 3, index as u64);
                let burst =
                    era_start == start_ms && (arrival as f64) < shape.launch_burst * count as f64;
                let span_ms = if burst { DAY_MS } else { era_ms };
                let start = era_start + rng.gen_range(0..span_ms);
                let hours = (mu + sigma * normal(&mut rng)).exp().max(0.25);
                let dust = rng.gen::<f64>() < shape.dust_share;
                let bits = if dust {
                    0.0..=shape.difficulty_bits.min(3.0)
                } else {
                    0.0..=shape.difficulty_bits
                };
                let address = address(shape.seed, index);
                identities.push(Identity {
                    program: Sha256::digest(address.as_bytes()).to_vec(),
                    address,
                    start_ms: start,
                    end_ms: (start + (hours * HOUR_MS as f64) as i64).min(end_ms),
                    difficulty: 2f64.powf(rng.gen_range(bits)),
                    zipf: if dust { -shape.dust_weight } else { 0.0 },
                });
                index += 1;
            }
            era_start += era_ms;
        }
        // Zipf ranks over the non-whales in a seeded order, after the whales.
        let mut ranked: Vec<usize> = (whales..identities.len()).collect();
        let mut rng = stream(shape.seed, 4, 0);
        for position in (1..ranked.len()).rev() {
            ranked.swap(position, rng.gen_range(0..=position));
        }
        for (rank, identity) in ranked.into_iter().enumerate() {
            // A negative placeholder marks a hobby rig and carries its scale.
            let scale = if identities[identity].zipf < 0.0 {
                -identities[identity].zipf
            } else {
                1.0
            };
            identities[identity].zipf = scale * ((rank + whales + 1) as f64).powf(-shape.zipf);
        }
        for (rank, whale) in identities.iter_mut().take(whales).enumerate() {
            whale.zipf = ((rank + 1) as f64).powf(-shape.zipf);
        }
        ensure!(identities.len() == shape.identities);
        Ok(identities)
    }

    fn day_of(&self, at_ms: i64) -> usize {
        usize::try_from((at_ms - self.start_ms) / DAY_MS)
            .unwrap_or(0)
            .min(self.dominant.len() - 1)
    }

    /// A worker's difficulty on `day`: its own, jittered by vardiff within a
    /// factor of two.
    fn day_difficulty(&self, identity: usize, day: usize) -> f64 {
        let mut rng = stream(self.shape.seed, 5, (identity as u64) << 20 | day as u64);
        let jitter: f64 = rng.gen_range(-0.5..=0.5);
        (self.identities[identity].difficulty * 2f64.powf(jitter)).max(1.0)
    }

    /// The rows of one 5-minute bucket, in acceptance order.
    fn bucket_rows(&self, bucket: usize) -> Vec<RowPlan> {
        let from = self.start_ms + bucket as i64 * BUCKET_MS;
        let to = from + BUCKET_MS;
        let day = self.day_of(from);
        let mut rng = stream(self.shape.seed, 6, bucket as u64);
        let active: Vec<usize> = (0..self.identities.len())
            .filter(|index| {
                let identity = &self.identities[*index];
                identity.start_ms < to && identity.end_ms > from
            })
            .collect();
        if active.is_empty() {
            return Vec::new();
        }
        let dominant = self.dominant[day].filter(|(whale, _)| active.contains(whale));
        let zipf_total: f64 = active
            .iter()
            .filter(|index| Some(**index) != dominant.map(|(whale, _)| whale))
            .map(|index| self.identities[*index].zipf)
            .sum();
        let difficulties: Vec<f64> = active
            .iter()
            .map(|index| self.day_difficulty(*index, day))
            .collect();
        // Work weights, then row weights: a row carries its worker's
        // difficulty, so rows are drawn by work over difficulty.
        let mut cumulative = Vec::with_capacity(active.len());
        let mut total = 0.0;
        for (position, index) in active.iter().enumerate() {
            let work = match dominant {
                Some((whale, fraction)) if whale == *index => {
                    if zipf_total > 0.0 {
                        zipf_total * fraction / (1.0 - fraction)
                    } else {
                        1.0
                    }
                }
                _ => self.identities[*index].zipf,
            };
            total += work / difficulties[position];
            cumulative.push(total);
        }
        let count = stochastic_round(
            self.rates[bucket]
                * (BUCKET_MS / 1_000) as f64
                * f64::from(self.shape.scale)
                * self.shape.density,
            &mut rng,
        );
        let mut rows = Vec::with_capacity(count + 1);
        let draw_row = |rng: &mut StdRng, position: usize, at_ms: i64, thinned: bool| {
            let rejected = rng.gen::<f64>() < self.shape.reject_rate;
            let reject = rejected.then(|| {
                let mut pick: f64 = rng.gen();
                REJECTS
                    .iter()
                    .find(|(_, share)| {
                        pick -= share;
                        pick <= 0.0
                    })
                    .unwrap_or(&REJECTS[0])
                    .0
            });
            let stale_grace = !rejected && rng.gen::<f64>() < self.shape.stale_grace_rate;
            RowPlan {
                at_ms,
                identity: active[position],
                // A drawn row stands for 1/density shares; a presence row is
                // one share at the worker's own difficulty, so a small miner's
                // credit stays small and accrues under the floor, as it does
                // unthinned.
                difficulty: ((difficulties[position]
                    / if thinned { self.shape.density } else { 1.0 })
                .round() as u128)
                    .max(1),
                reject,
                stale_grace,
            }
        };
        for _ in 0..count {
            let target = rng.gen::<f64>() * total;
            let position = cumulative
                .partition_point(|value| *value < target)
                .min(active.len() - 1);
            let at_ms = from + rng.gen_range(0..BUCKET_MS);
            rows.push(draw_row(&mut rng, position, at_ms, true));
        }
        // Every identity mines at least once on every day it is connected,
        // so thinning the rows never hides an identity from a day.
        let day_start = self.start_ms + day as i64 * DAY_MS;
        for (position, index) in active.iter().enumerate() {
            let first = self.identities[*index].start_ms.max(day_start);
            if (from..to).contains(&first) {
                rows.push(draw_row(&mut rng, position, first, false));
            }
        }
        rows.sort_by_key(|row| (row.at_ms, row.identity));
        rows
    }

    fn buckets(&self) -> usize {
        self.rates.len()
    }

    /// Places each block where the pool's cumulative accepted work crosses
    /// its share of the total, sets the network difficulty to the work per
    /// block, and assigns each block its final state and height.
    fn place_blocks(&mut self, phase_of_bucket: &[usize]) -> Result<()> {
        let buckets = self.buckets();
        let mut work = Vec::with_capacity(buckets);
        let mut rows_total = 0u64;
        let mut accepted = 0u64;
        let mut rejected = 0u64;
        let mut stale_grace = 0u64;
        let days = self.dominant.len();
        let mut day_work: Vec<HashMap<usize, f64>> = vec![HashMap::new(); days];
        let mut phase_rates: Vec<Vec<f64>> = vec![Vec::new(); self.shape.phases.len()];
        let mut with_shares = vec![false; self.identities.len()];
        for bucket in 0..buckets {
            let rows = self.bucket_rows(bucket);
            let mut bucket_work = 0u128;
            for row in &rows {
                rows_total += 1;
                with_shares[row.identity] = true;
                if row.reject.is_some() {
                    rejected += 1;
                    continue;
                }
                accepted += 1;
                stale_grace += u64::from(row.stale_grace);
                bucket_work += row.difficulty;
                *day_work[self.day_of(row.at_ms)]
                    .entry(row.identity)
                    .or_default() += row.difficulty as f64;
            }
            work.push(bucket_work);
            phase_rates[phase_of_bucket[bucket]].push(
                rows.len() as f64
                    / (BUCKET_MS / 1_000) as f64
                    / self.shape.density
                    / f64::from(self.shape.scale),
            );
        }
        let total: u128 = work.iter().sum();
        let blocks = self.shape.blocks as u128;
        ensure!(
            total >= blocks,
            "the shape produced {total} units of work for {blocks} blocks; raise the density"
        );
        self.network_difficulty = (total / blocks).max(1);
        // Block k is found where cumulative work first reaches
        // (2k + 1) / 2 of the work per block.
        let mut cumulative = 0u128;
        let mut next = 0u128;
        let threshold = |k: u128| total * (2 * k + 1) / (2 * blocks);
        for (bucket, bucket_work) in work.iter().enumerate() {
            if next >= blocks || cumulative + bucket_work < threshold(next) {
                cumulative += bucket_work;
                continue;
            }
            let rows = self.bucket_rows(bucket);
            for (row_index, row) in rows.iter().enumerate() {
                if row.reject.is_some() {
                    continue;
                }
                cumulative += row.difficulty;
                // One row finds one block; the next threshold waits for a
                // later row if a heavy share crosses two.
                if next < blocks && cumulative >= threshold(next) {
                    self.blocks.push(BlockPlan {
                        bucket,
                        row: row_index,
                        found_ms: row.at_ms,
                        state: BlockState::Mature,
                        offset: 0,
                    });
                    next += 1;
                }
            }
        }
        ensure!(
            self.blocks.len() == self.shape.blocks,
            "placed {} of {} blocks",
            self.blocks.len(),
            self.shape.blocks
        );
        // The newest blocks are immature: those found within the maturity
        // depth's worth of minutes of the end of history, and at least
        // `min_immature`. The rest settle, with a few disconnected.
        let recent = self
            .blocks
            .iter()
            .filter(|block| block.found_ms > self.end_ms - MATURITY_HEIGHTS as i64 * 60_000)
            .count();
        let immature = recent.max(self.shape.min_immature).min(self.blocks.len());
        ensure!(
            (immature as u64) < MATURITY_HEIGHTS,
            "{immature} immature blocks do not fit under the maturity depth"
        );
        let settled = self.blocks.len() - immature;
        for (index, block) in self.blocks.iter_mut().enumerate() {
            block.state = if index >= settled {
                BlockState::Immature
            } else if index % self.shape.inactive_every == self.shape.inactive_every / 2 {
                BlockState::Inactive
            } else if index % self.shape.reversed_every == self.shape.reversed_every / 3 {
                BlockState::Reversed
            } else if index % self.shape.rejected_every == self.shape.rejected_every / 5 {
                BlockState::Rejected
            } else {
                BlockState::Mature
            };
        }
        // Heights: settled on-chain blocks are consecutive; the immature
        // tail sits past a gap, so the tip is at least the maturity depth
        // above every settled block and less than it above every immature
        // one. A disconnected block competed for the height after the
        // on-chain block before it.
        let mut offset = 0u64;
        let mut first = true;
        let mut last_settled = 0u64;
        for block in self.blocks.iter_mut().take(settled) {
            if block.state.on_chain() {
                if !first {
                    offset += 1;
                }
                first = false;
                block.offset = offset;
                last_settled = offset;
            } else {
                block.offset = offset + u64::from(!first);
            }
        }
        let tail_start = if first {
            0
        } else {
            last_settled + MATURITY_HEIGHTS + 1 - immature as u64
        };
        for (position, block) in self.blocks.iter_mut().skip(settled).enumerate() {
            block.offset = tail_start + position as u64;
        }
        self.tip_offset = self
            .blocks
            .iter()
            .map(|block| block.offset)
            .max()
            .unwrap_or(0)
            + self.shape.tip_margin;
        self.block_at = self
            .blocks
            .iter()
            .enumerate()
            .map(|(index, block)| ((block.bucket, block.row), index))
            .collect();
        let per_day: Vec<usize> = day_work.iter().map(HashMap::len).collect();
        let top_share: Vec<f64> = day_work
            .iter()
            .filter(|work| !work.is_empty())
            .map(|work| {
                let sum: f64 = work.values().sum();
                work.values().copied().fold(0.0, f64::max) / sum
            })
            .collect();
        let states = |state: BlockState| self.blocks.iter().filter(|b| b.state == state).count();
        let phases: Vec<Value> = phase_rates
            .iter()
            .map(|rates| {
                let mean = rates.iter().sum::<f64>() / rates.len().max(1) as f64;
                let variance = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>()
                    / rates.len().max(1) as f64;
                json!({
                    "mean_rate": round3(mean),
                    "max_rate": round3(rates.iter().copied().fold(0.0, f64::max)),
                    "cv": round3(variance.sqrt() / mean.max(f64::MIN_POSITIVE)),
                })
            })
            .collect();
        self.summary = json!({
            "seed": self.shape.seed,
            "scale": self.shape.scale,
            "share_density": self.shape.density,
            "days": self.shape.days(),
            "rows": rows_total,
            "accepted": accepted,
            "rejected": rejected,
            "stale_grace": stale_grace,
            "represented_shares": (rows_total as f64 / self.shape.density).round(),
            "identities": self.identities.len(),
            "identities_with_shares": with_shares.iter().filter(|v| **v).count(),
            "identities_per_day": {"min": per_day.iter().min(), "max": per_day.iter().max()},
            "top_identity_work_share_per_day": {
                "min": round3(top_share.iter().copied().fold(1.0, f64::min)),
                "max": round3(top_share.iter().copied().fold(0.0, f64::max)),
                "days_at_or_above_0_82": top_share.iter().filter(|s| **s >= 0.82).count(),
            },
            "represented_rate_per_era": phases,
            "network_difficulty": self.network_difficulty.to_string(),
            "window_weight": (self.network_difficulty * qbit_prism::PRISM_WINDOW_MULTIPLIER).to_string(),
            "blocks": {
                "total": self.blocks.len(),
                "mature": states(BlockState::Mature),
                "immature": states(BlockState::Immature),
                "inactive": states(BlockState::Inactive),
                "reversed": states(BlockState::Reversed),
                "rejected": states(BlockState::Rejected),
            },
        });
        Ok(())
    }

    /// What was planned: rows, identities, the realised rate shape and the
    /// blocks by state.
    pub fn summary(&self) -> &Value {
        &self.summary
    }

    pub fn network_difficulty(&self) -> u128 {
        self.network_difficulty
    }

    /// The instant the seeded history ends.
    pub fn history_end_ms(&self) -> i64 {
        self.end_ms
    }

    /// Every identity's payout address, in seed order.
    pub fn addresses(&self) -> Vec<String> {
        self.identities.iter().map(|i| i.address.clone()).collect()
    }

    /// Heights of the on-chain blocks relative to the first, oldest first,
    /// and of the tip. A chain for [`MainnetPlan::write`] must put the
    /// blocks at exactly these offsets from its first block.
    pub fn chain_offsets(&self) -> (Vec<u64>, u64) {
        (
            self.blocks
                .iter()
                .filter(|block| block.state.on_chain())
                .map(|block| block.offset)
                .collect(),
            self.tip_offset,
        )
    }

    /// When each height of the plan's chain was mined, in Unix seconds,
    /// indexed by offset from the first on-chain block to the tip: each
    /// on-chain block at its found time, the heights between spread evenly,
    /// the tip at the end of history. Strictly increasing, as a chain's
    /// timestamps must be for a node to accept them one after another.
    pub fn chain_times(&self) -> Vec<i64> {
        let mut known: Vec<(u64, i64)> = self
            .blocks
            .iter()
            .filter(|block| block.state.on_chain())
            .map(|block| (block.offset, block.found_ms / 1_000))
            .collect();
        known.push((self.tip_offset, self.end_ms / 1_000));
        let mut times = Vec::with_capacity(self.tip_offset as usize + 1);
        let mut from = (0u64, known.first().map_or(self.start_ms / 1_000, |k| k.1));
        for (offset, at) in known {
            while (times.len() as u64) <= offset {
                let position = times.len() as u64;
                let span = offset.saturating_sub(from.0).max(1);
                let time =
                    from.1 + (at - from.1) * (position.saturating_sub(from.0)) as i64 / span as i64;
                let floor = times.last().map_or(i64::MIN, |last: &i64| last + 1);
                times.push(time.max(floor));
            }
            from = (offset, at);
        }
        times
    }

    /// A chain for the plan made of [`synthetic_chain_hash`]es from `base`,
    /// and its tip height. The rehearsal node serves the same hashes.
    pub fn synthetic_chain(&self, tag: &str, base: u64) -> (Vec<ChainBlock>, u64) {
        let (offsets, tip) = self.chain_offsets();
        let blocks = offsets
            .into_iter()
            .map(|offset| {
                let height = base + offset;
                ChainBlock {
                    height,
                    hash: synthetic_chain_hash(tag, height),
                    parent: synthetic_chain_hash(tag, height - 1),
                }
            })
            .collect();
        (blocks, base + tip)
    }
}

fn round3(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

/// Share rows waiting for one `INSERT ... SELECT FROM unnest(...)`.
#[derive(Default)]
struct ShareBatch {
    seq: Vec<i64>,
    share_id: Vec<String>,
    miner: Vec<String>,
    program: Vec<Vec<u8>>,
    difficulty: Vec<String>,
    template_height: Vec<i64>,
    job_id: Vec<String>,
    job_issued_ms: Vec<i64>,
    ntime: Vec<i64>,
    accepted_ms: Vec<i64>,
    accepted: Vec<bool>,
    reject: Vec<Option<String>>,
    writer_epoch: Vec<i64>,
    credit_policy: Vec<Option<String>>,
}

/// Everything else a block writes, flushed after the shares it references.
#[derive(Default)]
struct BlockBatch {
    blocks: Vec<Value>,
    audits: Vec<Value>,
    carry: Vec<Value>,
    outbox: Vec<Value>,
    ctv_sets: Vec<Value>,
    ctv_artifacts: Vec<Value>,
    ctv_attempts: Vec<Value>,
}

impl BlockBatch {
    fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.outbox.is_empty()
    }
}

/// The writer's view of the accepted history: the PPLNS window by work and
/// the newest shares for the next audit bundle.
struct WindowState {
    weight: u128,
    rows: VecDeque<(usize, u128)>,
    sum: u128,
    per_identity: HashMap<usize, u128>,
    recent: VecDeque<AcceptedShare>,
    cap: usize,
}

impl WindowState {
    fn push(&mut self, identity: usize, difficulty: u128, share: AcceptedShare) {
        self.rows.push_back((identity, difficulty));
        self.sum += difficulty;
        *self.per_identity.entry(identity).or_default() += difficulty;
        // Keep the oldest row only while it is still (partly) counted.
        while let Some(&(oldest, weight)) = self.rows.front() {
            if self.sum - weight < self.weight {
                break;
            }
            self.rows.pop_front();
            self.sum -= weight;
            let held = self
                .per_identity
                .get_mut(&oldest)
                .expect("tracked identity");
            *held -= weight;
            if *held == 0 {
                self.per_identity.remove(&oldest);
            }
        }
        self.recent.push_back(share);
        if self.recent.len() > self.cap {
            self.recent.pop_front();
        }
    }

    /// Counted work per identity: the oldest row is clipped to the window.
    fn counted(&self) -> BTreeMap<usize, u128> {
        let mut counted: BTreeMap<usize, u128> = self
            .per_identity
            .iter()
            .map(|(identity, work)| (*identity, *work))
            .collect();
        if self.sum > self.weight {
            let (oldest, _) = self.rows.front().expect("a window over weight has rows");
            let excess = self.sum - self.weight;
            let held = counted.get_mut(oldest).expect("tracked identity");
            *held -= excess;
            if *held == 0 {
                counted.remove(oldest);
            }
        }
        counted
    }
}

/// `amount` split over `weights` in proportion, remainders to the largest
/// fractional parts (ties to the lower identity), summing to `amount`.
fn apportion(amount: u128, weights: &BTreeMap<usize, u128>) -> BTreeMap<usize, u128> {
    let total: u128 = weights.values().sum();
    let mut shares: Vec<(usize, u128, u128)> = weights
        .iter()
        .map(|(identity, weight)| {
            let exact = amount * weight;
            (*identity, exact / total, exact % total)
        })
        .collect();
    let mut left = amount - shares.iter().map(|(_, share, _)| share).sum::<u128>();
    let mut order: Vec<usize> = (0..shares.len()).collect();
    order.sort_by(|a, b| shares[*b].2.cmp(&shares[*a].2).then(a.cmp(b)));
    for index in order {
        if left == 0 {
            break;
        }
        shares[index].1 += 1;
        left -= 1;
    }
    shares
        .into_iter()
        .map(|(identity, share, _)| (identity, share))
        .collect()
}

fn ms_text(ms: i64) -> String {
    ms.to_string()
}

impl MainnetPlan {
    /// Writes the planned ledger over `chain` (the on-chain blocks at
    /// [`MainnetPlan::chain_offsets`]), with audit bodies under `root`, and
    /// returns what was written with a summary of it.
    pub async fn write(
        &self,
        pool: &PgPool,
        root: &Path,
        chain: &[ChainBlock],
        coinbase_key: &ManifestSigningKey,
        ledger_key: &ManifestSigningKey,
    ) -> Result<(Seeded, Value)> {
        let (offsets, _) = self.chain_offsets();
        ensure!(
            chain.len() == offsets.len(),
            "the plan has {} on-chain blocks, the chain {}",
            offsets.len(),
            chain.len()
        );
        let base = chain.first().map_or(0, |block| block.height);
        for (block, offset) in chain.iter().zip(&offsets) {
            ensure!(
                block.height == base + offset,
                "chain block {} is at height {}, the plan needs {}",
                block.hash,
                block.height,
                base + offset
            );
        }
        let with_258 = self.shape.upgrade_258_day.is_some();
        apply_release_schema(pool, with_258).await?;
        if self.shape.credit_policy_not_valid {
            // What 001 leaves on a ledger created before the column: the
            // same CHECK, added NOT VALID and never validated.
            sqlx::raw_sql("ALTER TABLE qbit_share_ledger DROP CONSTRAINT qbit_share_ledger_credit_policy_check; ALTER TABLE qbit_share_ledger ADD CONSTRAINT qbit_share_ledger_credit_policy_check CHECK (credit_policy IS NULL OR credit_policy IN ('stale-grace')) NOT VALID")
                .execute(pool)
                .await?;
        }
        let nd = self.network_difficulty;
        let mut window = WindowState {
            weight: nd * qbit_prism::PRISM_WINDOW_MULTIPLIER,
            rows: VecDeque::new(),
            sum: 0,
            per_identity: HashMap::new(),
            recent: VecDeque::new(),
            cap: self.shape.audit_window_cap,
        };
        let mut balances: HashMap<usize, u128> = HashMap::new();
        let mut shares = ShareBatch::default();
        let mut pending = BlockBatch::default();
        let mut artifacts = Vec::with_capacity(self.blocks.len());
        let mut hashes = Vec::with_capacity(self.blocks.len());
        let mut pool_rollup: BTreeMap<(i32, i64), (i64, u128)> = BTreeMap::new();
        let mut miner_rollup: BTreeMap<(i32, i64, usize), (i64, u128)> = BTreeMap::new();
        let mut last_share: HashMap<usize, (i64, u128)> = HashMap::new();
        let mut chain_blocks = chain.iter();
        let mut previous_on_chain: Option<&ChainBlock> = None;
        let mut on_chain_seen = 0usize;
        let mut ordinal = 0i64;
        let mut seq = 0i64;
        let mut accepted_shares = 0i64;
        let mut counts = BTreeMap::<&str, u64>::new();
        let upgrade_ms = self
            .shape
            .upgrade_258_day
            .map(|day| self.start_ms + i64::from(day) * DAY_MS);
        let ctv_from_ms = self.end_ms - i64::from(self.shape.ctv_days) * DAY_MS;
        // Mainnet's pool fee, which also sweeps the dust under the floor.
        let fee_address = synthetic_address("prism-rehearsal-pool-fee");
        let mut policy = PayoutPolicy::day_one_default();
        policy.pool_fee_policy = Some(qbit_prism::PoolFeePolicy {
            fee_bps: u16::try_from(self.shape.pool_fee_bps)?,
            recipient_id: fee_address.clone(),
            order_key: fee_address.clone(),
            p2mr_program_hex: hex::encode(Sha256::digest(fee_address.as_bytes())),
        });
        let net_sats = u128::from(self.shape.coinbase_sats)
            * u128::from(10_000 - self.shape.pool_fee_bps)
            / 10_000;
        for bucket in 0..self.buckets() {
            for (row_index, row) in self.bucket_rows(bucket).into_iter().enumerate() {
                // A PostgreSQL sequence skips values an aborted insert took.
                seq += 1 + i64::from(seq % 20_011 == 20_010);
                let block = self.block_at.get(&(bucket, row_index)).copied();
                let identity = &self.identities[row.identity];
                let job = row.at_ms / JOB_MS;
                let job_id = format!("{job:08x}");
                let ntime = row.at_ms / 1_000;
                // Work is built on the tip below the next on-chain block.
                let template_height = offsets
                    .get(on_chain_seen)
                    .map_or(base + self.tip_offset, |offset| base + offset);
                // The block hash a found share's header hashes to.
                let (share_hash, block_place) = match block {
                    Some(index) if self.blocks[index].state.on_chain() => {
                        let place = chain_blocks.next().context("chain ran out of blocks")?;
                        (place.hash.clone(), Some(place.clone()))
                    }
                    Some(index) => {
                        let height = base + self.blocks[index].offset;
                        let parent = previous_on_chain
                            .map(|block| block.hash.clone())
                            .unwrap_or_else(|| "00".repeat(32));
                        (
                            orphan_hash(self.shape.seed, index),
                            Some(ChainBlock {
                                height,
                                hash: orphan_hash(self.shape.seed, index),
                                parent,
                            }),
                        )
                    }
                    None => (
                        hex::encode(Sha256::digest(format!(
                            "prism-rehearsal-share:{}:{seq}",
                            self.shape.seed
                        ))),
                        None,
                    ),
                };
                let share_id = format!(
                    "{job_id}:{:016x}:{ntime:08x}:{:08x}:{share_hash}",
                    seq as u64 ^ self.shape.seed,
                    (seq as u64).wrapping_mul(2_654_435_761) as u32
                );
                let epoch = 1 + (row.at_ms - self.start_ms) / (7 * DAY_MS);
                shares.seq.push(seq);
                shares.share_id.push(share_id.clone());
                shares.miner.push(identity.address.clone());
                shares.program.push(identity.program.clone());
                shares.difficulty.push(row.difficulty.to_string());
                shares.template_height.push(i64::try_from(template_height)?);
                shares.job_id.push(job_id.clone());
                shares.job_issued_ms.push(job * JOB_MS);
                shares.ntime.push(ntime);
                shares.accepted_ms.push(row.at_ms);
                shares.accepted.push(row.reject.is_none());
                shares.reject.push(row.reject.map(str::to_owned));
                shares.writer_epoch.push(epoch);
                shares
                    .credit_policy
                    .push(row.stale_grace.then(|| "stale-grace".to_owned()));
                if row.reject.is_none() {
                    accepted_shares += 1;
                    let seconds = row.at_ms.div_euclid(1_000);
                    for grain in [300i32, 3_600, 86_400] {
                        let epoch = seconds.div_euclid(i64::from(grain)) * i64::from(grain);
                        let pool_entry = pool_rollup.entry((grain, epoch)).or_default();
                        pool_entry.0 += 1;
                        pool_entry.1 += row.difficulty;
                        let miner_entry = miner_rollup
                            .entry((grain, epoch, row.identity))
                            .or_default();
                        miner_entry.0 += 1;
                        miner_entry.1 += row.difficulty;
                    }
                    last_share.insert(row.identity, (row.at_ms, row.difficulty));
                    window.push(
                        row.identity,
                        row.difficulty,
                        AcceptedShare {
                            share_seq: u64::try_from(seq)?,
                            share_id: share_id.clone(),
                            miner_id: identity.address.clone(),
                            order_key: identity.address.clone(),
                            p2mr_program_hex: hex::encode(&identity.program),
                            share_difficulty: row.difficulty,
                            network_difficulty: nd,
                            template_height,
                            job_id,
                            job_issued_at_ms: job * JOB_MS,
                            accepted_at_ms: row.at_ms,
                            ntime: u32::try_from(ntime)?,
                            credit_policy: row.stale_grace.then(|| "stale-grace".to_owned()),
                        },
                    );
                }
                if let (Some(index), Some(place)) = (block, block_place) {
                    let plan = &self.blocks[index];
                    ensure!(row.reject.is_none(), "a block was placed on a reject");
                    if plan.state.on_chain() {
                        on_chain_seen += 1;
                        previous_on_chain = chain.get(on_chain_seen - 1);
                    }
                    let recipients = window.counted();
                    let gross = apportion(net_sats, &recipients);
                    let found = window.recent.back().expect("the found share").clone();
                    let bundle = qbit_prism::build_audit_bundle(
                        window.recent.iter().cloned().collect(),
                        FoundBlock {
                            block_height: place.height,
                            coinbase_value_sats: self.shape.coinbase_sats,
                            network_difficulty: nd,
                            // The window was read when the found share was
                            // accepted, so every share in it is eligible.
                            anchor_job_issued_at_ms: found.accepted_at_ms,
                        },
                        vec![],
                        policy.clone(),
                        coinbase_key,
                        ledger_key,
                    )?;
                    let report = qbit_prism::verify_audit_bundle_with_ledger_public_key(
                        &bundle,
                        &ledger_key.public_key_hex(),
                    )?;
                    let canonical = qbit_prism::canonical_audit_bundle_bytes(&bundle)?;
                    let (inline, body_uri) = write_audit_layout(
                        root,
                        &place.hash,
                        index,
                        &bundle,
                        &report.audit_bundle_sha256_hex,
                        &canonical,
                    )?;
                    *counts
                        .entry(["sidecar", "external", "inline"][index % 3])
                        .or_default() += 1;
                    let state = plan.state;
                    let found_ms = plan.found_ms;
                    ordinal += i64::from(state != BlockState::Rejected);
                    pending.blocks.push(json!({
                        "hash": place.hash, "height": place.height, "parent": place.parent,
                        "ordinal": (state != BlockState::Rejected).then_some(ordinal),
                        "coinbase_txid": report.coinbase_txid,
                        "manifest": report.coinbase_manifest_sha256_hex,
                        "found": ms_text(found_ms),
                        "chain_state": state.chain_state(),
                        "maturity_state": state.maturity_state(),
                        "matured": (state == BlockState::Mature)
                            .then(|| ms_text(found_ms + MATURITY_HEIGHTS as i64 * 60_000)),
                        "disconnected": matches!(state, BlockState::Reversed | BlockState::Rejected)
                            .then(|| ms_text(found_ms + 20 * 60_000)),
                    }));
                    pending.audits.push(json!({
                        "hash": place.hash,
                        "inline": inline.map(|body| body.to_string()),
                        "sha": report.audit_bundle_sha256_hex,
                        "coinbase_tx_hex": report.coinbase_tx_hex,
                        "body_uri": body_uri,
                        "created": ms_text(found_ms + 1_000),
                    }));
                    // Each recipient's carry chain continues only through
                    // blocks that count; a disconnected block's rows record
                    // what it would have paid and move no balance.
                    let counts_balance = state.on_chain();
                    for (recipient, gross) in gross {
                        let prior = balances.get(&recipient).copied().unwrap_or(0);
                        let candidate = prior + gross;
                        let onchain = if candidate >= u128::from(self.shape.payout_floor_sats) {
                            candidate
                        } else {
                            0
                        };
                        let carry = candidate - onchain;
                        if counts_balance {
                            balances.insert(recipient, carry);
                        }
                        let identity = &self.identities[recipient];
                        pending.carry.push(json!({
                            "height": place.height, "hash": place.hash,
                            "miner": identity.address, "program": hex::encode(&identity.program),
                            "gross": gross.to_string(), "prior": prior.to_string(),
                            "candidate": candidate.to_string(), "onchain": onchain.to_string(),
                            "carry": carry.to_string(),
                            "action": if onchain > 0 { "onchain" } else { "accrued" },
                            "maturity": state.maturity_state(),
                            "created": ms_text(found_ms + 1_000),
                        }));
                    }
                    let candidate_sha = hex::encode(Sha256::digest(&canonical));
                    let v2 = upgrade_ms.is_some_and(|upgrade| found_ms >= upgrade);
                    pending.outbox.push(json!({
                        "hash": place.hash, "share_id": share_id, "sha": candidate_sha,
                        "state": if state == BlockState::Rejected { "abandoned" } else { "submitted" },
                        "attempts": if state == BlockState::Rejected { 3 } else { 1 },
                        "error": (state == BlockState::Rejected).then_some("submitblock rejected: bad-prevblk"),
                        "created": ms_text(found_ms), "completed": ms_text(found_ms + 2_000),
                        "version": if v2 { 2 } else { 1 },
                        "retired": v2.then(|| hex::encode(&Sha256::digest(format!("body:{}", place.hash))[..16])),
                        "parent": v2.then(|| place.parent.clone()),
                        "expected_height": v2.then_some(place.height),
                    }));
                    if found_ms >= ctv_from_ms {
                        self.ctv_rows(&mut pending, index, &place, &report, state, found_ms);
                    }
                    hashes.push(place.hash.clone());
                    artifacts.push(SeededArtifact {
                        block_hash: place.hash.clone(),
                        digest: report.audit_bundle_sha256_hex.clone(),
                        canonical,
                    });
                }
                if shares.seq.len() >= BATCH_ROWS {
                    flush(pool, &mut shares, &mut pending, with_258, nd).await?;
                }
            }
        }
        flush(pool, &mut shares, &mut pending, with_258, nd).await?;
        ensure!(
            hashes.len() == self.blocks.len(),
            "wrote {} of {} blocks",
            hashes.len(),
            self.blocks.len()
        );
        sqlx::query("SELECT setval('qbit_audit_publication_sequence_seq',$1)")
            .bind(ordinal.max(1))
            .execute(pool)
            .await?;
        self.write_candidates_and_state(pool, seq, &last_share, with_258)
            .await?;
        let rollup_rows =
            write_rollups(pool, &pool_rollup, &miner_rollup, &self.identities, seq).await?;
        // Production statistics come from autovacuum; give the planner the
        // same view before anything reads the ledger.
        sqlx::raw_sql("ANALYZE").execute(pool).await?;
        let seeded = finish(
            pool,
            accepted_shares,
            seq,
            MAINNET_SEQUENCE_HEADROOM,
            hashes,
            artifacts,
        )
        .await?;
        let mut summary = self.summary.clone();
        summary["last_share_seq"] = json!(seq);
        summary["audit_layouts"] = json!(counts);
        summary["rollup_rows"] = json!(rollup_rows);
        summary["tip_height"] = json!(base + self.tip_offset);
        Ok((seeded, summary))
    }

    fn ctv_rows(
        &self,
        pending: &mut BlockBatch,
        index: usize,
        place: &ChainBlock,
        report: &qbit_prism::AuditVerificationReport,
        state: BlockState,
        found_ms: i64,
    ) {
        let manifest_set = json!({
            "schema": "qbit.prism.ctv-manifest-set.v1",
            "block_hash": place.hash,
            "block_height": place.height,
        })
        .to_string();
        let set_sha = hex::encode(Sha256::digest(manifest_set.as_bytes()));
        let chunks = 1 + index % 2;
        let per_chunk = 40_000_000u64;
        pending.ctv_sets.push(json!({
            "hash": place.hash, "json": manifest_set, "sha": set_sha,
            "txid": report.coinbase_txid, "tx_hex": report.coinbase_tx_hex,
            "count": chunks, "sum": (per_chunk * chunks as u64).to_string(),
            "created": ms_text(found_ms + 1_000),
        }));
        for chunk in 0..chunks {
            let txid = hex::encode(Sha256::digest(format!("fanout:{}:{chunk}", place.hash)));
            let status = match state {
                BlockState::Mature => match index % 10 {
                    8 => "broadcastable",
                    9 => "failed",
                    _ => "confirmed",
                },
                BlockState::Immature => "awaiting_maturity",
                _ => "reorged",
            };
            let attempts: &[&str] = match status {
                "confirmed" => &["submitted", "accepted"],
                "failed" => &["rejected"],
                _ => &[],
            };
            let manifest = json!({"fanout_txid": txid, "chunk_index": chunk}).to_string();
            pending.ctv_artifacts.push(json!({
                "txid": txid, "hash": place.hash, "set_sha": set_sha,
                "manifest": manifest, "manifest_sha": hex::encode(Sha256::digest(manifest.as_bytes())),
                "chunk": chunk, "chunks": chunks, "parent_txid": report.coinbase_txid,
                "vout": 1 + chunk, "value": per_chunk.to_string(), "status": status,
                "attempts": attempts.len(), "last_status": attempts.last(),
                "first_attempt": (!attempts.is_empty()).then(|| ms_text(found_ms + 3_000_000)),
                "last_attempt": (!attempts.is_empty()).then(|| ms_text(found_ms + 3_060_000)),
                "status_counts": attempts.iter().fold(json!({}), |mut map, status| {
                    map[*status] = json!(map[*status].as_i64().unwrap_or(0) + 1);
                    map
                }).to_string(),
                "updated": ms_text(found_ms + 3_060_000),
            }));
            for (position, status) in attempts.iter().enumerate() {
                pending.ctv_attempts.push(json!({
                    "txid": txid, "status": status,
                    "at": ms_text(found_ms + 3_000_000 + position as i64 * 60_000),
                    "error": (*status == "rejected").then_some("min relay fee not met"),
                }));
            }
        }
    }

    async fn write_candidates_and_state(
        &self,
        pool: &PgPool,
        last_seq: i64,
        last_share: &HashMap<usize, (i64, u128)>,
        with_258: bool,
    ) -> Result<()> {
        // Candidates 2.x.x abandoned before submission: no share, no block.
        for index in 0..self.shape.abandoned_candidates {
            let at_ms = self.start_ms
                + (index as i64 + 1) * (self.end_ms - self.start_ms)
                    / (self.shape.abandoned_candidates as i64 + 1);
            sqlx::query("INSERT INTO qbit_block_candidate_outbox(block_hash,candidate,candidate_sha256,state,last_error,created_at,updated_at,completed_at) VALUES($1,NULL,$2,'abandoned','stale before submission',to_timestamp($3::double precision/1000),to_timestamp($3::double precision/1000),to_timestamp($3::double precision/1000))")
                .bind(hex::encode(Sha256::digest(format!("prism-rehearsal-abandoned:{}:{index}", self.shape.seed))))
                .bind(hex::encode(Sha256::digest(format!("abandoned-body:{index}"))))
                .bind(at_ms)
                .execute(pool)
                .await?;
        }
        let _ = with_258;
        // The drained writer's lease, expired at the end of history.
        let epoch = 1 + (self.end_ms - self.start_ms) / (7 * DAY_MS);
        sqlx::query("INSERT INTO qbit_ledger_writer_lease(singleton,writer_id,writer_epoch,writer_session_token,lease_expires_at,updated_at) VALUES(true,'prism-2x-coordinator',$1,$2,to_timestamp($3::double precision/1000),to_timestamp($3::double precision/1000))")
            .bind(epoch)
            .bind(format!("prism-2x-coordinator:{epoch}"))
            .bind(self.end_ms)
            .execute(pool)
            .await?;
        // Retained vardiff for identities that mined in the last day.
        let mut workers = 0;
        for (identity, (at_ms, difficulty)) in last_share {
            if *at_ms < self.end_ms - DAY_MS {
                continue;
            }
            let address = &self.identities[*identity].address;
            for (listener, worker) in [("main", "rig1"), ("main", "rig2")]
                .into_iter()
                .take(1 + identity % 2)
            {
                sqlx::query("INSERT INTO qbit_worker_difficulty(listener,worker_username,difficulty,evidence_at,updated_at) VALUES($1,$2,$3::numeric,to_timestamp($4::double precision/1000),to_timestamp($4::double precision/1000))")
                    .bind(listener)
                    .bind(format!("{address}.{worker}"))
                    .bind(((*difficulty as f64 * self.shape.density).round().max(1.0) as u64).to_string())
                    .bind(*at_ms)
                    .execute(pool)
                    .await?;
                workers += 1;
            }
        }
        ensure!(workers > 0, "no identity mined in the last day of history");
        // Allocations that rolled back still advanced the sequence.
        sqlx::query("SELECT setval('qbit_share_ledger_share_seq_seq',$1)")
            .bind(last_seq + MAINNET_SEQUENCE_HEADROOM)
            .execute(pool)
            .await?;
        Ok(())
    }
}

/// Writes the pending shares, then everything that references them, each
/// as one statement per kind.
async fn flush(
    pool: &PgPool,
    shares: &mut ShareBatch,
    pending: &mut BlockBatch,
    with_258: bool,
    network_difficulty: u128,
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    if !shares.seq.is_empty() {
        let batch = std::mem::take(shares);
        sqlx::query("INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,p2mr_program,share_difficulty,network_difficulty,template_height,job_id,job_issued_at,accepted_at,ntime,accepted,reject_reason,writer_id,writer_epoch,credit_policy) SELECT s,i,m,m,p,d::numeric,$15::numeric,h,j,to_timestamp(ji::double precision/1000),to_timestamp(a::double precision/1000),n,ok,r,'prism-2x-coordinator',e,c FROM unnest($1::bigint[],$2::text[],$3::text[],$4::bytea[],$5::text[],$6::bigint[],$7::text[],$8::bigint[],$9::bigint[],$10::bigint[],$11::bool[],$12::text[],$13::bigint[],$14::text[]) AS t(s,i,m,p,d,h,j,ji,n,a,ok,r,e,c)")
            .bind(batch.seq)
            .bind(batch.share_id)
            .bind(batch.miner)
            .bind(batch.program)
            .bind(batch.difficulty)
            .bind(batch.template_height)
            .bind(batch.job_id)
            .bind(batch.job_issued_ms)
            .bind(batch.ntime)
            .bind(batch.accepted_ms)
            .bind(batch.accepted)
            .bind(batch.reject)
            .bind(batch.writer_epoch)
            .bind(batch.credit_policy)
            .bind(network_difficulty.to_string())
            .execute(&mut *transaction)
            .await?;
    }
    if !pending.is_empty() {
        let batch = std::mem::take(pending);
        // Blocks before their carry rows: the carry-summary trigger counts
        // a row only when its block already counts. 2.x.x wrote a payout
        // entry beside every carry row.
        let carry = Value::Array(batch.carry);
        for (sql, rows) in [
            (BLOCKS_SQL, Value::Array(batch.blocks)),
            (AUDITS_SQL, Value::Array(batch.audits)),
            (CARRY_SQL, carry.clone()),
            (PAYOUTS_SQL, carry),
            (
                if with_258 { OUTBOX_258_SQL } else { OUTBOX_SQL },
                Value::Array(batch.outbox),
            ),
            (CTV_SETS_SQL, Value::Array(batch.ctv_sets)),
            (CTV_ARTIFACTS_SQL, Value::Array(batch.ctv_artifacts)),
            (CTV_ATTEMPTS_SQL, Value::Array(batch.ctv_attempts)),
        ] {
            if rows.as_array().is_some_and(Vec::is_empty) {
                continue;
            }
            sqlx::query(sql)
                .bind(rows)
                .execute(&mut *transaction)
                .await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

/// `r->>'<field>'` milliseconds as a timestamp; NULL stays NULL.
macro_rules! rows_sql {
    ($insert:literal, $select:literal) => {
        concat!(
            $insert,
            " SELECT ",
            $select,
            " FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS e(r,o) ORDER BY o"
        )
    };
}

const BLOCKS_SQL: &str = rows_sql!(
    "INSERT INTO qbit_pool_blocks(block_hash,audit_publication_sequence,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,found_at,chain_state,maturity_state,matured_at,disconnected_at)",
    "r->>'hash',(r->>'ordinal')::bigint,(r->>'height')::bigint,r->>'parent',r->>'coinbase_txid',r->>'manifest',to_timestamp((r->>'found')::double precision/1000),r->>'chain_state',r->>'maturity_state',to_timestamp((r->>'matured')::double precision/1000),to_timestamp((r->>'disconnected')::double precision/1000)"
);
const AUDITS_SQL: &str = rows_sql!(
    "INSERT INTO qbit_pool_audit_bundles(block_hash,audit_bundle,audit_bundle_sha256,coinbase_tx_hex,body_uri,created_at)",
    "r->>'hash',(r->>'inline')::jsonb,r->>'sha',r->>'coinbase_tx_hex',r->>'body_uri',to_timestamp((r->>'created')::double precision/1000)"
);
const CARRY_SQL: &str = rows_sql!(
    "INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state,created_at)",
    "(r->>'height')::bigint,r->>'hash',r->>'miner',r->>'miner',decode(r->>'program','hex'),(r->>'gross')::bigint,(r->>'prior')::numeric,(r->>'candidate')::numeric,(r->>'onchain')::bigint,(r->>'carry')::numeric,r->>'action',r->>'maturity',to_timestamp((r->>'created')::double precision/1000)"
);
const PAYOUTS_SQL: &str = rows_sql!(
    "INSERT INTO qbit_pool_payout_entries(block_hash,block_height,miner_id,payout_order_key,p2mr_program,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state,created_at)",
    "r->>'hash',(r->>'height')::bigint,r->>'miner',r->>'miner',decode(r->>'program','hex'),(r->>'onchain')::bigint,(r->>'carry')::numeric,r->>'action',r->>'maturity',to_timestamp((r->>'created')::double precision/1000)"
);
const OUTBOX_SQL: &str = rows_sql!(
    "INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256,state,attempt_count,last_error,created_at,updated_at,completed_at)",
    "r->>'hash',r->>'share_id',NULL,r->>'sha',r->>'state',(r->>'attempts')::integer,r->>'error',to_timestamp((r->>'created')::double precision/1000),to_timestamp((r->>'completed')::double precision/1000),to_timestamp((r->>'completed')::double precision/1000)"
);
const OUTBOX_258_SQL: &str = rows_sql!(
    "INSERT INTO qbit_block_candidate_outbox(block_hash,share_id,candidate,candidate_sha256,state,attempt_count,last_error,created_at,updated_at,completed_at,storage_version,retired_body_id,parent_hash,expected_height)",
    "r->>'hash',r->>'share_id',NULL,r->>'sha',r->>'state',(r->>'attempts')::integer,r->>'error',to_timestamp((r->>'created')::double precision/1000),to_timestamp((r->>'completed')::double precision/1000),to_timestamp((r->>'completed')::double precision/1000),(r->>'version')::integer,r->>'retired',r->>'parent',(r->>'expected_height')::bigint"
);
const CTV_SETS_SQL: &str = rows_sql!(
    "INSERT INTO qbit_ctv_fanout_sets(block_hash,manifest_set_json,manifest_set,manifest_set_sha256,settlement_mode,parent_coinbase_txid,parent_coinbase_tx_hex,fanout_count,fanout_output_sum_sats,covenant_output_value_sats,created_at)",
    "r->>'hash',r->>'json',(r->>'json')::jsonb,r->>'sha','hybrid_coinbase_ctv_fanout',r->>'txid',r->>'tx_hex',(r->>'count')::integer,(r->>'sum')::bigint,(r->>'sum')::bigint,to_timestamp((r->>'created')::double precision/1000)"
);
const CTV_ARTIFACTS_SQL: &str = rows_sql!(
    "INSERT INTO qbit_ctv_fanout_artifacts(fanout_txid,block_hash,manifest_set_sha256,manifest_json,manifest,manifest_sha256,precommitment_sha256,ctv_hash,commitment_witness_leaf_hex,chunk_index,chunk_count,parent_coinbase_txid,parent_coinbase_vout,fanout_tx_template_hex,fanout_tx_hex,covenant_output_value_sats,fanout_output_sum_sats,settlement_status,updated_at,broadcast_attempt_count,broadcast_attempt_detail_count,first_broadcast_attempt_at,last_broadcast_attempt_at,last_broadcast_attempt_status,broadcast_attempt_status_counts)",
    "r->>'txid',r->>'hash',r->>'set_sha',r->>'manifest',(r->>'manifest')::jsonb,r->>'manifest_sha',encode(sha256(convert_to('precommitment:'||(r->>'txid'),'UTF8')),'hex'),encode(sha256(convert_to('ctv:'||(r->>'txid'),'UTF8')),'hex'),'00',(r->>'chunk')::integer,(r->>'chunks')::integer,r->>'parent_txid',(r->>'vout')::integer,'00','00',(r->>'value')::bigint,(r->>'value')::bigint,r->>'status',to_timestamp((r->>'updated')::double precision/1000),(r->>'attempts')::bigint,(r->>'attempts')::bigint,to_timestamp((r->>'first_attempt')::double precision/1000),to_timestamp((r->>'last_attempt')::double precision/1000),r->>'last_status',(r->>'status_counts')::jsonb"
);
const CTV_ATTEMPTS_SQL: &str = rows_sql!(
    "INSERT INTO qbit_ctv_fanout_broadcast_attempts(fanout_txid,attempted_at,attempt_status,error)",
    "r->>'txid',to_timestamp((r->>'at')::double precision/1000),r->>'status',r->>'error'"
);

/// The dashboard rollups 2.x.x maintained incrementally, caught up to the
/// last share, as a drained coordinator leaves them.
async fn write_rollups(
    pool: &PgPool,
    pool_rollup: &BTreeMap<(i32, i64), (i64, u128)>,
    miner_rollup: &BTreeMap<(i32, i64, usize), (i64, u128)>,
    identities: &[Identity],
    last_seq: i64,
) -> Result<usize> {
    for chunk in pool_rollup.iter().collect::<Vec<_>>().chunks(BATCH_ROWS) {
        let (grains, epochs, counts, work): (Vec<i32>, Vec<i64>, Vec<i64>, Vec<String>) =
            chunk.iter().fold(
                (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut columns, ((grain, epoch), (count, work))| {
                    columns.0.push(*grain);
                    columns.1.push(*epoch);
                    columns.2.push(*count);
                    columns.3.push(work.to_string());
                    columns
                },
            );
        sqlx::query("INSERT INTO qbit_hashrate_rollup_pool(grain_seconds,bucket_epoch,accepted_share_count,accepted_share_difficulty) SELECT g,b,c,w::numeric FROM unnest($1::integer[],$2::bigint[],$3::bigint[],$4::text[]) AS t(g,b,c,w)")
            .bind(grains)
            .bind(epochs)
            .bind(counts)
            .bind(work)
            .execute(pool)
            .await?;
    }
    for chunk in miner_rollup.iter().collect::<Vec<_>>().chunks(BATCH_ROWS) {
        let mut grains = Vec::with_capacity(chunk.len());
        let mut epochs = Vec::with_capacity(chunk.len());
        let mut miners = Vec::with_capacity(chunk.len());
        let mut counts = Vec::with_capacity(chunk.len());
        let mut work = Vec::with_capacity(chunk.len());
        for ((grain, epoch, identity), (count, sum)) in chunk {
            grains.push(*grain);
            epochs.push(*epoch);
            miners.push(identities[*identity].address.clone());
            counts.push(*count);
            work.push(sum.to_string());
        }
        sqlx::query("INSERT INTO qbit_hashrate_rollup_miner(grain_seconds,bucket_epoch,miner_id,accepted_share_count,accepted_share_difficulty) SELECT g,b,m,c,w::numeric FROM unnest($1::integer[],$2::bigint[],$3::text[],$4::bigint[],$5::text[]) AS t(g,b,m,c,w)")
            .bind(grains)
            .bind(epochs)
            .bind(miners)
            .bind(counts)
            .bind(work)
            .execute(pool)
            .await?;
    }
    sqlx::query(
        "INSERT INTO qbit_hashrate_rollup_progress(singleton,last_share_seq) VALUES(true,$1)",
    )
    .bind(last_seq)
    .execute(pool)
    .await?;
    Ok(pool_rollup.len() + miner_rollup.len())
}
