//! Payout-window arithmetic and the pre-seeded window.
//!
//! The share generator is adapted from
//! `crates/qbit-prism-server/tests/support/window_fixture.rs` (the #264 gate
//! fixture). Two things change here: the network difficulty and the per-share
//! scaled difficulty are parameters rather than constants, because the harness
//! serves harder template bits than the stock fake node, and the seeded shares
//! must carry exactly the scaled difficulty that live shares carry, or the
//! window would not stay at the requested length once live shares displace the
//! seeded ones from the old end.

use anyhow::{ensure, Context, Result};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use qbit_prism::AcceptedShare;
use qbit_prism_server::codec;
use sqlx::PgPool;
use std::time::Instant;

/// `Ledger::snapshot` walks the ledger backwards until the summed
/// `share_difficulty` reaches `WINDOW_MULTIPLIER x network_difficulty`
/// (`crates/qbit-prism-server/src/ledger/window.rs`, `snapshot`).
pub const WINDOW_MULTIPLIER: u128 = 8;

/// Template bits the harness fake node serves. The stock `207fffff` makes
/// every valid share a block, because the codec caps the share target at the
/// network target (`codec::Job::from_manifest`). `1e7fffff` keeps the same
/// mantissa two bytes lower, giving a scaled network difficulty of
/// 65,536,000,000 and about 131,072 hashes per block.
pub const TEMPLATE_BITS: &str = "1e7fffff";

/// The lab-only difficulty both `check-config` and `capacity-evidence` refuse.
pub const FORBIDDEN_DIFFICULTY: f64 = 1e-9;

/// Serialized size of one native `AcceptedShare`, in bytes. The measured
/// production figure behind the #264 readiness review is ~581 B of JSON text
/// per share.
pub const DEFAULT_SEED_SHARE_BYTES: usize = 581;

/// 2023-11-14T22:13:20Z, from the #264 fixture. Far enough in the past that
/// every seeded share satisfies the snapshot anchor at any run date.
const JOB_ISSUED_AT_BASE_MS: i64 = 1_700_000_000_000;
const ACCEPTED_AT_OFFSET_MS: i64 = 1;
const RECIPIENTS: u64 = 5;
/// Display metadata on the row; unrelated to the window weight.
const ROW_NETWORK_DIFFICULTY: u128 = 1000;
const TEMPLATE_HEIGHT: u64 = 100;
const JOB_ID: &str = "seed-job";
const ORDER_KEY: &str = "k";
const SHARE_ID_INDEX_WIDTH: usize = 12;
const LOAD_BATCH_ROWS: u64 = 50_000;
/// Seeded `share_id`s must never collide with a run's live share identifiers,
/// which start with the run's payout address.
pub const SEED_SHARE_ID_PREFIX: &str = "seed-";

/// The difficulty settings that make a live share weigh exactly what the
/// window needs.
#[derive(Clone, Debug)]
pub struct WindowSolution {
    /// Scaled network difficulty of the served template bits.
    pub scaled_network_difficulty: u128,
    /// Total scaled weight `Ledger::snapshot` accumulates.
    pub window_weight: u128,
    /// Diff-1 share difficulty to configure on every frontend.
    pub share_difficulty: f64,
    /// The share target `codec` derives from `share_difficulty`.
    pub share_target: BigUint,
    /// Scaled difficulty each live and seeded share carries.
    pub scaled_share_difficulty: u128,
    /// Window length the operator asked for.
    pub requested_window: u64,
    /// Window length `ceil(weight / scaled_share_difficulty)` actually gives.
    pub computed_window: u64,
    /// Expected double-SHA-256 attempts per accepted share.
    pub hashes_per_share: f64,
    /// Expected double-SHA-256 attempts per block solution.
    pub hashes_per_block: f64,
}

/// Scaled network difficulty of compact bits, as the coordinator computes it
/// (`coordinator.rs`, `found_block.network_difficulty`).
pub fn scaled_network_difficulty(bits: u32) -> Result<u128> {
    codec::scaled_target_difficulty(&codec::target_from_compact(bits)?)
}

fn ceil_div(a: u128, b: u128) -> u128 {
    a.div_ceil(b)
}

/// Solve for the diff-1 share difficulty whose scaled weight makes the payout
/// window exactly `requested_window` shares long.
///
/// EP-VALIDATION: the solution is produced with the same `codec` functions the
/// server uses, and the resulting window length is asserted rather than
/// assumed, because `difficulty_target` rounds.
pub fn solve_window(bits: u32, requested_window: u64) -> Result<WindowSolution> {
    ensure!(requested_window > 0, "window share count must be positive");
    let network_target = codec::target_from_compact(bits)?;
    let scaled_network_difficulty = codec::scaled_target_difficulty(&network_target)?;
    let window_weight = scaled_network_difficulty
        .checked_mul(WINDOW_MULTIPLIER)
        .context("window weight overflows u128")?;
    let requested = u128::from(requested_window);
    ensure!(
        requested <= window_weight,
        "window of {requested_window} shares needs a scaled share difficulty below 1"
    );
    // `A / share_target` is the scaled difficulty of a target, so a target of
    // `A / S` has scaled difficulty `S`.
    let scale = codec::target_from_compact(0x207f_ffff)? * BigUint::from(1_000_000_u64);

    let mut solution = None;
    let first = ceil_div(window_weight, requested);
    // The achievable window lengths are dense at production sizes; a short
    // scan either side of the first estimate covers the rounding.
    for step in 0..1024u128 {
        for candidate in [first.checked_sub(step), first.checked_add(step)] {
            let Some(scaled) = candidate.filter(|value| *value > 0) else {
                continue;
            };
            if ceil_div(window_weight, scaled) != requested {
                continue;
            }
            let target = &scale / BigUint::from(scaled);
            if target < network_target {
                continue;
            }
            let Ok(difficulty) = codec::target_difficulty(&target) else {
                continue;
            };
            if let Some(found) = refine(difficulty, scaled, &network_target)? {
                solution = Some((found, scaled));
                break;
            }
        }
        if solution.is_some() {
            break;
        }
    }
    let (share_difficulty, scaled_share_difficulty) = solution.with_context(|| {
        format!(
            "no representable share difficulty gives a {requested_window}-share window at bits \
             {bits:08x}; pick a different window size"
        )
    })?;
    ensure!(
        share_difficulty != FORBIDDEN_DIFFICULTY,
        "the solved share difficulty is exactly the lab-only 1e-9 that check-config and \
         capacity-evidence refuse; pick a different window size"
    );
    let share_target = codec::difficulty_target(share_difficulty)?.max(network_target.clone());
    let computed_window = u64::try_from(ceil_div(window_weight, scaled_share_difficulty))?;
    ensure!(
        computed_window == requested_window,
        "solved window is {computed_window} shares, not the requested {requested_window}"
    );
    let two_256 = BigUint::from(1u8) << 256usize;
    let hashes_per_share = ratio(&two_256, &share_target);
    let hashes_per_block = ratio(&two_256, &network_target);
    Ok(WindowSolution {
        scaled_network_difficulty,
        window_weight,
        share_difficulty,
        share_target,
        scaled_share_difficulty,
        requested_window,
        computed_window,
        hashes_per_share,
        hashes_per_block,
    })
}

fn ratio(numerator: &BigUint, denominator: &BigUint) -> f64 {
    let shift = denominator.bits().saturating_sub(53);
    let n = (numerator >> shift as usize).to_f64().unwrap_or(f64::NAN);
    let d = (denominator >> shift as usize).to_f64().unwrap_or(f64::NAN);
    n / d
}

/// Nudge an `f64` difficulty until the server's own `difficulty_target` maps it
/// back onto the intended scaled weight. `difficulty_target` divides by the
/// exact binary rational of the `f64`, so the nearest double to the ideal ratio
/// is not always the one that lands on the target.
fn refine(seed: f64, scaled: u128, network_target: &BigUint) -> Result<Option<f64>> {
    if !seed.is_finite() || seed <= 0.0 {
        return Ok(None);
    }
    let mut candidates = vec![seed];
    let mut up = seed;
    let mut down = seed;
    for _ in 0..64 {
        up = up.next_up();
        down = down.next_down();
        candidates.push(up);
        candidates.push(down);
    }
    for candidate in candidates {
        if candidate <= 0.0 || !candidate.is_finite() || candidate == FORBIDDEN_DIFFICULTY {
            continue;
        }
        // Round-tripping through the string form is what the frontend actually
        // reads back out of its environment; a difficulty that does not survive
        // that round trip would configure a different target than the harness
        // solved for (EP-CONFIG).
        let rendered = format!("{candidate}");
        let Ok(parsed) = rendered.parse::<f64>() else {
            continue;
        };
        if parsed != candidate {
            continue;
        }
        let Ok(target) = codec::difficulty_target(candidate) else {
            continue;
        };
        if &target < network_target {
            continue;
        }
        if codec::scaled_target_difficulty(&target)? == scaled {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

/// A window of exactly `share_count` production-shaped shares, all carrying the
/// live scaled share difficulty.
#[derive(Clone, Debug)]
pub struct SeedPlan {
    share_count: u64,
    share_difficulty: u128,
    network_difficulty: u128,
    target_share_bytes: usize,
    miner_pad: usize,
}

/// Throughput of one [`SeedPlan::load`].
#[derive(Clone, Debug)]
pub struct SeedStats {
    pub rows: u64,
    pub seconds: f64,
    pub rows_per_second: f64,
    pub serialized_bytes: u64,
}

impl SeedPlan {
    pub fn new(
        share_count: u64,
        share_difficulty: u128,
        network_difficulty: u128,
        target_share_bytes: usize,
    ) -> Result<Self> {
        ensure!(share_count > 0, "seed share count must be positive");
        ensure!(
            share_difficulty > 0,
            "seed share difficulty must be positive"
        );
        ensure!(
            network_difficulty > 0,
            "seed network difficulty must be positive"
        );
        let mut plan = Self {
            share_count,
            share_difficulty,
            network_difficulty,
            target_share_bytes,
            miner_pad: 0,
        };
        let bare = serde_json::to_vec(&plan.share(1))?.len();
        ensure!(
            bare <= target_share_bytes,
            "unpadded seeded share is already {bare} bytes, over the {target_share_bytes}-byte \
             target; raise --seed-share-bytes"
        );
        plan.miner_pad = target_share_bytes - bare;
        Ok(plan)
    }

    pub fn share_count(&self) -> u64 {
        self.share_count
    }

    pub fn share_difficulty(&self) -> u128 {
        self.share_difficulty
    }

    pub fn target_share_bytes(&self) -> usize {
        self.target_share_bytes
    }

    fn miner_id(&self, index: u64) -> String {
        format!("m{}{}", index % RECIPIENTS, "x".repeat(self.miner_pad))
    }

    fn p2mr_program_hex(index: u64) -> String {
        format!("{:02x}", 0xaa + (index % RECIPIENTS)).repeat(32)
    }

    /// The non-ASCII character is inherited from the #258/#264 fixture: it
    /// keeps the UTF-8 byte length above the character length, as real worker
    /// names do. The prefix keeps seeded identifiers out of every run's
    /// live-share prefix.
    pub fn share_id(index: u64) -> String {
        format!(
            "{SEED_SHARE_ID_PREFIX}{}:é{:0width$}",
            index % RECIPIENTS,
            index,
            width = SHARE_ID_INDEX_WIDTH
        )
    }

    /// The share the seed writes at `share_seq == index` (1-based).
    pub fn share(&self, index: u64) -> AcceptedShare {
        AcceptedShare {
            share_seq: index,
            share_id: Self::share_id(index),
            miner_id: self.miner_id(index),
            order_key: ORDER_KEY.to_owned(),
            p2mr_program_hex: Self::p2mr_program_hex(index),
            share_difficulty: self.share_difficulty,
            network_difficulty: ROW_NETWORK_DIFFICULTY,
            template_height: TEMPLATE_HEIGHT,
            job_id: JOB_ID.to_owned(),
            job_issued_at_ms: JOB_ISSUED_AT_BASE_MS + i64::try_from(index).unwrap_or(i64::MAX),
            accepted_at_ms: JOB_ISSUED_AT_BASE_MS
                + ACCEPTED_AT_OFFSET_MS
                + i64::try_from(index).unwrap_or(i64::MAX),
            ntime: 1_700_000_000,
            credit_policy: None,
        }
    }

    /// Window length this seed produces on its own, before any live share.
    pub fn window_length(&self) -> u64 {
        let weight = self.network_difficulty * WINDOW_MULTIPLIER;
        u64::try_from(ceil_div(weight, self.share_difficulty)).unwrap_or(u64::MAX)
    }

    pub fn average_share_bytes(&self) -> Result<f64> {
        let probes = self.share_count.min(4_096);
        let mut total = 0u64;
        for index in 1..=probes {
            total += u64::try_from(serde_json::to_vec(&self.share(index))?.len())?;
        }
        Ok(total as f64 / probes as f64)
    }

    /// Batched server-side load, as in the #264 fixture: `Ledger::append` costs
    /// milliseconds per share and would need hours at production sizes.
    pub async fn load(&self, pool: &PgPool, writer_id: &str) -> Result<SeedStats> {
        let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
            .fetch_one(pool)
            .await?;
        ensure!(
            existing == 0,
            "the window seed needs an empty qbit_share_ledger, found {existing} rows"
        );
        let started = Instant::now();
        let mut first = 1u64;
        while first <= self.share_count {
            let last = (first + LOAD_BATCH_ROWS - 1).min(self.share_count);
            sqlx::query(
                "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,\
                 p2mr_program,share_difficulty,network_difficulty,template_height,job_id,\
                 job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) \
                 SELECT i,\
                 $15||(i%$3)::text||':é'||lpad(i::text,$4,'0'),\
                 'm'||(i%$3)::text||$5,\
                 $6,\
                 decode(repeat(lpad(to_hex(170+(i%$3)::int),2,'0'),32),'hex'),\
                 $7::text::numeric,$8::text::numeric,$9,$10,\
                 to_timestamp(($11+i)::double precision/1000),$12,\
                 to_timestamp(($13+i)::double precision/1000),\
                 NULL,true,$14,0 \
                 FROM generate_series($1::bigint,$2::bigint) AS g(i)",
            )
            .bind(i64::try_from(first)?)
            .bind(i64::try_from(last)?)
            .bind(i64::try_from(RECIPIENTS)?)
            .bind(i32::try_from(SHARE_ID_INDEX_WIDTH)?)
            .bind("x".repeat(self.miner_pad))
            .bind(ORDER_KEY)
            .bind(self.share_difficulty.to_string())
            .bind(ROW_NETWORK_DIFFICULTY.to_string())
            .bind(i64::try_from(TEMPLATE_HEIGHT)?)
            .bind(JOB_ID)
            .bind(JOB_ISSUED_AT_BASE_MS)
            .bind(1_700_000_000i64)
            .bind(JOB_ISSUED_AT_BASE_MS + ACCEPTED_AT_OFFSET_MS)
            .bind(writer_id)
            .bind(SEED_SHARE_ID_PREFIX)
            .execute(pool)
            .await
            .with_context(|| format!("loading seeded shares {first}..={last}"))?;
            sqlx::query(
                "INSERT INTO qbit_prism_share_hashes(header_hash,share_id) \
                 SELECT encode(sha256(convert_to(share_id,'UTF8')),'hex'),share_id \
                 FROM qbit_share_ledger WHERE share_seq BETWEEN $1 AND $2 \
                 ON CONFLICT DO NOTHING",
            )
            .bind(i64::try_from(first)?)
            .bind(i64::try_from(last)?)
            .execute(pool)
            .await?;
            first = last + 1;
        }
        sqlx::query("SELECT setval(pg_get_serial_sequence('qbit_share_ledger','share_seq'),$1)")
            .bind(i64::try_from(self.share_count)?)
            .execute(pool)
            .await?;
        let seconds = started.elapsed().as_secs_f64();
        let serialized_bytes =
            (self.average_share_bytes()? * self.share_count as f64).round() as u64;
        Ok(SeedStats {
            rows: self.share_count,
            seconds,
            rows_per_second: self.share_count as f64 / seconds.max(f64::MIN_POSITIVE),
            serialized_bytes,
        })
    }
}

/// Window length observed in the ledger: walk the accepted shares newest-first
/// and count how many it takes to reach the window weight.
pub async fn observed_window_length(pool: &PgPool, network_difficulty: u128) -> Result<u64> {
    let weight = network_difficulty
        .checked_mul(WINDOW_MULTIPLIER)
        .context("window weight overflows u128")?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT share_difficulty::text FROM qbit_share_ledger WHERE accepted \
         ORDER BY share_seq DESC LIMIT 2000000",
    )
    .fetch_all(pool)
    .await?;
    let mut remaining = weight;
    let mut count = 0u64;
    for (difficulty,) in rows {
        let difficulty: u128 = difficulty.parse()?;
        remaining = remaining.saturating_sub(difficulty);
        count += 1;
        if remaining == 0 {
            break;
        }
    }
    Ok(count)
}
