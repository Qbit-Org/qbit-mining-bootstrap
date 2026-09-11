//! Deterministic, production-shaped payout-window fixture for the JSONB
//! ceiling gate (issue #264).
//!
//! The row shape follows the #258 measurement fixture (`share()` in
//! `tests/prism_postgres_candidate_gate.py` and `tests/perf/candidate_storage.py`
//! at commit `f4aff1b40893f7886096c09989c50aebc0905e60`) mapped onto the native
//! [`AcceptedShare`] and the `qbit_share_ledger` row.
//!
//! Two deliberate deviations from that Python fixture are documented inline
//! below: the share difficulty is scaled down so that exactly `n` shares fill
//! the payout window, and `share_id` carries a fixed-width index. Both are
//! compensated by `miner_id` padding so that the serialized share stays at
//! [`TARGET_SHARE_BYTES`] regardless of `n`.
//!
//! Everything here is a pure function of the share index: no wall clock and no
//! OS randomness, so the same parameters always produce byte-identical shares.

use anyhow::{ensure, Context, Result};
use qbit_prism::AcceptedShare;
use sqlx::{PgPool, Row};
use std::time::Instant;

/// `Ledger::snapshot` walks the ledger backwards until the summed
/// `share_difficulty` reaches `WINDOW_MULTIPLIER x network_difficulty`
/// (`crates/qbit-prism-server/src/ledger.rs`, `snapshot`).
pub const WINDOW_MULTIPLIER: u128 = 8;

/// The in-process fake node serves the stock `207fffff` template bits, which
/// `codec::scaled_target_difficulty` maps to this network difficulty. The
/// coordinator feeds it straight into `Ledger::snapshot`.
pub const FAKE_NODE_NETWORK_DIFFICULTY: u128 = 1_000_000;

/// Total window weight the snapshot walk has to accumulate.
pub const WINDOW_WEIGHT: u128 = FAKE_NODE_NETWORK_DIFFICULTY * WINDOW_MULTIPLIER;

/// Serialized size of one native `AcceptedShare`, in bytes, at share index 1.
/// The measured production figure behind the #264 readiness review is ~581 B
/// of JSON text per share (372,257 shares, ~232 MB canonical).
pub const TARGET_SHARE_BYTES: usize = 581;

/// Acceptance band for the measured average, asserted by the fixture's own
/// test. The average drifts a few tenths of a byte above `TARGET_SHARE_BYTES`
/// because `share_seq` is a bare JSON number and widens from 1 to 6 digits.
pub const SHARE_BYTES_BAND: (f64, f64) = (560.0, 620.0);

/// 2023-11-14T22:13:20Z. Far enough in the past that every fixture share
/// satisfies the snapshot anchor (`max(ledger clock, now)`) at any run date.
const JOB_ISSUED_AT_BASE_MS: i64 = 1_700_000_000_000;
/// #258 puts `accepted_at_ms` exactly one millisecond after `job_issued_at_ms`.
const ACCEPTED_AT_OFFSET_MS: i64 = 1;
/// Distinct payout recipients, as in #258 (`miner_id = f"m{index % 5}"`).
const RECIPIENTS: u64 = 5;
/// #258 records a small `network_difficulty` on the row itself; it is display
/// metadata and is unrelated to the window weight above.
const ROW_NETWORK_DIFFICULTY: u128 = 1000;
/// The fake node's tip height. #258 used a placeholder `9`; the byte width
/// difference is absorbed by the `miner_id` padding.
const TEMPLATE_HEIGHT: u64 = 100;
const JOB_ID: &str = "job-a";
/// #258 gives every share the same `order_key`, so the payout universe stays
/// at `RECIPIENTS` accounts and the bundle size is dominated by the window.
const ORDER_KEY: &str = "k";
/// Zero-padded width of the share index inside `share_id`. #258 uses the bare
/// index; a fixed width keeps the serialized share size independent of `n`,
/// which is what makes the gate's two-point linear fit trustworthy.
const SHARE_ID_INDEX_WIDTH: usize = 12;
/// Rows per server-side `INSERT ... SELECT`. One statement per batch keeps the
/// load to a handful of round trips even at 400,000 shares.
const LOAD_BATCH_ROWS: u64 = 50_000;

/// A window of exactly `share_count` production-shaped shares.
#[derive(Clone, Debug)]
pub struct WindowPlan {
    share_count: u64,
    share_difficulty: u128,
    miner_pad: usize,
}

/// Throughput of one [`WindowPlan::load`].
#[derive(Clone, Debug)]
pub struct LoadStats {
    pub rows: u64,
    pub seconds: f64,
    pub rows_per_second: f64,
    pub serialized_bytes: u64,
}

impl WindowPlan {
    /// `share_count` must divide [`WINDOW_WEIGHT`] exactly so that the snapshot
    /// walk stops on exactly the requested number of shares.
    ///
    /// The alternative was to keep #258's 21-digit `share_difficulty` and scale
    /// the fake node's `bits` until `n` such shares fill the window. That needs
    /// a network difficulty of ~5e24 at 400,000 shares, which the 24-bit
    /// compact-bits mantissa cannot express exactly, so the window length would
    /// no longer be an exact function of `n`. Padding `miner_id` instead keeps
    /// both the window length and the bytes per share exact.
    pub fn new(share_count: u64) -> Result<Self> {
        ensure!(share_count > 0, "share count must be positive");
        let weight = u128::from(share_count);
        ensure!(
            weight <= WINDOW_WEIGHT,
            "share count {share_count} exceeds the window weight {WINDOW_WEIGHT}; \
             each share needs a difficulty of at least 1"
        );
        ensure!(
            WINDOW_WEIGHT.is_multiple_of(weight),
            "share count {share_count} must divide the window weight {WINDOW_WEIGHT} exactly, \
             otherwise the snapshot window would not contain exactly {share_count} shares"
        );
        let share_difficulty = WINDOW_WEIGHT / weight;
        let mut plan = Self {
            share_count,
            share_difficulty,
            miner_pad: 0,
        };
        // Pad `miner_id` (as #258 does with `row["miner_id"] += "x" * 240`)
        // until the share at index 1 serializes to exactly TARGET_SHARE_BYTES.
        // The probe uses a fixed index so the padding, and therefore the bytes
        // per share, are identical at every share count.
        let bare = serde_json::to_vec(&plan.share(1))?.len();
        ensure!(
            bare < TARGET_SHARE_BYTES,
            "unpadded share is already {bare} bytes, over the {TARGET_SHARE_BYTES}-byte target"
        );
        plan.miner_pad = TARGET_SHARE_BYTES - bare;
        Ok(plan)
    }

    pub fn share_count(&self) -> u64 {
        self.share_count
    }

    pub fn share_difficulty(&self) -> u128 {
        self.share_difficulty
    }

    /// The `network_difficulty` argument for `Ledger::snapshot`, chosen so the
    /// window is exactly `share_count` shares wide. Equal to the fake node's
    /// difficulty by construction, so the coordinator's own refresh agrees.
    pub fn window_network_difficulty(&self) -> u128 {
        FAKE_NODE_NETWORK_DIFFICULTY
    }

    fn miner_id(&self, index: u64) -> String {
        format!("m{}{}", index % RECIPIENTS, "x".repeat(self.miner_pad))
    }

    fn p2mr_program_hex(index: u64) -> String {
        // Five distinct 32-byte programs, one per recipient, so the payout
        // builder sees five real accounts. #258 reuses "ab" * 32 everywhere;
        // the byte width is identical.
        format!("{:02x}", 0xaa + (index % RECIPIENTS)).repeat(32)
    }

    fn share_id(index: u64) -> String {
        // The non-ASCII character is deliberate: #258 uses it to keep the
        // UTF-8 byte length above the character length, as real worker names do.
        format!(
            "miner-{}:é{:0width$}",
            index % RECIPIENTS,
            index,
            width = SHARE_ID_INDEX_WIDTH
        )
    }

    /// The share the fixture writes at `share_seq == index` (1-based).
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

    /// Exact average serialized size of the whole window, in bytes per share.
    pub fn average_share_bytes(&self) -> Result<f64> {
        let mut total = 0u64;
        for index in 1..=self.share_count {
            total += u64::try_from(serde_json::to_vec(&self.share(index))?.len())?;
        }
        Ok(total as f64 / self.share_count as f64)
    }

    /// Load the whole window with server-side batched inserts. `Ledger::append`
    /// costs milliseconds per share and would need hours at 400,000; this runs
    /// `LOAD_BATCH_ROWS` rows per statement with no per-row round trip.
    pub async fn load(&self, pool: &PgPool, writer_id: &str) -> Result<LoadStats> {
        let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM qbit_share_ledger")
            .fetch_one(pool)
            .await?;
        ensure!(
            existing == 0,
            "window fixture needs an empty qbit_share_ledger, found {existing} rows"
        );
        let started = Instant::now();
        let mut first = 1u64;
        while first <= self.share_count {
            let last = (first + LOAD_BATCH_ROWS - 1).min(self.share_count);
            // No INSERT trigger guards qbit_share_ledger; the trigger added by
            // migration 002 only rejects UPDATE, DELETE and TRUNCATE.
            sqlx::query(
                "INSERT INTO qbit_share_ledger(share_seq,share_id,miner_id,payout_order_key,\
                 p2mr_program,share_difficulty,network_difficulty,template_height,job_id,\
                 job_issued_at,ntime,accepted_at,credit_policy,accepted,writer_id,writer_epoch) \
                 SELECT i,\
                 'miner-'||(i%$3)::text||':é'||lpad(i::text,$4,'0'),\
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
            .execute(pool)
            .await
            .with_context(|| format!("loading shares {first}..={last}"))?;
            // The same backfill migration 002 runs for pre-existing history.
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
        Ok(LoadStats {
            rows: self.share_count,
            seconds,
            rows_per_second: self.share_count as f64 / seconds.max(f64::MIN_POSITIVE),
            serialized_bytes,
        })
    }

    /// Round-trip check: every loaded row must decode back to [`Self::share`].
    /// `land_candidate` re-reads the window from SQL and demands exact equality
    /// with the bundle, so a mismatch here would fail the landing phase later.
    pub async fn verify_round_trip(&self, pool: &PgPool, indexes: &[u64]) -> Result<()> {
        for &index in indexes {
            let row = sqlx::query(
                "SELECT share_seq,share_id,miner_id,payout_order_key,\
                 encode(p2mr_program,'hex') AS program,share_difficulty::text AS difficulty,\
                 network_difficulty::text AS network_difficulty,template_height,job_id,\
                 (extract(epoch FROM job_issued_at)*1000)::bigint AS job_issued_at_ms,\
                 (extract(epoch FROM accepted_at)*1000)::bigint AS accepted_at_ms,\
                 ntime,credit_policy FROM qbit_share_ledger WHERE share_seq=$1",
            )
            .bind(i64::try_from(index)?)
            .fetch_one(pool)
            .await
            .with_context(|| format!("share {index} is missing"))?;
            let actual = AcceptedShare {
                share_seq: u64::try_from(row.try_get::<i64, _>("share_seq")?)?,
                share_id: row.try_get("share_id")?,
                miner_id: row.try_get("miner_id")?,
                order_key: row.try_get("payout_order_key")?,
                p2mr_program_hex: row.try_get("program")?,
                share_difficulty: row.try_get::<String, _>("difficulty")?.parse()?,
                network_difficulty: row.try_get::<String, _>("network_difficulty")?.parse()?,
                template_height: u64::try_from(row.try_get::<i64, _>("template_height")?)?,
                job_id: row.try_get("job_id")?,
                job_issued_at_ms: row.try_get("job_issued_at_ms")?,
                accepted_at_ms: row.try_get("accepted_at_ms")?,
                ntime: u32::try_from(row.try_get::<i64, _>("ntime")?)?,
                credit_policy: row.try_get("credit_policy")?,
            };
            ensure!(
                actual == self.share(index),
                "share {index} did not round-trip: stored {actual:?} expected {:?}",
                self.share(index)
            );
        }
        Ok(())
    }
}

#[test]
fn fixture_shares_are_production_sized_and_deterministic() -> Result<()> {
    // A few thousand shares, as required by #264: assert the average bytes per
    // share of the native serialized form and print the exact figure.
    let plan = WindowPlan::new(4_000)?;
    let average = plan.average_share_bytes()?;
    println!(
        "window_fixture: n=4000 share_difficulty={} miner_pad={} average_share_bytes={average:.3}",
        plan.share_difficulty(),
        plan.miner_pad
    );
    assert!(
        average >= SHARE_BYTES_BAND.0 && average <= SHARE_BYTES_BAND.1,
        "average {average:.3} B/share is outside the {:?} band",
        SHARE_BYTES_BAND
    );
    assert_eq!(
        serde_json::to_vec(&plan.share(1))?.len(),
        TARGET_SHARE_BYTES,
        "share 1 must land exactly on the byte target"
    );
    assert_eq!(plan.share(7), plan.share(7), "shares must be deterministic");
    assert_eq!(
        serde_json::to_vec(&WindowPlan::new(4_000)?.share(9))?,
        serde_json::to_vec(&plan.share(9))?,
        "two plans with the same parameters must produce byte-identical shares"
    );
    Ok(())
}

#[test]
fn fixture_share_size_is_independent_of_the_window_size() -> Result<()> {
    // The gate fits size(n) = a + b*n through two reduced sizes and projects to
    // 400,000. That is only sound if bytes per share does not move with n.
    let mut sizes = Vec::new();
    for count in [5_000u64, 20_000, 50_000, 400_000] {
        let plan = WindowPlan::new(count)?;
        let one = serde_json::to_vec(&plan.share(1))?.len();
        sizes.push((count, one, plan.share_difficulty(), plan.miner_pad));
    }
    println!("window_fixture: (n, bytes(share 1), share_difficulty, miner_pad) = {sizes:?}");
    assert!(
        sizes.iter().all(|entry| entry.1 == TARGET_SHARE_BYTES),
        "share 1 must be {TARGET_SHARE_BYTES} B at every window size: {sizes:?}"
    );
    Ok(())
}

#[test]
fn fixture_rejects_window_sizes_it_cannot_fill_exactly() {
    for bad in [0u64, 3, 7, 300_000, 8_000_001] {
        assert!(
            WindowPlan::new(bad).is_err(),
            "share count {bad} cannot fill the window exactly and must be rejected"
        );
    }
    for good in [1u64, 5_000, 20_000, 50_000, 100_000, 200_000, 400_000] {
        assert!(
            WindowPlan::new(good).is_ok(),
            "share count {good} divides the window weight and must be accepted"
        );
    }
}
