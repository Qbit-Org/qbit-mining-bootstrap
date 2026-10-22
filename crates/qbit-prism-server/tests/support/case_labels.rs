//! #708's shape on union's ledger: one payout program paid under two labels
//! that differ only in case, the address as its miner typed it and in
//! lowercase. 2.x keeps one carry chain per program, whatever label a block
//! paid it under, so each row's prior is the balance the row before it left,
//! under either label. The labels interleave over heights, and both are paid
//! on chain.
use anyhow::{ensure, Result};
use sqlx::PgPool;

pub const UPPER: &str = "QB1CASE708LABEL";
pub const LOWER: &str = "qb1case708label";

/// `(height, label, gross, onchain)` in chain order, one row per block.
pub const ROWS: [(i64, &str, i64, i64); 7] = [
    (41, UPPER, 400, 0),
    (42, UPPER, 300, 0),
    (43, LOWER, 500, 1_200),
    (44, UPPER, 250, 0),
    (45, LOWER, 100, 0),
    (46, UPPER, 900, 1_250),
    (47, UPPER, 60, 0),
];

/// The program's balance after the last row.
pub const BALANCE: i64 = 60;

/// The rows 001's and 011's rule reports, which sums each label apart: every
/// row but the first two uppercase ones, whose label had seen the whole
/// chain so far.
pub const LABEL_RULE_FINDINGS: [i64; 5] = [43, 44, 45, 46, 47];

/// The last row under each label: shifting one breaks only that row.
pub const LAST_LOWER: i64 = 45;
pub const LAST_UPPER: i64 = 47;

pub const ALL_FIELDS: &str = "prior_balance,candidate_balance,carry_forward_balance";

pub fn program() -> String {
    "c5".repeat(32)
}

pub fn block_hash(height: i64) -> String {
    format!("{height:02x}").repeat(32)
}

pub fn label(height: i64) -> &'static str {
    ROWS.iter()
        .find(|row| row.0 == height)
        .map(|row| row.1)
        .expect("a fixture height")
}

/// Confirmed, mature, unmarked blocks and the carry rows 2.x wrote for them,
/// on a 2.x or a native schema.
pub async fn seed(pool: &PgPool) -> Result<()> {
    let mut balance = 0;
    for (height, label, gross, onchain) in ROWS {
        let hash = block_hash(height);
        sqlx::query("INSERT INTO qbit_pool_blocks(block_hash,block_height,parent_hash,coinbase_txid,payout_manifest_sha256,chain_state,maturity_state,matured_at) VALUES($1,$2,repeat('00',32),repeat('ab',32),repeat('ac',32),'confirmed','mature',clock_timestamp())")
            .bind(&hash).bind(height).execute(pool).await?;
        let candidate = balance + gross;
        sqlx::query("INSERT INTO qbit_payout_carry_forward(block_height,block_hash,miner_id,payout_order_key,p2mr_program,gross_amount_sats,prior_balance_sats,candidate_balance_sats,onchain_amount_sats,carry_forward_balance_sats,action,maturity_state) VALUES($1,$2,$3,$3,decode($4,'hex'),$5,$6,$7,$8,$9,$10,'mature')")
            .bind(height).bind(&hash).bind(label).bind(program())
            .bind(gross).bind(balance).bind(candidate).bind(onchain).bind(candidate - onchain)
            .bind(if onchain > 0 { "onchain" } else { "accrued" })
            .execute(pool).await?;
        balance = candidate - onchain;
    }
    ensure!(balance == BALANCE, "the fixture ends at {balance}");
    Ok(())
}

/// The validator's findings on the program as `(height, label, reason)`, in
/// chain order.
pub async fn findings(pool: &PgPool) -> Result<Vec<(i64, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT block_height,miner_id,mismatch_reason FROM qbit_carry_forward_integrity_mismatches() WHERE p2mr_program=decode($1,'hex') ORDER BY block_height,carry_forward_seq",
    )
    .bind(program())
    .fetch_all(pool)
    .await?)
}

/// What 001's and 011's rule reports on the program.
pub fn label_rule_findings() -> Vec<(i64, String, String)> {
    LABEL_RULE_FINDINGS
        .iter()
        .map(|&height| (height, label(height).to_owned(), ALL_FIELDS.to_owned()))
        .collect()
}

/// `(mismatch_count, current_drift_count)` of the integrity report.
pub async fn report(pool: &PgPool) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (r->>'mismatch_count')::bigint,(r->>'current_drift_count')::bigint FROM qbit_carry_forward_integrity_report() r",
    )
    .fetch_one(pool)
    .await?)
}

/// Shift the stored prior, candidate and carry of the row at `height` by
/// `by` sats: a break in the chain at that row alone, which leaves every
/// delta, and so every balance, as it was.
pub async fn shift(pool: &PgPool, height: i64, by: i64) -> Result<()> {
    sqlx::query("UPDATE qbit_payout_carry_forward SET prior_balance_sats=prior_balance_sats+$2,candidate_balance_sats=candidate_balance_sats+$2,carry_forward_balance_sats=carry_forward_balance_sats+$2 WHERE block_hash=$1")
        .bind(block_hash(height)).bind(by).execute(pool).await?;
    Ok(())
}

/// `(program, balance)` from `qbit_current_carry_forward_balances()`.
pub type Current = Vec<(String, String)>;
/// `(label, order key, program, balance, active rows)` from the per-label
/// summary that function sums.
pub type Summary = Vec<(String, String, String, String, i64)>;

/// Every current balance by program, and the per-label summary rows.
pub async fn balances(pool: &PgPool) -> Result<(Current, Summary)> {
    let current = sqlx::query_as(
        "SELECT encode(p2mr_program,'hex'),balance_sats::text FROM qbit_current_carry_forward_balances() ORDER BY p2mr_program",
    )
    .fetch_all(pool)
    .await?;
    let summary = sqlx::query_as(
        "SELECT miner_id,payout_order_key,encode(p2mr_program,'hex'),balance_sats::text,active_row_count FROM qbit_payout_carry_forward_current ORDER BY p2mr_program,miner_id COLLATE \"C\",payout_order_key COLLATE \"C\"",
    )
    .fetch_all(pool)
    .await?;
    Ok((current, summary))
}
