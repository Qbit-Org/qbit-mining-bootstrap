//! Migration 016's solver attribution for the blocks the pool found before
//! it (#672).
//!
//! 016 moves each block's solving share onto `qbit_pool_blocks`. For the
//! blocks already there, the solver is the latest accepted share whose ID
//! ends in the block's hash, found by one probe of
//! `qbit_share_ledger_accepted_block_suffix_idx` per block: the lookup the
//! dashboard queries made on every request before 016. As one UPDATE in
//! 016's file, those probes were a single statement under the migration
//! transaction's statement timeout, two random reads each, and on a cold
//! cache their cost grows with the ledger. On mainnet-shaped ledgers with
//! all 13,566 blocks, from a fresh restore on a quiet disk, the statement
//! took 6.7 s at 8.25M shares and 11.2 s at 16.5M. On a busy disk it took
//! about 30 s, and the 15 s default refused it (#582's measured rehearsal on
//! a saturated host).
//!
//! The migrator now makes the probes right after 016's file, still inside
//! the migration transaction, in statements of at most `BLOCKS` blocks taken
//! in `block_hash` order. The attribution, the locks and the atomicity are
//! the single statement's; only the statements are short, whatever the
//! cache. A block whose solving share is not found keeps NULL columns, as
//! before, and the keyset moves past it.
use super::*;
use std::time::{Duration, Instant};

/// Blocks per statement. A probe is two random reads: at the 2.2 ms one
/// took on a saturated disk, a statement of 128 takes under 0.3 s, and even
/// at 50 ms a probe it stays under half the default statement timeout.
const BLOCKS: i64 = 128;

/// One statement: the solvers of the next `$2` blocks without one after
/// `block_hash` `$1`, returning the last block it covered and how many it
/// attributed. The probe is the one 016's single statement made.
const BATCH: &str = "WITH batch AS (SELECT block_hash FROM qbit_pool_blocks WHERE solver_share_id IS NULL AND block_hash>$1 ORDER BY block_hash LIMIT $2), solved AS (UPDATE qbit_pool_blocks block SET solver_miner_id=solver.miner_id,solver_share_id=solver.share_id,solver_share_difficulty=solver.share_difficulty,solver_network_difficulty=solver.network_difficulty FROM (SELECT batch.block_hash,share.miner_id,share.share_id,share.share_difficulty,share.network_difficulty FROM batch CROSS JOIN LATERAL (SELECT share.miner_id,share.share_id,share.share_difficulty,share.network_difficulty FROM qbit_share_ledger share WHERE share.accepted AND length(share.share_id)>=65 AND lower(right(share.share_id,64))=batch.block_hash ORDER BY share.accepted_at DESC,share.share_seq DESC LIMIT 1) share) solver WHERE block.block_hash=solver.block_hash RETURNING 1) SELECT (SELECT max(block_hash) FROM batch),(SELECT count(*) FROM solved)";

/// Attribute every block without a solver, inside the migration transaction
/// that applied 016's file.
pub(super) async fn attribute(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let started = Instant::now();
    let mut after = String::new();
    let mut statements: u32 = 0;
    let mut attributed: i64 = 0;
    let mut slowest = Duration::ZERO;
    loop {
        let statement = Instant::now();
        let (last, solved): (Option<String>, i64) = sqlx::query_as(BATCH)
            .bind(&after)
            .bind(BLOCKS)
            .fetch_one(&mut **tx)
            .await
            .with_context(|| {
                format!("migration 16: attributing block solvers after block_hash {after:?}")
            })?;
        slowest = slowest.max(statement.elapsed());
        let Some(last) = last else { break };
        statements += 1;
        attributed += solved;
        after = last;
    }
    tracing::info!(
        version = 16,
        attributed,
        statements,
        slowest_ms = u64::try_from(slowest.as_millis()).unwrap_or(u64::MAX),
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "block solvers attributed in bounded statements"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_makes_the_probe_016s_single_statement_made() {
        // The lookup 016's file ran for every block before #672, and the
        // dashboard queries before 016: the latest accepted share whose ID
        // ends in the block's hash.
        let probe = "SELECT share.miner_id,share.share_id,share.share_difficulty,share.network_difficulty FROM qbit_share_ledger share WHERE share.accepted AND length(share.share_id)>=65 AND lower(right(share.share_id,64))=batch.block_hash ORDER BY share.accepted_at DESC,share.share_seq DESC LIMIT 1";
        assert!(BATCH.contains(probe), "{BATCH}");
        assert!(BATCH.contains(
            "WHERE solver_share_id IS NULL AND block_hash>$1 ORDER BY block_hash LIMIT $2"
        ));
    }
}
