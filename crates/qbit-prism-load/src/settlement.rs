//! How each of the run's own blocks settled its payouts (#548).
//!
//! For every block the run's node accepted, the side report's `settlement`
//! block counts the miner recipients the server's landing paid directly in
//! the coinbase, paid through CTV fanout chunks, and carried forward. The
//! counts come from the rows the landing itself wrote, in the one
//! transaction that records the block (`ledger/blocks.rs`):
//!
//! - `qbit_payout_carry_forward`: one row per miner account the payout
//!   policy settled, `onchain` or `accrued`. The server writes miner
//!   accounts only, so the pool fee is never counted as a recipient.
//! - `qbit_ctv_fanout_artifacts`: one row per fanout chunk, whose manifest
//!   lists the chunk's outputs.
//!
//! An `onchain` account named by one of the block's fanout outputs was paid
//! through fanout, every other `onchain` account directly, and every
//! `accrued` one was carried. Because the landing is one transaction, a
//! block whose `qbit_pool_blocks` row exists has all of its fanout rows: a
//! landed block with none settled every recipient directly, which is a
//! measured 0, not a missing one. A block the node accepted but the server
//! never landed is listed as unmeasured, and a failed read leaves every
//! count `null` with the error (EP-OBSERVABILITY).

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::BTreeMap;

/// How one landed block settled.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BlockSettlement {
    pub block_hash: String,
    pub block_height: i64,
    /// `qbit_pool_blocks.chain_state` when it was read.
    pub chain_state: String,
    /// `qbit_ctv_fanout_sets.settlement_mode`, or `direct_coinbase` for a
    /// landed block with no fanout set.
    pub settlement_mode: String,
    pub direct_recipients: u64,
    pub fanout_recipients: u64,
    pub carried_recipients: u64,
    pub fanout_chunks: u64,
    /// Every output of the block's fanout transactions. More than
    /// `fanout_recipients` only when a non-miner account, the pool fee, was
    /// itself routed through a chunk.
    pub fanout_outputs: u64,
    /// Fanout chunks whose manifest lists no output the read can see. Any
    /// makes the block unmeasured: those chunks' recipients would otherwise
    /// count as direct ones.
    pub unreadable_chunks: u64,
}

const QUERY: &str = "\
WITH fanout AS (
    SELECT a.block_hash, o->>'recipient_id' AS miner_id, o->>'order_key' AS order_key,
           decode(o->>'p2mr_program_hex', 'hex') AS p2mr_program
    FROM qbit_ctv_fanout_artifacts a
    CROSS JOIN LATERAL jsonb_array_elements(a.manifest->'precommitment'->'outputs') o
    WHERE a.block_hash = ANY($1)
), accounts AS (
    SELECT c.block_hash, c.action,
           EXISTS (SELECT 1 FROM fanout f
                   WHERE f.block_hash = c.block_hash AND f.miner_id = c.miner_id
                     AND f.order_key = c.payout_order_key
                     AND f.p2mr_program = c.p2mr_program) AS fanned
    FROM qbit_payout_carry_forward c
    WHERE c.block_hash = ANY($1)
)
SELECT b.block_hash, b.block_height, b.chain_state,
       COALESCE(s.settlement_mode, 'direct_coinbase') AS settlement_mode,
       (SELECT count(*) FROM accounts c WHERE c.block_hash = b.block_hash
            AND c.action = 'onchain' AND NOT c.fanned) AS direct_recipients,
       (SELECT count(*) FROM accounts c WHERE c.block_hash = b.block_hash
            AND c.action = 'onchain' AND c.fanned) AS fanout_recipients,
       (SELECT count(*) FROM accounts c WHERE c.block_hash = b.block_hash
            AND c.action = 'accrued') AS carried_recipients,
       (SELECT count(*) FROM qbit_ctv_fanout_artifacts a
            WHERE a.block_hash = b.block_hash) AS fanout_chunks,
       (SELECT count(*) FROM fanout f WHERE f.block_hash = b.block_hash) AS fanout_outputs,
       (SELECT count(*) FROM qbit_ctv_fanout_artifacts a
            WHERE a.block_hash = b.block_hash
              AND COALESCE(jsonb_array_length(
                      CASE WHEN jsonb_typeof(a.manifest->'precommitment'->'outputs') = 'array'
                           THEN a.manifest->'precommitment'->'outputs' END), 0) = 0)
           AS unreadable_chunks
FROM qbit_pool_blocks b
LEFT JOIN qbit_ctv_fanout_sets s ON s.block_hash = b.block_hash
WHERE b.block_hash = ANY($1)
ORDER BY b.block_height, b.block_hash";

/// Read how each of `block_hashes` settled. A hash the server never landed
/// has no row and is absent from the result.
pub async fn read(pool: &PgPool, block_hashes: &[String]) -> Result<Vec<BlockSettlement>> {
    if block_hashes.is_empty() {
        return Ok(Vec::new());
    }
    type Row = (String, i64, String, String, i64, i64, i64, i64, i64, i64);
    let rows: Vec<Row> = sqlx::query_as(QUERY)
        .bind(block_hashes)
        .fetch_all(pool)
        .await
        .context("reading the run's landed blocks' settlement")?;
    rows.into_iter()
        .map(
            |(
                hash,
                height,
                chain_state,
                mode,
                direct,
                fanout,
                carried,
                chunks,
                outputs,
                unreadable,
            )| {
                let count = |value: i64| u64::try_from(value).context("negative count");
                Ok(BlockSettlement {
                    block_hash: hash,
                    block_height: height,
                    chain_state,
                    settlement_mode: mode,
                    direct_recipients: count(direct)?,
                    fanout_recipients: count(fanout)?,
                    carried_recipients: count(carried)?,
                    fanout_chunks: count(chunks)?,
                    fanout_outputs: count(outputs)?,
                    unreadable_chunks: count(unreadable)?,
                })
            },
        )
        .collect()
}

pub const SOURCE: &str = "the server's own landing rows for each block this run's node \
                          accepted: qbit_payout_carry_forward (miner accounts, onchain or \
                          accrued) and the outputs of qbit_ctv_fanout_artifacts' manifests, \
                          read after the frontends stopped";

pub const DEFINITIONS: &str = "per landed block, miner recipients only (the pool fee is not a \
                               recipient): fanout_recipients are the onchain accounts one of \
                               the block's fanout outputs pays, direct_recipients the other \
                               onchain accounts, carried_recipients the accrued accounts, whose \
                               balance carries to a later block. A landed block with no fanout \
                               chunk measured fanout_recipients 0. An accepted block the server \
                               recorded no landing for, or with any fanout chunk whose \
                               manifest lists no readable output, is in unmeasured_blocks and \
                               in no count. A landed block counts whatever its chain_state \
                               (blocks_by_chain_state): its settlement ran even if the block \
                               was later reorged";

/// How many measured blocks are in each `chain_state`: a block that landed and
/// was later reorged still settled, and is counted, so the totals say how
/// many of them there are.
fn blocks_by_chain_state(blocks: &[BlockSettlement]) -> BTreeMap<&str, u64> {
    let mut states = BTreeMap::new();
    for block in blocks {
        *states.entry(block.chain_state.as_str()).or_default() += 1;
    }
    states
}

/// The side report's `settlement` block. `accepted` is every block hash the
/// run's node accepted, in order; `read` is [`read`]'s result for them.
pub fn report(
    ctv_settlement: bool,
    accepted: &[String],
    read: Result<Vec<BlockSettlement>>,
) -> Value {
    let blocks = match read {
        Ok(blocks) => blocks,
        Err(error) => {
            return json!({
                "ctv_settlement": ctv_settlement,
                "source": SOURCE,
                "definitions": DEFINITIONS,
                "accepted_blocks": accepted.len(),
                "measured_blocks": Value::Null,
                "unmeasured_blocks": Value::Null,
                "totals": Value::Null,
                "totals_unavailable_reason": format!(
                    "the settlement read failed: {}",
                    crate::frontend::redact_secrets_in_text(&format!("{error:#}"))
                ),
                "blocks": Value::Null,
            });
        }
    };
    // A chunk whose manifest lists no output the query can read would turn
    // its recipients into direct ones: a block with any such chunk is not
    // measured, rather than a silent 0 (EP-OBSERVABILITY).
    let (blocks, unreadable): (Vec<BlockSettlement>, Vec<BlockSettlement>) = blocks
        .into_iter()
        .partition(|block| block.unreadable_chunks == 0);
    let mut unmeasured: Vec<Value> = accepted
        .iter()
        .filter(|hash| {
            !blocks
                .iter()
                .chain(&unreadable)
                .any(|block| &block.block_hash == *hash)
        })
        .map(|hash| {
            json!({
                "block_hash": hash,
                "reason": "the node accepted it and the server recorded no landing for it",
            })
        })
        .collect();
    unmeasured.extend(unreadable.iter().map(|block| {
        json!({
            "block_hash": block.block_hash,
            "reason": format!(
                "{} of its {} fanout chunk(s) list no output at \
                 manifest.precommitment.outputs, so its fanout recipients cannot be told \
                 from its direct ones",
                block.unreadable_chunks, block.fanout_chunks
            ),
        })
    }));
    let (totals, reason) = if blocks.is_empty() {
        let reason = if accepted.is_empty() {
            "no block of the run's own was accepted, so no payout settled"
        } else {
            "no accepted block's landing could be measured; see unmeasured_blocks"
        };
        (Value::Null, Some(reason))
    } else {
        let sum = |field: fn(&BlockSettlement) -> u64| blocks.iter().map(field).sum::<u64>();
        (
            json!({
                "direct_recipients": sum(|block| block.direct_recipients),
                "fanout_recipients": sum(|block| block.fanout_recipients),
                "carried_recipients": sum(|block| block.carried_recipients),
                "fanout_chunks": sum(|block| block.fanout_chunks),
                "blocks_with_fanout": blocks.iter().filter(|block| block.fanout_chunks > 0).count(),
                "blocks_by_chain_state": blocks_by_chain_state(&blocks),
                "covers": "measured_blocks only",
            }),
            None,
        )
    };
    json!({
        "ctv_settlement": ctv_settlement,
        "source": SOURCE,
        "definitions": DEFINITIONS,
        "accepted_blocks": accepted.len(),
        "measured_blocks": blocks.len(),
        "unmeasured_blocks": unmeasured,
        "totals": totals,
        "totals_unavailable_reason": reason,
        "blocks": blocks,
    })
}
