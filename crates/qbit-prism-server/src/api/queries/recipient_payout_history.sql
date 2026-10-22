-- Parameterized shared-database read model.

WITH recent AS (
    SELECT
        payout.*,
        block.block_height AS parent_block_height,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_pool_payout_entries payout
    JOIN qbit_pool_blocks block
      ON block.block_hash = payout.block_hash
    WHERE payout.miner_id = $1
      AND payout.maturity_state <> 'reversed'
      AND block.chain_state = 'confirmed'
      AND block.maturity_state <> 'reversed'
    ORDER BY payout.block_height DESC, payout.payout_entry_seq DESC
    LIMIT $2
)
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', payout.block_hash,
    'block_height', payout.parent_block_height,
    'coinbase_txid', payout.coinbase_txid,
    'payout_manifest_sha256', payout.payout_manifest_sha256,
    'recipient_id', payout.miner_id,
    'order_key', payout.payout_order_key,
    'p2mr_program_hex', encode(payout.p2mr_program, 'hex'),
    'onchain_amount_sats', payout.onchain_amount_sats,
    'carry_forward_balance_sats', payout.carry_forward_balance_sats::text,
    'action', payout.action,
    'maturity_state', payout.maturity_state,
    'created_at', payout.created_at::text
) ORDER BY payout.block_height DESC, payout.payout_entry_seq DESC), '[]'::json)
FROM recent payout;
