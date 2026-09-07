-- Parameterized shared-database read model.

SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block.block_hash,
    'block_height', block.block_height,
    'coinbase_txid', block.coinbase_txid,
    'payout_manifest_sha256', block.payout_manifest_sha256,
    'recipient_id', payout.miner_id,
    'order_key', payout.payout_order_key,
    'p2mr_program_hex', encode(payout.p2mr_program, 'hex'),
    'onchain_amount_sats', payout.onchain_amount_sats,
    'carry_forward_balance_sats', payout.carry_forward_balance_sats::text,
    'action', payout.action,
    'maturity_state', payout.maturity_state,
    'created_at', payout.created_at::text
) ORDER BY payout.block_height DESC, payout.payout_entry_seq DESC), '[]'::json)
FROM (
    SELECT *
    FROM qbit_pool_payout_entries
    WHERE miner_id = $1
    ORDER BY block_height DESC, payout_entry_seq DESC
    LIMIT $2
) payout
JOIN qbit_pool_blocks block
  ON block.block_hash = payout.block_hash;
