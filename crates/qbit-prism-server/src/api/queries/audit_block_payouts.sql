-- Parameterized shared-database read model.

SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'block_height', block_height,
    'coinbase_txid', coinbase_txid,
    'payout_manifest_sha256', payout_manifest_sha256,
    'chain_state', chain_state,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'onchain_amount_sats', onchain_amount_sats,
    'carry_forward_balance_sats', carry_forward_balance_sats::text,
    'action', action,
    'maturity_state', maturity_state
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_audit_block_payouts($1);
