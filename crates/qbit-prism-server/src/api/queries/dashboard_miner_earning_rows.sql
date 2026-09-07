-- Parameterized shared-database read model.

WITH filtered AS (
    SELECT
        carry.carry_forward_seq,
        carry.block_hash,
        carry.block_height,
        carry.miner_id,
        carry.payout_order_key,
        carry.p2mr_program,
        carry.gross_amount_sats,
        carry.onchain_amount_sats,
        carry.settlement_fee_sats,
        carry.carry_forward_balance_sats,
        carry.action,
        carry.maturity_state,
        carry.created_at,
        block.found_at,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_payout_carry_forward carry
    JOIN qbit_pool_blocks block
      ON block.block_hash = carry.block_hash
    WHERE carry.miner_id = $1
      AND carry.maturity_state <> 'reversed'
      AND block.chain_state <> 'reversed'
      AND block.maturity_state <> 'reversed'
),
page_base AS (
    SELECT *
    FROM filtered
    ORDER BY block_height DESC, carry_forward_seq DESC
    LIMIT $2 OFFSET $3
),
block_totals AS (
    SELECT block_hash, sum(gross_amount_sats) AS block_gross_amount_sats
    FROM qbit_payout_carry_forward
    WHERE block_hash IN (SELECT block_hash FROM page_base)
    GROUP BY block_hash
),
page_rows AS (
    SELECT
        page_base.*,
        block_totals.block_gross_amount_sats,
        CASE
            WHEN block_totals.block_gross_amount_sats > 0 THEN
                (page_base.gross_amount_sats::numeric * 100::numeric / block_totals.block_gross_amount_sats::numeric)::text
            ELSE '0'
        END AS reward_share_percent
    FROM page_base
    JOIN block_totals
      ON block_totals.block_hash = page_base.block_hash
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'recipient_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'gross_amount_sats', gross_amount_sats,
            'onchain_amount_sats', onchain_amount_sats,
            'settlement_fee_sats', settlement_fee_sats,
            'carry_forward_balance_sats', carry_forward_balance_sats::text,
            'action', action,
            'maturity_state', maturity_state,
            'created_at', created_at::text,
            'found_at', found_at::text,
            'block_gross_amount_sats', block_gross_amount_sats,
            'reward_share_percent', reward_share_percent
        ) ORDER BY block_height DESC, carry_forward_seq DESC)
        FROM page_rows
    ), '[]'::json)
);
