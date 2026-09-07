-- Parameterized shared-database read model.

WITH filtered AS (
    SELECT
        payout.payout_entry_seq,
        payout.block_hash,
        payout.block_height,
        payout.miner_id,
        payout.payout_order_key,
        payout.p2mr_program,
        payout.onchain_amount_sats,
        payout.carry_forward_balance_sats,
        payout.action,
        payout.maturity_state,
        payout.created_at,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_pool_payout_entries payout
    JOIN qbit_pool_blocks block
      ON block.block_hash = payout.block_hash
    WHERE payout.miner_id = $1
      AND payout.maturity_state <> 'reversed'
      AND block.chain_state <> 'reversed'
      AND block.maturity_state <> 'reversed'
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY block_height DESC, payout_entry_seq DESC
    LIMIT $2 OFFSET $3
),
resolved AS (
    SELECT
        page_rows.*,
        fanout.fanout_txid,
        fanout.fanout_vout,
        fanout.fanout_amount_sats,
        fanout.fanout_fee_sats,
        fanout.fanout_gross_amount_sats,
        fanout.fanout_status
    FROM page_rows
    LEFT JOIN LATERAL (
        SELECT
            artifact.fanout_txid,
            artifact.settlement_status AS fanout_status,
            (output.value->>'vout')::integer AS fanout_vout,
            (output.value->>'amount_sats')::bigint AS fanout_amount_sats,
            COALESCE((output.value->>'fee_sats')::bigint, 0) AS fanout_fee_sats,
            COALESCE(
                (output.value->>'gross_amount_sats')::bigint,
                (output.value->>'amount_sats')::bigint
            ) AS fanout_gross_amount_sats
        FROM qbit_ctv_fanout_artifacts artifact
        CROSS JOIN LATERAL jsonb_array_elements(artifact.manifest->'precommitment'->'outputs') AS output(value)
        WHERE artifact.block_hash = page_rows.block_hash
          AND page_rows.action = 'onchain'
          AND output.value->>'recipient_id' = page_rows.miner_id
          AND output.value->>'order_key' = page_rows.payout_order_key
          AND output.value->>'p2mr_program_hex' = encode(page_rows.p2mr_program, 'hex')
        ORDER BY artifact.chunk_index ASC, (output.value->>'vout')::integer ASC
        LIMIT 1
    ) fanout ON TRUE
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
            'onchain_amount_sats', onchain_amount_sats,
            'carry_forward_balance_sats', carry_forward_balance_sats::text,
            'action', action,
            'maturity_state', maturity_state,
            'created_at', created_at::text,
            'fanout_txid', fanout_txid,
            'fanout_vout', fanout_vout,
            'fanout_amount_sats', fanout_amount_sats,
            'fanout_fee_sats', fanout_fee_sats,
            'fanout_gross_amount_sats', fanout_gross_amount_sats,
            'fanout_status', fanout_status
        ) ORDER BY block_height DESC, payout_entry_seq DESC)
        FROM resolved
    ), '[]'::json)
);
