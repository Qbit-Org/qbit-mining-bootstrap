-- Parameterized shared-database read model.

WITH total AS (
    SELECT count(*) AS total_count
    FROM qbit_pool_blocks
    WHERE chain_state = 'confirmed'
),
page_blocks AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256
    FROM qbit_pool_blocks block
    WHERE block.chain_state = 'confirmed'
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT $1 OFFSET $2
),
rows AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256,
        COALESCE(bundle.found_block_network_difficulty::text, bundle.audit_bundle#>>'{found_block,network_difficulty}') AS audit_network_difficulty,
        COALESCE(bundle.found_block_bits, bundle.audit_bundle#>>'{found_block,bits}') AS audit_bits,
        COALESCE(bundle.found_block_coinbase_value_sats::text, bundle.audit_bundle#>>'{found_block,coinbase_value_sats}') AS audit_coinbase_value_sats,
        bundle.audit_bundle_sha256,
        solver.miner_id AS solver_recipient_id,
        solver.share_difficulty::text AS solver_share_difficulty,
        solver.network_difficulty::text AS solver_network_difficulty,
        solver.share_id AS solver_share_id
    FROM page_blocks block
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = block.block_hash
    LEFT JOIN LATERAL (
        SELECT share.miner_id, share.share_difficulty, share.network_difficulty, share.share_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
)
SELECT json_build_object(
    'total_count', (SELECT total_count FROM total),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'height', rows.block_height,
            'hash', rows.block_hash,
            'found_at', to_char(rows.found_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'network_difficulty', COALESCE(rows.audit_network_difficulty, rows.solver_network_difficulty, '0'),
            'bits', COALESCE(rows.audit_bits, '00000000'),
            'solver_recipient_id', COALESCE(rows.solver_recipient_id, ''),
            'solver_worker_name', CASE WHEN rows.solver_share_id IS NULL THEN null WHEN position('.' IN regexp_replace(rows.solver_share_id, ':[^:]*$', '')) > 0 THEN NULLIF(substring(regexp_replace(rows.solver_share_id, ':[^:]*$', '') FROM position('.' IN regexp_replace(rows.solver_share_id, ':[^:]*$', '')) + 1), '') ELSE null END,
            'solver_share_difficulty', rows.solver_share_difficulty,
            'reward_window_weight', CASE
                WHEN rows.audit_network_difficulty IS NULL THEN null
                ELSE (rows.audit_network_difficulty::numeric * 8::numeric)::text
            END,
            'coinbase_value_bits', COALESCE(rows.audit_coinbase_value_sats::bigint, 0),
            'audit_bundle_sha256', rows.audit_bundle_sha256,
            'payout_manifest_sha256', rows.payout_manifest_sha256,
            'explorer_url', null
        ) ORDER BY rows.block_height DESC, rows.found_at DESC)
        FROM rows
    ), '[]'::json)
);
