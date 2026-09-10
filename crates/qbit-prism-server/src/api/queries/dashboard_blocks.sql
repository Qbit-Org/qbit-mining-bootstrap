-- Parameterized shared-database read model.

WITH public_blocks AS (
    SELECT block.*, CASE WHEN block.chain_state='inactive' AND block.audit_publication_sequence IS NOT NULL THEN 'reversed' ELSE block.chain_state END AS public_chain_state
    FROM qbit_pool_blocks block
), total AS (
    SELECT count(*) AS total_count
    FROM public_blocks
    WHERE ($3 = 'all' OR ($3 = 'active' AND public_chain_state = 'confirmed') OR ($3 = 'reversed' AND public_chain_state = 'reversed'))
),
page_blocks AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.public_chain_state AS chain_state,
        COALESCE(block.disconnected_at,block.inactive_since) AS disconnected_at,
        block.payout_manifest_sha256
    FROM public_blocks block
    WHERE ($3 = 'all' OR ($3 = 'active' AND block.public_chain_state = 'confirmed') OR ($3 = 'reversed' AND block.public_chain_state = 'reversed'))
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT $1 OFFSET $2
),
rows AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.chain_state,
        block.disconnected_at,
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
        )::jsonb || CASE WHEN $3 = 'active' THEN '{}'::jsonb ELSE jsonb_build_object('chain_state', rows.chain_state, 'disconnected_at', CASE WHEN rows.chain_state = 'reversed' THEN to_char(rows.disconnected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) END ORDER BY rows.block_height DESC, rows.found_at DESC)
        FROM rows
    ), '[]'::json)
);
