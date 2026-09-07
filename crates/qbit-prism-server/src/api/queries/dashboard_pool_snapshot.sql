-- Parameterized shared-database read model.

WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
),
latest_block_row AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256
    FROM qbit_pool_blocks block
    WHERE block.chain_state <> 'reversed'
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT 1
),
latest_block AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256,
        bundle.audit_bundle_sha256,
        solver.miner_id AS solver_recipient_id,
        solver.share_id AS solver_share_id
    FROM latest_block_row block
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = block.block_hash
    LEFT JOIN LATERAL (
        SELECT share.miner_id, share.share_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
),
window_rows AS (
    SELECT window_row.*
    FROM bounds
    CROSS JOIN LATERAL qbit_prism_window(bounds.ended_at, ($1::text::numeric * 8)::numeric) AS window_row
),
window_summary AS (
    SELECT
        count(*) AS included_share_count,
        to_char(min(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS oldest_share_accepted_at,
        to_char(max(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS newest_share_accepted_at
    FROM window_rows
),
rollups AS (
    SELECT
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '1 hour'), 0)::text AS h1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours'), 0)::text AS h3_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '24 hours'), 0)::text AS h24_difficulty,
        count(DISTINCT miner_id) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours') AS participants_3h
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND accepted_at >= bounds.ended_at - interval '24 hours'
      AND accepted_at <= bounds.ended_at
)
SELECT json_build_object(
    'h1_difficulty', (SELECT h1_difficulty FROM rollups),
    'h3_difficulty', (SELECT h3_difficulty FROM rollups),
    'h24_difficulty', (SELECT h24_difficulty FROM rollups),
    'participants_3h', (SELECT participants_3h FROM rollups),
    'blocks_found_total', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state <> 'reversed'),
    'prism_blocks_total', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state <> 'reversed'),
    'total_mined_bits', COALESCE((
        SELECT sum(carry.gross_amount_sats)
        FROM qbit_payout_carry_forward carry
        JOIN qbit_pool_blocks block
          ON block.block_hash = carry.block_hash
        WHERE block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
          AND carry.maturity_state <> 'reversed'
    ), 0),
    'latest_block', COALESCE((
        SELECT json_build_object(
            'height', latest_block.block_height,
            'hash', latest_block.block_hash,
            'found_at', to_char(latest_block.found_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'age_seconds', GREATEST(0, floor(extract(epoch FROM (clock_timestamp() - latest_block.found_at)))::bigint),
            'solver_recipient_id', COALESCE(latest_block.solver_recipient_id, ''),
            'solver_worker_name', CASE WHEN latest_block.solver_share_id IS NULL THEN null WHEN position('.' IN regexp_replace(latest_block.solver_share_id, ':[^:]*$', '')) > 0 THEN NULLIF(substring(regexp_replace(latest_block.solver_share_id, ':[^:]*$', '') FROM position('.' IN regexp_replace(latest_block.solver_share_id, ':[^:]*$', '')) + 1), '') ELSE null END
        )
        FROM latest_block
    ), 'null'::json),
    'oldest_share_accepted_at', (SELECT oldest_share_accepted_at FROM window_summary),
    'newest_share_accepted_at', (SELECT newest_share_accepted_at FROM window_summary),
    'included_share_count', (SELECT included_share_count FROM window_summary)
);
