-- Parameterized shared-database read model.

WITH snapshot_clock AS (
    SELECT clock_timestamp() AS ended_at
),
window_rows AS MATERIALIZED (
    SELECT window_row.*
    FROM snapshot_clock
    CROSS JOIN LATERAL qbit_prism_window(
        snapshot_clock.ended_at,
        ($1::text::numeric * 8)::numeric
    ) AS window_row
),
window_summary AS (
    SELECT
        count(*) AS included_share_count,
        COALESCE(sum(counted_difficulty), 0) AS counted_window_weight,
        min(accepted_at) AS oldest_share_accepted_at
    FROM window_rows
),
grouped AS (
    SELECT
        miner_id,
        count(*) AS included_share_count,
        sum(counted_difficulty) AS counted_share_difficulty,
        max(accepted_at) AS last_share_at
    FROM window_rows
    GROUP BY miner_id
),
blocks AS (
    SELECT solver.miner_id, count(*) AS blocks_found_total
    FROM qbit_pool_blocks block
    JOIN LATERAL (
        SELECT share.miner_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
    WHERE block.chain_state = 'confirmed'
    GROUP BY solver.miner_id
),
ranked AS (
    SELECT
        row_number() OVER (ORDER BY grouped.counted_share_difficulty DESC, grouped.miner_id ASC) AS rank,
        grouped.miner_id,
        grouped.included_share_count,
        grouped.counted_share_difficulty,
        grouped.last_share_at,
        COALESCE(blocks.blocks_found_total, 0) AS blocks_found_total
    FROM grouped
    LEFT JOIN blocks
      ON blocks.miner_id = grouped.miner_id
),
filtered AS (
    SELECT *
    FROM ranked
    WHERE ($2::text IS NULL OR strpos(lower(ranked.miner_id), lower($2)) > 0) AND ($3::text IS NULL OR ranked.miner_id = $3)
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY rank ASC
    LIMIT $4 OFFSET $5
)
SELECT json_build_object(
    'ended_at', to_char(snapshot_clock.ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'oldest_share_accepted_at', to_char(window_summary.oldest_share_accepted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'observed_span_seconds', CASE
        WHEN window_summary.oldest_share_accepted_at IS NULL THEN null
        ELSE GREATEST(
            0,
            floor(extract(epoch FROM (snapshot_clock.ended_at - window_summary.oldest_share_accepted_at)))::bigint
        )
    END,
    'counted_window_weight', window_summary.counted_window_weight::text,
    'included_share_count', window_summary.included_share_count,
    'participant_count', (SELECT count(*) FROM ranked),
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'rank', page_rows.rank,
            'recipient_id', page_rows.miner_id,
            'display_name', null,
            'included_share_count', page_rows.included_share_count,
            'counted_share_difficulty', page_rows.counted_share_difficulty::text,
            'share_percent', CASE
                WHEN window_summary.counted_window_weight > 0 THEN
                    (page_rows.counted_share_difficulty * 100::numeric / window_summary.counted_window_weight)::text
                ELSE null
            END,
            'blocks_found_total', page_rows.blocks_found_total,
            'last_share_at', to_char(page_rows.last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ) ORDER BY page_rows.rank ASC)
        FROM page_rows
    ), '[]'::json)
)
FROM snapshot_clock
CROSS JOIN window_summary;
