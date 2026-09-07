-- Parameterized shared-database read model.

	WITH snapshot_clock AS (
	    SELECT clock_timestamp() AS ended_at
	),
	bounds AS (
	    SELECT ended_at, ended_at - interval '3 hours' AS started_at
	    FROM snapshot_clock
	),
	windowed AS (
	    SELECT ledger.*
	    FROM qbit_share_ledger ledger, bounds
	    WHERE ledger.accepted
	      AND ledger.accepted_at >= bounds.started_at
	      AND ledger.accepted_at <= bounds.ended_at
	),
grouped AS (
    SELECT
        miner_id,
        sum(share_difficulty) AS accepted_share_difficulty,
        max(accepted_at) AS last_share_at
    FROM windowed
    GROUP BY miner_id
),
filtered AS (
    SELECT *
    FROM grouped
    WHERE ($1::text IS NULL OR strpos(lower(grouped.miner_id), lower($1)) > 0)
),
blocks AS (
    SELECT solver.miner_id, count(*) AS blocks_found
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
    WHERE block.chain_state <> 'reversed'
    GROUP BY solver.miner_id
),
totals AS (
    SELECT
        COALESCE(sum(accepted_share_difficulty), 0) AS total_difficulty,
        count(*) AS participant_count
    FROM filtered
),
ranked AS (
    SELECT
        row_number() OVER (ORDER BY accepted_share_difficulty DESC, filtered.miner_id ASC) AS rank,
        filtered.miner_id,
        filtered.accepted_share_difficulty,
        filtered.last_share_at,
        COALESCE(blocks.blocks_found, 0) AS blocks_found
    FROM filtered
    LEFT JOIN blocks
      ON blocks.miner_id = filtered.miner_id
),
page_rows AS (
    SELECT *
    FROM ranked
    ORDER BY rank ASC
    LIMIT $2 OFFSET $3
)
SELECT json_build_object(
    'started_at', (SELECT to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM bounds),
    'ended_at', (SELECT to_char(ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM bounds),
    'total_difficulty', (SELECT total_difficulty::text FROM totals),
    'participant_count', (SELECT participant_count FROM totals),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'rank', page_rows.rank,
            'recipient_id', page_rows.miner_id,
            'display_name', null,
            'accepted_share_difficulty', page_rows.accepted_share_difficulty::text,
            'share_percent', CASE
                WHEN (SELECT total_difficulty FROM totals) > 0 THEN
                    (page_rows.accepted_share_difficulty * 100::numeric / (SELECT total_difficulty FROM totals))::text
                ELSE null
            END,
            'blocks_found', page_rows.blocks_found,
            'last_share_at', to_char(page_rows.last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ) ORDER BY page_rows.rank ASC)
        FROM page_rows
    ), '[]'::json)
);
