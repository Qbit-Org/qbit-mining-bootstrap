-- Parameterized shared-database read model: one miner's share of the reward
-- window, the figures the miner page reads from
-- dashboard_reward_leaderboard.sql's row for that miner and its totals,
-- without grouping, ranking and attributing blocks to every miner (#857).
-- $1: network difficulty; $2: the miner.
WITH snapshot_clock AS (
    SELECT clock_timestamp() AS ended_at
),
window_rows AS MATERIALIZED (
    SELECT window_row.miner_id, window_row.counted_difficulty
    FROM snapshot_clock
    CROSS JOIN LATERAL qbit_prism_window(
        snapshot_clock.ended_at,
        ($1::text::numeric * 8)::numeric
    ) AS window_row
),
window_summary AS (
    SELECT
        COALESCE(sum(counted_difficulty), 0) AS counted_window_weight,
        count(*) FILTER (WHERE miner_id = $2) AS included_share_count,
        sum(counted_difficulty) FILTER (WHERE miner_id = $2) AS counted_share_difficulty
    FROM window_rows
)
SELECT json_build_object(
    'counted_window_weight', counted_window_weight::text,
    -- Null when the miner has no share in the window, as it then has no row.
    'counted_share_difficulty', CASE WHEN included_share_count > 0 THEN counted_share_difficulty::text END,
    'share_percent', CASE
        WHEN included_share_count > 0 AND counted_window_weight > 0 THEN
            (counted_share_difficulty * 100::numeric / counted_window_weight)::text
        ELSE null
    END
)
FROM window_summary;
