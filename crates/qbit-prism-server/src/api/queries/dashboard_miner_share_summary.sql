-- Parameterized shared-database read model.

WITH bounds AS (
    SELECT clock_timestamp() AS now_at
),
pool AS (
    SELECT COALESCE(sum(share_difficulty), 0)::text AS h3_difficulty
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND accepted_at >= bounds.now_at - interval '3 hours'
      AND accepted_at <= bounds.now_at
),
miner_rollups AS (
    SELECT
        count(*) FILTER (WHERE accepted_at >= bounds.now_at - interval '3 hours') AS accepted_3h,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '1 minute'), 0)::text AS m1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '5 minutes'), 0)::text AS m5_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '10 minutes'), 0)::text AS m10_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '3 hours'), 0)::text AS h3_difficulty,
        COALESCE(sum(share_difficulty), 0)::text AS h24_difficulty
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND miner_id = $1
      AND accepted_at >= bounds.now_at - interval '24 hours'
      AND accepted_at <= bounds.now_at
),
miner_last AS (
    SELECT
        to_char(max(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_share_at
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND miner_id = $1
      AND accepted_at <= bounds.now_at
)
SELECT json_build_object(
    'accepted_3h', (SELECT accepted_3h FROM miner_rollups),
    'm1_difficulty', (SELECT m1_difficulty FROM miner_rollups),
    'm5_difficulty', (SELECT m5_difficulty FROM miner_rollups),
    'm10_difficulty', (SELECT m10_difficulty FROM miner_rollups),
    'h3_difficulty', (SELECT h3_difficulty FROM miner_rollups),
    'h24_difficulty', (SELECT h24_difficulty FROM miner_rollups),
    'pool_h3_difficulty', (SELECT h3_difficulty FROM pool),
    'last_share_at', (SELECT last_share_at FROM miner_last)
);
