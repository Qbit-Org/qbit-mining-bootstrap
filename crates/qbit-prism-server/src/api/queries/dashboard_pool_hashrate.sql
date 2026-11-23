-- Parameterized shared-database read model: the pool summary's 1-, 3- and
-- 24-hour accepted share difficulty and 3-hour participants from the raw
-- ledger, for a database without the rollup schema (#857).
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
)
SELECT json_build_object(
    'h1_difficulty', COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '1 hour'), 0)::text,
    'h3_difficulty', COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours'), 0)::text,
    'h24_difficulty', COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '24 hours'), 0)::text,
    'participants_3h', count(DISTINCT miner_id) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours')
)
FROM qbit_share_ledger, bounds
WHERE accepted
  AND accepted_at >= bounds.ended_at - interval '24 hours'
  AND accepted_at <= bounds.ended_at;
