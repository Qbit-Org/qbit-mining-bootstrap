-- Parameterized shared-database read model: the pool summary's 1-, 3- and
-- 24-hour accepted share difficulty and 3-hour participants, equal to
-- dashboard_pool_hashrate.sql's raw scan of the last 24 hours (#857). Each
-- window [ended_at - span, ended_at] reads:
--   * the 5-minute rollups of the buckets wholly inside it, before the
--     bucket ended_at falls in, which hold the shares at or below the
--     rollup watermark;
--   * raw shares at or below the watermark in its two partial buckets: the
--     slice from its start to the first whole bucket, and the slice from the
--     current bucket's start to ended_at;
--   * raw shares past the watermark, the rollup lag.
-- The three spans are whole multiples of 5 minutes, so every window's
-- current slice is the same one. Without a watermark row, or with one whose
-- share is older than the 24 hours, the first two are empty and the third is
-- every share in the last 24 hours, read by its time range.
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
), progress AS (
    SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton
), watermark AS (
    -- Ready while the share at the watermark is inside the 24 hours: the
    -- shares past it are then about the 24 hours' newest, and the tail walk
    -- is no longer than the range it saves. A watermark further behind
    -- (rollups disabled, stalled, or still backfilling) reads the 24 hours'
    -- raw range instead. Either way the answer is exact.
    SELECT COALESCE((SELECT last_share_seq FROM progress), -1) AS last_share_seq,
           COALESCE((
               SELECT ledger.accepted_at >= bounds.ended_at - interval '24 hours'
               FROM qbit_share_ledger ledger, bounds
               WHERE ledger.share_seq <= (SELECT last_share_seq FROM progress)
               ORDER BY ledger.share_seq DESC
               LIMIT 1
           ), false) AS ready
), grid AS (
    SELECT ended_at,
           ended_at - interval '24 hours' AS h24_at,
           ended_at - interval '3 hours' AS h3_at,
           ended_at - interval '1 hour' AS h1_at,
           floor(extract(epoch FROM ended_at) / 300)::bigint * 300 AS current_bucket,
           ceil(extract(epoch FROM ended_at - interval '24 hours') / 300)::bigint * 300 AS h24_bucket,
           ceil(extract(epoch FROM ended_at - interval '3 hours') / 300)::bigint * 300 AS h3_bucket,
           ceil(extract(epoch FROM ended_at - interval '1 hour') / 300)::bigint * 300 AS h1_bucket
    FROM bounds
), rolled AS (
    SELECT
        COALESCE(sum(rollup.accepted_share_difficulty) FILTER (WHERE rollup.bucket_epoch >= grid.h1_bucket), 0) AS h1,
        COALESCE(sum(rollup.accepted_share_difficulty) FILTER (WHERE rollup.bucket_epoch >= grid.h3_bucket), 0) AS h3,
        COALESCE(sum(rollup.accepted_share_difficulty), 0) AS h24
    FROM qbit_hashrate_rollup_pool rollup, grid
    WHERE (SELECT ready FROM watermark)
      AND rollup.grain_seconds = 300
      AND rollup.bucket_epoch >= grid.h24_bucket
      AND rollup.bucket_epoch < grid.current_bucket
), boundary_rows AS (
    -- Four disjoint accepted_at slices, each bounded at both ends so it reads
    -- one index range, tagged with the window it belongs to ('now' belongs to
    -- all three).
    SELECT 'now' AS slice, ledger.miner_id, ledger.share_difficulty::numeric AS share_difficulty
    FROM qbit_share_ledger ledger, grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq <= (SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at >= to_timestamp(grid.current_bucket)
      AND ledger.accepted_at <= grid.ended_at
    UNION ALL
    SELECT 'h24', ledger.miner_id, ledger.share_difficulty::numeric
    FROM qbit_share_ledger ledger, grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq <= (SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at >= grid.h24_at
      AND ledger.accepted_at < to_timestamp(grid.h24_bucket)
    UNION ALL
    SELECT 'h3', ledger.miner_id, ledger.share_difficulty::numeric
    FROM qbit_share_ledger ledger, grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq <= (SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at >= grid.h3_at
      AND ledger.accepted_at < to_timestamp(grid.h3_bucket)
    UNION ALL
    SELECT 'h1', ledger.miner_id, ledger.share_difficulty::numeric
    FROM qbit_share_ledger ledger, grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq <= (SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at >= grid.h1_at
      AND ledger.accepted_at < to_timestamp(grid.h1_bucket)
), tail AS (
    -- The shares past the watermark, found by share_seq alone. Bounded on
    -- both sides, the walk covers only them, and behind OFFSET 0 no time
    -- predicate can turn it into a walk over every share a range holds. The
    -- time test filters the stream.
    SELECT ledger.accepted_at, ledger.miner_id, ledger.share_difficulty::numeric AS share_difficulty
    FROM (
        SELECT accepted_at, miner_id, share_difficulty
        FROM qbit_share_ledger
        WHERE (SELECT ready FROM watermark) AND accepted
          AND share_seq > (SELECT last_share_seq FROM watermark)
          AND share_seq <= (SELECT max(share_seq) FROM qbit_share_ledger)
        OFFSET 0
    ) ledger, grid
    WHERE ledger.accepted_at >= grid.h24_at
      AND ledger.accepted_at <= grid.ended_at
    UNION ALL
    -- Not ready, the 24 hours' raw range, as dashboard_pool_hashrate.sql
    -- reads it, not a walk over every share past the watermark.
    SELECT ledger.accepted_at, ledger.miner_id, ledger.share_difficulty::numeric
    FROM qbit_share_ledger ledger, grid
    WHERE NOT (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.accepted_at >= grid.h24_at
      AND ledger.accepted_at <= grid.ended_at
), raw AS (
    SELECT
        COALESCE(sum(share_difficulty) FILTER (WHERE slice IN ('now', 'h1')), 0) AS h1,
        COALESCE(sum(share_difficulty) FILTER (WHERE slice IN ('now', 'h3')), 0) AS h3,
        COALESCE(sum(share_difficulty) FILTER (WHERE slice IN ('now', 'h24')), 0) AS h24
    FROM boundary_rows
), tail_sums AS (
    SELECT
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= grid.h1_at), 0) AS h1,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= grid.h3_at), 0) AS h3,
        COALESCE(sum(share_difficulty), 0) AS h24
    FROM tail, grid
), participants AS (
    SELECT rollup.miner_id
    FROM qbit_hashrate_rollup_miner rollup, grid
    WHERE (SELECT ready FROM watermark)
      AND rollup.grain_seconds = 300
      AND rollup.bucket_epoch >= grid.h3_bucket
      AND rollup.bucket_epoch < grid.current_bucket
    UNION
    SELECT miner_id FROM boundary_rows WHERE slice IN ('now', 'h3')
    UNION
    SELECT tail.miner_id FROM tail, grid WHERE tail.accepted_at >= grid.h3_at
)
SELECT json_build_object(
    'h1_difficulty', ((SELECT h1 FROM rolled) + (SELECT h1 FROM raw) + (SELECT h1 FROM tail_sums))::text,
    'h3_difficulty', ((SELECT h3 FROM rolled) + (SELECT h3 FROM raw) + (SELECT h3 FROM tail_sums))::text,
    'h24_difficulty', ((SELECT h24 FROM rolled) + (SELECT h24 FROM raw) + (SELECT h24 FROM tail_sums))::text,
    'participants_3h', (SELECT count(*) FROM participants)
);
