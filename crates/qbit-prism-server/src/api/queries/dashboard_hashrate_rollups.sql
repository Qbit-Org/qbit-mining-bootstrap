WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
), progress AS (
    SELECT last_share_seq FROM qbit_hashrate_rollup_progress WHERE singleton
), watermark AS (
    SELECT COALESCE((SELECT last_share_seq FROM progress),-1) AS last_share_seq,
           EXISTS(SELECT 1 FROM progress) AS ready
), range_bounds AS (
    SELECT ended_at,
        floor(extract(epoch FROM ended_at)/$1::bigint)::bigint*$1::bigint AS current_bucket,
        CASE WHEN $2::bigint IS NULL THEN NULL ELSE COALESCE(to_timestamp($3::double precision),ended_at)-make_interval(secs=>$2::double precision) END AS started_at
    FROM bounds
), grid AS (
    SELECT *,ceil(extract(epoch FROM started_at)/$1::bigint)::bigint*$1::bigint AS first_full_bucket
    FROM range_bounds
), rolled AS (
    SELECT bucket_epoch,accepted_share_count,accepted_share_difficulty
    FROM qbit_hashrate_rollup_pool,grid
    WHERE $4::text IS NULL AND (SELECT ready FROM watermark)
      AND grain_seconds=$1 AND bucket_epoch<grid.current_bucket
      AND (grid.first_full_bucket IS NULL OR bucket_epoch>=grid.first_full_bucket)
    UNION ALL
    SELECT bucket_epoch,accepted_share_count,accepted_share_difficulty
    FROM qbit_hashrate_rollup_miner,grid
    WHERE miner_id=$4 AND (SELECT ready FROM watermark)
      AND grain_seconds=$1 AND bucket_epoch<grid.current_bucket
      AND (grid.first_full_bucket IS NULL OR bucket_epoch>=grid.first_full_bucket)
), boundary_rows AS (
    -- The current bucket and the leading partial one, as two disjoint
    -- accepted_at ranges bounded at both ends. Written as an OR of the two,
    -- or with an IS NULL test on the range start, they bound no index range,
    -- and the planner walks every share at or below the watermark instead.
    -- GREATEST and LEAST skip a NULL start or first full bucket.
    SELECT ledger.accepted_at,ledger.share_difficulty
    FROM qbit_share_ledger ledger,grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq<=(SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at>=GREATEST(to_timestamp(grid.current_bucket),grid.started_at)
      AND ledger.accepted_at<=grid.ended_at
      AND ($4::text IS NULL OR ledger.miner_id=$4)
    UNION ALL
    -- Without a range start this slice is empty: accepted_at>=NULL holds for
    -- no row.
    SELECT ledger.accepted_at,ledger.share_difficulty
    FROM qbit_share_ledger ledger,grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq<=(SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at>=grid.started_at
      AND ledger.accepted_at<LEAST(to_timestamp(grid.first_full_bucket),to_timestamp(grid.current_bucket))
      AND ($4::text IS NULL OR ledger.miner_id=$4)
), boundary AS (
    SELECT floor(extract(epoch FROM accepted_at)/$1::bigint)::bigint*$1::bigint AS bucket_epoch,
           count(*) AS accepted_share_count,sum(share_difficulty) AS accepted_share_difficulty
    FROM boundary_rows
    GROUP BY bucket_epoch
), tail AS (
    -- The shares past the watermark, found by share_seq alone. Bounded on
    -- both sides, the walk covers only them, and behind OFFSET 0 no time or
    -- miner predicate can turn it into a walk over every share a range or a
    -- miner holds. The time and miner tests filter the stream.
    SELECT floor(extract(epoch FROM ledger.accepted_at)/$1::bigint)::bigint*$1::bigint AS bucket_epoch,
           count(*) AS accepted_share_count,sum(ledger.share_difficulty) AS accepted_share_difficulty
    FROM (
        SELECT accepted_at,miner_id,share_difficulty
        FROM qbit_share_ledger
        WHERE accepted AND share_seq>(SELECT last_share_seq FROM watermark)
          AND share_seq<=(SELECT max(share_seq) FROM qbit_share_ledger)
        OFFSET 0
    ) ledger,grid
    WHERE ledger.accepted_at<=grid.ended_at
      AND (grid.started_at IS NULL OR ledger.accepted_at>=grid.started_at)
      AND ($4::text IS NULL OR ledger.miner_id=$4)
    GROUP BY bucket_epoch
), merged AS (
    SELECT bucket_epoch,sum(accepted_share_count)::bigint AS accepted_share_count,
           sum(accepted_share_difficulty) AS accepted_share_difficulty
    FROM (SELECT * FROM rolled UNION ALL SELECT * FROM boundary UNION ALL SELECT * FROM tail) rows
    GROUP BY bucket_epoch
)
SELECT COALESCE(json_agg(json_build_object(
    'timestamp',to_char(to_timestamp(bucket_epoch) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'accepted_share_count',accepted_share_count,'accepted_share_difficulty',accepted_share_difficulty::text
) ORDER BY bucket_epoch),'[]'::json) FROM merged;
