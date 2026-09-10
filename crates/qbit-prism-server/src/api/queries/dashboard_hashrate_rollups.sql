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
), boundary AS (
    SELECT floor(extract(epoch FROM ledger.accepted_at)/$1::bigint)::bigint*$1::bigint AS bucket_epoch,
           count(*) AS accepted_share_count,sum(ledger.share_difficulty) AS accepted_share_difficulty
    FROM qbit_share_ledger ledger,grid
    WHERE (SELECT ready FROM watermark) AND ledger.accepted
      AND ledger.share_seq<=(SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at<=grid.ended_at
      AND (grid.started_at IS NULL OR ledger.accepted_at>=grid.started_at)
      AND (ledger.accepted_at>=to_timestamp(grid.current_bucket) OR ledger.accepted_at<to_timestamp(grid.first_full_bucket))
      AND ($4::text IS NULL OR ledger.miner_id=$4)
    GROUP BY bucket_epoch
), tail AS (
    SELECT floor(extract(epoch FROM ledger.accepted_at)/$1::bigint)::bigint*$1::bigint AS bucket_epoch,
           count(*) AS accepted_share_count,sum(ledger.share_difficulty) AS accepted_share_difficulty
    FROM qbit_share_ledger ledger,grid
    WHERE ledger.accepted AND ledger.share_seq>(SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at<=grid.ended_at
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
