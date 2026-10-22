WITH progress AS (
    SELECT COALESCE((
        SELECT last_share_seq
        FROM qbit_hashrate_rollup_progress
        WHERE singleton
    ), 0) AS last_share_seq
),
batch AS (
    SELECT
        ledger.share_seq,
        ledger.accepted,
        ledger.accepted_at,
        ledger.miner_id,
        ledger.share_difficulty
    FROM qbit_share_ledger ledger
    WHERE ledger.share_seq > (SELECT last_share_seq FROM progress)
    ORDER BY ledger.share_seq ASC
    LIMIT $1
),
batch_stats AS (
    SELECT
        count(*) AS scanned,
        COALESCE(max(batch.share_seq), (SELECT last_share_seq FROM progress)) AS next_share_seq
    FROM batch
),
advance AS (
    INSERT INTO qbit_hashrate_rollup_progress (singleton, last_share_seq)
    SELECT true, (SELECT next_share_seq FROM batch_stats)
    ON CONFLICT (singleton) DO UPDATE
        SET last_share_seq = EXCLUDED.last_share_seq,
            updated_at = clock_timestamp()
        WHERE qbit_hashrate_rollup_progress.last_share_seq = (SELECT last_share_seq FROM progress)
    RETURNING last_share_seq
),
grains AS (
    SELECT grain_seconds
    FROM (VALUES (300), (3600), (86400)) AS grain(grain_seconds)
),
pool_rollup AS (
    INSERT INTO qbit_hashrate_rollup_pool (
        grain_seconds,
        bucket_epoch,
        accepted_share_count,
        accepted_share_difficulty
    )
    SELECT
        grains.grain_seconds,
        floor(extract(epoch FROM batch.accepted_at) / grains.grain_seconds)::bigint * grains.grain_seconds AS bucket_epoch,
        count(*) AS accepted_share_count,
        sum(batch.share_difficulty) AS accepted_share_difficulty
    FROM batch, grains
    WHERE batch.accepted
      AND EXISTS (SELECT 1 FROM advance)
    GROUP BY grains.grain_seconds, bucket_epoch
    ON CONFLICT (grain_seconds, bucket_epoch) DO UPDATE
        SET accepted_share_count = qbit_hashrate_rollup_pool.accepted_share_count
                + EXCLUDED.accepted_share_count,
            accepted_share_difficulty = qbit_hashrate_rollup_pool.accepted_share_difficulty
                + EXCLUDED.accepted_share_difficulty
    RETURNING 1
),
miner_rollup AS (
    INSERT INTO qbit_hashrate_rollup_miner (
        grain_seconds,
        bucket_epoch,
        miner_id,
        accepted_share_count,
        accepted_share_difficulty
    )
    SELECT
        grains.grain_seconds,
        floor(extract(epoch FROM batch.accepted_at) / grains.grain_seconds)::bigint * grains.grain_seconds AS bucket_epoch,
        batch.miner_id,
        count(*) AS accepted_share_count,
        sum(batch.share_difficulty) AS accepted_share_difficulty
    FROM batch, grains
    WHERE batch.accepted
      AND EXISTS (SELECT 1 FROM advance)
    GROUP BY grains.grain_seconds, bucket_epoch, batch.miner_id
    ON CONFLICT (grain_seconds, bucket_epoch, miner_id) DO UPDATE
        SET accepted_share_count = qbit_hashrate_rollup_miner.accepted_share_count
                + EXCLUDED.accepted_share_count,
            accepted_share_difficulty = qbit_hashrate_rollup_miner.accepted_share_difficulty
                + EXCLUDED.accepted_share_difficulty
    RETURNING 1
)
SELECT (SELECT scanned FROM batch_stats)::bigint,
       (SELECT next_share_seq FROM batch_stats)::bigint,
       EXISTS (SELECT 1 FROM advance);
