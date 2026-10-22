WITH in_range AS (
    SELECT floor(extract(epoch FROM found_at)/$1::bigint)::bigint*$1::bigint AS bucket_epoch,
           block_hash,block_height,found_at
    FROM qbit_pool_blocks
    WHERE chain_state = 'confirmed' AND found_at <= clock_timestamp()
      AND ($2::double precision IS NULL OR found_at >= to_timestamp($2))
), ranked AS (
    SELECT *,row_number() OVER(PARTITION BY bucket_epoch ORDER BY found_at DESC,block_height DESC) AS rank
    FROM in_range
), points AS (
    SELECT bucket_epoch,count(*) AS block_count,
           jsonb_agg(jsonb_build_object('height',block_height,'hash',block_hash,'found_at',to_char(found_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')) ORDER BY found_at DESC,block_height DESC) FILTER(WHERE rank<=3) AS blocks
    FROM ranked GROUP BY bucket_epoch
)
SELECT jsonb_build_object('total_blocks',COALESCE((SELECT sum(block_count)::bigint FROM points),0),
    'points',COALESCE((SELECT jsonb_agg(jsonb_build_object('timestamp',to_char(to_timestamp(bucket_epoch) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),'block_count',block_count,'blocks',blocks,'truncated',block_count>3) ORDER BY bucket_epoch) FROM points),'[]'::jsonb));
