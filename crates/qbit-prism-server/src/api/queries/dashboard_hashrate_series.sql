-- Parameterized shared-database read model.

	WITH bounds AS (
	    SELECT clock_timestamp() AS ended_at
	),
	bucketed AS (
	    SELECT
	        floor(extract(epoch FROM ledger.accepted_at) / $1::bigint)::bigint * $1::bigint AS bucket_epoch,
	        count(*) AS accepted_share_count,
	        sum(ledger.share_difficulty) AS accepted_share_difficulty
	    FROM qbit_share_ledger ledger, bounds
	    WHERE ledger.accepted
	      AND ledger.accepted_at <= bounds.ended_at
	      AND ($2::bigint IS NULL OR ledger.accepted_at >= to_timestamp($3::double precision) - make_interval(secs => $2::double precision))
	      AND ($4::text IS NULL OR ledger.miner_id = $4)
	    GROUP BY bucket_epoch
	)
SELECT COALESCE(json_agg(json_build_object(
    'timestamp', to_char(to_timestamp(bucket_epoch) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'accepted_share_count', accepted_share_count,
    'accepted_share_difficulty', accepted_share_difficulty::text
) ORDER BY bucket_epoch ASC), '[]'::json)
FROM bucketed;
