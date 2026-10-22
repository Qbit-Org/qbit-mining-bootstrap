-- Parameterized shared-database read model.

WITH bounds AS (
    SELECT clock_timestamp() AS now_at
),
named AS (
    SELECT
        CASE
            WHEN username = $1 THEN 'default'
            WHEN left(username, length($1)+1) = ($1 || '.') THEN COALESCE(NULLIF(substr(username, length($1)+2), ''), 'default')
            WHEN position('.' IN username) > 0 THEN COALESCE(NULLIF(substring(username FROM position('.' IN username) + 1), ''), 'default')
            ELSE 'default'
        END AS worker_name,
        share_difficulty,
        accepted_at
    FROM (
        SELECT
            regexp_replace(share_id, ':[^:]*$', '') AS username,
            share_difficulty,
            accepted_at
        FROM qbit_share_ledger
        WHERE accepted
          AND miner_id = $1
          -- 3 hours is the largest rollup window this endpoint reports
          -- (h3_difficulty), so the scan never needs the miner's full history.
          AND accepted_at >= (SELECT now_at FROM bounds) - interval '3 hours'
    ) shares
),
grouped AS (
    SELECT
        worker_name,
        max(accepted_at) AS last_share_at,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= (SELECT now_at FROM bounds) - interval '1 minute'), 0)::text AS m1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= (SELECT now_at FROM bounds) - interval '3 hours'), 0)::text AS h3_difficulty,
        max(accepted_at) >= (SELECT now_at FROM bounds) - interval '10 minutes' AS active
    FROM named
    GROUP BY worker_name
),
filtered AS (
    SELECT *
    FROM grouped
    WHERE ($2::text IS NULL OR strpos(lower(worker_name), lower($2)) > 0) AND (NOT $3::boolean OR active)
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY active DESC, worker_name ASC
    LIMIT $4 OFFSET $5
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'active_count', (SELECT count(*) FROM grouped WHERE active),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'worker_name', worker_name,
            'status', CASE WHEN active THEN 'active' ELSE 'inactive' END,
            'last_share_at', to_char(last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'm1_difficulty', m1_difficulty,
            'h3_difficulty', h3_difficulty
        ) ORDER BY active DESC, worker_name ASC)
        FROM page_rows
    ), '[]'::json)
);
