# PRISM payout-artifact before/after measurement

This is a production runbook for a human operator. Every database statement is
read-only. Do not run it from a development workspace, and do not reset
`pg_stat_statements`: compare cumulative snapshots by `queryid` instead.

Use matching observation windows (one hour minimum; 24 hours preferred) before
and after rollout. Record the rollout commit and image digest, UTC start/end,
tip count, accepted-share count, connected-miner range, and network difficulty.
Keep all raw CSV and log extracts with the rollout evidence.

## 1. Capture `pg_stat_statements`

Run this immediately before and after each observation window and save the
output as CSV:

```sql
COPY (
    SELECT
        now() AT TIME ZONE 'UTC' AS captured_at_utc,
        queryid,
        calls,
        total_exec_time,
        mean_exec_time,
        rows,
        shared_blks_hit,
        shared_blks_read,
        shared_blk_read_time,
        temp_blks_read,
        temp_blks_written,
        left(regexp_replace(query, E'\\s+', ' ', 'g'), 500) AS query
    FROM pg_stat_statements
    WHERE dbid = (
        SELECT oid FROM pg_database WHERE datname = current_database()
    )
    ORDER BY total_exec_time DESC
    LIMIT 25
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);
```

For each `queryid`, subtract the opening snapshot from the closing snapshot.
Report calls, total execution time, shared blocks read, shared-block read time,
and temp blocks. Calculate the interval mean as
`total_exec_time_delta / calls_delta`. Keep both the overall top 25 and a
filtered view for the normalized query beginning `WITH RECURSIVE pages AS (`.

Expected after rollout: the pages CTE runs only for cold start, explicit
reconcile/correction fallback, invariant fallback, and periodic self-check. It
should disappear from steady-state top consumers.

## 2. Explain the full-rescan oracle

Take `anchor_ms`, `window_shares`, and network difficulty from one coordinator
build event. Set `window_weight` to `16 * network_difficulty`. Use the same
window weight before and after rollout. Run the following in `psql`, supplying
integer variables `anchor_ms` and `window_weight`:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5min';

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH RECURSIVE pages AS (
    SELECT page.min_share_seq,
           page.page_weight,
           page.page_weight AS cumulative_weight
    FROM LATERAL (
        SELECT min(page_rows.share_seq) AS min_share_seq,
               COALESCE(sum(page_rows.share_difficulty), 0)::numeric AS page_weight
        FROM (
            SELECT ledger.share_seq, ledger.share_difficulty
            FROM qbit_share_ledger ledger
            WHERE ledger.accepted
              AND ledger.job_issued_at <=
                  to_timestamp((:anchor_ms)::double precision / 1000.0)
              AND ledger.accepted_at <=
                  to_timestamp((:anchor_ms)::double precision / 1000.0)
            ORDER BY ledger.share_seq DESC
            LIMIT 4096
        ) page_rows
    ) page
    UNION ALL
    SELECT page.min_share_seq,
           page.page_weight,
           pages.cumulative_weight + page.page_weight
    FROM pages
    CROSS JOIN LATERAL (
        SELECT min(page_rows.share_seq) AS min_share_seq,
               COALESCE(sum(page_rows.share_difficulty), 0)::numeric AS page_weight
        FROM (
            SELECT ledger.share_seq, ledger.share_difficulty
            FROM qbit_share_ledger ledger
            WHERE ledger.accepted
              AND ledger.job_issued_at <=
                  to_timestamp((:anchor_ms)::double precision / 1000.0)
              AND ledger.accepted_at <=
                  to_timestamp((:anchor_ms)::double precision / 1000.0)
              AND ledger.share_seq < pages.min_share_seq
            ORDER BY ledger.share_seq DESC
            LIMIT 4096
        ) page_rows
    ) page
    WHERE pages.cumulative_weight < (:window_weight)::numeric
      AND pages.min_share_seq IS NOT NULL
),
page_cutoff AS (
    SELECT min(min_share_seq) AS min_share_seq
    FROM pages
    WHERE min_share_seq IS NOT NULL
),
ranked AS (
    SELECT ledger.*,
           sum(ledger.share_difficulty) OVER (
               ORDER BY ledger.share_seq DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )::numeric AS cumulative_difficulty
    FROM qbit_share_ledger ledger
    CROSS JOIN page_cutoff
    WHERE ledger.accepted
      AND ledger.job_issued_at <=
          to_timestamp((:anchor_ms)::double precision / 1000.0)
      AND ledger.accepted_at <=
          to_timestamp((:anchor_ms)::double precision / 1000.0)
      AND ledger.share_seq >= page_cutoff.min_share_seq
),
rows AS (
    SELECT *
    FROM ranked
    WHERE cumulative_difficulty - share_difficulty
          < (:window_weight)::numeric
)
SELECT count(*), min(share_seq), max(share_seq), sum(share_difficulty)
FROM rows;

ROLLBACK;
```

Capture execution/planning time, recursive loops, shared hit/read blocks, read
I/O time, and temp blocks. The isolated full oracle may remain expensive; the
success criterion is that its call frequency collapses.

After rollout, also explain one normal delta query from
`pg_stat_statements`. Confirm both timestamp-range branches use indexes, the
returned rows approximate `delta_rows`, and no recursive pages CTE appears.

## 3. Extract artifact events

Use the exact UTC observation range. Preserve the original timestamped log and
also extract parseable coordinator payloads:

```sh
rg '"event": "payout_artifact_' "$PRISM_COORDINATOR_LOG" \
  | sed -E 's/^.*prism coordinator: //' \
  > "$PRISM_ARTIFACT_EVENTS"
jq -e . "$PRISM_ARTIFACT_EVENTS" >/dev/null
```

### Build duration p50/p95

```sh
jq -s '
  def percentile($p):
    sort as $v
    | if length == 0 then null else $v[((length * $p | ceil) - 1)] end;
  [.[]
   | select(.event == "payout_artifact_built")
   | .duration_seconds]
  | {
      count: length,
      p50_seconds: percentile(0.50),
      p95_seconds: percentile(0.95),
      max_seconds: (if length == 0 then null else max end)
    }
' "$PRISM_ARTIFACT_EVENTS"
```

Repeat grouped by `.window_build_mode`. Report `incremental`, `full_rescan`,
`self_check_match`, `self_check_mismatch`, and
`incremental_self_check_failed` separately. A self-check mismatch is a release
blocker and must be investigated even though runtime resets to the full oracle.

### Time spent building during publication

Set `PRISM_OBSERVATION_SECONDS` to the exact interval length. Debounced retags
have their own event and are intentionally excluded:

```sh
jq -s --argjson seconds "$PRISM_OBSERVATION_SECONDS" '
  [.[]
   | select(.event == "payout_artifact_built")
   | select(.during_publication == true)
   | .duration_seconds]
  | {
      publication_build_seconds: (add // 0),
      observation_seconds: $seconds,
      duty_cycle_fraction: ((add // 0) / $seconds),
      duty_cycle_percent: (((add // 0) / $seconds) * 100)
    }
' "$PRISM_ARTIFACT_EVENTS"
```

The after-rollout target is less than 20% of wall-clock. The legacy baseline
does not label every synchronous scan consistently; record that limitation and
cross-check adjacent `payout_artifact_installed` events with
`during_publication:true`.

Also retain counts and bounded-work fields:

```sh
jq -s '
  group_by(.event + ":" + (.window_build_mode // "legacy"))
  | map({
      key: (.[0].event + ":" + (.[0].window_build_mode // "legacy")),
      count: length,
      delta_rows: ([.[].delta_rows // 0] | add),
      touched_pages: ([.[].touched_pages // 0] | add)
    })
' "$PRISM_ARTIFACT_EVENTS"
```

## 4. Sidecar failure rate and reconcile symptom

Adjust the sidecar expression only if its deployed message differs, and keep
sample matching lines:

```sh
PRISM_SIDECAR_FAILURES=$(rg -c \
  'mining\.subscribe.*(fail|timeout|timed out|error)' \
  "$PRISM_SIDECAR_LOG" || true)
awk -v failures="$PRISM_SIDECAR_FAILURES" \
    -v seconds="$PRISM_OBSERVATION_SECONDS" \
  'BEGIN { printf "failures=%d rate_per_hour=%.3f\n", failures, failures * 3600 / seconds }'

PRISM_RECONCILE_TIMEOUTS=$(rg -c \
  'reconcile prefetch join exceeded 20s' \
  "$PRISM_COORDINATOR_LOG" || true)
awk -v failures="$PRISM_RECONCILE_TIMEOUTS" \
    -v seconds="$PRISM_OBSERVATION_SECONDS" \
  'BEGIN { printf "events=%d rate_per_hour=%.3f\n", failures, failures * 3600 / seconds }'
```

Before rollout, confirm the sidecar calculation reproduces the known 50–115
failures/hour range. Expected after rollout: approximately zero background
sidecar failures, zero normal-operation reconcile-prefetch timeouts, and
`PrismStratumCheckFailureRateHigh` clears after its lookback interval.

## 5. Comparison record

| Metric | Before | After | Target |
|---|---:|---:|---:|
| Artifact build p50 / p95 (seconds) | | | Incremental scales with delta |
| Publication build duty cycle | | | <20% |
| Full pages-CTE calls/hour | | | Cold/correction/check only |
| Pages-CTE execution time/hour | | | Near-zero background |
| Pages-CTE shared blocks read/hour | | | Near-zero background |
| Reconcile-prefetch >20s events/hour | | | 0 |
| Sidecar subscribe failures/hour | 50–115 | | Approximately 0 background |
| `PrismStratumCheckFailureRateHigh` | Firing | | Clear |

Do not claim the production win from local simulation. Until both captures are
complete, report these as expected effects and attach the raw before/after
evidence to the rollout record.
