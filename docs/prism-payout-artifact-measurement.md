# PRISM payout-artifact before/after measurement

This is a production runbook for a human operator. Every database statement is
read-only. Do not run it from a development workspace, and do not reset
`pg_stat_statements`: compare cumulative snapshots by `queryid` instead.

Use matching observation windows (one hour minimum; 24 hours preferred) before
and after rollout. Record the rollout commit and image digest, UTC start/end,
tip count, accepted-share count, connected-miner range, and network difficulty.
Keep all raw CSV and log extracts with the rollout evidence.

## 1. Capture `pg_stat_statements`

Run both captures immediately before and after each observation window. Save
the reset timestamp beside each CSV; any change in `stats_reset` invalidates
the subtraction (restart the observation window instead of comparing across a
Postgres restart or statistics reset):

```sql
SELECT now() AT TIME ZONE 'UTC' AS captured_at_utc, stats_reset
FROM pg_stat_statements_info;
```

The capture keeps the overall top 25 plus every known full-window, delta, and
carry-balance query even when one falls outside the top-N at one boundary:

```sql
COPY (
    WITH normalized AS (
        SELECT
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
            regexp_replace(query, E'\\s+', ' ', 'g') AS normalized_query
        FROM pg_stat_statements
        WHERE dbid = (
            SELECT oid FROM pg_database WHERE datname = current_database()
        )
    ), ranked AS (
        SELECT *, row_number() OVER (ORDER BY total_exec_time DESC) AS total_rank
        FROM normalized
    )
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
        left(normalized_query, 500) AS query
    FROM ranked
    WHERE total_rank <= 25
       OR normalized_query LIKE '%WITH RECURSIVE pages AS (%'
       OR (
           normalized_query LIKE '%qbit_share_ledger%'
           AND normalized_query LIKE '%accepted_at > to_timestamp%'
           AND normalized_query LIKE '%job_issued_at > to_timestamp%'
       )
       OR normalized_query LIKE '%qbit_current_carry_forward_balances()%'
    ORDER BY total_exec_time DESC
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);
```

For each `queryid`, subtract the opening snapshot from the closing snapshot.
Report calls, total execution time, shared blocks read, shared-block read time,
and temp blocks. Calculate the interval mean as
`total_exec_time_delta / calls_delta`. Keep separate filtered views for the
pages oracle, delta query, and carry aggregate. If a captured queryid is absent
from the explicit filters at either boundary, mark it not comparable and fix
the capture before the next window; top-N membership alone must never imply a
zero counter.

Expected after rollout: the pages CTE runs only for cold start, explicit
reconcile/correction fallback, invariant fallback, and periodic self-check. It
should disappear from steady-state top consumers.

The carry-balance aggregate should likewise be limited to cold publication or
forced payout mutations. Normal and debounced artifact logs should report
`prior_balances_source:"published"`.

## 2. Explain the full-rescan oracle

Take `anchor_ms`, `window_shares`, and network difficulty from one coordinator
build event. Set `window_weight` to `16 * network_difficulty`. Use the same
window weight before and after rollout. Run the following in `psql`, supplying
integer variables `anchor_ms` and `window_weight`:

`EXPLAIN ANALYZE` executes the query. Prefer a production replica with
representative data; otherwise run off-peak with operator approval and retain
the statement timeout below. It is read-only, but it can still consume material
I/O and CPU.

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

Set `PRISM_OBSERVATION_SECONDS` to the exact interval length. Include actual
builds, debounced retags, publication aborts, and cached found-block fallbacks
so the duty calculation cannot hide remaining publication work:

```sh
jq -s --argjson seconds "$PRISM_OBSERVATION_SECONDS" '
  [.[]
   | select(.event == "payout_artifact_built"
            or .event == "payout_artifact_build_debounced"
            or .event == "payout_artifact_build_aborted"
            or .event == "payout_artifact_found_block_cached")
   | select(.during_publication == true)
   | (.duration_seconds // 0)]
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
| Carry-balance aggregate calls/hour | | | Cold/payout mutation only |
| Reconcile-prefetch >20s events/hour | | | 0 |
| Sidecar subscribe failures/hour | 50–115 | | Approximately 0 background |
| `PrismStratumCheckFailureRateHigh` | Firing | | Clear |

Do not claim the production win from local simulation. Until both captures are
complete, report these as expected effects and attach the raw before/after
evidence to the rollout record.

## 6. Full-rescan attribution (issue #228)

Every `payout_artifact_built` and `payout_artifact_build_debounced` line now
says why the build was requested. Read these fields together:

| Field | Meaning |
|---|---|
| `invalidation_cause` | The payout-state source cause the candidate was captured with, verbatim (`external_tip`, `accepted_block_preview`, `accepted_block_preview_withdrawn`, `direct_block_uncertain`, `payout_only`, `startup`); `null` for a background preparation, which carries no candidate. |
| `force_origin` | What marked the build as a payout mutation: `caller_flag` (the caller passed `force_full_window_rescan`), `reserved_cause` (the captured cause was `accepted_block_preview_withdrawn` and this preparation consumed it), `caller_flag+reserved_cause`, or `none`. |
| `cause_inherited` | `true` when the capture found a cause another preparation had already acted on. The source tuple is deliberately sticky (candidate supersession and the finalizer's pending-cause read depend on it); before this change every such capture re-forced the full oracle. An inherited cause never forces anything now, so a `true` here with `force_origin: "none"` is the expected shape. |
| `payout_mutated` | Balances were re-read from the ledger because pool-block, payout-entry or carry rows moved (a reorg, a maturity, a withdrawal), by either flag below. |
| `prior_balances_read_forced` | The caller passed `force_prior_balances_read` (the reorg reconciler after a confirmed mutation, issue #224): the accepted-share ledger is append-only, so the window advances from its delta and only the carry aggregate is reread. Never a full rescan. |
| `window_oracle_forced` | The caller passed `force_full_window_rescan`: the fail-closed full oracle, which also rereads balances. In production that is the preview-withdrawal path and the reconciler's error path after an uncertain mutation. |
| `self_check_deferred` | An overdue periodic self-check was left for the next build off the tip-refresh path because this build's cached window was inside the accepted staleness bound (below). |
| `self_check_recentered` | The periodic self-check re-centered an oversized window at the live weight (issue #207). |
| `phase_seconds` | The phase split of this build, see section 7. |

Which builds run the full oracle (`window_build_mode: "full_rescan"`), by
`full_rescan_reason`:

| Reason | Trigger | Expected rate |
|---|---|---|
| `cold_start` | No cached window (process start, or reuse re-enabled). | Once per process. |
| `late_visible_append` | The append-invalidation epoch moved: a share committed below an already-walked anchor. The epoch check is the guard that keeps a failed reorg check from resuming append-only state; it is unchanged. | Rare; alert if sustained. |
| `snapshot_window_weight_out_of_band`, `anchor_regression`, `window_pipeline_mode_changed`, `incremental_invariant_failed`, `window_mirror_divergence`, `window_value_out_of_range`, `delta_api_unavailable` | Genuine epoch breaks detected inside materialization. | Rare. |
| `window_daemon_state_lost` | The Rust daemon no longer holds the base digest after a respawn or LRU eviction. Recovery is one oracle read that re-prepares the daemon. | Tracks daemon restarts. |
| `window_daemon_busy`, `window_daemon_unavailable`, `window_value_out_of_range`, `window_mirror_divergence` (after a `self_check_recentered: true` build) | A periodic self-check adopted a window the daemon could not be prepared for at the time, and the reason names what declined the preparation; the next advance rebuilt through the oracle. A deliberate coordinator adoption, never reported as daemon state loss (issue #207). | Rare. |
| `reconcile_invalidation` | A caller passed `force_full_window_rescan`: the preview-withdrawal path and the reconciler's error path after an uncertain mutation. Since PR #230, reorg reconciliation and payout maturity ask for a prior-balances reread instead (`prior_balances_read_forced: true`, an incremental build), so this reason no longer tracks block cadence. | Withdrawals and reconciler errors only. |

Self-check builds (`self_check_match` / `self_check_mismatch`) report
`periodic_self_check`, with `self_check_recentered: true` when the cached
weight sat more than 25% above the live snapshot weight and the window was
re-centered at the live weight. With the Rust pipeline on, the re-center
prepares the daemon for the adopted digest in the same build, so the next
build advances incrementally; a second full ledger snapshot on the following
build is the regression issue #207 fixed. The rescan reason vocabulary is
closed (`lab/prism/accepted_preview_telemetry.py`, exported as
`qbit_prism_payout_window_full_rescan_seconds{reason,path}`), so the
re-center is a flag on the build rather than a reason of its own.

`payout_artifact_tip_refresh_cached` marks a routine candidate that found the
preparation lock busy and reused the armed window instead of queueing (see
section 8). It counts under `qbit_prism_payout_artifact_events_total{event="tip_refresh_cached"}`
once the tip-refresh exporter lists the event; until then count it from logs.

Per-hour full-rescan count from the extract in section 3:

```sh
jq -r '
  select(.event == "payout_artifact_built" and .window_build_mode == "full_rescan")
  | "\(.full_rescan_reason) \(.force_origin) \(.cause_inherited)"
' "$PRISM_ARTIFACT_EVENTS" | sort | uniq -c
```

## 7. Build phase attribution (issue #228)

`qbit_prism_payout_window_build_phase_seconds{phase=...,outcome=...}` is a
histogram over the same buckets as the other payout timings, observed once
per build for each of a closed phase set, under a closed `outcome`:
`completed`, `debounced`, `aborted` (a lost generation race, an anchor past
the audit ceiling, an empty window) or `failed` (the build died with the
ledger). Every exit observes, so a build slow enough to lose its generation
race keeps the time it measured instead of vanishing from the family; read
the `aborted` and `failed` cells first during an incident. No per-tip,
per-generation or per-reason label exists in this family.

| `phase` | What it measures |
|---|---|
| `ledger_read` | Every PostgreSQL read the build issued: the full oracle, the delta query, the periodic self-check's oracle read, and the carry-balance aggregate when balances were not reused. |
| `record_conversion` | In-process work on records: folding into pages, canonical JSON and digests, mirror verification. |
| `daemon_prepare` | Round trips to the Rust daemon (`prepare_window` full or advance), including any window upload. |
| `lock_wait` | Time queued for the payout-state preparation lock before the build could start. |

The same split is on each build line as `phase_seconds`. The phases account
for the build's `duration_seconds` up to artifact construction and the cheap
accepted-share count. A build whose `ledger_read` dominates is a database
problem; one whose `lock_wait` dominates is queued behind another build; one
whose `daemon_prepare` dominates is the daemon or its upload.

## 8. Accepted staleness on the tip-refresh path (issue #228)

`PRISM_TIP_REFRESH_WINDOW_STALENESS_SECONDS` (60 s, in `lab/prism/payout_state.py`)
is the staleness the coordinator accepts for a routine candidate build, the
one a tip refresh publishes for a new tip. Two rules key on it:

- If the preparation lock is busy and an armed window exists whose anchor is
  no older than the bound (and whose payout generation, append epoch and
  balances are current, and whose difficulty is inside the snapshot-weight
  band), the candidate reuses that window instead of queueing behind the
  lock holder. This is the found-block degrade of issue #188 applied to the
  tip-refresh path.
- If a periodic self-check is overdue when a routine candidate builds, and
  the cached window's anchor is within the bound of the anchor being built,
  the check is deferred, but never for more than one staleness bound past
  its due time. The grace is measured from the last self-check attempt, not
  from the last build, so candidates arriving faster than the bound cannot
  push it out. A deferral also marks the check owed: the next payout
  publication requests a background preparation, which normally runs the
  check off the critical path well inside the grace. If nothing else runs
  it, the first routine build after the grace runs it on the critical path.

Why 60 s: it is one background re-anchor interval. In steady state the
re-anchor keeps the armed window younger than this, so a window inside the
bound is one the delivery path is already serving to new jobs as fresh, and
a fanout that reuses it declares an anchor no older than reuse itself
declares. Past the bound the wave pays the (incremental) build: the
re-anchor has already flagged that window for replacement, and carrying it
into a new payout generation would let a stalled background walker pin a
stale anchor for every following generation. It sits well inside the 300 s
audit ceiling that bounds how far any served window may trail the live
ledger.

What this does not bound: the reorg reconciler acquires the preparation lock
before it captures its source, so a wave still waits for a background build
that holds the lock through a periodic self-check. That wait is one oracle
read at the self-check cadence (two in 48 h on mainnet), not one per
invalidation.

The bookkeeping behind `cause_inherited` is one integer, the highest source
generation any candidate preparation has acted on, plus a one-shot
entitlement carried by the tuple a reservation hands back to its caller. A
capture acts only above that mark; the reserver's own preparation acts once
by presenting its reserved tuple, whatever happened at other generations in
between. Nothing accumulates, so there is nothing to prune or to watch.

## 9. Deliberate re-center versus daemon state loss (issue #207)

| Signal | Deliberate re-center | Genuine daemon eviction or restart |
|---|---|---|
| Self-check build line | `full_rescan_reason: "periodic_self_check"`, `self_check_recentered: true` | not applicable |
| Next build, daemon prepared | `window_build_mode: "incremental"`, no reason | `full_rescan_reason: "window_daemon_state_lost"` |
| Next build, daemon could not be prepared at adoption | the daemon's outcome at adoption: `window_daemon_busy`, `window_daemon_unavailable`, `window_value_out_of_range` or `window_mirror_divergence` | `window_daemon_state_lost` |
| Daemon counters | one extra `window_prepares` at the self-check build | `spawns` increments on a restart |

Both recoveries take the existing path: one full oracle read that
re-prepares the daemon, after which the cache is a daemon mirror again. The
byte-exact parity of the window, the 5/8 band floor, the 2x ceiling and the
>1.25x re-center threshold are unchanged.

## 10. Testnet soak thresholds (issue #228)

Operator-run; the deterministic equivalents are in
`tests/test_prism_payout_state.py` (`PayoutWindowRescanBoundTests`) and
`tests/test_prism_payout_window_daemon_recenter.py`. Run a 24 h soak on
testnet with `PRISM_WINDOW_PIPELINE_RUST=1` and extract events as in section
3, then:

```sh
jq -s '
  [.[] | select(.event == "payout_artifact_built")] as $built
  | {
      full_rescans: ($built | map(select(.window_build_mode == "full_rescan")) | length),
      reconcile_invalidation: ($built | map(select(.full_rescan_reason == "reconcile_invalidation")) | length),
      daemon_state_lost: ($built | map(select(.full_rescan_reason == "window_daemon_state_lost")) | length),
      inherited_forces: ($built | map(select(.cause_inherited == true and .force_origin != "none")) | length),
      p95_build_seconds: ($built | map(.duration_seconds) | sort | .[((length * 0.95 | ceil) - 1)])
    }
' "$PRISM_ARTIFACT_EVENTS"
```

Pass criteria for the 24 h window, before any mainnet rollout:

| Measure | Threshold |
|---|---|
| Full rescans per hour (`full_rescans / 24`) | at most 2 |
| `reconcile_invalidation` full rescans | 0 |
| `window_daemon_state_lost` full rescans | at most the daemon `spawns` count over the window |
| `inherited_forces` | 0 |
| p95 `duration_seconds` of `payout_artifact_built` | at most 3 s |
| Tip-refresh waves with elapsed above 3 s whose outcome is `fanout_superseded` | none attributable to a payout build (`phase_seconds.lock_wait` under 1 s on every build during the wave) |
