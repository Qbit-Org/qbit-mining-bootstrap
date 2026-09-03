# PRISM Overload and Backlog Alert Specification

Issue #188. This document specifies the alerting rules for PRISM's
landing-path, delivery, and backlog overload classes. It is a
specification only.

## Status: not live

**These alerts are not live.** This repository contains no Prometheus,
Alertmanager, or Grafana configuration and does not own deployment
monitoring. Nothing here fires until the separate deployment/Prometheus
repository transcribes these rules into its own rule files and applies
them. Treat this document as the source of truth for *what* to alert on
and *why*; that repository remains the source of truth for *how* the
rules are deployed, routed, and silenced.

## Ordering: #184 must land before these rules are deployed

**#184 must be present in the deployed PRISM build before the external
rule repository applies these alerts.** In the combined delivery, #184 is
integrated ahead of this specification; the requirement below governs
external rule deployment against any given running build.

Every body-metric rule depends on a *successful* scrape, and the scrape
status of a degraded `/metrics` differs across #184:

- **Before #184:** a stale complete snapshot was served as **HTTP 503**.
  Prometheus records a failed scrape for a 503 and discards the response
  body, so every series in this document went *absent* in precisely the
  incident where the endpoint was degraded.
- **After #184:** a stale complete snapshot is served as **HTTP 200 with
  freshness metadata**, so `qbit_prism_metrics_snapshot_stale`,
  `..._age_seconds`, and the whole body are ingested and can be alerted
  on. Only **warm-up, when no complete snapshot exists yet**, still
  returns 503.

Deploying these rules against a pre-#184 build would therefore invert
their meaning: an endpoint failing its own health contract would produce
no firing alerts at all, and that silence would be indistinguishable from
a healthy pool. Confirm #184 is in the target build, then apply these
rules, and always apply `PrismScrapeDown` (below) alongside them — body
metrics cannot report their own total absence, and the warm-up 503 window
remains unscrapable by construction.

## Signal inventory

All names and export sites below were verified against the source. Every
series is a single time series per target; none carries labels beyond the
target's own `job`/`instance`. Two rows include the accompanying #188 gauge
change: the new cap gauge and the corrected semantics of the
unresolved-transitions count.

| Metric | Type | Exported at | Status |
| --- | --- | --- | --- |
| `qbit_prism_accepted_parent_preview_wait_timeouts_total` | counter | `lab/prism/metrics.py` | already present |
| `qbit_prism_accepted_parent_unresolved_transitions` | gauge, exact admission-fence depth | `lab/prism/metrics.py` | present, **semantics corrected by the accompanying #188 gauge change** |
| `qbit_prism_accepted_parent_unresolved_depth_max` | gauge | `lab/prism/metrics.py` | **added by the accompanying #188 gauge change** |
| `qbit_prism_accepted_parent_unresolved_oldest_seconds` | gauge, oldest over timestamped transitions, `-1` when none | `lab/prism/metrics.py` | already present |
| `qbit_prism_block_candidates_pending` | gauge, `-1` if unavailable | `lab/prism/metrics.py` | already present |
| `qbit_prism_block_candidate_oldest_pending_seconds` | gauge, `-1` if unavailable | `lab/prism/metrics.py` | already present |
| `qbit_prism_stratum_semantic_current_work_ratio` | gauge, `1.0` when no clients | `lab/prism/metrics.py` | already present |
| `qbit_prism_stratum_authorized_connections` | gauge | `lab/prism/metrics.py` | already present |
| `qbit_prism_refresh_pending` | gauge, 0/1 | `lab/prism/progress_health.py` | already present |
| `qbit_prism_refresh_pending_age_seconds` | gauge, `0` when not pending | `lab/prism/progress_health.py` | already present |
| `qbit_prism_metrics_snapshot_available` | gauge, 0/1; startup coverage only — stays `1` after warm-up even when the refresh is stale | `lab/prism/observability.py` | already present |
| `qbit_prism_metrics_snapshot_stale` | gauge, 0/1 | `lab/prism/observability.py` | already present |
| `qbit_prism_metrics_snapshot_age_seconds` | gauge, `-1` before first success | `lab/prism/observability.py` | already present |
| `up` | gauge, 0/1 | synthesised by Prometheus | not a PRISM series |

What the gauge task adds and corrects:

**Adds the cap.** Before it, the configured unresolved-depth cap
(`PRISM_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX`, default 8, in
`lab/prism/coordinator_config.py`) was readable only from process
configuration, so no rule could express "at or near the cap" without
hard-coding a deployment-specific constant. Exporting the cap as
`qbit_prism_accepted_parent_unresolved_depth_max` makes the
`PrismUnresolvedParentDepthAtCap` rule a *ratio against the safety
contract itself*, which stays correct when an operator retunes the cap.

**Corrects the count.** `qbit_prism_accepted_parent_unresolved_transitions`
now reports the **exact depth used by the admission fence** — every landed
transition whose durable bookkeeping is unresolved, including landed
transitions that carry no timestamp. It was previously derived from the
timestamped-ages list and so could under-report relative to the depth that
actually gates job issuance (`_await_pending_parent_payout_preview` in
`lab/prism/payout_state.py`). With the correction, rule 2 compares like
with like: the gauge and the fence agree exactly, so the at-cap alert fires
precisely when issuance stops.

`qbit_prism_accepted_parent_unresolved_oldest_seconds` is deliberately
**not** changed to match that population. It remains the maximum age over
transitions with a known landing timestamp, and reports `-1` when there
are none. An untimestamped transition therefore contributes to the depth
gauge but not to the age gauge — which is correct, since no age is known
for it and none is invented. The practical consequence for alerting: the
depth signal (rule 2) is the complete one, and the age signal (rule 3) can
read `-1` or a low value while depth is nonzero. Use rule 2, not rule 3,
to judge whether the fence is close to blocking.

Explicitly **not** used: current-tip coverage
(`current_tip_job_coverage`) and the initial-job queue series
(`pending_initial_jobs`, `pending_initial_job_capacity`). They are
similarly named but measure different populations; the semantic ratio is
the signal that accounts for both template fingerprint and payout
generation.

## Threshold provenance

Thresholds fall into two honestly distinct classes.

**Configuration- and contract-derived (transfers to mainnet).** These are
read from the code's own safety contract and stay valid at any cadence:

- the accepted-parent preview wait budget, 5.0 s
  (`DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS` in
  `lab/prism/payout_state.py`);
- the unresolved-depth cap, default 8
  (`DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX` in
  `lab/prism/coordinator_config.py`), compared as a ratio via the exported
  gauge rather than as a literal;
- the pending-refresh health deadline, 15.0 s
  (`DEFAULT_PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS` in
  `lab/prism/coordinator_config.py`);
- the degraded-coverage line, 0.95, borrowed from the in-process health
  evaluator, which treats a sustained loss of five percent of *current-tip*
  job coverage as degraded in `lab/prism/observability.py`; the
  semantic ratio is the stricter sibling of that coverage ratio and is not
  itself thresholded in-process, so 0.95 is applied here by analogy rather
  than read from an existing rule;
- the snapshot staleness floor, 15.0 s (`MINIMUM_HEALTH_STALE_SECONDS` in
  `lab/prism/observability.py`); the effective budget is
  `max(3 × refresh interval, 15 s)`.

**Capture-derived (testnet-cadence-dependent, must be re-baselined).**
The 2026-08-20 testnet4 capture ramped from 0.124 to 32.24 accepted
shares/s over five minutes — a ~260× increase — and under that load
reached 3,120 durable pending candidates, 160 s oldest pending age, and a
semantic coverage floor of 0.077. Absolute candidate counts, candidate
rates, and any short-window count-based timeout thresholds derived from
that run are functions of testnet4 block cadence and of that specific
ramp. They are starting points, **not mainnet constants**. Each such
threshold is marked *cadence-dependent: yes* below and must be
re-baselined against a mainnet-cadence capture before it is trusted to
page anyone.

Where a class could be expressed either as a count or as an age, this
specification prefers the age or the configuration ratio, because those
transfer across cadences and the counts do not.

## Alert rules

Severity convention: `warning` means investigate within the working day;
`critical` means page. `for` is the pending window.

### 1. `PrismAcceptedParentPreviewWaitTimeouts`

- **Purpose:** child job builds are timing out waiting for an
  accepted-parent payout preview, so job issuance is being blocked behind
  unfinished parent bookkeeping.
- **Condition (warning):**
  `increase(qbit_prism_accepted_parent_preview_wait_timeouts_total[15m]) > 0`
- **Condition (critical):**
  `increase(qbit_prism_accepted_parent_preview_wait_timeouts_total[15m]) > 10`
- **`for`:** `5m` (warning), `10m` (critical)
- **Interpretation / first action:** any increment means at least one
  child build waited the full 5 s preview budget and failed closed. Check
  `qbit_prism_accepted_parent_unresolved_oldest_seconds` and the
  landing-class ledger timings
  (`qbit_prism_block_ledger_call_timeouts_total{call_class="landing"}`)
  to find which stage of the landing tail is not completing.
- **Cadence-dependent:** the *warning* (any increment) is not — it is a
  fail-closed event and is meaningful at any cadence. The *critical* count
  of 10 per 15 min **is** cadence-dependent: it counts events whose
  natural rate scales with block and job-rebuild frequency. Re-baseline
  it.

### 2. `PrismUnresolvedParentDepthNonzero` / `PrismUnresolvedParentDepthAtCap`

- **Purpose:** landed accepted-block transitions whose durable bookkeeping
  is unresolved stack prospective balance chains on unfinished work. At
  the cap, job issuance stops entirely
  (`_await_pending_parent_payout_preview` in
  `lab/prism/payout_state.py`).
- **Condition (warning, nonzero and sustained):**
  `qbit_prism_accepted_parent_unresolved_transitions > 0`
- **Condition (warning, near cap):**
  ```
  qbit_prism_accepted_parent_unresolved_transitions
    >= on(job, instance)
      (qbit_prism_accepted_parent_unresolved_depth_max - 1)
  ```
- **Condition (critical, at cap):**
  ```
  qbit_prism_accepted_parent_unresolved_transitions
    >= on(job, instance) qbit_prism_accepted_parent_unresolved_depth_max
  ```
- **`for`:** `10m` (nonzero), `2m` (near cap), `1m` (at cap)
- **Interpretation / first action:** a transient nonzero depth is normal
  right after a block lands; a depth that does not drain within ten
  minutes means the accounting lane is not resolving. At the cap, new
  child jobs are already being refused with `TemplateRefreshBlocked` —
  this is a mining outage, not a warning sign. Look at the landing-class
  ledger call durations and at whether the accounting lane is wedged. With
  the #188 gauge task's correction, this gauge is the exact depth the
  admission fence uses, so the at-cap condition and the refusal coincide
  rather than the alert trailing it.
- **Cadence-dependent:** no. Both are expressed against the exported cap,
  which is the configured safety contract.

### 3. `PrismUnresolvedParentAgeOverBudget`

- **Purpose:** the oldest unresolved accepted-parent transition has
  outlived the preview wait budget, so any child build selecting that
  parent will time out.
- **Condition (warning):**
  `qbit_prism_accepted_parent_unresolved_oldest_seconds > 5`
- **Condition (critical):**
  `qbit_prism_accepted_parent_unresolved_oldest_seconds > 60`
- **`for`:** `2m` (warning), `5m` (critical)
- **Interpretation / first action:** 5 s is exactly the preview wait
  budget, so this alert fires at the moment the backlog starts causing
  build timeouts rather than after. Expect rule 1 to follow if this is not
  resolved. The `-1` sentinel (no timestamped unresolved transitions) is
  naturally excluded by a `>` comparison against a positive threshold.
  Note the population difference from rule 2: this gauge covers only
  transitions with a known landing timestamp, so it can read `-1` or a low
  value while the depth gauge is nonzero. That is not a bug and no age is
  invented for an untimestamped transition — but it means rule 3 must not
  be used to rule out a depth problem. Rule 2 is the complete signal.
- **Cadence-dependent:** no. Both thresholds are anchored to the
  configured 5 s budget.

### 4. `PrismBlockCandidateBacklog` / `PrismBlockCandidateOldestPending`

- **Purpose:** durable block candidates are accumulating without reaching
  a terminal outcome.
- **Condition (warning, count):** `qbit_prism_block_candidates_pending > 250`
- **Condition (critical, count):** `qbit_prism_block_candidates_pending > 1000`
- **Condition (warning, age):**
  `qbit_prism_block_candidate_oldest_pending_seconds > 60`
- **Condition (critical, age):**
  `qbit_prism_block_candidate_oldest_pending_seconds > 300`
- **Condition (warning, unavailable):**
  `qbit_prism_block_candidates_pending == -1`
- **`for`:** `10m` (counts), `5m` (ages), `10m` (unavailable)
- **Interpretation / first action:** prefer the **age** alerts as the
  primary signal; a large but fast-draining queue is healthy, and a small
  queue whose head is five minutes old is not. On the age alert, check the
  submitter's retry backoff series
  (`qbit_prism_block_submitter_retry_backoff_active`) to distinguish an
  intentional interruptible wait from a wedged lane.
- **Cadence-dependent:** the **count** thresholds are — yes. They are
  scaled from the capture's 3,120-candidate peak (warning at roughly 8% of
  it, critical at roughly a third) under a testnet4 share rate of
  32.24/s, and mean nothing at a different cadence. The **age**
  thresholds transfer far better and should be the ones that page; the
  capture's 160 s oldest age sits between them. Re-baseline the counts.

### 5. `PrismSemanticWorkCoverageLoss`

- **Purpose:** authorized clients are mining work that no longer matches
  the current template fingerprint and payout generation.
- **Condition (warning):**
  ```
  qbit_prism_stratum_semantic_current_work_ratio < 0.95
    and on(job, instance) qbit_prism_stratum_authorized_connections > 0
  ```
- **Condition (critical):**
  ```
  qbit_prism_stratum_semantic_current_work_ratio < 0.5
    and on(job, instance) qbit_prism_stratum_authorized_connections > 0
  ```
- **`for`:** `3m` (warning), `5m` (critical)
- **Interpretation / first action:** sustained coverage loss means miners
  are producing shares against superseded work; those shares are wasted
  hashrate and will show up as stale/unknown rejections. Check whether
  job delivery or template refresh is the stalled side — rule 6 covers
  the refresh side directly.
- **Cadence-dependent:** no. The ratio is normalised by client count, and
  0.95 is carried over from the in-process degraded-coverage line for the
  sibling current-tip ratio (see *Threshold provenance*). The capture's
  0.077 floor is context for how far this can fall, not the threshold.

### 6. `PrismRefreshPendingPastDeadline`

- **Purpose:** current template or payout work still requires publication
  or delivery, past the documented health deadline.
- **Condition (warning):**
  ```
  qbit_prism_refresh_pending == 1
    and on(job, instance) qbit_prism_refresh_pending_age_seconds > 15
  ```
- **Condition (critical):**
  ```
  qbit_prism_refresh_pending == 1
    and on(job, instance) qbit_prism_refresh_pending_age_seconds > 60
  ```
- **`for`:** `1m` (warning), `5m` (critical)
- **Interpretation / first action:** 15 s is the default
  `PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS`, i.e. the point at which
  the coordinator's own health evaluator considers the refresh overdue.
  Firing here means the process is already reporting itself degraded on
  this axis. If the deployment overrides that environment variable, the
  literal `15` must be changed to match, since the deadline is not
  currently exported as a series.
- **Cadence-dependent:** no. Both thresholds derive from the documented
  default deadline.

### 7. `PrismMetricsSnapshotStale` / `PrismMetricsSnapshotUnavailable` / `PrismScrapeDown`

- **Purpose:** detect that the observability path itself has failed, so
  that the silence of every rule above is never read as health.
- **Condition (warning, stale):** `qbit_prism_metrics_snapshot_stale == 1`
- **Condition (critical, no complete snapshot):**
  `qbit_prism_metrics_snapshot_available == 0`
- **Condition (warning, age):**
  `qbit_prism_metrics_snapshot_age_seconds > 45`
- **Condition (critical, scrape loss):** `up{job="prism"} == 0`
- **`for`:** `2m` (stale, no complete snapshot, age), `2m` (scrape loss)
- **Interpretation / first action:** on `up == 0`, treat every other
  PRISM alert in this document as *unknown*, not *clear*, and check the
  coordinator process and the `/metrics` status code directly. Post-#184,
  a 503 means warm-up — no complete snapshot has been published yet —
  which is a distinct failure from the process being down and from a
  refresh that has stalled after warm-up. **The stale and age conditions
  are the ones that detect a stalled refresh at runtime**, because
  post-#184 that state is served as 200 and is therefore ingested.
  `qbit_prism_metrics_snapshot_available` is **startup coverage only**:
  once warm-up has succeeded, it is part of the last served complete
  payload and stays `1` even while the refresh is stale, so it does not
  detect refresh failure. It also may not be ingested at all during the
  warm-up window it describes, since that window is exactly when the
  endpoint answers 503 — keep it for the case where a partial payload is
  still served, and rely on `up == 0` for warm-up that never completes.
- **Cadence-dependent:** no. 45 s is 3 × the 15 s staleness floor; if the
  deployment sets a metrics refresh interval above 15 s, the effective
  budget becomes `3 × refresh` and this literal should be raised to match.

## False-positive and silence cautions

**Scrape status and the 503 window.** Prometheus does not ingest the body
of a failed scrape, so any state served as 503 is invisible to body-metric
rules. Before #184 this swallowed the whole stale case:
`qbit_prism_metrics_snapshot_stale == 1` could essentially never be
*observed*, because the condition that set it to 1 was the same condition
that stopped the series being ingested. After #184 a stale complete
snapshot is served as 200 with freshness metadata, so the stale and age
conditions in rule 7 become genuinely observable and are the intended
detectors for a stalled refresh. What remains unscrapable is the warm-up
window with no complete snapshot, which still answers 503 — meaning
`qbit_prism_metrics_snapshot_available == 0` may itself go uningested in
exactly the situation it describes. `up == 0` is therefore the detector
that actually fires in the total-failure and never-warmed-up cases.
**Never deploy the body-metric rules without `PrismScrapeDown`.**

**No metric can report its own absence.** Every rule in sections 1–6 is
silent when the target is gone. Route `PrismScrapeDown` to the same
destination as the critical rules, and consider an Alertmanager inhibition
so that scrape loss suppresses the (now meaningless) body-metric alerts
rather than the reverse.

**`-1` sentinels are values, not gaps.** Four gauges in the #188 rules use
`-1`: `qbit_prism_accepted_parent_unresolved_oldest_seconds` (no unresolved
transitions *carrying a landing timestamp* — see rule 3, which can read
`-1` while the depth gauge is nonzero),
`qbit_prism_block_candidates_pending` and
`qbit_prism_block_candidate_oldest_pending_seconds` (state unavailable),
and `qbit_prism_metrics_snapshot_age_seconds` (before first success); issue
#198's `qbit_prism_block_candidate_cleanup_retry_oldest_seconds` (nothing
owed) is a fifth, covered in its own section below. All
threshold comparisons above are `>` against positive numbers, so `-1`
never triggers them — but any rule added later using `<`, `avg_over_time`,
`min_over_time`, or a rate must exclude `-1` explicitly, or it will read
"unavailable" as "excellent". The dedicated
`qbit_prism_block_candidates_pending == -1` rule exists precisely because
the healthy-looking `-1` would otherwise hide a lost candidate store.

**The zero-client ratio reads 1.0, not 0.**
`qbit_prism_stratum_semantic_current_work_ratio` is computed as
`semantic_current / authorized if authorized else 1.0`
(`_mining_delivery_snapshot_serialized` in
`lab/prism/observability.py`). With no authorized connections it
reports *perfect* coverage. The
`qbit_prism_stratum_authorized_connections > 0` guard therefore does not
prevent a false positive — the ratio cannot false-positive at zero
clients — it makes explicit that this alert measures coverage *among
connected miners* and is not a pool-liveness signal. A pool that has lost
every miner is invisible here and needs a separate alert on
`qbit_prism_stratum_authorized_connections` itself, which this document
does not specify.

**Counter resets.** `qbit_prism_accepted_parent_preview_wait_timeouts_total`
is an in-process counter that resets to 0 on restart. Rule 1 uses
`increase()`, which handles resets correctly; a naive
`... > N` on the raw counter would both latch forever after one incident
and silently clear on every deploy. Do not "simplify" it to a raw
comparison. Note also that `increase()` over a short window on a
low-frequency counter is extrapolated and can report fractional values —
the `> 0` form is robust to this, the `> 10` critical form less so, which
is a further reason to re-baseline it.

**Restart flapping.** After a coordinator restart, unresolved depth,
pending candidates, and refresh age all start from a replay state rather
than a steady state. The `for` windows above are chosen to ride out a
normal restart; if a deployment's restarts routinely exceed them, add a
startup-grace inhibition rather than lengthening every window.

## Re-baselining checklist

Before these rules are trusted on mainnet:

1. Capture a mainnet-cadence load run equivalent to the 2026-08-20
   testnet4 capture.
2. Recompute the *cadence-dependent* thresholds only — the critical
   timeout count in rule 1 and both candidate-count thresholds in rule 4.
3. Leave the configuration-derived thresholds alone unless the underlying
   defaults change; if `PRISM_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX`,
   `PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS`, or the metrics refresh
   interval is overridden in deployment, update rules 2, 6, and 7
   respectively.
4. Confirm the target build contains #184 — that a stale complete snapshot
   is served as 200 with freshness metadata rather than 503 — before
   relying on rule 7's stale and age conditions, which are the runtime
   detectors. Remember that the remaining 503 case, warm-up with no
   complete snapshot, is covered by `up == 0` rather than by
   `qbit_prism_metrics_snapshot_available`.

## Issue #198: the bounded collapse cleanup-retry backlog

Issue #198. The decided-height collapse (#183, #196, #181 item 2) wins rows
with one fenced batch write and then tears down each won row's in-memory
state — its payout preview or tombstone, its pending-share floor holder,
its retry, outstanding and tip-observation markers, its terminal-outcome
record and its abandonment accounting. A won row is durably terminal the
moment the write returns, so a cleanup step that fails has no durable
replay source left: the coordinator keeps one in-memory retry record per
such row (`_CollapsedCandidateCleanup` in `lab/prism/block_candidates.py`)
and the accounting lane retries one hash per pass. Under a *systemic*
cleanup fault that registry grew by one record per row the collapse won,
with no bound at all, each record retaining its exact pending-share holder
objects and pinning its terminal-outcome fence against eviction.

The bound this section documents is applied to **admission, never to
cleanup authority**. Once the registry holds the configured number of
records, the collapse stops handing rows to its fenced bulk
terminalization; every row it declines stays durable and pending and takes
the ordinary per-row path. Nothing already terminal ever loses its record,
its holders or its fence, and #196's selection predicate and fencing are
untouched: the same rows are selected, only fewer of them are admitted per
pass. If a forced replay walk fills a page after backpressure engages, it
adopts that one bounded page and pauses pagination until the replay queue has
drained. The startup enumeration remains owed, so job builds stay blocked and
the walk resumes from the durable outbox instead of materializing every later
page in the unbounded replay queue.

### Signal inventory

All names were verified against `lab/prism/metrics.py`
(`block_candidate_cleanup_backlog_metrics_lines`). Every series is a single
unlabelled time series per target; none can grow with the candidate
population or carry a hash. The backlog snapshot they render is copied
under the coordinator lock in one O(backlog) walk, which the bound keeps at
a few thousand entries at most.

| Metric | Type | Meaning |
| --- | --- | --- |
| `qbit_prism_block_candidate_cleanup_retry_backlog` | gauge | Durably terminal collapsed rows whose in-memory cleanup is still owed (registered or actively retrying authority depth). |
| `qbit_prism_block_candidate_cleanup_retry_backlog_max` | gauge | The configured admission bound, exported so rules compare against the running contract rather than a literal. |
| `qbit_prism_block_candidate_cleanup_retry_oldest_seconds` | gauge, `-1` when nothing is owed | Age of the oldest owed cleanup measured from its *first* deferral (a failed retry re-registers the record without resetting this). |
| `qbit_prism_block_candidate_cleanup_retry_pending_share_holders` | gauge | Pending-share floor holders the backlog retains, by exact object identity. |
| `qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins` | gauge | Terminal-outcome fences the backlog pins against eviction. Equals the depth whenever the apply's fence publication succeeded; a smaller value names records whose fence is still owed to the `terminal-outcome` step. |
| `qbit_prism_block_candidate_cleanup_backpressure_active` | gauge, 0/1 | Whether the backlog is at its bound and bulk terminalization is refusing new rows. |
| `qbit_prism_block_candidate_cleanup_backpressure_total` | counter | Occasions on which the bound preserved at least one row. |
| `qbit_prism_block_candidate_collapse_total{outcome="backlog_deferred"}` | counter, closed label set | Rows preserved by the bound (the existing collapse-outcome family gained this one fixed outcome). |

The accompanying warning is rate limited to one line per 60 s
(`BLOCK_CANDIDATE_CLEANUP_BACKPRESSURE_LOG_SECONDS`) and every field is a
count or a closed-vocabulary name; the `backlog_deferred` counter still
moves per row while the line is suppressed:

```text
prism coordinator: collapsed block candidate cleanup backpressure engaged caller=replay-page rows_preserved=1024 admitted=0 backlog=4096 backlog_max=4096 oldest_seconds=12.345; admitting them to bulk terminalization would take the cleanup-retry backlog past its bound, so they stay durable and pending for the per-row path until it drains
```

`caller` is `replay-page` (the replay-adoption page walk) or `dequeue` (the
dequeue-time stale sibling skip); `admitted` is how many rows the same pass
still handed on, so `admitted=2 backlog=0 backlog_max=2 rows_preserved=4`
reads as "two filled an empty backlog, four were declined".

### Configuration knob

`PRISM_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX` (default
`DEFAULT_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX = 4096`, loaded into
`BlockConfig.candidate_cleanup_retry_backlog_max` in
`lab/prism/coordinator_config.py`). Startup refuses a non-integer, a
non-positive value, and any value above
`MAX_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX = 8192`, which restates the
terminal-outcome registry bound (`MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES`; a
test pins the two equal): every retry record pins one fence, and a backlog
allowed past the registry would leave the fence eviction with nothing it
may drop. At runtime the service reads the coordinator attribute
`block_candidate_cleanup_retry_backlog_max` first (the seam tests and
embedders pin), then the loaded block config, then the default. Runtime values
are clamped to the same 1..8192 safety range: a non-positive override degrades
into "admit one row at a time from an empty backlog", while an over-limit
override cannot outgrow the terminal-fence registry.

### Measurement basis

Measured with `tests/prism_candidate_storm.py --cleanup-fault all
--credit-shares` (CPython 3.12.3, Linux x86_64, in-memory ledger, one
thread) at the observed 3,120-candidate cardinality, restart view unless
noted, one fresh storm per run. `CleanupFaultInjector` breaks exactly one
cleanup dependency at its shipped seam — the seven cleanup steps plus the
page-scope holder index whose failure aborts the apply — while counting
every seam per hash; the walk, the fence, the registry and the retry pass
are the shipped code. Two independent memory readings are reported: a
`sys.getsizeof` walk of the shipped registry (records, step sets, hash
keys, and the exact holder objects retained, each charged once), and the
change in live `tracemalloc` bytes attributed to the block-candidate owner
module across the faulted walk, taken as a marginal figure against a clean
walk of the same storm.

| Fault (restart view, 3,120 rows) | Records | Holders | Pins | Registry bytes | Bytes / record (deep) | Owner-traced marginal bytes / record | Recovery passes | Recovery s | Records / s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| payout-preview-withdrawal | 3,119 | 3,119 | 3,119 | 3,018,422 | 967.8 | 560.3 | 3,119 | 0.118 | 26,335 |
| finalize-retry | 3,119 | 3,119 | 3,119 | 3,018,411 | 967.8 | 559.7 | 3,119 | 0.055 | 57,066 |
| retry-state | 3,119 | 3,119 | 3,119 | 3,018,408 | 967.7 | 559.7 | 3,119 | 0.057 | 54,914 |
| outstanding-and-tip-observation | 3,119 | 3,119 | 3,119 | 3,018,428 | 967.8 | 559.7 | 3,119 | 0.065 | 47,792 |
| terminal-outcome | 3,119 | 3,119 | 3,119 | 3,018,413 | 967.8 | 559.7 | 3,119 | 0.068 | 46,173 |
| abandonment-accounting | 3,119 | 3,119 | 3,119 | 3,018,419 | 967.8 | 412.6 | 3,119 | 0.060 | 51,846 |
| pending-share-floor | 3,119 | 3,119 | 3,119 | 3,018,416 | 967.8 | 562.3 | 3,119 | 0.062 | 50,125 |
| floor-index (apply abort) | 3,119 | 0 | 3,119 | 3,351,703 | 1,074.6 | 774.2 | 3,119 | 8.577 | 364 |

Of the 967.8 deep bytes per record, 357.3 are the retained
`PendingShare` holder itself; a record that retains none costs 610.5 bytes
(the live view, where only the 32 queued wakeups own a holder, measures
566.9 bytes per record over 3,119 records and 1,004 bytes per record when
only the 32 holder-owing records exist). The abort-shaped record is larger
(1,074.6 bytes: it owes all seven steps and must re-index its holders) and
drains far slower (364 records/s) because each retry re-scans the whole
replay queue for its holders — an O(n²) cost at storm size that is inherent
to the abort contract, not to the bound.

Every fault also sustained 64 consecutive failing retry passes with depth,
holders and pins unchanged, then recovered with zero double cleanups (each
seam ran exactly once more per hash), zero cleanup calls after a record was
discharged, zero terminal rows replay-adopted, zero node offers, and the
decided winner's floor holder retained while the other 3,119 were released
once each by identity (floor 3,120 → 1). The walk itself took 0.68–1.17 s
per 3,120 rows.

With the bound pinned below the storm (`--backlog-max 1024`): the first
1,024-row page filled the backlog, the remaining three pages (2,096 rows,
the winner among them) were preserved in three engagements, sustained
failure left the 1,024 records intact, recovery drained them in 0.022 s,
and the next walk collapsed the 2,095 preserved siblings in 0.25 s with the
same clean recovery proof. The optional 312,000-candidate run was
attempted (`--candidates 312000 --cleanup-fault retry-state
--credit-shares --no-tracemalloc`) and stopped after 12 min 47 s of CPU
time at about 1.7 GB resident without completing; no figure from it is
used, and none is needed for the derivation.

### Threshold policy: how 4,096 was derived

Two anchors bound the default and two checks confirm it:

1. **Upper anchor — the fence registry.** Every record pins its terminal
   outcome. `MAX_BLOCK_CANDIDATE_TERMINAL_OUTCOMES` is 8,192, so the
   backlog may pin at most half of it and still leave the eviction the
   other half to work with: `≤ 8192 / 2 = 4096`. The knob's hard limit is
   the full 8,192.
2. **Lower anchor — absorption.** The bound must not engage on the storm
   this machinery was built for, nor inside a single page: `≥ 3119`
   (the observed storm's siblings) and `≥ 1024`
   (`MAX_BLOCK_REPLAY_ENUMERATION_ROWS`).
3. **Memory check.** `4096 × 967.8 B ≈ 3.96 MB` with every holder retained
   (`≈ 4.4 MB` for abort-shaped records; `≈ 8.8 MB` at the 8,192 limit).
   The unbounded 312,000-record case the issue describes would have been
   `≈ 302–335 MB` of retained records plus their pinned fences.
4. **Drain check.** On an idle accounting lane `4096 / 26,335 ≈ 0.16 s`
   for a step fault and `4096 / 364 ≈ 11 s` for the abort shape; on a busy
   lane the cadence (`DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS`,
   8) bounds the drain at `4096 × 8 = 32,768` completed work items, which
   is the shipped cadence contract rather than a property of this bound.

`4096` is the only power of two that satisfies both anchors, and both
checks hold at it. It is configuration-derived, not cadence-dependent: a
mainnet storm of a different size changes how quickly the bound is
*reached*, not whether it is correct.

### Alert rules

#### 8. `PrismCollapseCleanupBacklog` / `PrismCollapseCleanupBackpressure`

- **Purpose:** collapsed rows are owing in-memory cleanup that the
  accounting lane is not finishing, and at the bound the collapse has
  stopped bulk-terminalizing new rows.
- **Condition (warning, owed and not draining):**
  `qbit_prism_block_candidate_cleanup_retry_backlog > 0`
- **Condition (warning, oldest owed cleanup):**
  `qbit_prism_block_candidate_cleanup_retry_oldest_seconds > 300`
- **Condition (warning, near the bound):**
  ```
  qbit_prism_block_candidate_cleanup_retry_backlog
    >= on(job, instance)
      (qbit_prism_block_candidate_cleanup_retry_backlog_max * 0.5)
  ```
- **Condition (critical, at the bound):**
  `qbit_prism_block_candidate_cleanup_backpressure_active == 1`
- **Condition (warning, engaged recently):**
  `increase(qbit_prism_block_candidate_cleanup_backpressure_total[15m]) > 0`
- **`for`:** `10m` (owed), `5m` (oldest), `5m` (near), `2m` (at bound),
  `0m` (engaged)
- **Interpretation / first action:** a transient nonzero depth right after
  a storm collapses is normal and drains within seconds (see the measured
  throughput). A depth that holds for ten minutes, or an oldest age past
  300 s — roughly seventeen consecutive failed attempts under the shipped
  0.25 s-doubling-to-30 s backoff — means one cleanup dependency is
  persistently failing; `qbit_prism_block_candidate_collapse_total{outcome="cleanup_retry_failed"}`
  climbs in step and the coordinator log names the failing step
  (`collapsed block candidate cleanup failed step=… hash=…`, detailed for
  the first three per pass). At the bound the process is protecting its
  own memory: preserved siblings take the per-row path, so expect the #181
  symptoms to return for them — one `submitblock`, ~6 chain reads and two
  ledger writes per preserved row, visible as `backlog_deferred` climbing
  and `qbit_prism_block_candidates_pending` draining slowly rather than in
  page-sized steps. That is the intended degradation, not a second fault.
- **Cadence-dependent:** no. Every threshold is against the exported bound
  or the in-process retry pacing.

### Operator symptoms and the recovery contract

Symptoms while the bound is engaged, in the order they appear:

1. `cleanup_failed` and then `cleanup_retry_failed` outcomes climb; the
   backlog, holders and pins gauges rise together.
2. `qbit_prism_block_candidate_cleanup_backpressure_active` flips to 1 and
   one `cleanup backpressure engaged` line prints (then at most one per
   60 s); `backlog_deferred` and `..._backpressure_total` climb per pass.
3. Preserved rows stay in `qbit_prism_block_candidates_pending` and are
   disposed of by the per-row path at per-row cost. Nothing is abandoned
   on an unknown, nothing is offered twice, and no terminal row is
   re-adopted: the fence published at the write still holds for every
   backlog entry.

The recovery contract, proven by `tests/test_prism_block_candidates.py`
(deterministically, including the active-retry window and at the shipped
bound) and corroborated at 3,120 rows by
`tests/test_prism_candidate_storm.py`: once the failing dependency heals,
the accounting lane drains one hash per retry pass — immediately while
idle, otherwise once per `DEFAULT_BLOCK_ACCOUNTING_CLEANUP_RETRY_WORK_ITEMS`
completed work items — and each hash runs exactly the steps it still owes,
once. Fences stay published throughout; holders are released exactly once
by object identity and only for the collapsed row's own object; records,
holders and pins are never shed while the fault persists. The moment the
depth drops below the bound admission resumes, and the next enumeration
walk collapses the preserved rows in bulk again. No operator action is needed
beyond fixing the dependency, and no restart is required for durable recovery.
A controlled restart is nevertheless a valid short-term mitigation: the
backlog, retained holders, and terminal-outcome pins are process-local and are
cleared, while the durable terminal rows are not selected by pending-row replay
and therefore are neither re-adopted nor re-offered. Restarting does not repair
the cleanup dependency; new collapses can rebuild the backlog and re-engage
backpressure until the underlying fault is fixed.

Re-baselining: rerun `python3 tests/prism_candidate_storm.py --cleanup-fault
all --credit-shares` (add `--cleanup-view live`, `--backlog-max N`) and
re-derive step 3 and step 4 above from its `registry_bytes_per_record` and
`retry_records_per_second`; the two anchors are configuration constants
and do not move with cadence.
