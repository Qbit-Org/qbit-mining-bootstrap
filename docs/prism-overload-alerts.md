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

**`-1` sentinels are values, not gaps.** Four gauges here use `-1`:
`qbit_prism_accepted_parent_unresolved_oldest_seconds` (no unresolved
transitions *carrying a landing timestamp* — see rule 3, which can read
`-1` while the depth gauge is nonzero),
`qbit_prism_block_candidates_pending` and
`qbit_prism_block_candidate_oldest_pending_seconds` (state unavailable),
and `qbit_prism_metrics_snapshot_age_seconds` (before first success). All
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
