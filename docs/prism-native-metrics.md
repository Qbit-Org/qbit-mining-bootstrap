# Native cutover metrics

The coordinator (`run`) owns one process-local registry shared by Stratum,
background collectors, and the runtime monitor. `/metrics` renders cached
observations, with #277 freshness, collector age/availability, and runtime state evaluated at scrape time.
Scraping performs no database, node, or filesystem I/O. Public-api metrics retain
their existing contract.

All names below have the `qbit_prism_` prefix. Every family has HELP and TYPE
metadata, including families declared without samples. Histogram boundaries in
seconds are 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, and +Inf.
Histograms also export `_sum` and `_count`.

## Cutover inventory

| Family suffix | Type | Labels | Producer / status |
| --- | --- | --- | --- |
| `share_ack_seconds` | histogram | `result=accepted,rejected` | Complete `mining.submit` frame receipt to successful response write, using Tokio's monotonic clock; partial-frame waiting and post-ACK hints excluded |
| `rejections_total` | counter | `reason_id` | Stratum rejection decision; includes failed rejection-response writes |
| `stale_shares_total` | counter | none | Stale-job and unknown-job rejections |
| `duplicate_shares_total` | counter | none | Duplicate rejection decisions |
| `low_difficulty_shares_total` | counter | none | Low-difficulty rejection decisions |
| `grace_credited_shares_total` | counter | none | Coordinator's actual stale decision, only after durable `Ok(true)` acceptance |
| `stratum_pending_initial_jobs` | gauge | none | Authorized local sessions awaiting usable work, including reauthorization |
| `stratum_oldest_pending_initial_job_seconds` | gauge | none | Oldest such wait, from authorization to successful job notification; zero when none wait |
| `stratum_current_tip_coverage_gap_seconds` | gauge | none | Continuous native generation coverage below 95%, reset at or above 95%; unknown before the first snapshot |
| `stratum_semantic_current_work_ratio` | gauge | none | Existing generation coverage divided by authorized sessions; one with no authorized sessions |
| `block_submit_seconds` | histogram | none | **Declared, not yet populated**; A/#266 owns locally validated proof to first node-offer timestamp transport |
| `block_candidates_pending` | gauge | none | PostgreSQL count of outbox rows with `state='pending'`, including claimed/retry-delayed rows |
| `block_candidate_oldest_pending_seconds` | gauge | none | Oldest pending creation time relative to the same PostgreSQL transaction timestamp; clamped to zero for future timestamps |
| `database_pool_acquire_seconds` | histogram | `result=success,failure` | Collector's actual `PgPool::acquire` only, excluding transaction BEGIN and queries; other ledger acquisition sites remain unwired |
| `database_advisory_lock_wait_seconds` | histogram | `lock=migration,order,settlement`; `result=success,failure` | **Declared, not yet populated**; A/C own accounting-lock call sites after #283 |
| `collector_available` | gauge | `collector=database,process` | One only while the latest completed attempt succeeded and its observation is at most 30 seconds old |
| `collector_success` | gauge | `collector=database,process` | Latest attempt succeeded (1), failed/cancelled (0), or no attempt completed (-1) |
| `collector_age_seconds` | gauge | `collector=database,process` | Monotonic age of last success; -1 before success; failure does not refresh it |
| `process_resident_memory_bytes` | gauge | none | Linux `/proc/self/status` VmRSS in bytes; -1 when unsupported, failed, or stale |
| `runtime_lag_seconds` | gauge | none | Runtime sampler wake lateness; -1 before its first observation |
| `runtime_poll_lag_seconds` | gauge | `task` | Largest active poll or completed poll retained for 60 to 61 seconds |
| `runtime_progress_age_seconds` | gauge | `task` | Oldest active explicit operation's time since progress; zero when idle |
| `runtime_task_stalled` | gauge | `task` | Active poll beyond two seconds or explicit operation beyond its budget |

Reject labels are the closed `RejectReason` enum: `stale-job`,
`duplicate-share`, `low-difficulty`, `malformed-submit`, `unauthorized-worker`,
`unknown-job`, `invalid-extranonce`, `invalid-ntime-or-nonce`,
`backend-rpc-unavailable`,
`internal-error`, `pool-closed`, and `ledger-confirmation-failed`. Unknown internal reason IDs map to
`internal-error`; protocol responses are unchanged.

Task labels are `refresh`, `submit`, `block_wait`, `broadcast`, `rollup`,
`health_publisher`, `stratum_listener`, `stratum_session`, and `collector`.
Session futures and server-owned background futures are monitored. Only the
health publisher currently registers an explicit operation-progress budget.
Idle socket reads and asynchronous waits are not blocked polls. A blocked poll
can degrade live `/healthz` and `health_state` when a surviving worker serves the
probe; a completed long poll retains timing evidence but does not keep readiness
failed. An entirely blocked runtime cannot serve HTTP.

## Failure and compatibility contract

A failed response write is not an ACK. A durable share can be credited while its
ACK write fails; cancellation before a rejection or durable acceptance produces
neither event. Existing accepted/rejected totals keep their original Stratum
accounting boundary. The grace hook does not reinterpret the caller's grace
hint; it uses the coordinator's stale decision after the database confirms credit.

Collectors run every ten seconds. Database collection uses a read-only,
repeatable-read transaction with a three-second overall deadline, a two-second
statement timeout, and a 500 ms lock timeout. Failure or cancellation makes the
measurements unknown (-1), sets success/availability to zero, and leaves the
last-success timestamp intact. Observations expire after 30 seconds even if no
new attempt finishes. A newer collection attempt supersedes an older result;
late completion or cancellation cannot replace the newer publication. A real
zero count, age, or RSS remains valid after successful collection.

Pool timing pre-registers both result labels at count zero and records
observations only for acquisition attempts that complete, including completed
acquisition errors. Overall collector cancellation during acquisition
does not invent a completed wait. The collector status records that failure.
Candidate count and age describe database time; this is not a monotonic latency
measurement. A/#266 must update the pending predicate if outbox states change.

The existing twelve coordinator families keep their names and types:
`health_state`, `runtime_workers`, `connections`, `authorized_clients`,
`pending_job_builds`, `authorized_with_current_work`,
`authorized_missing_current_work`, `accepted_shares_total`,
`rejected_shares_total`, `blocks_total`, `job_delivery_successes_total`, and
`job_delivery_failures_total`. The #277 `metrics_snapshot_available`,
`metrics_snapshot_stale`, and `metrics_snapshot_age_seconds` names, labels,
HELP/TYPE lines, headers, and atomic body/timestamp publication remain unchanged.
The coordinator adds the three health compatibility aliases whose native sources
are known; the fixture deliberately identifies unmapped legacy fields.

No new configuration setting is introduced. Worker slots, per-worker series,
node gauges, rollup-lag series, cardinality/privacy qualification, payout build
and landing-phase instrumentation are outside this trimmed change.

## Follow-up ownership

#279 must treat first-offer and advisory-lock timing as **declared, not yet
populated** and must not attach alerts to these families yet. Pool timing covers
only the collector, not ledger hot paths. A/C wire the remaining timing sites
after #283/#266; this PR references #278 rather than closing it.

The #280 rebase must preserve the narrow hooks in `stratum::request` (complete
frame time, rejection decision, and successful write) and the durable
`MiningBackend::submit` branch in `coordinator.rs`; its extracted
`coordinator/miner_submit.rs` inherits the latter. No metric family is waiting
for #280 to become populated.

The coverage-gap threshold preserves the strict `< 0.95` boundary of the
[2.x producer](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/504846cc0b72e8f86ed17f896d4ccbbe196a31dc/lab/prism/observability.py#L317).
Its native observed boundary is authorized clients holding current-generation
work (semantic coverage); it is a metrics-only timer and does not change health
readiness decisions. This preserves the legacy alert threshold/reset semantics.

Collector gauges are refreshed together from their in-memory measurements at
scrape time, including when the health publisher stalls. This does not renew
#277's cached-body publication timestamp or the collector's last-success time.

Registry injection is required by `ApiState::new`, `Coordinator::new`, and
`run_listener`. Primary and high-difficulty listeners explicitly receive the same
registry; listener configuration no longer creates its own telemetry state.
Four legacy block-processing rejection labels (`candidate-audit-mismatch`,
`submitblock-rejected`, `block-stale`, `ledger-confirmation-superseded`) have no
producer in this slice and are excluded until their owning follow-up wires them.

The ACK clock starts when this session reads a complete frame from its buffered
socket, not when bytes first reach the kernel. A pipelined second submit queued
behind earlier request work is therefore not a measure of the miner's full wait.
`runtime_lag_seconds` reports only the latest sampler tick, which can return to
near zero within 100 ms after recovery; completed tracked polls retain their
maximum separately in `runtime_poll_lag_seconds`. The health `ledger_backend`
alias uses `postgres-native`, matching the other native API responses, while the
existing `backend` field retains its storage-engine value `postgres`.
