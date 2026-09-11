# Native metric inventory

The coordinator (`run`) owns one process-local registry shared by Stratum,
background collectors, and the runtime monitor. `/metrics` renders cached
observations, with #277 freshness, collector age/availability, and runtime state evaluated at scrape time.
Scraping performs no database, node, or filesystem I/O. Public-api metrics retain
their existing contract.

The generated table below is the sole inventory for both roles: **38 coordinator
families and 14 public families**. Names, types and meanings for `run` come from
[registry.rs](../crates/qbit-prism-server/src/metrics/registry.rs#L42), with bounded
label values from [labels.rs](../crates/qbit-prism-server/src/metrics/labels.rs#L15).
[Compatibility annotations](prism-metric-metadata.json) add replacement names
and the pre-existing public producer contract. Regenerate with
`python3 scripts/generate_prism_metrics.py`; the ungated observability tests
scrape real HTTP listeners for both roles and both public replica modes, compare
families in both directions, and check the generator for drift.

Every coordinator family has HELP and TYPE
metadata, including families declared without samples. The public role retains
its existing untyped exposition; the inventory records the counter/gauge intent
from its producer. Public response/cache label sets are lazy and appear after a
request; replica gauges appear only with `PRISM_PUBLIC_REPLICA_MODE=require`.
Histogram boundaries in
seconds are 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, and +Inf.
Histograms also export `_sum` and `_count`.
Bucket samples add `le`; the table lists producer labels. `job`, `instance` and
`network` are deployment scrape labels, not native metric dimensions. See the
[deployed-alert migration and draft diff](prism-alert-migration.md) for the
consumer contract and threshold evidence.

Before the first complete health/metrics publication, scrapes expose the full
initialized registry while snapshot availability remains 0, snapshot age remains
-1, and `x-prism-metrics-state` remains `unavailable` without an `Age` header.
This includes declared histogram metadata without samples for unwired producers;
rendering the startup registry does not create a publication timestamp.

## Cutover inventory

<!-- generated-inventory:start -->
<!-- Run: python3 scripts/generate_prism_metrics.py -->

| Family | Type | Labels | Role | Meaning / status | 2.x.x name replaced |
| --- | --- | --- | --- | --- | --- |
| `qbit_prism_accepted_shares_total` | counter | none | run | Shares accepted by this instance since process start. Process-local counter; legacy canonical ledger count was persistent. | `qbit_prism_accepted_shares_total` |
| `qbit_prism_authorized_clients` | gauge | none | run | Current local authorized Stratum connections. | `qbit_prism_stratum_authorized_connections` |
| `qbit_prism_authorized_missing_current_work` | gauge | none | run | Authorized connections missing the current semantic work generation. | none |
| `qbit_prism_authorized_with_current_work` | gauge | none | run | Authorized connections holding the current semantic work generation. | `qbit_prism_stratum_clients_with_current_tip_jobs` |
| `qbit_prism_block_candidate_oldest_pending_seconds` | gauge | none | run | Oldest cluster-wide pending candidate age, or -1 when unknown. | `qbit_prism_block_candidate_oldest_pending_seconds` |
| `qbit_prism_block_candidates_pending` | gauge | none | run | Cluster-wide nonterminal candidate count, or -1 when unknown. | `qbit_prism_block_candidates_pending` |
| `qbit_prism_block_submit_seconds` | histogram | none | run | Locally validated block proof to first node offer; requires the offer owner's timestamp boundary. Declared, rule deferred to A/#266; no production observations yet. | `qbit_prism_block_submit_seconds` |
| `qbit_prism_blocks_total` | counter | none | run | Blocks confirmed by this instance since process start. Native confirmed-block process counter; not the legacy node-acceptance accounting boundary. | `qbit_prism_blocks_accepted_total` |
| `qbit_prism_collector_age_seconds` | gauge | `collector=database,process` | run | Monotonic age of the last successful collector observation, or -1 before success. | none |
| `qbit_prism_collector_available` | gauge | `collector=database,process` | run | Whether a collector has a complete successful observation. | none |
| `qbit_prism_collector_success` | gauge | `collector=database,process` | run | Whether the latest collector attempt succeeded, or -1 before an attempt. | none |
| `qbit_prism_connections` | gauge | none | run | Current local Stratum connections. | `qbit_prism_connected_clients`, `qbit_prism_stratum_active_connections` |
| `qbit_prism_database_advisory_lock_wait_seconds` | histogram | `lock=migration,order,settlement`; `result=success,failure` | run | Database advisory transaction lock wait by lock and outcome. Declared, rule deferred to #283 and A/C accounting-lock owners; no production observations yet. | none |
| `qbit_prism_database_pool_acquire_seconds` | histogram | `result=success,failure` | run | Actual database pool acquisition wait by outcome. Collector acquisitions only; ledger hot paths remain unwired. | none |
| `qbit_prism_duplicate_shares_total` | counter | none | run | Duplicate share rejections. | `qbit_prism_duplicate_shares_total` |
| `qbit_prism_grace_credited_shares_total` | counter | none | run | Durably accepted shares credited by stale grace. | `qbit_prism_grace_credited_shares_total` |
| `qbit_prism_health_state` | gauge | none | run | Whether this instance is ready to serve mining work. | none |
| `qbit_prism_job_delivery_failures_total` | counter | none | run | Failed local job deliveries. | none |
| `qbit_prism_job_delivery_successes_total` | counter | none | run | Successful local job deliveries. | none |
| `qbit_prism_low_difficulty_shares_total` | counter | none | run | Low difficulty share rejections. | `qbit_prism_low_difficulty_shares_total` |
| `qbit_prism_metrics_snapshot_age_seconds` | gauge | none | run | Monotonic age of the metrics snapshot, or -1 before the first publication. | `qbit_prism_metrics_snapshot_age_seconds` |
| `qbit_prism_metrics_snapshot_available` | gauge | none | run | Whether a complete metrics snapshot has been published. | `qbit_prism_metrics_snapshot_available` |
| `qbit_prism_metrics_snapshot_stale` | gauge | none | run | Whether the metrics snapshot is missing or exceeds the health freshness budget. | `qbit_prism_metrics_snapshot_stale` |
| `qbit_prism_pending_job_builds` | gauge | none | run | Current local pending job deliveries. Delivery count replaces the operational intent of queue depth, not its implementation. | `qbit_prism_job_delivery_queue_depth` |
| `qbit_prism_process_resident_memory_bytes` | gauge | none | run | Process resident memory bytes from procfs, or -1 when unknown. | `qbit_prism_process_resident_memory_bytes` |
| `qbit_prism_public_cache_total` | counter | `state=HIT,MISS,STALE,BYPASS` | public-api | Public route cache outcomes; appears after a public route request. | `qbit_prism_public_cache_total` |
| `qbit_prism_public_database_outage_refusals_total` | counter | none | public-api | Uncached database reads refused during database unavailability. | `qbit_prism_public_database_outage_refusals_total` |
| `qbit_prism_public_degraded_responses_total` | counter | none | public-api | Cached HIT responses served during database unavailability. | `qbit_prism_public_degraded_responses_total` |
| `qbit_prism_public_ledger_probe_age_seconds` | gauge | none | public-api | Monotonic age of the last completed readiness probe (including failure); -1 before the first attempt. | `qbit_prism_public_ledger_probe_age_seconds` |
| `qbit_prism_public_ledger_ready` | gauge | none | public-api | Schema/read probe succeeded and remains within its freshness budget; zero before a probe, after failure, or when stale. Does not include the separate replica heartbeat refusal. | `qbit_prism_public_ledger_ready` |
| `qbit_prism_public_replica_apply_backlog_bytes` | gauge | none | public-api (replica=require) | Last observed receive-LSN minus replay-LSN bytes; -1 if unknown; may remain cached after probe failure. | `qbit_prism_public_replica_apply_backlog_bytes` |
| `qbit_prism_public_replica_heartbeat_age_seconds` | gauge | none | public-api (replica=require) | Last observed public WAL-receiver heartbeat age plus monotonic time since observation; -1 if unknown/disconnected. | `qbit_prism_public_replica_heartbeat_age_seconds` |
| `qbit_prism_public_replica_in_recovery` | gauge | none | public-api (replica=require) | Last observed public read database recovery state; zero if never observed. Not the dedicated HA standby. | `qbit_prism_public_replica_in_recovery` |
| `qbit_prism_public_replica_max_lag_seconds` | gauge | none | public-api (replica=require) | Configured public read replica maximum lag/heartbeat-age budget in seconds. | `qbit_prism_public_replica_max_lag_seconds` |
| `qbit_prism_public_replica_refusals_total` | counter | none | public-api | Database-reading routes refused by the public replica contract. | `qbit_prism_public_replica_refusals_total` |
| `qbit_prism_public_replica_replay_lag_seconds` | gauge | none | public-api (replica=require) | Time since the last replayed transaction plus observation age; can increase on an idle primary and is not a byte-backlog or HA failover-loss measurement; -1 if unknown. | `qbit_prism_public_replica_replay_lag_seconds` |
| `qbit_prism_public_requests_total` | counter | none | public-api | Every routed request, including /healthz and /metrics; never use in a request-rate rule. | `qbit_prism_public_requests_total` |
| `qbit_prism_public_responses_total` | counter | `status` (HTTP status code) | public-api | HTTP responses by status, including health probes; appears after the first response. | `qbit_prism_public_responses_total` |
| `qbit_prism_public_staleness_refusals_total` | counter | none | public-api | Responses refused for exceeding an endpoint cache-age budget. | `qbit_prism_public_staleness_refusals_total` |
| `qbit_prism_rejected_shares_total` | counter | none | run | Shares rejected by this instance since process start. | none |
| `qbit_prism_rejections_total` | counter | `reason_id=stale-job,duplicate-share,low-difficulty,malformed-submit,unauthorized-worker,unknown-job,invalid-extranonce,invalid-ntime-or-nonce,backend-rpc-unavailable,internal-error,pool-closed,ledger-confirmation-failed` | run | Share rejections by canonical bounded reason ID. | `qbit_prism_rejections_total` |
| `qbit_prism_runtime_lag_seconds` | gauge | none | run | Latest observed runtime sampler wake lateness, or -1 before the first observation. Runtime-stall intent replaces lease wake delay; no native writer lease. | `qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds` |
| `qbit_prism_runtime_poll_lag_seconds` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector` | run | Maximum active poll duration or completed poll duration retained for 60 to 61 seconds, by task. | none |
| `qbit_prism_runtime_progress_age_seconds` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector` | run | Oldest active operation time since progress; zero when idle. | none |
| `qbit_prism_runtime_task_stalled` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector` | run | Whether an active poll or operation exceeds its progress budget. | none |
| `qbit_prism_runtime_workers` | gauge | none | run | Configured Tokio runtime worker threads. | none |
| `qbit_prism_share_ack_seconds` | histogram | `result=accepted,rejected` | run | Complete mining.submit frame arrival to completed response write, by outcome. | `qbit_prism_share_ack_seconds` |
| `qbit_prism_stale_shares_total` | counter | none | run | Shares rejected as stale or unknown jobs. | `qbit_prism_stale_shares_total` |
| `qbit_prism_stratum_current_tip_coverage_gap_seconds` | gauge | none | run | Continuous age of native current-generation coverage below 95 percent, or -1 before observation. | `qbit_prism_stratum_current_tip_coverage_gap_seconds` |
| `qbit_prism_stratum_oldest_pending_initial_job_seconds` | gauge | none | run | Oldest first usable work wait, or -1 before observation. | `qbit_prism_stratum_oldest_pending_initial_job_seconds` |
| `qbit_prism_stratum_pending_initial_jobs` | gauge | none | run | Authorized clients awaiting first usable work, or -1 before observation. | `qbit_prism_stratum_pending_initial_jobs` |
| `qbit_prism_stratum_semantic_current_work_ratio` | gauge | none | run | Fraction of authorized connections with current semantic work; one when no clients are authorized. -1 before observation; one when no clients are authorized. | `qbit_prism_stratum_semantic_current_work_ratio` |

<!-- generated-inventory:end -->

Reject and task labels come from the closed enums rendered in the table above.
Unknown internal rejection reason IDs map to `internal-error`; protocol responses
are unchanged.

Session futures and server-owned background futures are monitored. Only the
health publisher currently registers an explicit operation-progress budget,
covering health and metrics publication only. It uses the same effective
`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds as #277 freshness and ends
immediately after publication; later heartbeat/prune waits are outside that
deadline. Those futures still receive ordinary blocked-poll monitoring.
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

The existing coordinator families retain their names and types; their complete
contracts appear in the generated table above. The #277 `metrics_snapshot_available`,
`metrics_snapshot_stale`, and `metrics_snapshot_age_seconds` names, labels,
HELP/TYPE lines, headers, and atomic body/timestamp publication remain unchanged.
Deployment alert compatibility was checked against the frozen `SwapLabsInc/qbit-tools`
templates at `a007a14260548cb93e277882672d788fe9a3f95e`. The seven overlapping
families keep their names/types/labels. The [alert migration](prism-alert-migration.md)
replaces legacy rules and joins on retired `stratum_authorized_connections`. The existing
public-role `qbit_prism_public_requests_total` includes `/healthz`, `/metrics`,
and other routed requests, so its rate is not a dashboard-only request rate.

The coordinator adds the three health compatibility aliases whose native sources
are known; the fixture deliberately identifies unmapped legacy fields: `ready_miner_count` (accepted-share participants) and `max_blocks` (the 2.x accepted-block pool-close cap) have no native health equivalents.

No new configuration setting is introduced. Worker slots, per-worker series,
node gauges, rollup-lag series, cardinality/privacy qualification, payout build
and landing-phase instrumentation are outside this trimmed change.

## Follow-up ownership

First-offer and advisory-lock timing remain **declared, not yet populated**.
The native alert specification attaches no firing rules to these families. Pool timing covers
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
