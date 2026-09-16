# Native metric inventory

The coordinator (`run`) owns one process-local registry shared by Stratum,
background collectors, and the runtime monitor. `/metrics` renders cached
observations, with the pool-acquisition histogram, collector measurements,
#277 freshness, collector age/availability, and runtime state read at scrape time.
Scraping performs no database, node, or filesystem I/O. Public-api metrics retain
their existing contract.

The generated table below is the sole inventory for both roles: **42 coordinator
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
Default histogram boundaries in seconds are 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
1, 2.5, 5, 10, 30, and +Inf. Only `qbit_prism_share_ack_seconds` adds 15 and 20.
Histograms also export `_sum` and `_count`. See the
[histogram consumer guide](prism-metrics-histogram-consumers.md) for the five
added series per process, elapsed-time attribution, quantile changes and
mixed-version queries.
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
| `qbit_prism_block_candidate_oldest_pending_seconds` | gauge | none | run | Oldest cluster-wide pending candidate age, or -1 when unknown. Counts every unfinished outbox state: pending, offer_reserved, offered and reconciliation (A/#266). | `qbit_prism_block_candidate_oldest_pending_seconds` |
| `qbit_prism_block_candidates_pending` | gauge | none | run | Cluster-wide nonterminal candidate count, or -1 when unknown. Counts every unfinished outbox state: pending, offer_reserved, offered and reconciliation (A/#266). | `qbit_prism_block_candidates_pending` |
| `qbit_prism_block_submit_seconds` | histogram | none | run | Locally validated block proof to first node offer; requires the offer owner's timestamp boundary. Observed once per block by the offering frontend after its one submitblock call returned, from the enqueuing frontend's proof-observation wall clock to the offering frontend's wall clock immediately before the send (A/#266). No sample for a row without a proof time, for a negative interval (host clock skew) or on a recovery; a crash between the call and its outcome commit may lose the sample. No firing rule. | `qbit_prism_block_submit_seconds` |
| `qbit_prism_blocks_total` | counter | none | run | Blocks confirmed by this instance since process start. Native confirmed-block process counter; not the legacy node-acceptance accounting boundary. | `qbit_prism_blocks_accepted_total` |
| `qbit_prism_collector_age_seconds` | gauge | `collector=database,process` | run | Monotonic age of the last successful collector observation, or -1 before success. | none |
| `qbit_prism_collector_available` | gauge | `collector=database,process` | run | Whether a collector has a complete successful observation. | none |
| `qbit_prism_collector_success` | gauge | `collector=database,process` | run | Whether the latest collector attempt succeeded, or -1 before an attempt. | none |
| `qbit_prism_connections` | gauge | none | run | Current local Stratum connections. | `qbit_prism_connected_clients`, `qbit_prism_stratum_active_connections` |
| `qbit_prism_database_advisory_lock_wait_seconds` | histogram | `lock=migration,order,settlement`; `result=success,failure` | run | Database advisory transaction lock wait by lock and outcome. Client-observed duration of the `pg_advisory_xact_lock` statement, including one database round trip, recorded by the coordinator's ledger for the migration, order and settlement locks; the migration lock is taken only when the coordinator initializes the schema. `failure` includes lock timeout (`PRISM_DATABASE_LOCK_TIMEOUT_MS`, default 5 seconds), statement timeout, deadlock and connection errors, and waits abandoned by cancellation. The CPFP funding lock is not observed (#328). Series appear on their first observation, so a restart's first failure is not visible to `increase()`. | none |
| `qbit_prism_database_pool_acquire_seconds` | histogram | `result=success,failure` | run | Actual database pool acquisition wait by outcome. Client-observed `PgPool::acquire` time: waiting for a pool permit, the idle-connection liveness ping and, when the pool grows, connection setup; excludes transaction BEGIN and the queries that follow. Recorded by the metrics collector, including its own cancellations, and by instrumented coordinator ledger transactions and selected direct ledger queries, including payout-revision reads and heartbeats. Non-transactional coverage remains partial under #352; rollup worker transactions, public API queries and other pool traffic outside these acquisition paths are not timed. `failure` includes acquire errors, the 15-second acquire timeout and acquisitions abandoned by cancellation, recorded with the elapsed wait. Buckets, count and sum are read together from the live registry on each scrape, independently of cached-body publication; scraping does not create observations or renew snapshot freshness. | none |
| `qbit_prism_duplicate_shares_total` | counter | none | run | Duplicate share rejections. | `qbit_prism_duplicate_shares_total` |
| `qbit_prism_grace_credited_shares_total` | counter | none | run | Durably accepted shares credited by stale grace. | `qbit_prism_grace_credited_shares_total` |
| `qbit_prism_health_state` | gauge | none | run | Whether this instance is ready to serve mining work. | none |
| `qbit_prism_job_delivery_failures_total` | counter | none | run | Failed local job deliveries. | none |
| `qbit_prism_job_delivery_successes_total` | counter | none | run | Successful local job deliveries. | none |
| `qbit_prism_late_confirmed_shares_total` | counter | none | run | Shares accepted after the share commit deadline once their in-flight ledger commit was confirmed. | none |
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
| `qbit_prism_rejections_total` | counter | `reason_id=stale-job,duplicate-share,low-difficulty,malformed-submit,unauthorized-worker,unknown-job,invalid-extranonce,invalid-ntime-or-nonce,backend-rpc-unavailable,internal-error,pool-closed,ledger-confirmation-failed,ledger-outcome-unknown,unrecognised` | run | Share rejections by canonical bounded reason ID. Present unknown or empty IDs map to unrecognised; missing IDs and explicit internal-error retain internal-error. Normalization does not change the protocol response. | `qbit_prism_rejections_total` |
| `qbit_prism_runtime_lag_seconds` | gauge | none | run | Latest observed runtime sampler wake lateness, or -1 before the first observation. Runtime-stall intent replaces lease wake delay; no native writer lease. | `qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds` |
| `qbit_prism_runtime_poll_lag_seconds` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector` | run | Maximum active poll duration or completed poll duration retained for 60 to 61 seconds, by task. | none |
| `qbit_prism_runtime_progress_age_seconds` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector` | run | Oldest active operation time since progress; zero when idle. | none |
| `qbit_prism_runtime_task_stalled` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector` | run | Whether an active poll or operation exceeds its progress budget. | none |
| `qbit_prism_runtime_workers` | gauge | none | run | Configured Tokio runtime worker threads. | none |
| `qbit_prism_share_ack_seconds` | histogram | `result=accepted,rejected` | run | Complete mining.submit frame arrival to completed response write, by outcome. Uses the default histogram ladder plus 15 and 20 seconds; these are elapsed ACK bounds, not measured ledger deadlines. | `qbit_prism_share_ack_seconds` |
| `qbit_prism_stale_job_rejections_total` | counter | `cause=resume_expired,fee_floor,parent_grace,payout_revision` | run | Stale-job share rejections by the internal decision that refused them. Each series counts one existing stale-job decision: resumed-job absolute expiry, CTV relay-fee floor, stale parent or failed stale-grace parent check, and payout-revision mismatch, attributed in that execution order. The wire reason and message are unchanged and still counted by `qbit_prism_rejections_total{reason_id="stale-job"}`. Stale-grace credit is not a rejection. Process-local; every series starts at zero. | none |
| `qbit_prism_stale_shares_total` | counter | none | run | Shares rejected as stale or unknown jobs. | `qbit_prism_stale_shares_total` |
| `qbit_prism_stratum_connection_limit` | gauge | none | run | Configured global Stratum connection limit, not currently available permits; -1 before a listener starts. Set from `PRISM_STRATUM_MAX_CONNECTIONS` when a Stratum listener starts. The primary and high-difficulty listeners share this limit and `qbit_prism_connections`. | none |
| `qbit_prism_stratum_connection_refusals_total` | counter | `reason=global_limit,username_limit` | run | Stratum connections refused by an existing admission limit, by closed reason. `global_limit` counts a newly accepted socket closed because `PRISM_STRATUM_MAX_CONNECTIONS` permits were exhausted; `username_limit` counts a `mining.authorize` refused by `PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME`. Same-username reauthorization and reuse of a retained username permit never count. Authorization refusals are not share rejections. Process-local; both series start at zero. | none |
| `qbit_prism_stratum_current_tip_coverage_gap_seconds` | gauge | none | run | Continuous age of native current-generation coverage below 95 percent, or -1 before observation. | `qbit_prism_stratum_current_tip_coverage_gap_seconds` |
| `qbit_prism_stratum_oldest_pending_initial_job_seconds` | gauge | none | run | Oldest first usable work wait, or -1 before observation. | `qbit_prism_stratum_oldest_pending_initial_job_seconds` |
| `qbit_prism_stratum_pending_initial_jobs` | gauge | none | run | Authorized clients awaiting first usable work, or -1 before observation. | `qbit_prism_stratum_pending_initial_jobs` |
| `qbit_prism_stratum_semantic_current_work_ratio` | gauge | none | run | Fraction of authorized connections with current semantic work; one when no clients are authorized. -1 before observation; one when no clients are authorized. | `qbit_prism_stratum_semantic_current_work_ratio` |

<!-- generated-inventory:end -->

Reject and task labels come from the closed enums rendered in the table above.
Present but unrecognised rejection reason IDs, including an empty string, map
to `unrecognised`; arbitrary input cannot create more than the 14 closed reason
series. Explicit `internal-error` and missing (`None`) reason IDs retain the
`internal-error` label. Every reason counter is initialized to zero, so the first
event is observable by `increase()`. The fallback counts an actual rejection;
it is a label-drift signal, never an accepted share or proof of health.

Normalization happens only in the submit observation path. The current
reason-less username-limit refusal belongs to `mining.authorize`, so it is
counted by connection-refusal telemetry and never by share-rejection or ACK
metrics. Missing reasons supplied by a submit backend retain their prior
classification. Protocol error codes, messages and reason metadata are unchanged.

Reason-filtered consumers of `internal-error` now exclude present unknown IDs;
use `unrecognised` to identify reasons the enum does not recognize. Summing all
reasons still counts the same rejections, and the checked-in per-reason rejection
ratio includes the new label automatically. Its grouping can now split a former
`internal-error` total, so the per-reason alert decision can change. The explicit
ledger-confirmation reason filter retains its two existing reasons. No alert
rules are added by this follow-up.

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

Share acknowledgements follow the ledger outcome (#324). A share-pass append
whose COMMIT was already in flight at `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` can
still be accepted within `share_commit_grace` (5 s); each such acceptance
increments `qbit_prism_late_confirmed_shares_total`. A share-pass submission
carrying a found-block candidate is never refused, so it can be confirmed later
still, up to `block_only_ack_timeout`; those acceptances are counted the same
way. The counter is keyed on when the append itself finished, not on when the
acknowledgement was processed. Three outcomes are answered
`ledger-outcome-unknown`, never `ledger-confirmation-failed`: a COMMIT still in
flight after the grace period, a COMMIT failure other than a severity-ERROR
reply, and, under the sync-rep guard, a COMMIT that took at least the ledger
sessions' `statement_timeout`, because that may be a synchronous-replication
wait cancelled after local commit. Such a share may still be credited, and the
warning log names its `share_id`. Unknown answers are rejections under the
protocol, so they count in `qbit_prism_rejected_shares_total`,
`qbit_prism_rejections_total{reason_id="ledger-outcome-unknown"}` and
`qbit_prism_share_ack_seconds{result="rejected"}`. Block-only proofs wait for
their candidate's disposition, and share-pass appends that carry a found block
wait for their append, up to `block_only_ack_timeout`
(`max(60 s, PRISM_SHARE_COMMIT_TIMEOUT_SECONDS)`); either is answered
`ledger-outcome-unknown` if still pending then. Such an ACK between 30 and 60
seconds lands only in the `+Inf` bucket. Neither `share_commit_grace` nor
`block_only_ack_timeout` is an environment variable.

Collectors run every ten seconds. Database collection uses a read-only,
repeatable-read transaction with a three-second overall deadline, a two-second
statement timeout, and a 500 ms lock timeout. Failure or cancellation makes the
measurements unknown (-1), sets success/availability to zero, and leaves the
last-success timestamp intact. Observations expire after 30 seconds even if no
new attempt finishes. A newer collection attempt supersedes an older result;
late completion or cancellation cannot replace the newer publication. A real
zero count, age, or RSS remains valid after successful collection.

Pool timing pre-registers both result labels at count zero. Each started
collector acquisition records one observation: success when acquired, or failure
on acquisition error or cancellation, including the three-second overall
deadline. Since #328 instrumented coordinator ledger transactions record
acquisition the same way, including one abandoned by cancellation. Selected direct
ledger queries now use the same boundary, including payout-revision reads and
heartbeats; [the acquisition guide](prism-pool-acquire-metrics.md) lists the covered
callers and remaining #352 work. Rollup worker transactions, public API queries
and other pool traffic outside these acquisition paths are not timed. Duration is the monotonic elapsed pool wait until
acquisition completes or is cancelled; subsequent transaction work is excluded.
Collector status also records collection failure or cancellation separately.
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

## Diagnosing Stratum admission saturation

These signals observe the existing limits; they do not add limits or change
readiness.

- `qbit_prism_stratum_connection_limit` is the configured
  `PRISM_STRATUM_MAX_CONNECTIONS`, shared by the primary and high-difficulty
  listeners, or -1 until a listener starts. It is capacity, not free permits:
  compare it with `qbit_prism_connections` to see the remaining room.
- `qbit_prism_stratum_connection_refusals_total{reason="global_limit"}`
  increases when a new TCP connection is closed at the limit. The miner sees an
  accepted connection closed without any JSON-RPC response. A rising count with
  `qbit_prism_connections` at the limit is admission saturation.
- `reason="username_limit"` increases when `mining.authorize` returns
  `[20,"too many connections for username",null]` under
  `PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME`. The connection stays open and
  keeps any earlier authorization. Work still retained for that username keeps
  its slot until it expires, so a quick reconnect can be refused briefly.
  Same-username reauthorization is never counted.
- Reconnects with no refusal increase, and connections below the limit, point
  to the transport path or the miner rather than server admission.
- Readiness (`qbit_prism_health_state`, `mining.get_health`) says whether this
  instance can serve work to miners already connected. It does not reserve
  spare connection capacity: a ready instance at its limit still refuses new
  sockets.

Both counters are process-local, start at zero for every reason so `increase()`
sees the first refusal, and appear in the cached body with the other Stratum
series at its next publication. Neither count is a share rejection.

## Follow-up ownership

First-offer timing remains **declared, not yet populated**; A/#266 wires it.
Since #328, instrumented coordinator ledger transactions record into
`database_pool_acquire_seconds`, and the coordinator's migration, order and
settlement advisory-lock acquisitions record into
`database_advisory_lock_wait_seconds`; the CPFP funding lock is not timed.
Selected non-transaction ledger queries are timed through `Ledger::acquire`,
with remaining caller coverage tracked in #352; this is not an acquisition census. The native alert specification
attaches no firing rule to first-offer or advisory-lock timing.
`PrismDatabasePoolWaitHigh` describes both collector and instrumented ledger
acquisitions, including cancellation observations from #328 and #345. The collector
acquires from the same pool as the ledger, so pool exhaustion can fail collection
while the histogram records valid waits. The health publisher also awaits this
pool, so #351 overlays the live pool histogram on every scrape, including before
the first publication and while the cached body is stale. All buckets, count and
sum come from one registry read; rendering does not create observations or renew
the cached-body timestamp. This rule requires a successful scrape (`up == 1`)
and the existing per-instance minimum of ten observations in five minutes,
independently of collector or cached-body availability. Failed or missing scrapes
suppress it, and frozen counts age out of the sample window. Cached gauges retain
their freshness gates and stale/missing snapshots retain their unknown-data alerts.
Deploy the live-histogram producer to every coordinator target before activating
this expression: older producers still cache pool observations, so their successful
scrapes alone cannot establish this contract. The landing order for #336 is #334,
then #343, then #351; the first two have landed.
#328 references #278 rather than closing it.

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

Collector gauges and the pool histogram are refreshed from one registry read at
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
