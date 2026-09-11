# Native alert migration (#279)

This is the cutover consumer contract for the 3.x.x native runtime. The single
[metric inventory](prism-native-metrics.md) is generated from #278's registry.
The [historical overload appendix](prism-overload-alerts.md) remains historical;
its undeployed rules are not re-specified here.

The input is `SwapLabsInc/qbit-tools`, `origin/main` at
`a007a14260548cb93e277882672d788fe9a3f95e`, file
`ansible/roles/qbit_monitoring_stack/templates/grafana-alert-rules-prism.yml.j2`.
The [frozen manifest](../tests/fixtures/prism-deployed-alerts.json) records its
SHA-256, all 78 definitions (including gated definitions), and all 53 referenced
`qbit_prism_*` tokens. Jinja values are deployment variables, not metric labels
or evidence that a gate is enabled. Backup, node-exporter lifecycle, and sidecar
producers remain external to the native inventory.

## Review and apply the draft

The [unified rules-file diff](prism-alert-rules-qbit-tools.patch) is the deployment
change for operator review. It is not applied by bootstrap and must not be used
with a 2.x.x image. No changes to qbit-tools are made by this work.

```sh
# In a disposable copy of qbit-tools at a007a142 (or review/rebase against its newer main):
git apply --check /path/to/prism-alert-rules-qbit-tools.patch
# After operator review and coordinated native cutover:
git apply /path/to/prism-alert-rules-qbit-tools.patch
```

The draft preserves the 34 external definitions and their existing gates.
`qbit_monitoring_stack_prism_alerts_enabled` enables the native coordinator rules;
the public, connected-client and sidecar gates keep their meaning. Python
`overload_alerts_enabled` and `writer_lease_alerts_enabled` gates no longer
control native observability. The separate PostgreSQL HA group is required by
D3 and deliberately reports missing instrumentation. Provisioning `deleteRules`
removes every legacy UID that is not reused; omission alone does not remove an
existing Grafana rule. Native critical-named bounds are warnings without page
labels until #291 qualifies the native thresholds; existing external paging is
preserved. Sidecar miner-impact and host-loss pages remain the outage net.

Before cutover, render with the deployment's actual Ansible variables and
confirm the scrape selectors: `qbit-prism`, `qbit-prism-public-read`, and the new
primary exporter job `qbit-postgres-primary`, all with `network`. Exporter-owned
labels (`result`, `reason_id`, `collector`, `task`) come from the registry;
`job`, `instance`, and `network` are Prometheus target labels. Evaluate native
ratios per instance, never sum replicated candidate/RSS gauges across hosts.
The connected-client floor preserves the deployed network-wide total and only
evaluates when every configured coordinator has a fresh observation.
For a permitted rollback to 2.x.x, restore its template/gates and explicitly
delete any newly introduced native/D3 UIDs that should no longer run. This rules
change does not authorize database rollback or mixed-version writer operation.

Regenerate after editing the JSON specifications or compatibility mappings:

```sh
python3 scripts/generate_prism_metrics.py
python3 scripts/generate_prism_alerts.py --snapshot /path/to/read-only-snapshot
cargo +1.89.0 test --locked -p qbit-prism-server --test observability
python3 scripts/test_prism_alert_promql.py --promtool /path/to/promtool
```

## Truthful signals and threshold qualification

Native body-based rules require `metrics_snapshot_available == 1` and
`metrics_snapshot_stale == 0`, using #277's effective freshness budget rather
than hard-coding a refresh interval. Snapshot unavailable/stale alerts and the
endpoint/no-series nets remain alertable on missing data. A fresh failed public
probe is distinct from a stale probe; `public_ledger_ready` already rejects both
failure and staleness, and the public role exposes `public_ledger_probe_age_seconds`
instead of the coordinator's snapshot gauge families.

Candidate and RSS rules additionally require the relevant
`collector_available == 1`. Unknown, failed or expired collections render -1 and
fire the collector-unavailable rule; a successful zero remains healthy. Unknown
work gauges have their own rule. Runtime wake/poll/progress signals are evaluated
at scrape time and intentionally remain usable while the body publisher is
stale. The latest wake lag can reset within 100 ms of recovery, so the wake-delay
replacement also reads retained completed polls and the native progress budget.
The five-minute lookback retains an episode for evaluation; it is not the old
lease scheduler-slack policy or a reconstruction of wake frequency.

The ACK p99 uses the native completed-response-write histogram. At least 100
observations are required in five minutes so an idle miner does not create a
quantile alarm. Reject ratios preserve canonical `reason_id` and use accepted
plus canonical rejection decisions as their denominator; ACK timing is not a
durable-credit counter. The collector pool histogram covers only completed
`PgPool::acquire` calls; a cancelled acquisition is represented by collector
failure, not a fabricated duration. **Proof-to-first-offer and advisory-lock
waits are declared, rule deferred to A/#266 and #283** respectively; neither has
a firing rule.

Numeric bounds without native measurement are explicitly **provisional, measure
in #291** in each rule's `basis` and provisioned description. This includes RSS
4 GiB (must be reconciled with container headroom), ACK 1 s, reject 5%, pool wait
0.5 s, backlog counts 2/5, oldest candidate ages 15/60 s, first-work/gap 15 s,
critical coverage 50%, and timing/sample windows. Native implementation evidence
supports the strict 95% coverage boundary, two-second blocked-poll budget,
30-second collector expiry, and #277 freshness calculation; it does not establish
production latency or memory SLOs. #291 must exercise each signal, recovery,
startup, scrape loss, stale publisher and collector failure before cutover.

`qbit_prism_public_requests_total` counts `/healthz`, `/metrics` and other routed
requests. **It must never feed a request-rate rule.** The public 5xx rule is a
response-error signal (including failed probes); there is no public request-rate
rule in this draft. This preserves the caveat in the D1 Sources evidence and
[#260's measured D1 addendum](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/260).

## D3: deployment-provided primary/standby rules

The [two PostgreSQL rules](prism-postgres-alert-rules.json) are emitted as the
separate `qbit-prism-postgres-ha-alerts` group in the diff. Required by D3:
**replay lag above 5 seconds for 1 minute**, and **zero replication rows for
`application_name='prism_standby_1'` for 1 minute**. Missing, failed and unknown
measurements also fire. The topology is one asynchronous standby: ACKs require
local durability and do not wait for replay; the loss window remains replication
lag or standby downtime. These alerts do not imply lossless failover.

**Deployment-provided: requires postgres_exporter on the PRIMARY; verify presence
in #281/#291.** The dedicated failover standby is not the public read replica.
Never use `qbit_prism_public_replica_*` for this group. Its inventory is external
and deliberately excluded from the native-family test.

Signal semantics must be configured in the deployment exporter, starting with:

```sql
SELECT application_name, state, EXTRACT(EPOCH FROM replay_lag) AS replay_lag
FROM pg_stat_replication
WHERE application_name = 'prism_standby_1';

SELECT 'prism_standby_1' AS application_name, COUNT(*)::double precision AS count
FROM pg_stat_replication
WHERE application_name = 'prism_standby_1';
```

The [ready-to-install query extension](prism-postgres-exporter-queries.yaml)
combines these into one aggregate query with an explicit -1 sentinel for NULL
and zero rows when disconnected; the exact stanza is also included in the diff.
Expose the replay signal as `pg_stat_replication_replay_lag{application_name}`
in seconds and the second as `pg_stat_replication_count{application_name}`; also
expose `pg_up` on the same primary target. Use `job="qbit-postgres-primary"` and
`network`, with only one primary writer target per network. A NULL replay lag
must be absent or -1, never silently coerced to healthy zero. NULL can occur on
an idle caught-up connection; until the deployment exporter can prove caught-up
state independently, this is an explicit unknown notification, not evidence of
loss. The count query must emit zero even when there are no matching rows.

These are an explicit deployment query contract, not a claim that every
postgres_exporter version enables these names by default. The
[upstream replication collector](https://github.com/prometheus-community/postgres_exporter/blob/master/collector/pg_stat_replication.go)
exports WAL byte positions/differences, which cannot be compared to a five-second
budget. #281 must configure the SQL-backed seconds/count signals through its
chosen exporter version and #291 must verify connected, disconnected, lagged,
NULL, failed-query and missing-exporter cases. The native rule test never invents
these families in #278's registry.

<!-- generated-migration:start -->
<!-- Run: python3 scripts/generate_prism_alerts.py -->

## Every deployed alert

78 definitions: **22 migrated, 34 unchanged external, 22 with no replacement**.
This includes definitions behind deployment Jinja flags; the snapshot does not record their effective values.

| Deployed UID | Deployed alert | Native rule / disposition | Reason |
| --- | --- | --- | --- |
| `qbit-prism-metrics-nodata` | PrismMetricsNoData | `PrismMetricsNoData` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-metrics-endpoint-down` | PrismMetricsEndpointDown | `PrismMetricsEndpointDown` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-connected-clients` | PRISM Connected Clients | `PRISM Connected Clients` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-stratum-nodata` | PrismStratumHealthNoData | `PrismStratumHealthNoData` (unchanged-external) | No native replacement needed: deployment-owned Stratum sidecar rule and producer are retained byte-for-byte. |
| `qbit-prism-stratum-down-warning` | PrismStratumUnavailableWarning | `PrismStratumUnavailableWarning` (unchanged-external) | No native replacement needed: deployment-owned Stratum sidecar rule and producer are retained byte-for-byte. |
| `qbit-prism-stratum-down` | PrismStratumUnavailable | `PrismStratumUnavailable` (unchanged-external) | No native replacement needed: deployment-owned Stratum sidecar rule and producer are retained byte-for-byte. |
| `qbit-prism-stratum-failure-rate` | PrismStratumCheckFailureRateHigh | `PrismStratumCheckFailureRateHigh` (unchanged-external) | No native replacement needed: deployment-owned Stratum sidecar rule and producer are retained byte-for-byte. |
| `qbit-prism-share-writer-backlog` | PrismShareWriterBacklog | `PrismShareAckP99High`, `PrismDatabasePoolWaitHigh` (migrated) | Native shares are durably written before ACK; Python append-queue depth has no native counterpart. Replace latency/pressure intent, not queue semantics. |
| `qbit-prism-share-writer-backlog-critical` | PrismShareWriterBacklogCritical | `PrismShareAckP99High`, `PrismRejectRatioByReasonHigh` (migrated) | No native 100k volatile ACKed-share queue or overflow-loss threshold; use ACK/rejection impact, with no unqualified critical page. |
| `qbit-prism-block-candidate-backlog` | PrismBlockCandidateBacklog | `PrismBlockCandidateBacklog` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-shares-recovered-to-disk` | PrismSharesRecoveredToDisk | no replacement (no-replacement) | No replacement: native has no local Python append spool/recovery-file producer; D3 spool follow-up is an open A decision. |
| `qbit-prism-share-append-failures` | PrismShareAppendFailures | `PrismRejectRatioByReasonHigh` (migrated) | Native ledger-confirmation-failed and backend-rpc-unavailable reason IDs expose failed submits; canonical reasons replace append retries. |
| `qbit-prism-shares-replayed` | PrismSharesReplayed | no replacement (no-replacement) | No replacement: no native Python recovery-file replay counter or local spool. |
| `qbit-prism-job-build-orphan-evicted` | PrismJobBuildOrphanEvictions | no replacement (no-replacement) | No replacement: native job delivery has no Python executor orphan-eviction state. |
| `qbit-prism-stats-reconcile-stale` | PrismAcceptedStatsReconcileStale | no replacement (no-replacement) | No replacement: native process counters are not Python accepted-statistics reconciliation; rollup metrics deferred from #278. |
| `qbit-prism-lease-proven-age` | PrismWriterLeaseServerProvenAgeNearCap | no replacement (no-replacement) | No replacement: PostgreSQL coordination replaces the Python writer lease; server-proven-cap policy does not exist. |
| `qbit-prism-lease-monitor-wake` | PrismWriterLeaseMonitorWakeDelayHigh | `PrismRuntimeLagHigh` (migrated) | Same runtime-stall intent; native runtime wake, retained poll and progress-budget observations replace Python lease-monitor timing. |
| `qbit-prism-lease-monitor-guarantee` | PrismWriterLeaseMonitorExitGuaranteeBreached | no replacement (no-replacement) | No replacement: no native Python monitor exit-before-adoption guarantee or breach counter; runtime delay has its own alert. |
| `qbit-prism-lease-surplus-invalid` | PrismWriterLeaseStabilitySurplusExhausted | no replacement (no-replacement) | No replacement: no Python lease stability-surplus policy in the native runtime. |
| `qbit-prism-preview-publish-latency` | PrismAcceptedPreviewPublicationLatencyHigh | no replacement (no-replacement) | No replacement: native accepted-preview/landing-phase instrumentation is absent; proof-to-first-offer is a different boundary and is declared, rule deferred to A/#266. |
| `qbit-prism-preview-publish-latency-crit` | PrismAcceptedPreviewPublicationLatencyCritical | no replacement (no-replacement) | No replacement: native accepted-preview/landing-phase instrumentation is absent; proof-to-first-offer is a different boundary and is declared, rule deferred to A/#266. |
| `qbit-prism-preview-publish-degraded` | PrismAcceptedPreviewPublicationDegraded | no replacement (no-replacement) | No replacement: native accepted-preview/landing-phase instrumentation is absent; proof-to-first-offer is a different boundary and is declared, rule deferred to A/#266. |
| `qbit-prism-parent-preview-timeouts` | PrismAcceptedParentPreviewWaitTimeouts | no replacement (no-replacement) | No replacement: native accepted-preview/landing-phase instrumentation is absent; proof-to-first-offer is a different boundary and is declared, rule deferred to A/#266. |
| `qbit-prism-parent-preview-timeouts-crit` | PrismAcceptedParentPreviewWaitTimeoutsCritical | no replacement (no-replacement) | No replacement: native accepted-preview/landing-phase instrumentation is absent; proof-to-first-offer is a different boundary and is declared, rule deferred to A/#266. |
| `qbit-prism-unresolved-parent-depth` | PrismUnresolvedParentDepthNonzero | no replacement (no-replacement) | No replacement: Python unresolved-parent transition cache and its depth/cap/age contract are absent; durable pending-candidate alerts do not claim equivalent semantics. |
| `qbit-prism-unresolved-parent-near-cap` | PrismUnresolvedParentDepthNearCap | no replacement (no-replacement) | No replacement: Python unresolved-parent transition cache and its depth/cap/age contract are absent; durable pending-candidate alerts do not claim equivalent semantics. |
| `qbit-prism-unresolved-parent-at-cap` | PrismUnresolvedParentDepthAtCap | no replacement (no-replacement) | No replacement: Python unresolved-parent transition cache and its depth/cap/age contract are absent; durable pending-candidate alerts do not claim equivalent semantics. |
| `qbit-prism-unresolved-parent-age` | PrismUnresolvedParentAgeOverBudget | no replacement (no-replacement) | No replacement: Python unresolved-parent transition cache and its depth/cap/age contract are absent; durable pending-candidate alerts do not claim equivalent semantics. |
| `qbit-prism-unresolved-parent-age-crit` | PrismUnresolvedParentAgeCritical | no replacement (no-replacement) | No replacement: Python unresolved-parent transition cache and its depth/cap/age contract are absent; durable pending-candidate alerts do not claim equivalent semantics. |
| `qbit-prism-candidate-backlog-critical` | PrismBlockCandidateBacklogCritical | `PrismBlockCandidateBacklogCritical` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-block-candidate-oldest` | PrismBlockCandidateOldestPending | `PrismBlockCandidateOldestPending` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-candidate-oldest-critical` | PrismBlockCandidateOldestPendingCritical | `PrismBlockCandidateOldestPendingCritical` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-candidate-metrics-unavailable` | PrismBlockCandidateMetricsUnavailable | `PrismBlockCandidateMetricsUnavailable` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-cleanup-backlog-depth` | PrismCollapseCleanupBacklog | no replacement (no-replacement) | No replacement: Python collapse-cleanup retry queue/backpressure producer is absent; do not reinterpret native pending candidate rows as cleanup state. |
| `qbit-prism-cleanup-backlog-oldest` | PrismCollapseCleanupBacklogOldest | no replacement (no-replacement) | No replacement: Python collapse-cleanup retry queue/backpressure producer is absent; do not reinterpret native pending candidate rows as cleanup state. |
| `qbit-prism-cleanup-backlog-near-bound` | PrismCollapseCleanupBacklogNearBound | no replacement (no-replacement) | No replacement: Python collapse-cleanup retry queue/backpressure producer is absent; do not reinterpret native pending candidate rows as cleanup state. |
| `qbit-prism-cleanup-backpressure-active` | PrismCollapseCleanupBackpressure | no replacement (no-replacement) | No replacement: Python collapse-cleanup retry queue/backpressure producer is absent; do not reinterpret native pending candidate rows as cleanup state. |
| `qbit-prism-cleanup-backpressure-engaged` | PrismCollapseCleanupBackpressureEngaged | no replacement (no-replacement) | No replacement: Python collapse-cleanup retry queue/backpressure producer is absent; do not reinterpret native pending candidate rows as cleanup state. |
| `qbit-prism-semantic-work-coverage` | PrismSemanticWorkCoverageLoss | `PrismSemanticWorkCoverageLoss` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-semantic-coverage-critical` | PrismSemanticWorkCoverageLossCritical | `PrismSemanticWorkCoverageLossCritical` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-refresh-pending` | PrismRefreshPendingPastDeadline | `PrismTimeToUsableWorkHigh`, `PrismCurrentWorkGapHigh` (migrated) | Observe the effect on usable work; native refresh has no Python pending-deadline state. |
| `qbit-prism-refresh-pending-critical` | PrismRefreshPendingPastDeadlineCritical | `PrismTimeToUsableWorkHigh`, `PrismSemanticWorkCoverageLossCritical` (migrated) | Coverage and first-work delay replace miner-impact intent; native thresholds require #291 measurement. |
| `qbit-prism-metrics-snapshot-stale` | PrismMetricsSnapshotStale | `PrismMetricsSnapshotStale` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-metrics-snapshot-unavailable` | PrismMetricsSnapshotUnavailable | `PrismMetricsSnapshotUnavailable` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-metrics-snapshot-age` | PrismMetricsSnapshotAgeHigh | `PrismMetricsSnapshotAgeHigh` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-public-staleness-refusal` | PrismPublicStalenessRefusalsIncreasing | `PrismPublicStalenessRefusalsIncreasing` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-public-ledger-unready` | PrismPublicLedgerUnready | `PrismPublicLedgerUnready` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-public-5xx-sustained` | PrismPublic5xxSustained | `PrismPublic5xxSustained` (migrated) | Native producer is populated; preserve the deployed signal with the explicit native bounds and guards in the specification. |
| `qbit-prism-backup-metrics-nodata` | PrismBackupMetricsNoData | `PrismBackupMetricsNoData` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-collector-stale` | PrismBackupCollectorStale | `PrismBackupCollectorStale` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-full-stale-warning` | PrismBackupFullStaleWarning | `PrismBackupFullStaleWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-full-stale-critical` | PrismBackupFullStaleCritical | `PrismBackupFullStaleCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-diff-stale-warning` | PrismBackupDifferentialStaleWarning | `PrismBackupDifferentialStaleWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-diff-stale-critical` | PrismBackupDifferentialStaleCritical | `PrismBackupDifferentialStaleCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-check-warning` | PrismBackupCheckWarning | `PrismBackupCheckWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-check-critical` | PrismBackupCheckCritical | `PrismBackupCheckCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-stale-warning` | PrismBackupWalArchiveStaleWarning | `PrismBackupWalArchiveStaleWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-stale-critical` | PrismBackupWalArchiveStaleCritical | `PrismBackupWalArchiveStaleCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-fail-warning` | PrismBackupWalArchiveFailureWarning | `PrismBackupWalArchiveFailureWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-fail-critical` | PrismBackupWalArchiveFailureCritical | `PrismBackupWalArchiveFailureCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-space-warning` | PrismBackupWalFilesystemSpaceWarning | `PrismBackupWalFilesystemSpaceWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-space-critical` | PrismBackupWalFilesystemSpaceCritical | `PrismBackupWalFilesystemSpaceCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-audit-stale-warning` | PrismBackupAuditSnapshotStaleWarning | `PrismBackupAuditSnapshotStaleWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-audit-stale-critical` | PrismBackupAuditSnapshotStaleCritical | `PrismBackupAuditSnapshotStaleCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-audit-slow-warning` | PrismBackupAuditDurationWarning | `PrismBackupAuditDurationWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-audit-slow-critical` | PrismBackupAuditDurationCritical | `PrismBackupAuditDurationCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-job-fail-warning` | PrismBackupScheduledJobFailureWarning | `PrismBackupScheduledJobFailureWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-job-fail-critical` | PrismBackupScheduledJobFailureCritical | `PrismBackupScheduledJobFailureCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-coverage-critical` | PrismBackupCombinedCoverageCritical | `PrismBackupCombinedCoverageCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-db-growth-warning` | PrismBackupDatabaseGrowthWarning | `PrismBackupDatabaseGrowthWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-db-growth-critical` | PrismBackupDatabaseGrowthCritical | `PrismBackupDatabaseGrowthCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-audit-growth-warning` | PrismBackupAuditGrowthWarning | `PrismBackupAuditGrowthWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-audit-growth-critical` | PrismBackupAuditGrowthCritical | `PrismBackupAuditGrowthCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-growth-warning` | PrismBackupWalGenerationGrowthWarning | `PrismBackupWalGenerationGrowthWarning` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-backup-wal-growth-critical` | PrismBackupWalGenerationGrowthCritical | `PrismBackupWalGenerationGrowthCritical` (unchanged-external) | No native replacement needed: deployment-owned backup/node-exporter rule and producer are retained byte-for-byte. |
| `qbit-prism-coordinator-collector-stale` | PrismCoordinatorCollectorStale | `PrismCoordinatorCollectorStale` (unchanged-external) | No native replacement needed: deployment-owned Docker lifecycle textfile rule and producer are retained byte-for-byte. |
| `qbit-prism-coordinator-restart` | PrismCoordinatorUnexpectedRestart | `PrismCoordinatorUnexpectedRestart` (unchanged-external) | No native replacement needed: deployment-owned Docker lifecycle textfile rule and producer are retained byte-for-byte. |
| `qbit-prism-coordinator-restart-loop` | PrismCoordinatorRestartLoop | `PrismCoordinatorRestartLoop` (unchanged-external) | No native replacement needed: deployment-owned Docker lifecycle textfile rule and producer are retained byte-for-byte. |

## Every historical name

All **46** distinct tokens in the historical appendix are mapped below, including histogram suffixes and names retained by native producers.

| Historical 2.x.x name | Native replacement / disposition | Reason |
| --- | --- | --- |
| `qbit_prism_accepted_block_landing_phase_seconds` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_block_landing_phase_seconds_count` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_block_landing_phase_seconds_max` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_block_landing_phase_seconds_sum` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_block_preview_publication_seconds` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_block_preview_publication_seconds_bucket` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_block_preview_publication_seconds_count` | no replacement | No replacement: accepted landing/preview publication is not proof-to-first-node-offer; landing phases are outside trimmed #278. First-offer timing is declared, rule deferred to A/#266. |
| `qbit_prism_accepted_parent_preview_wait_timeouts_total` | no replacement | No replacement: Python unresolved-parent/preview-wait state machine is absent; pending candidate rows are a different contract. |
| `qbit_prism_accepted_parent_unresolved_depth_max` | no replacement | No replacement: Python unresolved-parent/preview-wait state machine is absent; pending candidate rows are a different contract. |
| `qbit_prism_accepted_parent_unresolved_oldest_seconds` | no replacement | No replacement: Python unresolved-parent/preview-wait state machine is absent; pending candidate rows are a different contract. |
| `qbit_prism_accepted_parent_unresolved_transitions` | no replacement | No replacement: Python unresolved-parent/preview-wait state machine is absent; pending candidate rows are a different contract. |
| `qbit_prism_block_candidate_cleanup_backpressure_active` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_cleanup_backpressure_total` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_cleanup_retry_backlog` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_cleanup_retry_backlog_max` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_cleanup_retry_oldest_seconds` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_cleanup_retry_pending_share_holders` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_cleanup_retry_terminal_outcome_pins` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_collapse_total` | no replacement | No replacement: Python collapse/cleanup retry holders, pins and backpressure state have no native producer. |
| `qbit_prism_block_candidate_oldest_pending_seconds` | `qbit_prism_block_candidate_oldest_pending_seconds` | Same name retained by #278/#277; use native documented meaning and unknown/staleness guards. |
| `qbit_prism_block_candidates_pending` | `qbit_prism_block_candidates_pending` | Same name retained by #278/#277; use native documented meaning and unknown/staleness guards. |
| `qbit_prism_block_ledger_call_timeouts_total` | no replacement | No replacement: Python ledger timeout/submitter backoff series are absent; candidate age retains backlog intent without claiming cause. |
| `qbit_prism_block_submitter_retry_backoff_active` | no replacement | No replacement: Python ledger timeout/submitter backoff series are absent; candidate age retains backlog intent without claiming cause. |
| `qbit_prism_ledger_read_calls_total` | no replacement | No replacement: Python read-admission gate and execution timers are absent; native database_pool_acquire_seconds times collector acquisition only and cannot replace query/lock/gate timing. |
| `qbit_prism_ledger_read_execute_seconds_total` | no replacement | No replacement: Python read-admission gate and execution timers are absent; native database_pool_acquire_seconds times collector acquisition only and cannot replace query/lock/gate timing. |
| `qbit_prism_ledger_read_execute_timeouts_total` | no replacement | No replacement: Python read-admission gate and execution timers are absent; native database_pool_acquire_seconds times collector acquisition only and cannot replace query/lock/gate timing. |
| `qbit_prism_ledger_read_gate_timeouts_total` | no replacement | No replacement: Python read-admission gate and execution timers are absent; native database_pool_acquire_seconds times collector acquisition only and cannot replace query/lock/gate timing. |
| `qbit_prism_ledger_read_gate_wait_seconds_total` | no replacement | No replacement: Python read-admission gate and execution timers are absent; native database_pool_acquire_seconds times collector acquisition only and cannot replace query/lock/gate timing. |
| `qbit_prism_metrics_snapshot_age_seconds` | `qbit_prism_metrics_snapshot_age_seconds` | Same name retained by #278/#277; use native documented meaning and unknown/staleness guards. |
| `qbit_prism_metrics_snapshot_available` | `qbit_prism_metrics_snapshot_available` | Same name retained by #278/#277; use native documented meaning and unknown/staleness guards. |
| `qbit_prism_metrics_snapshot_stale` | `qbit_prism_metrics_snapshot_stale` | Same name retained by #278/#277; use native documented meaning and unknown/staleness guards. |
| `qbit_prism_payout_artifact_events_total` | no replacement | No replacement: payout artifact/rescan/balance-read instrumentation is outside trimmed #278; no deployed rule to rewrite. |
| `qbit_prism_payout_window_full_rescan_seconds` | no replacement | No replacement: payout artifact/rescan/balance-read instrumentation is outside trimmed #278; no deployed rule to rewrite. |
| `qbit_prism_payout_window_full_rescan_seconds_count` | no replacement | No replacement: payout artifact/rescan/balance-read instrumentation is outside trimmed #278; no deployed rule to rewrite. |
| `qbit_prism_prior_balances_reads_total` | no replacement | No replacement: payout artifact/rescan/balance-read instrumentation is outside trimmed #278; no deployed rule to rewrite. |
| `qbit_prism_refresh_pending` | `qbit_prism_stratum_oldest_pending_initial_job_seconds`, `qbit_prism_stratum_current_tip_coverage_gap_seconds` | Replacement for alert intent only: time to usable work, not Python refresh-task state. |
| `qbit_prism_refresh_pending_age_seconds` | `qbit_prism_stratum_oldest_pending_initial_job_seconds`, `qbit_prism_stratum_current_tip_coverage_gap_seconds` | Replacement for alert intent only: time to usable work, not Python refresh-task state. |
| `qbit_prism_reorg_reconcile_errors_total` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_reorg_reconcile_lookups_total` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_reorg_reconcile_pass_seconds` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_reorg_reconcile_pass_seconds_count` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_reorg_reconcile_pass_seconds_sum` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_reorg_reconcile_step_seconds` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_reorg_reconcile_step_seconds_sum` | no replacement | No replacement: Python reconciler pass/step/error series are outside trimmed native instrumentation; do not add undeployed historical rules. |
| `qbit_prism_stratum_authorized_connections` | `qbit_prism_authorized_clients` | Native local authorized clients; update semantic-coverage joins. |
| `qbit_prism_stratum_semantic_current_work_ratio` | `qbit_prism_stratum_semantic_current_work_ratio` | Same name retained by #278/#277; use native documented meaning and unknown/staleness guards. |

## Native rule evidence

The executable PromQL and Grafana conditions are in [prism-native-alert-rules.json](prism-native-alert-rules.json).
These are the only rewritten native rules; the historical appendix is not an active rule specification.

| Rule | Threshold evidence | Producer contract |
| --- | --- | --- |
| PrismMetricsNoData | Preserves the deployed three-minute scrape coverage grace. | [registry.rs#L43](../crates/qbit-prism-server/src/metrics/registry.rs#L43), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismMetricsEndpointDown | Preserves the deployed three-minute scrape outage grace. | [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PRISM Connected Clients | Preserves the deployed network-wide floor: sum all local native connection counts, below ten for three minutes; evaluate only with fresh measurements for every configured target. | [registry.rs#L45](../crates/qbit-prism-server/src/metrics/registry.rs#L45), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismMetricsSnapshotStale | #277 computes staleness at scrape time from max(3 * health refresh, 15s); no copied age constant. | [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismMetricsSnapshotUnavailable | #277 computes staleness at scrape time from max(3 * health refresh, 15s); no copied age constant. | [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismMetricsSnapshotAgeHigh | #277 computes staleness at scrape time from max(3 * health refresh, 15s); no copied age constant. | [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [registry.rs#L80](../crates/qbit-prism-server/src/metrics/registry.rs#L80), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismShareAckP99High | One-second histogram boundary; at least 100 completed ACKs in five minutes. Provisional, measure in #291. | [registry.rs#L55](../crates/qbit-prism-server/src/metrics/registry.rs#L55), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L15](../crates/qbit-prism-server/src/metrics/labels.rs#L15), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismRejectRatioByReasonHigh | Five percent per canonical reason over five minutes, with at least 100 acceptance/rejection decisions. Provisional, measure in #291. | [registry.rs#L50](../crates/qbit-prism-server/src/metrics/registry.rs#L50), [registry.rs#L56](../crates/qbit-prism-server/src/metrics/registry.rs#L56), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L25](../crates/qbit-prism-server/src/metrics/labels.rs#L25), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismTimeToUsableWorkHigh | Authorization to successfully written first job; 15 seconds is a provisional bound, measure in #291. | [registry.rs#L61](../crates/qbit-prism-server/src/metrics/registry.rs#L61), [registry.rs#L62](../crates/qbit-prism-server/src/metrics/registry.rs#L62), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismCurrentWorkGapHigh | Native timer advances only while semantic coverage is strictly below 0.95; 15-second duration is provisional, measure in #291. | [registry.rs#L46](../crates/qbit-prism-server/src/metrics/registry.rs#L46), [registry.rs#L63](../crates/qbit-prism-server/src/metrics/registry.rs#L63), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismSemanticWorkCoverageLoss | Native semantic generation coverage; warning matches the strict 95-percent timer boundary. Critical 50 percent and three-minute dwell: Provisional, measure in #291. | [registry.rs#L46](../crates/qbit-prism-server/src/metrics/registry.rs#L46), [registry.rs#L64](../crates/qbit-prism-server/src/metrics/registry.rs#L64), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismSemanticWorkCoverageLossCritical | Native semantic generation coverage; warning matches the strict 95-percent timer boundary. Critical 50 percent and three-minute dwell: Provisional, measure in #291. | [registry.rs#L46](../crates/qbit-prism-server/src/metrics/registry.rs#L46), [registry.rs#L64](../crates/qbit-prism-server/src/metrics/registry.rs#L64), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismWorkMetricsUnavailable | Unknown sentinels must never masquerade as healthy work coverage. | [registry.rs#L62](../crates/qbit-prism-server/src/metrics/registry.rs#L62), [registry.rs#L64](../crates/qbit-prism-server/src/metrics/registry.rs#L64), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismBlockCandidateBacklog | Native pending outbox rows include claimed/retry-delayed candidates; count bounds 2/5 and age bounds 15/60 seconds. Provisional, measure in #291. | [registry.rs#L66](../crates/qbit-prism-server/src/metrics/registry.rs#L66), [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismBlockCandidateBacklogCritical | Native pending outbox rows include claimed/retry-delayed candidates; count bounds 2/5 and age bounds 15/60 seconds. Provisional, measure in #291. | [registry.rs#L66](../crates/qbit-prism-server/src/metrics/registry.rs#L66), [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismBlockCandidateOldestPending | Native pending outbox rows include claimed/retry-delayed candidates; count bounds 2/5 and age bounds 15/60 seconds. Provisional, measure in #291. | [registry.rs#L67](../crates/qbit-prism-server/src/metrics/registry.rs#L67), [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismBlockCandidateOldestPendingCritical | Native pending outbox rows include claimed/retry-delayed candidates; count bounds 2/5 and age bounds 15/60 seconds. Provisional, measure in #291. | [registry.rs#L67](../crates/qbit-prism-server/src/metrics/registry.rs#L67), [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismBlockCandidateMetricsUnavailable | Collector failure, cancellation or expiry after 30 seconds invalidates both gauges. | [registry.rs#L66](../crates/qbit-prism-server/src/metrics/registry.rs#L66), [registry.rs#L67](../crates/qbit-prism-server/src/metrics/registry.rs#L67), [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismDatabasePoolWaitHigh | Pool p99 >0.5 seconds from at least ten completed acquisitions in five minutes; provisional, measure in #291. | [registry.rs#L68](../crates/qbit-prism-server/src/metrics/registry.rs#L68), [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L16](../crates/qbit-prism-server/src/metrics/labels.rs#L16), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismRuntimeLagHigh | Two-second native blocked-poll budget; five-minute evidence window and three-minute dwell replace the legacy lease-wake episode, provisional, measure in #291. | [registry.rs#L74](../crates/qbit-prism-server/src/metrics/registry.rs#L74), [registry.rs#L75](../crates/qbit-prism-server/src/metrics/registry.rs#L75), [registry.rs#L77](../crates/qbit-prism-server/src/metrics/registry.rs#L77), [labels.rs#L19](../crates/qbit-prism-server/src/metrics/labels.rs#L19), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismResidentMemoryHigh | Four GiB is an unqualified initial RSS bound; set against actual container headroom. Provisional, measure in #291. | [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L73](../crates/qbit-prism-server/src/metrics/registry.rs#L73), [registry.rs#L78](../crates/qbit-prism-server/src/metrics/registry.rs#L78), [registry.rs#L79](../crates/qbit-prism-server/src/metrics/registry.rs#L79), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismProcessMetricsUnavailable | Process collector expires after 30 seconds or on failure; unsupported platforms are unknown. | [registry.rs#L70](../crates/qbit-prism-server/src/metrics/registry.rs#L70), [registry.rs#L73](../crates/qbit-prism-server/src/metrics/registry.rs#L73), [labels.rs#L18](../crates/qbit-prism-server/src/metrics/labels.rs#L18), [metrics.rs#L39](../crates/qbit-prism-server/src/metrics.rs#L39) |
| PrismPublicStalenessRefusalsIncreasing | Any refusal is an actual endpoint staleness-budget breach; five-minute window is provisional, measure in #291. | [public_service.rs#L309](../crates/qbit-prism-server/src/api/public_service.rs#L309), [public_service.rs#L320](../crates/qbit-prism-server/src/api/public_service.rs#L320) |
| PrismPublicLedgerUnready | Readiness already rejects missing/failed probes and ages >= max(3 * probe interval, 15s). | [public_service.rs#L309](../crates/qbit-prism-server/src/api/public_service.rs#L309), [public_service.rs#L320](../crates/qbit-prism-server/src/api/public_service.rs#L320) |
| PrismPublic5xxSustained | Any sustained 5xx response, preserving deployed failure intent; five-minute window is provisional, measure in #291. | [public_service.rs#L309](../crates/qbit-prism-server/src/api/public_service.rs#L309), [public_service.rs#L320](../crates/qbit-prism-server/src/api/public_service.rs#L320) |

<!-- generated-migration:end -->
