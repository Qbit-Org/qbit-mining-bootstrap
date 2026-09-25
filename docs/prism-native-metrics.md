# Native metric inventory

The coordinator (`run`) owns one process-local registry shared by Stratum,
background collectors, and the runtime monitor. `/metrics` renders cached
observations, with the pool-acquisition histogram, collector measurements,
#277 freshness, collector age/availability, and runtime state read at scrape time.
Scraping performs no database, node, or filesystem I/O. Public-api metrics retain
their existing contract.

The generated table below is the sole inventory for both roles: **48 coordinator
families and 15 public families**. Names, types and meanings for `run` come from
[registry.rs](../crates/qbit-prism-server/src/metrics/registry.rs#L42), with bounded
label values from [labels.rs](../crates/qbit-prism-server/src/metrics/labels.rs#L15).
[Compatibility annotations](prism-metric-metadata.json) add replacement names
and the pre-existing public producer contract. Regenerate with
`python3 scripts/generate_prism_metrics.py`; the ungated observability tests
scrape real HTTP listeners for both roles and both public replica modes, compare
families in both directions, and check the generator for drift.

Every coordinator family has HELP and TYPE
metadata, including families declared without samples. The public role retains
its existing untyped exposition, except
`qbit_prism_public_audit_artifact_refusals_total`, which carries both; the
inventory records the counter/gauge intent from its producer. Public response/cache label sets are lazy and appear after a
request; replica gauges appear only with `PRISM_PUBLIC_REPLICA_MODE=require`.
Default histogram boundaries in seconds are 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
1, 2.5, 5, 10, 30, and +Inf. `qbit_prism_share_ack_seconds` adds 15 and 20.
`qbit_prism_ctv_fanout_broadcaster_chunk_rows` counts rows, not seconds, and
exports the single finite bound 1; every other histogram uses the default
ladder. Histograms also export `_sum` and `_count`. See the
[histogram consumer guide](prism-metrics-histogram-consumers.md) for the five
added series per process, elapsed-time attribution, quantile changes,
mixed-version queries and the chunk-row histogram.
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
| `qbit_prism_accepted_block_revision_work_pending_seconds` | gauge | none | run | Monotonic age of the oldest known locally observed acceptance awaiting revision work delivery; -1 when only unknown tracking remains, zero when none; each frontend tracks every block it observes, and a frontend with no connected miners keeps waiting. Computed at scrape time and overlaid on cached metric bodies. A second acceptance never resets the oldest known unresolved delivery wait; a failed build does not clear it. Unknown intervals are exposed independently by accepted_block_revision_work_tracking_unknown, so they cannot mask a separate measured stall. Lost ordering history makes pending age unknown. An offer the node accepted without an active-chain observation is an unlanded acceptance (#493): excluded here and reported by accepted_block_unlanded_seconds until an active-chain observation lands it on its original acceptance clock or a proven orphan closes it. | none |
| `qbit_prism_accepted_block_revision_work_tracking_unknown` | gauge | none | run | Whether any local landing observation is unknown or incomplete; independent of known pending delivery age. Process-local boolean, zero at a fresh start and one for incomplete observation, lost target knowledge or bounded tracking saturation. May be one alongside a positive known pending age; never treat a known age as proof that all tracking is complete. No labels. | none |
| `qbit_prism_accepted_block_to_revision_work_seconds` | histogram | `result=published,degraded,superseded` | run | Frontend-local definitive acceptance observation to first successful mining.notify write carrying compatible post-landing payout work, in seconds; each frontend samples every block from its own observation, so a sum across instances counts one block once per frontend. Frontend-local monotonic timing; no inference of a peer node acceptance timestamp. Every result ends at successful mining.notify write, not prepared publication or proof-to-first-offer. Superseded means a later payout revision arrived before delivery and measures through replacement-work delivery. The pending gauge retains unresolved delivery age. Finite buckets: 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 307 and 600 seconds. | `qbit_prism_accepted_block_preview_publication_seconds` |
| `qbit_prism_accepted_block_unlanded_seconds` | gauge | none | run | Monotonic age of the oldest definitive submitblock acceptance this frontend has not yet observed on the node's active chain, such as a lost tip race until its orphan proof; never part of the known pending delivery age; zero when none, -1 when none is tracked and identity tracking is saturated; while the terminal probe that carries peer orphan evidence fails, the pending gauge reads -1 and the tracking-unknown gauge one even if only unlanded acceptances are open. Computed at scrape time and overlaid on cached metric bodies (#493). Set by the offering frontend at a definitive null submitblock reply; an active-chain observation (the post-offer landing observation, a reconcile proof or a settlement) lands the block, which then joins accepted_block_revision_work_pending_seconds on its original acceptance clock; a committed proven orphan closes it without a histogram sample. An unlanded acceptance never contributes to the known pending age, never attributes a build deadline and is not unknown tracking: a lost tip race is a known state. -1 only when none is tracked and identity tracking is saturated, so an unlanded acceptance may be untracked; a tracked unlanded age stays visible beside saturation, and the tracking-unknown gauge is one either way. While the terminal probe (the ten-second collector read that carries peer orphan evidence) fails or is cancelled, an unlanded-only frontend reads tracking unknown and a -1 pending age, because the lost race's orphan evidence is unreadable; this gauge keeps its age, and the next successful read restores the known state. Warned by PrismAcceptedBlockUnlanded at 30 minutes, never paged. | none |
| `qbit_prism_accepted_shares_total` | counter | none | run | Shares accepted by this instance since process start. Process-local counter; legacy canonical ledger count was persistent. | `qbit_prism_accepted_shares_total` |
| `qbit_prism_authorized_clients` | gauge | none | run | Current local authorized Stratum connections. | `qbit_prism_stratum_authorized_connections` |
| `qbit_prism_authorized_missing_current_work` | gauge | none | run | Authorized connections missing the current semantic work generation. | none |
| `qbit_prism_authorized_with_current_work` | gauge | none | run | Authorized connections holding the current semantic work generation. | `qbit_prism_stratum_clients_with_current_tip_jobs` |
| `qbit_prism_block_candidate_oldest_landing_failed_seconds` | gauge | none | run | Oldest cluster-wide time since the offer reservation of a reconciliation row whose audit landing has not committed (no pool-block row) or whose last error names a landing refusal; zero when none, or -1 when unknown. Read in the same database snapshot as the other candidate gauges (#493 review M1): the oldest reconciliation row whose landing has not committed (the landing transaction is the only writer of the block's qbit_pool_blocks row, so its absence is the durable fact) or whose last_error carries the landing-failed prefix the coordinator and the collector share (a landed row that no longer authenticates): a row the node accepted or rejected whose audit rebuild, verification or ledger landing keeps refusing and is retried at the reconciliation backoff. A retry that fails for a transient node or database reason, or a refused operator recovery, overwrites last_error but not the missing pool-block row, so the age never reads zero while nothing has landed. Measured from offer_reserved_at, the start of the post-offer lifecycle, so pre-offer retry backoff never counts toward it. Zero for a lost race (not-active reason) and for a rejection (its reply). PrismBlockCandidateLandingFailed warns at 60 s for 3 m and PrismBlockCandidateLandingFailedCritical pages at 307 s for 1 m, the same floor and dwell the revision-work critical applies when the landing fails after the frontend's own active-chain observation, so a won block whose landing keeps failing is never silent. -1 with the other candidate gauges when the collector fails. | none |
| `qbit_prism_block_candidate_oldest_pending_seconds` | gauge | none | run | Oldest cluster-wide pending candidate age, or -1 when unknown. Counts every unfinished outbox state: pending, offer_reserved, offered and reconciliation (A/#266). Excludes rows settled as proven orphans (#415, migration 015): those are terminal, so a lost tip race leaves these gauges once a different block has PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS confirmations at its height. Since #493 the paging rule reads block_candidate_oldest_unacknowledged_seconds instead, so an acknowledged lost race in reconciliation warns but never pages; this gauge keeps its all-unfinished meaning and feeds the 15-second warning and the 30-minute stuck warning. | `qbit_prism_block_candidate_oldest_pending_seconds` |
| `qbit_prism_block_candidate_oldest_unacknowledged_seconds` | gauge | none | run | Oldest cluster-wide candidate age the node has not accepted: pending and offer-reserved rows, offered rows whose one submitblock outcome is unknown (not a row adopted on the node's active-chain evidence), and rows the node rejected unless the reply names a side-chain block; zero when every unfinished row was accepted, or -1 when unknown. Read in the same database snapshot as the two all-unfinished gauges (#493): the unfinished rows the node has not accepted are pending and offer_reserved rows, offered or reconciliation rows whose one submitblock outcome is unknown (a transport failure or timeout, or a reservation whose call was lost with its frontend; a row adopted on the node's own evidence that its block is active carries the adoption reply and is excluded), and rows the node definitively rejected unless the reply names a side-chain block (inconclusive, duplicate, duplicate-inconclusive: a lost tip race). A node-accepted lost race awaiting its orphan proof and a landing that keeps failing after acceptance are unfinished but accepted, so they age only block_candidate_oldest_pending_seconds (and the landing-failed gauge). PrismBlockCandidateOldestPendingCritical pages on this gauge, falling back to the all-unfinished age on a target that does not export it yet; PrismBlockCandidateStuck warns on the all-unfinished age at 30 minutes. -1 with the other candidate gauges when the collector fails. | none |
| `qbit_prism_block_candidates_orphaned_total` | counter | none | run | Offered block candidates this instance settled as proven orphans since process start. Counts offered-candidate orphan settlements whose successful completion this instance observed after commit (#415): its landed audit is durable, and one coherent tip observation showed a different block active at its height with at least PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS confirmations. May undercount if cancellation or restart occurs after the database commit and before the process records completion. Never counts a failed observation; a reorg that later reactivates the block is credited by the reorg reconciler without touching this counter. Process-local; starts at zero. No firing rule. | none |
| `qbit_prism_block_candidates_pending` | gauge | none | run | Cluster-wide nonterminal candidate count, or -1 when unknown. Counts every unfinished outbox state: pending, offer_reserved, offered and reconciliation (A/#266). Excludes rows settled as proven orphans (#415, migration 015): those are terminal, so a lost tip race leaves these gauges once a different block has PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS confirmations at its height. Since #493 the paging rule reads block_candidate_oldest_unacknowledged_seconds instead, so an acknowledged lost race in reconciliation warns but never pages; this gauge keeps its all-unfinished meaning and feeds the 15-second warning and the 30-minute stuck warning. | `qbit_prism_block_candidates_pending` |
| `qbit_prism_block_submit_seconds` | histogram | none | run | Locally validated block proof to first node offer; requires the offer owner's timestamp boundary. Observed once per block by the offering frontend after its one submitblock call returned, from the enqueuing frontend's proof-observation wall clock to the offering frontend's wall clock immediately before the send (A/#266). No sample for a row without a proof time, for a negative interval (host clock skew) or on a recovery; a crash between the call and its outcome commit may lose the sample. No firing rule. | `qbit_prism_block_submit_seconds` |
| `qbit_prism_blocks_total` | counter | none | run | Blocks confirmed by this instance since process start. Native confirmed-block process counter; not the legacy node-acceptance accounting boundary. | `qbit_prism_blocks_accepted_total` |
| `qbit_prism_capture_offer_decisions_total` | counter | `decision=offered,abandoned_ceiling,abandoned_disabled` | run | Offer decisions this instance made for pending blocks on the current tip whose payout revision was superseded (#478 block capture), by decision. #478 block capture. `offered`: the block's positive as-issued float (the most debt its confirmation can create, in any order) was within `PRISM_CAPTURE_OVERPAY_CEILING_BPS` of the coinbase value, and the block was reserved for its one offer, decided in the reservation's transaction. `abandoned_ceiling`: the float exceeded the ceiling and the pending block was abandoned; the WARN log names the bound and the ceiling, and `qbit_prism_payout_divergences` keeps both. `abandoned_disabled`: the ceiling is 0, so the block was abandoned as before #478 (recorded as decision `disabled`). The miner of an abandoned capture is answered `stale-job`, never `ledger-confirmation-failed`. A bound that cannot be computed abandons nothing: the offer fails and is retried. Process-local; every series starts at zero. No firing rule. | none |
| `qbit_prism_carry_forward_debt_sats` | gauge | none | run | Sum of negative carry-forward balances, in sats, read from the canonical balances at this instance's latest balance change or full refresh, or -1 before one. Read from the canonical balances after every confirmation or deactivation this frontend commits and at every full refresh snapshot; zero when no account is negative. Process-local. No firing rule. | none |
| `qbit_prism_collector_age_seconds` | gauge | `collector=database,process` | run | Monotonic age of the last successful collector observation, or -1 before success. | none |
| `qbit_prism_collector_available` | gauge | `collector=database,process` | run | Whether a collector has a complete successful observation. | none |
| `qbit_prism_collector_success` | gauge | `collector=database,process` | run | Whether the latest collector attempt succeeded, or -1 before an attempt. | none |
| `qbit_prism_connections` | gauge | none | run | Current local Stratum connections. | `qbit_prism_connected_clients`, `qbit_prism_stratum_active_connections` |
| `qbit_prism_ctv_fanout_broadcaster_chunk_rows` | histogram | none | run | Fanouts attempted per native broadcaster chunk; each chunk contains one row. Values are rows, not seconds: instead of the default time ladder this histogram exports the single finite bound `le="1"` plus `+Inf`, so `_count` is the number of claimed fanout attempts and `_sum` equals `_count`. The 2.x.x ladder (1, 2, 5, 10, 25, 50, 100) is not exported; a query for a higher `le` matches no series. | `qbit_prism_ctv_fanout_broadcaster_chunk_rows` |
| `qbit_prism_ctv_fanout_broadcaster_chunk_seconds` | histogram | none | run | Claimed fanout attempt duration including status persistence, in seconds. Default histogram ladder in seconds; one observation per claimed attempt, recorded even when persisting the attempt's completion fails. | `qbit_prism_ctv_fanout_broadcaster_chunk_seconds` |
| `qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total` | counter | none | run | CTV passes deferred at a fanout boundary for a newer or unpublished tip. | `qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total` |
| `qbit_prism_database_advisory_lock_wait_seconds` | histogram | `lock=migration,order,settlement`; `result=success,failure` | run | Database advisory transaction lock wait by lock and outcome. Client-observed duration of the `pg_advisory_xact_lock` statement, including one database round trip, recorded by the coordinator's ledger for the migration, order and settlement locks; the migration lock is taken only when the coordinator initializes the schema. `failure` includes lock timeout (`PRISM_DATABASE_LOCK_TIMEOUT_MS`, default 5 seconds), statement timeout, deadlock and connection errors, and waits abandoned by cancellation. The CPFP funding lock is not observed (#328). Series appear on their first observation, so a restart's first failure is not visible to `increase()`. | none |
| `qbit_prism_database_pool_acquire_seconds` | histogram | `result=success,failure` | run | Actual database pool acquisition wait by outcome. Client-observed `PgPool::acquire` time: waiting for a pool permit, the idle-connection liveness ping and, when the pool grows, connection setup; excludes transaction BEGIN and the queries that follow. Recorded by the metrics collector, instrumented coordinator ledger transactions and covered direct ledger queries, rollup transactions and share-partition maintenance. The histogram aggregates observed checkout attempts at covered callers. Public-role read pools, private audit HTTP endpoints and shared API read-model helpers, startup/migration gates, operator tools and telemetry-free compatibility wrappers remain excluded; see the acquisition guide for the source census and exclusion policy. `failure` includes acquire errors, the 15-second acquire timeout and acquisitions abandoned by cancellation, recorded with the elapsed wait. Buckets, count and sum are read together from the live registry on each scrape, independently of cached-body publication; scraping does not create observations or renew snapshot freshness. | none |
| `qbit_prism_divergent_landing_overpay_sats_total` | counter | none | run | Carry-forward debt, in sats, created by the divergent confirmations this instance committed. The sum over committed divergent confirmations of the carry-forward debt each created: per account, the increase in max(0, -balance) when the block's rows start to count. Unrecovered while the indebted miner earns no further gross; see the ledger operations guide for the accepted-cost bound. Process-local; starts at zero. No firing rule. | none |
| `qbit_prism_divergent_landings_total` | counter | none | run | Confirmations this instance committed whose landed rows started to count on canonical balances other than their as-issued prior balances. Counted at the confirmation, when a landed block's rows start to count: it is divergent when those rows meet canonical balances other than the as-issued ones its coinbase commits to (#478 captures, and the pre-existing case of a block whose work predates an earlier confirmation). Each is recorded in `qbit_prism_payout_divergences`, in the confirming transaction, with its per-account overpay; a frontend without #478 records and counts nothing. Process-local; starts at zero. No firing rule. | none |
| `qbit_prism_duplicate_shares_total` | counter | none | run | Duplicate share rejections. | `qbit_prism_duplicate_shares_total` |
| `qbit_prism_grace_credited_shares_total` | counter | none | run | Durably accepted shares credited by stale grace. | `qbit_prism_grace_credited_shares_total` |
| `qbit_prism_hashrate_rollup_watermark_lag_seconds` | gauge | none | run | Monotonic time since this frontend last completed a caught-up hashrate rollup pass, or -1 before its first; no sample when the rollup is disabled. The stored watermark is a share sequence, not a time; a caught-up pass is one that advanced the watermark within its batch bound, leaving no unfolded share. A pass that lost the race to another frontend leaves the stamp unchanged, so a frontend that keeps losing reads high: alert on the minimum across instances. -1 runs from loop start until its first such pass, and PRISM_HASHRATE_ROLLUP_ENABLED=0 leaves the family declared with no sample at all. | none |
| `qbit_prism_health_state` | gauge | none | run | Whether this instance is ready to serve mining work. | none |
| `qbit_prism_job_delivery_failures_total` | counter | none | run | Failed local job deliveries. | none |
| `qbit_prism_job_delivery_successes_total` | counter | none | run | Successful local job deliveries. | none |
| `qbit_prism_late_confirmed_shares_total` | counter | none | run | Shares accepted after the share commit deadline once their in-flight ledger commit was confirmed. | none |
| `qbit_prism_low_difficulty_shares_total` | counter | none | run | Low difficulty share rejections. | `qbit_prism_low_difficulty_shares_total` |
| `qbit_prism_metrics_snapshot_age_seconds` | gauge | none | run | Monotonic age of the metrics snapshot, or -1 before the first publication. | `qbit_prism_metrics_snapshot_age_seconds` |
| `qbit_prism_metrics_snapshot_available` | gauge | none | run | Whether a complete metrics snapshot has been published. | `qbit_prism_metrics_snapshot_available` |
| `qbit_prism_metrics_snapshot_stale` | gauge | none | run | Whether the metrics snapshot is missing or exceeds the health freshness budget. | `qbit_prism_metrics_snapshot_stale` |
| `qbit_prism_node_initial_block_download` | gauge | none | run | Whether the node reported initial block download in the latest answered getblockchaininfo, or -1 when unknown. Recorded whenever the latest getblockchaininfo answered with the boolean, including the 1 that then makes readiness fail. -1 means startup, a failed call, or a field that was not a boolean. | none |
| `qbit_prism_node_observation_age_seconds` | gauge | none | run | Monotonic age of the last answered getblockchaininfo, or -1 before one; it grows while the node is unreachable. Stamped when the call answered, not when the whole readiness check passed, so a reachable but unready node is distinguishable from an unreachable one. Computed from a monotonic clock at scrape time. Peers and the sync flag come from the latest attempt to start, so a slower older attempt landing late cannot roll them back, while the age keeps the latest answer any attempt received. | none |
| `qbit_prism_node_peers` | gauge | none | run | Node peer connections from the latest node observation, or -1 when unknown. Recorded only when the readiness attempt reached getnetworkinfo and it answered, which is never on regtest, during initial block download, or while blocks and headers disagree; observation adds no RPC call. -1 means the latest attempt did not learn the count, including RPC failure; the gauge never keeps an earlier reading. An answered zero is a real zero, and it is below every configured PRISM_MIN_PEERS floor. | none |
| `qbit_prism_pending_job_builds` | gauge | none | run | Current local pending job deliveries. Delivery count replaces the operational intent of queue depth, not its implementation. | `qbit_prism_job_delivery_queue_depth` |
| `qbit_prism_process_resident_memory_bytes` | gauge | none | run | Process resident memory bytes from procfs, or -1 when unknown. | `qbit_prism_process_resident_memory_bytes` |
| `qbit_prism_public_audit_artifact_refusals_total` | counter | none | public-api | Audit artifact requests refused with 503 audit_artifact_busy because PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT audit artifacts were already in flight. Public-api role only: the mining process serves the same route on its operator listener and applies the same cap there, but has no public-service metrics owner, so those refusals are uncounted. | `qbit_prism_public_audit_artifact_refusals_total` |
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
| `qbit_prism_refresh_seconds` | histogram | `trigger=initial,tip,revision,balances,reanchor,shares,template,fee`; `acquisition=delta,full,cached` | run | Template refresh from entry to published work, in seconds, by what triggered the rebuild and how its window was acquired. Observed once per refresh that published rebuilt work, from the refresh's entry (after the poll or block-wait wake) to its publication, on the coordinator's monotonic clock. A refresh that reused the published work unchanged, or that failed, records nothing. `trigger` names what invalidated the published work, by the label's own precedence when several inputs changed on one poll: `initial` (nothing published yet), then `tip` (the template parent changed), `revision` (the payout revision changed), `balances` (the carry-forward balances changed), `reanchor` (the cached window aged past `PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS`, or no cached window was held while work was published), `shares` (new shares with no cached bundle), `fee` (the relay fee floor changed) and `template` (any other template or publication change, including a cached window that no longer matches the published reference). The reuse test itself checks these inputs in a different order; the label is a classification of the same inputs, never a decision. On a same-template poll the revision, balances and share inputs are the ledger probe's; on a new template the ledger is not probed at entry, and the label compares the published work with the window the rebuild used, so a revision or balance change that had landed by the rebuild's admission is named ahead of `template`. `acquisition` says where the window came from: `delta` or `full` for a fresh ledger snapshot by that path, `cached` when the previous refresh's window was reused and only the body was rebuilt. Series appear on their first observation. Default buckets. | none |
| `qbit_prism_refresh_window_acquisitions_total` | counter | `outcome=advanced,no_prior,empty_prior,tail_mismatch,anchor_regressed,cutoff_regressed,delta_too_large,no_evidence,leaf_changed,margin_too_large,partial,witness_changed,count_mismatch,invariant` | run | Refresh payout-window snapshots by acquisition outcome: advanced by the delta path, or the reason the full scan ran. Counted once per refresh snapshot by the coordinator's ledger, at the decision between the delta path and the full newest-first scan. `advanced` is the delta path: the retired window's rows were reused for the fresh anchor, cutoff and network difficulty, including a retarget in either direction and a multi-page delta. Every other outcome names why the full scan ran: `no_prior` (no retired window was exclusively owned, as at the first refresh or after a cancelled build kept a reference), `empty_prior` (the retired window had no rows), `tail_mismatch` (the retired window's last row was not its accepted cutoff, which only legacy or raw rows with a future timestamp at the top of the ledger can cause), `anchor_regressed`, `cutoff_regressed`, `delta_too_large` (more than 262,144 new sequence slots), `no_evidence` (the retired window had no single-leaf witness, as across a partition boundary), `leaf_changed` (the retained range's partition was detached, reattached or restored, or the writer timeline changed, before the delta was read), `margin_too_large` (a heavier target needed more than 16 older pages), `partial` (history ran out before the target, so the full scan decides), `witness_changed` (the merged range's leaf, incarnation or timeline differed when re-read after the pages, including a margin that crossed into another leaf), `count_mismatch` (the anchored row count of the merged range differed, as after a retroactive insert or newly eligible timestamp) and `invariant` (the merged window failed its own ordering or crossing check, logged at error). Every outcome is initialized to zero. A high full-scan share is a cost signal, never an error; `invariant` is a defect signal. | none |
| `qbit_prism_rejected_shares_total` | counter | none | run | Shares rejected by this instance since process start. | none |
| `qbit_prism_rejections_total` | counter | `reason_id=stale-job,duplicate-share,low-difficulty,malformed-submit,unauthorized-worker,unknown-job,invalid-extranonce,invalid-ntime-or-nonce,backend-rpc-unavailable,internal-error,pool-closed,ledger-confirmation-failed,ledger-outcome-unknown,unrecognised` | run | Share rejections by canonical bounded reason ID. Present unknown or empty IDs map to unrecognised; missing IDs and explicit internal-error retain internal-error. Normalization does not change the protocol response. | `qbit_prism_rejections_total` |
| `qbit_prism_revision_work_build_timeouts_total` | counter | none | run | Existing build deadlines actually hit while a known accepted-block revision work wait is open on this frontend; deadlines hit while only unknown tracking remains are not counted. Counts actual existing deadline hits, once per operation; success, cancellation and ordinary errors are not timeout events. No deadline or fallback policy is introduced. Only a known open wait attributes a deadline hit here (#493): while tracking is unknown and no known wait remains (a lost or cancelled settlement COMMIT reply, an unsettled orphan verdict, bounded-tracking saturation or lost ordering history), a deadline hit is an ordinary job_delivery_failures_total event, accepted_block_revision_work_tracking_unknown reports the unknown state and the pending gauge stays -1; it is not counted here and does not stamp a wait recovered after the deadline degraded; a wait recovered before the deadline counts and is degraded as usual. | `qbit_prism_accepted_parent_preview_wait_timeouts_total` |
| `qbit_prism_runtime_lag_seconds` | gauge | none | run | Latest observed runtime sampler wake lateness, or -1 before the first observation. Runtime-stall intent replaces lease wake delay; no native writer lease. | `qbit_prism_lease_heartbeat_monitor_wake_delay_window_max_seconds` |
| `qbit_prism_runtime_poll_lag_seconds` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector,share_partitions` | run | Maximum active poll duration or completed poll duration retained for 60 to 61 seconds, by task. | none |
| `qbit_prism_runtime_progress_age_seconds` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector,share_partitions` | run | Oldest active operation time since progress; zero when idle. | none |
| `qbit_prism_runtime_task_stalled` | gauge | `task=refresh,submit,block_wait,broadcast,rollup,health_publisher,stratum_listener,stratum_session,collector,share_partitions` | run | Whether an active poll or operation exceeds its progress budget. | none |
| `qbit_prism_runtime_workers` | gauge | none | run | Configured Tokio runtime worker threads. | none |
| `qbit_prism_share_ack_seconds` | histogram | `result=accepted,rejected` | run | Complete mining.submit frame arrival to completed response write, by outcome. Uses the default histogram ladder plus 15 and 20 seconds; these are elapsed ACK bounds, not measured ledger deadlines. | `qbit_prism_share_ack_seconds` |
| `qbit_prism_share_ledger_partition_lead_rows` | gauge | none | run | Rows of attached share ledger partition headroom above the next share_seq, or -1 when unknown. max(upper_seq) over the attached rows of qbit_prism_share_partitions minus the next share_seq, read by the database collector with the pending-candidate counts (#144). -1 means the collector's last attempt failed or the ledger is not partitioned yet; it is never a report of exhausted headroom. Falling steadily means the maintenance task is not attaching the lead: the next append past the last bound is refused and retried once after attaching it. | none |
| `qbit_prism_stale_job_rejections_total` | counter | `cause=resume_expired,fee_floor,parent_grace,payout_revision` | run | Stale-job share rejections by the internal decision that refused them. Each series counts one existing stale-job decision: resumed-job absolute expiry, CTV relay-fee floor, stale parent or failed stale-grace parent check, and payout-revision mismatch, attributed in that execution order. The wire reason and message are unchanged and still counted by `qbit_prism_rejections_total{reason_id="stale-job"}`. Stale-grace credit is not a rejection. Since #478 a block-bearing proof from ordinary work on the active tip is captured whatever its payout revision, so it is no longer counted under `payout_revision`. Still counted: block-only work a same-parent payout replacement retired (its plain shares, and the share of a block captured from it) under `payout_revision`, or `parent_grace` after a tip change; and a capture abandoned before its offer, under `payout_revision` for a ceiling refusal or capture off and `parent_grace` for a lost race. Process-local; every series starts at zero. | none |
| `qbit_prism_stale_shares_total` | counter | none | run | Shares rejected as stale or unknown jobs. | `qbit_prism_stale_shares_total` |
| `qbit_prism_stratum_connection_limit` | gauge | none | run | Configured global Stratum connection limit, not currently available permits; -1 before a listener starts. Set from `PRISM_STRATUM_MAX_CONNECTIONS` when a Stratum listener starts. The primary and high-difficulty listeners share this limit and `qbit_prism_connections`. | none |
| `qbit_prism_stratum_connection_refusals_total` | counter | `reason=global_limit,username_limit,ip_limit,malformed_frame_budget,unknown_job_budget,authorize_budget` | run | Stratum connections refused by an admission limit or disconnected by a per-session budget, by closed reason. `global_limit` counts a newly accepted socket closed because `PRISM_STRATUM_MAX_CONNECTIONS` permits were exhausted; `username_limit` counts a `mining.authorize` refused by `PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME`; `ip_limit` counts a newly accepted socket closed at `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP`, before any permit, task or query. `malformed_frame_budget`, `unknown_job_budget` and `authorize_budget` count a session disconnected for exceeding `PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL`, `PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL` or `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL`, once per session. Same-username reauthorization, reuse of a retained username permit and every disabled limit never count. Refusals are not share rejections, and no series carries a peer address. Process-local; every series starts at zero. | none |
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
heartbeats. Rollup transactions and share-partition maintenance also record
acquisitions. [The acquisition guide](prism-pool-acquire-metrics.md) lists the
source census and explicit exclusions: public-role read pools, private audit
HTTP endpoints and shared API read-model helpers, startup/migration gates,
operator tools and telemetry-free compatibility wrappers. The histogram aggregates
observed checkout attempts at covered callers. Duration is the monotonic elapsed pool wait until
acquisition completes or is cancelled; subsequent transaction work is excluded.
Collector status also records collection failure or cancellation separately.
Candidate count and age describe database time; this is not a monotonic latency
measurement. A/#266 must update the pending predicate if outbox states change.

`qbit_prism_share_ledger_partition_lead_rows` is read in the same transaction
as those two, so a collection failure leaves all three unknown together. It is
the number of `share_seq` values the attached share ledger partitions still
cover above the next one the sequence will hand out (#144): about 67 million
with the default width and lead, several hours of appends at any rate this pool
has run at. It falls as shares are appended and steps back up each time the
maintenance task attaches a partition, so a value that only falls means the
task is not running or its calls are failing, which
`PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS` in
[prism-configuration.md](prism-configuration.md) describes. Reaching zero does
not lose a share: the append that finds no partition attaches the lead itself
and retries once. A never-partitioned ledger and a failed read both read -1;
neither is a report of exhausted headroom.

The pending predicate is the four unfinished outbox states (`pending`,
`offer_reserved`, `offered`, `reconciliation`). Since #415 (migration 015) a
reconciliation row whose block lost a tip race does not stay in that set
forever: once its audit has landed and one coherent tip observation shows a
*different* block active at its height with at least
`PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS` confirmations (default 6, counted from
the observed tip as `tip_height - height + 1`), the post-offer settlement marks
the row terminal as `orphaned`, in the same transaction as its reason and its
block's `inactive` chain state. Both pending gauges then stop counting the row.
The process increments `qbit_prism_block_candidates_orphaned_total` after it
observes a successful commit; cancellation or restart between commit and
recording can lose that increment. The row releases its document, block bytes
and window reference while preserving offer metadata and the reason for the
operator surface (#268); it is never claimed or offered again. A failed observation settles nothing, so
unknown stays distinct from zero: the row waits in reconciliation and the
gauges report -1 only when the collector itself fails. A later reorg that
reactivates the block is credited by the ordinary reorg reconciler from the
landed audit, deferred share included, without reopening the row or touching
the counter. Rows a frontend cannot land (a rebuild refused for good, a missing
balance snapshot) are not orphans and stay in the pending set with their reason
in `last_error`; that backlog is operator work.

The paging rule does not read that all-unfinished age (#493). The same
snapshot also reports `qbit_prism_block_candidate_oldest_unacknowledged_seconds`,
the oldest age among unfinished rows the node has not accepted: `pending` and
`offer_reserved` rows, offered rows whose one `submitblock` outcome is
`unknown` (a row adopted on the node's own active-chain evidence carries the
adoption reply and is excluded), and rows the node definitively rejected
unless the reply names a side-chain block (`inconclusive`, `duplicate`,
`duplicate-inconclusive`, a lost tip race). `PrismBlockCandidateOldestPendingCritical`
pages on it at 60 seconds, so a found block that is not reaching the node and
a block the node refused (`bad-*`, `high-hash`, `duplicate-invalid`: the pool
built an invalid block) still page exactly as before, and an unknown outcome
keeps paging because receipt is unproven and the block is never offered
again. A node-accepted lost race is unfinished but accepted: it ages only the
all-unfinished gauge, which the 15-second warning still reports and
`PrismBlockCandidateStuck` warns on at 30 minutes without paging. A landing
that keeps failing after the offer is reported by its own gauge,
`qbit_prism_block_candidate_oldest_landing_failed_seconds`, the time since
the offer reservation of the oldest reconciliation row whose landing has not committed (the landing transaction
is the only writer of the block's `qbit_pool_blocks` row, so its absence is
the durable fact, and a transient retry failure or a refused operator
recovery that overwrites `last_error` cannot zero it) or whose `last_error`
carries the landing-failed prefix the coordinator and the collector share (a
landed row that no longer authenticates): `PrismBlockCandidateLandingFailed` warns
at 60 seconds for three minutes and `PrismBlockCandidateLandingFailedCritical`
pages at 307 seconds for one minute, the floor and dwell the revision-work
critical applies when the landing fails after the frontend's own active-chain
observation, so a won block whose ledger landing keeps failing is never
silent whichever step refused; a lost race and a rejection leave that gauge
at zero. All four gauges read -1 together when the collector fails. Rollout
order: frontends first, then rules; until a target exports the unacknowledged
series the paging expression falls back to its all-unfinished age, so the
base behaviour (its lost-race page included) persists on that target.

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
payout-build duration remains outside the trimmed metrics scope; the node gauges
and the rollup-lag series in the inventory above were added by the #278 hardening
remainder and still add no setting. #458 adds the landing observation contract below. The subsequent
[#278 cardinality/privacy qualification](prism-metrics-cardinality-privacy.md)
pins the complete run-role wire census and tests identifier-bearing inputs
through Stratum and the candidate collector without expanding the family set.

## Accepted block to delivered revision work (#458)

`qbit_prism_accepted_block_to_revision_work_seconds` begins when this frontend
classifies its actual `submitblock` reply as definitive acceptance, or first
obtains a coherent active-chain proof for the block. A recovered unknown offer
starts at that proof, never at an invented earlier acceptance time. A peer
frontend starts at its own proof during reconciliation; these frontend-local
intervals must not be presented as a shared node-acceptance clock. Mature
historical blocks do not start new observations.

`published` ends after `write_all` successfully writes the complete first
`mining.notify` frame whose job carries the associated post-landing revision.
This is socket-write completion, not an acknowledgement from mining hardware.
Prepared publication, job construction, issuance persistence, first node offer
and a failed socket write are not completion. The local settlement observer
uses the revision from the same committed transaction, including a no-op
confirmation or reactivation. A peer proof associates the first revision this
frontend can prove includes that block; it does not reconstruct an unseen
original confirmation revision. An in-flight local settlement cannot be
replaced by this peer-proof association. If its reply is delayed, actual write
and revision-observation clocks are retained so later association preserves
which event happened first.

A definitive `submitblock` acceptance is not an active-chain observation: the
block may have lost a tip race (#493). Such an acceptance is tracked as
**unlanded**, reported by `qbit_prism_accepted_block_unlanded_seconds` (the
age of the oldest one; zero when none; -1 only when none is tracked and
identity tracking is saturated) and never part of the known pending age or of build-deadline
attribution, because it has no revision work to deliver. The post-offer
landing observation, a reconcile proof or a settlement observation lands it:
the same identity then joins the known pending age on its original
acceptance clock, so the histogram still measures from the definitive
acceptance. A committed proven orphan closes it without a sample. Only the
offering frontend ever holds an unlanded acceptance; a peer starts at its own
active-chain proof. An unlanded acceptance is a known state, not unknown
tracking, with one deliberate exception: while the terminal probe that
carries peer orphan evidence fails or is cancelled, an unlanded-only
frontend reads tracking unknown and a -1 pending age, because the lost
race's orphan evidence is unreadable; the unlanded gauge keeps the age and
the next successful read restores the known state. A build deadline
attributed through another known wait never marks an unlanded block
degraded: it has no revision work yet.

`superseded` means a later revision was observed before delivery of the target
revision. Its histogram interval still ends at actual replacement-work delivery,
never at the revision observation. The pending gauge retains the original
acceptance age through supersession and a second accepted block, until a
successful write of compatible current replacement work. A late write of an
already superseded revision cannot clear that wait. Thus stalls are visible
before the first completed histogram sample. The gauge is computed from a
monotonic clock at render time and overlaid on cached metric bodies.
With no connected miners there is no qualifying notify delivery: pending age
continues to rise even when prepared work is ready, and the warning rule
reports it. The metric does not reinterpret an empty listener as successful
miner delivery. Every frontend observes every active immature block from its
own reconcile proof, so each frontend carries its own wait for the same block
and a sum across instances counts one block once per frontend; the paging rule
therefore requires an authorized miner on the reporting frontend (#493).

`qbit_prism_revision_work_build_timeouts_total` increments only in the existing
Stratum job-build timeout branch, once for that operation when a known
accepted-block wait accepted before the build was open at its start or when
the deadline fired. Its budget includes the existing admission wait. Only
known waits attribute a deadline (#493): while this frontend's tracking is
unknown and no known wait remains (a lost or cancelled settlement COMMIT
reply, an unsettled orphan verdict, bounded-tracking saturation or lost
ordering history), a build deadline is an ordinary
`qbit_prism_job_delivery_failures_total` event; the tracking-unknown gauge
reports the unknown state, the pending gauge stays -1, and the deadline is
neither counted here nor stamps a wait recovered after it `degraded`. A wait
recovered by a proven first confirmation before the deadline is counted and
`degraded` as usual.
Success, ordinary errors, cancellation, node-offer timeouts and issuance or
socket-write deadlines are not counted as job-build timeouts. A subsequent
qualifying delivery is `degraded` if an applicable build deadline was hit before
that write. A timeout of an older operation cannot degrade a later acceptance.
Native refresh currently has no overall deadline and this instrumentation adds
none; no refresh deadline or fallback-delivery policy is inferred from elapsed
time. The histogram differs from both proof-to-first-offer and the #275/#444
harness approximation, which do not observe this actual socket boundary.

Tracking is process-local and bounded to 4,096 accepted block identities,
4,096 revisions and 4,096 delivery timestamps while delivery remains unresolved.
Repeated writes at the same revision share a timestamp until another acceptance
arrives, preserving the first eligible write for each acceptance. Completed identities
are retained as deduplication tombstones until a coherent mature accounting
checkpoint retires their height. A monotonic height watermark then rejects
delayed proofs even after the identity is released; unresolved intervals are
never retired by that checkpoint. Revision events are released when all known
deliveries resolve. Nothing is evicted to make a duplicate look new.
Exceeding a bound sets `accepted_block_revision_work_tracking_unknown` to
**1 until restart**. Missing identities have no fabricated observations; known
pending ages remain visible after identity-only saturation. Lost revision or
delivery history additionally sets pending age to -1 and suppresses terminal
samples because supersession or the first delivery can no longer be proven.
Within this horizon,
a tracked block has at most one terminal sample, and exactly one once its
revision and qualifying delivery are both known. Process restart loses
unresolved intervals and deduplication history, and begins new local proof
clocks rather than replaying peer timestamps. If a local settlement commits but
its exact-revision reply is lost or cancelled, tracking is unknown and the interval
stays unresolved: a current-revision proof cannot safely reconstruct that
original revision. A later proven first confirmation can recover an attempt
that did not commit; an already-confirmed replay cannot invent the lost fact.
The same unknown boundary applies when a peer confirms a locally accepted
block during audit landing, before the local settlement observer starts. Only
a newly observed already-confirmed peer block may begin at its current local
proof revision; a later proof cannot upgrade an earlier unresolved acceptance.
Universal exactly-once reporting beyond these knowledge/capacity boundaries
requires durable observation metadata and is not claimed here.

Unknown tracking is independent of known pending age: an unknown interval A
cannot hide a measured interval B crossing the critical threshold. Pending age
is -1 when no known interval remains but tracking is unknown, and zero only
when the observed empty state is known. The separate unknown gauge stays one
even while a known interval's positive age is reported. A rejected transaction
before COMMIT does not invent a lost outcome. Operator recovery uses the same
committed revision evidence as ordinary settlement, and a proven first
confirmation remains authoritative regardless of observer arrival order.

A successfully committed proven orphan closes its delivery wait without a
histogram sample: it has no eligible delivery target. Its identity remains a
deduplication tombstone through the same mature watermark, including if later
chain reconciliation reactivates it; this does not change payout credit or
block-confirmation counting. The shared ledger observer arms uncertainty only
at the existing orphan COMMIT boundary, so a rejected precommit verdict leaves
the measured wait intact. An in-flight, lost or cancelled orphan COMMIT cannot
be relabeled as successful delivery or upgraded by a later revision proof.

Each frontend discovers peer-settled orphans through the existing ten-second
metrics collector cadence. One extra read-only lookup checks at most 4,096
locally unresolved hashes against the terminal outbox state; it is skipped
when that set is empty and shares the existing three-second total collection
deadline and read-only snapshot. No background loop, accounting lock, schema,
write, or work-publication dependency is added. Positive durable orphan
evidence closes only a still-tracked identity, even if its original COMMIT
reply was lost; negative evidence never guesses the original outcome.
A failed or cancelled terminal read preserves pending state and exposes
tracking uncertainty, and an older completion cannot erase a newer failure.
Before the next successful collection, the accepting frontend still reports
its locally known wait; this is a local knowledge boundary, not the peer's
orphan commit time.

A failed or cancelled refresh preserves known pending age. If no unresolved
acceptance is known, that failed observation yields -1 rather than a fabricated
healthy zero; a later successful refresh restores the known empty zero. An
older cancelled refresh cannot overwrite a newer completed observation. No
block, job, worker, frontend or height label is emitted.

There are **five concrete alert rules**: `PrismAcceptedRevisionWorkPending`
warns above one second and for a missing pending series or a failed scrape,
`PrismAcceptedRevisionWorkTrackingUnknown` warns for unknown tracking (a -1
age, the tracking-unknown gauge or a missing unknown gauge) after a two-minute
dwell, `PrismAcceptedRevisionWorkPendingCritical` uses the provisional
307-second incident-duration floor, `PrismRevisionWorkBuildTimeouts` warns on
a five-minute increase, and `PrismAcceptedBlockUnlanded` warns when an
unlanded acceptance is 30 minutes old for three minutes. The budget and
timeout warnings have zero additional dwell and alert on no data, so a
missing measurement is always visible. The unknown warning dwells (#493
point 7): a failed template refresh marks tracking unknown only until the
next successful refresh, one poll interval later, so a single transient
refresh error never spans the eight consecutive 15-second scrapes the dwell
requires, while a lost settlement reply, an unsettled orphan verdict,
saturation or lost ordering persist and still warn. The paging
critical is gated (#493): the age must be known, the target scraped and at
least one miner authorized on that frontend, with a one-minute dwell and
no-data and evaluation errors treated as OK. It is deliberately not gated on
the metrics snapshot: the pending gauge is a live overlay rendered on every
scrape, and a database-pool stall that also stalls the health publisher must
still page. The miner count is a snapshot family, so while the snapshot is
stale the gate uses a count a few refresh intervals old; a false page on a
frontend whose miners left inside that window is accepted over a missed
stall. The dwell only absorbs a brief gate gap on a gauge that is monotonic
during a stall and stays well under the five-minute hold that delayed #413.
If every miner leaves a frontend during a stall, this page resolves and
nothing else pages for that frontend: the coverage rules cannot fire with
zero authorized clients and the connected-clients rule is a cluster-wide
non-paging warning, so only the pending warning keeps reporting the age until
a miner reconnects and receives work. A lost race on the offering frontend
never pages (#493 point 1): its acceptance is unlanded, outside the known
age, until an active-chain observation lands it or its orphan is proven. A
routine lost race is proven within a few block intervals (six competitor
confirmations at the 60-second target spacing plus the reconciliation retry
backoff), well inside the 30-minute unlanded warning; past that the row is
not being reconciled, the node is not advancing or the orphan verdict keeps
failing, which is warned operator work, never a page. The one-second warning
is a budget target; 307 seconds is not evidence of early detection. Scrape and
evaluation intervals still add delay. Thresholds must be measured in #291;
this repository change neither applies the generated external patch nor
claims operational deployment. Five historical definitions consolidate into
these native rules, with the two timeout severities sharing one warning.

The required PostgreSQL/fake-node/socket tests are in
`crates/qbit-prism-server/tests/landing_metrics.rs`; state ordering, saturation,
unknown measurements and live cached-body overlays are in
`crates/qbit-prism-server/src/metrics/landing/tests.rs`. The independent census
is 51 run-role families, 214 startup series and 312 populated series on this
base, with exactly these four new families relative to #450; #493 adds the
unlabeled unlanded gauge and the unlabeled unacknowledged and landing-failed
candidate ages.

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
- `reason="ip_limit"` increases when a newly accepted socket is closed at
  `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP`. Like `global_limit` the miner sees an
  accepted connection closed with no JSON-RPC response, and the decision is
  taken at accept, before any permit, session task or database query. The label
  never names the address; the matching structured tracing event carries
  `peer`, so search the log when this rises. Both listeners share one budget
  per address. See
  [prism-b4-stratum-admission.md](prism-b4-stratum-admission.md) before
  raising the cap.
- `reason="malformed_frame_budget"`, `"unknown_job_budget"` and
  `"authorize_budget"` increase once per session, when a per-session rate
  budget disconnects it. The session receives its error response first, then
  the close. A rise with no operator change means either a misbehaving client
  or a budget set below an honest rate: an unknown-job burst is normal
  immediately after a tip or payout-revision change, so compare it with
  `qbit_prism_rejections_total{reason_id="unknown-job"}` before lowering
  anything.
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

First-offer timing is populated by A/#266's offer owner. It remains declared
without samples until an actual first-offer observation occurs; this startup
state is included in the cardinality qualification.
Since #328, instrumented coordinator ledger transactions record into
`database_pool_acquire_seconds`, and the coordinator's migration, order and
settlement advisory-lock acquisitions record into
`database_advisory_lock_wait_seconds`; the CPFP funding lock is not timed.
Covered non-transaction ledger queries are timed through `Ledger::acquire`;
rollup and share-partition maintenance use the same acquisition timer. The
[acquisition guide](prism-pool-acquire-metrics.md) records the source census,
explicit exclusions and CI guard added for #352. The histogram counts observed
checkout attempts at covered callers, not all process acquisitions. The native alert
specification attaches no firing rule to first-offer timing: `block_submit_seconds`
is observed at most once per found block (#391), too sparse for a rate or
percentile alarm. Order and settlement advisory-lock waits are covered by
`PrismDatabaseAdvisoryLockWaitHigh` and `PrismDatabaseAdvisoryLockFailuresCritical`;
`lock="migration"` has no rule because that lock is taken only at schema
initialization, where waiting out an online migration is expected.
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

## Reading the node gauges and the rollup lag

`qbit_prism_node_peers`, `qbit_prism_node_initial_block_download` and
`qbit_prism_node_observation_age_seconds` come from one observation site, the
readiness chain check, and record only what that attempt already fetched. They
add no RPC call, so a check that short-circuits before `getnetworkinfo` -
regtest, a node in initial block download, or a node whose blocks and headers
disagree - leaves the peer count unknown at -1. Unknown never means the last
reading: a refused `getnetworkinfo` reports -1 rather than the peer count of an
earlier attempt, so the gauge cannot advertise a peer floor nobody confirmed. An
answered zero is a real zero, and it is below every configured `PRISM_MIN_PEERS`.

The sync flag is recorded whenever `getblockchaininfo` answers with the boolean,
including the 1 that then makes readiness fail, and reads -1 only when that call
failed or answered with something that is not a boolean. The observation age is
stamped on that answered call rather than on a fully successful check, so a node
that answers but is not ready keeps a small age while an unreachable node's age
grows without bound. It is monotonic and computed at scrape time. Concurrent
refresh, block-wait, candidate and broadcaster observations are ordered by the
instant each attempt began, so a slower older attempt landing late cannot roll
back a newer observation. That ordering governs the peer count and the sync
flag, which belong to one attempt; the age instead keeps the latest answer any
attempt received, so a superseded attempt whose call answered last still proves
the node was reachable then.

`qbit_prism_hashrate_rollup_watermark_lag_seconds` is not derived from the
stored watermark: `qbit_hashrate_rollup_progress.last_share_seq` is a share
sequence, not a time, and its `updated_at` uses the database clock. The series
is instead the monotonic time since this frontend last completed a rollup pass
that advanced the watermark without filling its batch bound, which is the pass
that left no unfolded share behind. An idle pool scans nothing and still
advances, so it stays caught up; a backlog, a failing pass or a stopped loop
grows the lag without bound. Competing frontends race, and the loser advances
nothing, so a frontend that keeps losing reads high while the cluster is healthy:
alert on the minimum across instances, never on a single one. The series reads
-1 from loop start until its first caught-up pass, and while
`PRISM_HASHRATE_ROLLUP_ENABLED=0` the family is declared without any sample,
because a frontend that never runs the rollup has no lag to report.
