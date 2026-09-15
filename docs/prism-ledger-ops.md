# PRISM Ledger Operations

This is the operating contract for the native Rust Prism server. The database
retains the original accounting tables and functions while additive migrations
provide active-active application instances. For the upgrade procedure, see
[Rust migration](prism-rust-migration.md).

## Share commit and ordering

All instances insert accepted shares into `qbit_share_ledger` in one PostgreSQL
database. A short transaction-scoped advisory lock orders insertion and snapshot
creation. PostgreSQL assigns the canonical `share_seq`; sequence gaps are
permitted, and sequence order is authoritative. Local arrival timestamps,
frontend counters, and per-worker summaries do not determine reward order.

The submission path validates identity, job, header, and target before durable
admission. A normal successful Stratum response follows the database commit,
with `synchronous_commit=on`. A share meeting the network target also persists
its complete candidate intent atomically with the share. Connection loss after
commit can lose the reply without losing the share.

An exact replay returns the existing share without another reward credit.
Conflicting reuse of a share identifier fails. The native global proof-hash
registry prevents the same newly submitted proof receiving separate credits
under different usernames or on different servers. Historical rows are retained
unchanged during migration, including any duplicates accepted by older code.

There is no Python batch queue or process-wide ledger writer lease. All healthy
instances may append; transaction locks protect a shared ordering boundary.
Database connection and statement/lock limits bound resource use. Session
extranonce allocation also uses the shared database sequence and never cycles.

## Snapshot and payout boundary

`qbit_prism_window(anchor_job_issued_at, window_weight)` selects eligible shares
newest first by `share_seq`, counting a partial oldest share when the requested
weight is reached. Both `job_issued_at` and `accepted_at` must be no later than
the anchor. The audit wrapper fixes the reward weight to eight times network
difficulty.

Native snapshots record a database-coordinated monotonic anchor, share range,
and payout revision. Share acceptance and snapshot creation use the same
ordering lock, so equal wall-clock timestamps or host clock differences cannot
let a later share enter an earlier snapshot. Published jobs bind their snapshot;
later accepted work cannot change their committed coinbase.

Payout-changing operations serialize under the settlement transaction lock.
They advance the shared payout revision, and job publication checks that its
revision remains current. A cluster fingerprint binds genesis, ledger and
manifest public keys, reward multiplier, payout policy, and CTV policy. A
mismatched instance fails startup. Coordinators can use different local resource
limits and synchronized qbit nodes on the same chain.

A pool with no historical shares issues solver-paid bootstrap work. There is no
three-miner gate. A network-valid candidate below its assigned share target is
stored without ordinary share credit. Active-chain confirmation inserts its
deferred share at network difficulty; a losing candidate receives no credit.

## Durable block candidates

`qbit_block_candidate_outbox` stores complete candidate evidence before a node
submission can be lost to a process crash. Workers claim pending rows with
expiring, token-fenced database claims. Multiple instances may process different
candidates; a stale claimant cannot overwrite a successor's accounting result.

The worker submits the preserved block bytes, observes the active chain, and
persists verified block/audit/payout state transactionally. Transient failures
leave durable retry work. A lost RPC reply is resolved by querying the chain or
re-offering the same block; an accepted duplicate does not create another payout.
Terminal candidates retain the evidence needed for replay identity while large
pending payloads can be released. Deferred below-share-target credit is tied to
the same durable candidate lifecycle.

PostgreSQL and qbitd do not share a transaction. Duplicate node offers are
possible after an interrupted attempt; accounting effects are idempotent and
claim-fenced. Do not infer active-chain acceptance from a socket write or a
missing RPC reply.

## Blocks, balances, and reorgs

Core durable tables remain:

| Table | Purpose |
| --- | --- |
| `qbit_share_ledger` | Canonical accepted share history |
| `qbit_pool_blocks` | Pool blocks and active-chain/maturity state |
| `qbit_pool_payout_entries` | Per-recipient payout records |
| `qbit_payout_carry_forward` | Auditable balance deltas |
| `qbit_pool_audit_bundles` | Canonical audit metadata and stored body |
| `qbit_ctv_fanout_sets` | Committed fanout sets |
| `qbit_ctv_fanout_artifacts` | Transactions, maturity, and broadcast state |

Current balances replay active confirmed carry-forward deltas. Zero net balances
need no current row; negative balances remain visible debt offsetting future
rewards. `qbit_carry_forward_integrity_report()` checks stored prior, candidate,
and carry values against replay and publishes `audit_head_sha256`. Preserve that
head with independent release/recovery records.

Coinbase maturity is 1,000 blocks: a height-H payout becomes mature only at tip
height H+1,000 or later. An immature disconnected block is marked inactive, so
its balances stop contributing; it can reactivate. Terminal reversal preserves
audit history and marks payout/carry rows reversed. A mature disconnect sets a
shared fatal state and stops ordinary accounting until investigated. Other
instances must not continue with a different interpretation of that event.

## Audit storage and retention

Native accepted-block audits store the non-share bundle fields and a
`share_snapshot_sha256` reference to `qbit_prism_audit_snapshots`. A snapshot
records the canonical share interval, anchor, count, and digest. Its shares are
reconstructed from the immutable ledger. Bootstrap snapshots may retain their
synthetic share inline. The reader verifies the reconstructed share digest and
canonical bundle SHA before returning a logical v1/v1.1 bundle.

Share UPDATE, DELETE, and TRUNCATE are prohibited. Removing a share could break
both future accounting and already published audit hashes. No supported pruning
or share-compaction command exists. Keep the canonical share history and all
referenced snapshot rows. A future archive design must preserve exact range
reconstruction and verification before relaxing this invariant.

Imported historical external audits retain their verified inline bundle body;
they are not silently rewritten into references to potentially incomplete
legacy share history. Import preserves their published canonical SHA. Legacy
body-ref and v2 segment formats remain supported by the Rust offline loaders.
Back up external bodies and their segments until import and restore validation
are complete, and retain the original backup under the migration retention plan.

## CTV recovery and broadcasting

Committed fanout artifacts are durable database records. Broadcasting requires
a mature active parent and a current claim. Workers record transaction/package
outcomes and retry state so another instance can resume after a crash. Failed,
reorged, or completed artifacts remain visible through the audit/public API.

To reconstruct missing artifact sets from verified database audits:

```sh
qbit-prism-server backfill-ctv
```

Run `import-audits` first for historical file-backed bodies. Backfill verifies
the trusted ledger key, canonical digest, and recorded coinbase before repairing
rows; matching existing artifacts are idempotent. It processes stored audits,
replacing the former Python tool's individual-path and block-filter CLI.

To process a single batch using the normal broadcaster policy:

```sh
qbit-prism-server broadcast-ctv
```

The integrated periodic worker uses `PRISM_CTV_BROADCASTER_ENABLED=1`. An optional
CPFP wallet and fee configuration must be consistent with the intended operating
policy. Durable claims coordinate work across instances; node RPCs may still
receive an identical transaction more than once after a lost reply.

Confirmed fanouts are observed every five seconds until 1,000 confirmations;
afterward the latest deep checkpoint is checked every 60 seconds. A shallow
fanout disconnect returns the transaction to broadcast work. Disconnection of a
deep checkpoint halts the shared cluster for explicit reconciliation.

Without transaction indexing, the broadcaster uses a durable block-scan cursor
and chain anchor. `PRISM_CTV_SPEND_SCAN_BLOCKS` bounds each pass (default 32,
range 1–256); a reorg resets the cursor. The node must retain the historical
blocks needed by that scan. A pruned/unavailable block range cannot be treated
as proof that a fanout is unspent.

For positive CPFP sponsorship, use a dedicated wallet. Broadcasters that create
or recover unsigned packages must reach the same sponsorship wallet RPC service;
unrelated wallets with the same name cannot sign each other's reserved inputs.
Other nodes can replay an already signed package without opening that wallet.
The database reserves
funding before wallet locking and preserves the exact signed child before
submission. A replacement process recovers the reservation and replays that
same package rather than selecting fresh funding after a lost reply. Funding
is not unlocked until the node observes it spent, including a mempool spend.
Automatic replacement fee bumps and abandoned-reservation release are not
implemented; retain and reconcile the durable reservation when handling those
cases manually.

## Retry, replay and deadline contract

The ledger never replays SQL automatically. After a `statement_timeout`
(`PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, default 15000), a `lock_timeout`
(`PRISM_DATABASE_LOCK_TIMEOUT_MS`, default 5000), a closed connection or a lost
acknowledgement, the error reaches the caller and the ledger neither re-sends
the statement nor re-runs the transaction. This is distinct from normal
traversal: a paged read sends its page query repeatedly by design, and audit
materialization reads the snapshot row and its share range through separate
pool checkouts. A statement cancelled or a socket closed before `COMMIT`
aborts the open transaction, so the claim and state that authorized the call
remain as they were; that holds only when the transaction did not commit. A
`CommandComplete` received before `COMMIT` proves execution, not durability. A
`COMMIT` whose acknowledgement is lost leaves the outcome unknown to the
caller: the transaction may or may not have committed. The caller's next
observation must come from the durable claim and state, never from a replay.
Every later attempt is a separate public operation with its own claim.

| Operation (public API) | Class | Automatic re-execution | What runs later, and who authorizes it | Coverage |
| --- | --- | --- | --- | --- |
| Candidate terminal disposition: `finish_candidate`, `finish_candidate_at_revision` | never-retried mutation | none | The same live claim may invoke again after a reported error; otherwise the worker calls `retry_candidate`. A consumed claim is rejected and writes nothing. | dynamic: `candidate_terminal_timeout_executes_once` |
| Candidate backoff: `retry_candidate` | explicit later operation | none | Releases the claim, records the error and advances `next_attempt_at` by `min(60, attempt_count)` seconds. It does not re-run the failed terminal write. A fresh `claim_candidate` after that time is the next attempt, with a new token; the old token stays rejected. | dynamic: `candidate_backoff_requires_new_claim` |
| CTV attempt journal: `finish_fanout` | never-retried mutation; one journal row per authorized claim | none | Every recorded attempt, `failed` included, releases the claim and schedules `next_broadcast_attempt_at` (10 s per attempt, at most 3600 s). A fresh `claim_fanout` after that time is the next attempt and its own journal row; the old token stays rejected. | dynamic: `fanout_journal_timeout_executes_once`, `broadcast_retry_requires_new_claim` |
| Claims: `claim_candidate`, `claim_fanout`, `renew_candidate_claim`, `renew_fanout_claim` | public reinvocation | none | One token per block hash or fanout. Distinct hashes have independent leases. Candidate terminal dispositions acquire the shared `SETTLEMENT_LOCK`, then `ORDER_LOCK`. The cited test verifies that a disposition does not touch a sibling candidate's locked outbox row; it does not establish concurrent execution of dispositions. | dynamic: `distinct_hashes_hold_independent_claims` |
| Landing: `land_candidate`, `land_candidate_at_revision` | dependency reread, idempotent for an identical audit | none | Re-invocation re-reads the stored audit digest and header bits and accepts only an identical audit. A superseded payout revision is a reported error with no block, audit, payout, carry or fanout row written; recovery at a proven newer revision is an explicit call. | dynamic: `superseded_landing_reports_failure`; existing `ledger_postgres::active_candidate_can_land_at_proven_new_chain_revision` |
| Expired or lost claim across a halt | never revived | none | Clearing `fatal_error` restores authority for new work only; the token that expired during the halt is still rejected by every operation and a new claim is required. | dynamic: `restored_authority_requires_fresh_claim` |
| Share append: `append`, `append_at_revision` | public reinvocation, idempotent by share identity | none | A miner or frontend resubmission is a new call; the share identity and proof-hash registry return the existing row without a second credit. | inventory: `ledger_postgres::global_duplicates_idempotence_and_config_fencing`, `postgres_failover` |
| Page traversal: `read_window`, `snapshot`, audit materialization | normal page traversal | none | A page is not a retry. `read_window` pages inside one `REPEATABLE READ READ ONLY` transaction and reports an incomplete range as an error instead of a partial window. `snapshot` fixes its anchor and revision in one short transaction, then pages immutable rows at or before that anchor in a second one. Audit materialization reads the snapshot row and the share range through separate checkouts; the share count, snapshot digest and bundle digest authenticate the result, and a mismatch is an error. | inventory: `window_reference`, `window_read_oracle` |
| Dependency reread or repair: `save_issued_job` with a repair payload, `backfill_ctv`, `import_legacy_audits` | dependency reread/repair | none | Cold-path calls that re-read the durable dependency and verify its identity, or rebuild a missing row idempotently. None replays a failed write. | inventory: `issued_job_dependency`, `ledger_postgres::ctv_artifacts_wait_for_maturity_and_claims_are_fenced`, `ledger_postgres::legacy_audit_import_validates_envelope_hash_and_pinned_key` |
| Reconciliation: `pool_blocks_for_reconcile`, `reconcile_blocks`, `reconcile_blocks_at_revision`, `observe_fanout` | public reinvocation, revision fenced | none | Periodic calls. `pool_blocks_for_reconcile` is one query. A stale expected revision (for `observe_fanout`, when the observation carries one) is an error that changes nothing. | inventory: `ledger_postgres::verified_landing_reconstructs_audit_and_reorgs_are_revision_fenced` |
| Session reservation release: `SessionId::release`, drop cleanup | best-effort cleanup | none | A lost cleanup reply leaves the outcome unknown: the reservation may have been deleted or retained. A retained reservation is reclaimed once its owner is recorded as stopped. | inventory: `ledger_postgres::session_sequence` |

"Dynamic" rows are exercised by
`crates/qbit-prism-server/tests/ledger_single_execution.rs` against a disposable
PostgreSQL. The two timeout tests route one ledger pool through a test-only
protocol proxy that frames both directions of the wire and counts the
targeted statement in `Execute` and `Query` frames across every connection,
including a same-text replay the aborted transaction rejects and a second
copy of the statement inside one simple `Query` frame. Server rejections are
recorded separately from executions and every one in the observed window
must be the targeted statement's own failure, so a replay under a different
statement text, which the server refuses at `Parse` without any `Execute`,
also fails the test. An execution whose statement text the proxy did not
learn fails the test instead of escaping the count. The targeted statement
is identified while it runs by a marker NOTICE from a statement-level
fixture trigger on the durable table, not by its SQL text or its position.
They seed a real server `statement_timeout` on the terminal write and a lost
acknowledgement before and after `COMMIT`, and read the durable outcome back
directly; a re-invocation after a lost acknowledgement is checked to run on
a connection other than the one the fault closed. The durability of the
lost-`COMMIT` case is proven by the proxy observing the server's `COMMIT`
completion and by reading the state back, not by the caller's view.
"Inventory" rows are a static review of the code, backed by the existing
tests named; they were not re-tested for this contract.

```sh
PRISM_TEST_DATABASE_URL=postgresql://test_user@127.0.0.1:5432/test_db \
  cargo test --locked -p qbit-prism-server --test ledger_single_execution -- --nocapture
```

### Deadlines

The legacy writer's final-partial-batch and progress-between-batches deadline
workflow has no native counterpart. There is no batch writer: each accepted
share commits in its own transaction, so no deadline can expire inside a
batch and no batch is retried as a unit. Two native mechanisms stand in for
it:

- Issued-job late expiry. `save_issued_job` checks the job's absolute
  deadline after taking the shared settlement lock and before locking its
  dependency row, and again just before commit, so a deadline that elapsed
  while waiting on row locks is a reported error that retrying cannot reset,
  and the job is not saved. `prune_expired_jobs` removes expired
  rows in bounded batches.
- Per-item import failure and restart. `import-audits` processes one legacy
  audit at a time, each verified off the runtime threads and stored in its
  own transaction. A failing item stops the command with an error; earlier
  items stay imported, there is no partially imported item, and rerunning the
  command resumes with the rows that still lack canonical bytes.

### Native snapshot rejection and legacy segments

A native audit body references an immutable share snapshot in
`qbit_prism_audit_snapshots`. Landing rejects a snapshot that is empty, out of
canonical order, or different from the ledger's rows for its range, before any
obligation is recorded. Materialization verifies the share count, the
reconstructed share digest and the canonical bundle digest, and returns an
error instead of a repaired or substituted body. Imported canonical bytes are
checked on both read paths: raw canonical-byte serving requires the declared
digest and a JSON object, and materialization into a logical body
additionally rejects an audit envelope and parses the bytes as a flat bundle.

The legacy `audit-body-ref` and v2 segment envelopes are still parsed by the
shared audit parser and the offline loaders, for import and verification. The
legacy segment lifecycle (gap backfill from the ledger, quarantine of a
segment when the ledger has no rows, and the conflicting-duplicate raise)
belonged to the retired filesystem audit store and is not a native lifecycle:
natively, a body is either reconstructed exactly from immutable rows or served
from imported canonical bytes, and a mismatch is a read error to investigate,
never a repair or quarantine step.

## HA database and shutdown

Point every instance at the same writable primary endpoint. Do not route ledger
queries to a lagging read replica or distribute writes among independent
PostgreSQL primaries. Sum `PRISM_DATABASE_MAX_CONNECTIONS` across frontends and
reserve capacity for migrations, monitoring, backup, and failover administration.

The cluster records its highest observed cumulative chain work. A node that is
still synchronizing, follows a lower-work tip, or disagrees at equal work cannot
advance accounting. Share commits are fenced by the current revision as well.
This prevents a lagging node from reversing another frontend's accepted blocks.
For manual regtest invalidation/reconsideration, extend the intended branch past
the previous work record before expecting the pool to resume; do not clear the
record to accommodate a lagging production node.

Keep PostgreSQL `fsync=on`, `full_page_writes=on`, and
`synchronous_commit=on`. A local durable commit protects against a coordinator
crash. Zero acknowledged-share loss on database-primary failure additionally
requires synchronous standby flush and a promotion policy restricted to a
standby containing acknowledged commits. An HA endpoint does not establish this
by itself. Test primary failure and client reconnection using the actual
replication, proxy, and storage configuration.

SIGTERM closes listener admission and asks tasks to drain before the database
pool closes. The native server bounds shutdown drain to 30 seconds; unfinished
candidate/CTV intents remain in PostgreSQL and become reclaimable after their
claims expire. There is no legacy writer-lease release barrier. Observe each
frontend's health before restoring traffic after a restart.

Backups require the database, signing-key recovery material, and any unimported
external audit bodies/segments. Use independent base backups plus WAL archives
for point-in-time recovery; replication is not a replacement for backups.
Restore into isolation and verify share order, audit hashes, carry-forward
integrity, CTV state, and API reads before declaring recovery complete.

## Fatal-state recovery

A disconnected mature pool block or deep confirmed CTV fanout records a shared
fatal state in `qbit_prism_cluster.fatal_error`. The message names the
`block_hash` or `fanout_txid`, says `manual reconciliation required`, and names
`qbit-prism-server fatal-state clear --reason <text>` as the recovery command. Every
ledger write transaction then fails with `cluster halted: ...`, and commands
that open the ledger for writing fail at startup. Only the audited command below
ends the halt; there is no public API route and no force flag.

Clearing restores authority for new work only. It does not approve or forgive
accounting, and claims that expired during the halt still require fresh claims.

### Recovery commands

```sh
qbit-prism-server migrate
qbit-prism-server fatal-state show
qbit-prism-server fatal-state clear --reason "<nonblank explanation>"
```

Apply migration 010 with `migrate` before recovery. It adds the
`qbit_prism_fatal_state_events` audit table, works while the cluster is halted,
and does not register a frontend. Confirm on the writer:

```sql
SELECT version FROM qbit_prism_schema_migrations WHERE version = 10;
```

`fatal-state show` reads PostgreSQL only. It neither starts nor registers an
instance and needs no signing configuration. It prints JSON with `fatal_error`,
`set_at`, `block_hash`, `fanout_txid`, and `halted`, and exits nonzero while
halted and zero otherwise. `set_at` is null for a state recorded before
migration 010, whose set time is unknown. A database read failure is also a
nonzero exit, so keep the JSON with the exit status.

`fatal-state clear` uses the normal server configuration (database, qbit RPC,
chain/genesis, payout, and signing settings) to verify cluster identity. Under
the settlement, ordering, and instance locks it refuses unless:

- every stored `qbit_prism_instances` row has `status.state` `stopped` or
  `drained`;
- no live legacy writer lease exists;
- the current chain is stable and still contains every mature pool block and
  every deep confirmed fanout checkpoint; and
- normal block reconciliation leaves no unresolved disconnection and the
  carry-forward integrity report passes.

The clear and its audit `INSERT` commit in one transaction. Failures before
commit roll both back and leave the cluster halted; a lost response during
commit requires checking the durable state before retrying. The event records `fatal_error`,
`fatal_error_set_at`, `reason`, `operator_identity` (PostgreSQL `session_user`),
`database_role` (`current_user`), `cleared_at`, the `instances` snapshot, and
`reconciliation` (`genesis_hash`, `tip_hash`, `tip_height`, `blocks_checked`,
`deep_fanouts_checked`, and `integrity`).

The event identifies a database login, not a person. Prefer an individual
PostgreSQL login for `clear`. When a shared login is unavoidable, put the
incident ID, operator identity, and evidence reference in the reason.

### 1. Preserve evidence first

Collect evidence before stopping, restarting, or reconfiguring anything, and
store it with the incident record:

1. The `fatal-state show` JSON and exit status.
2. The disconnected block or fanout record, pool blocks at the affected heights,
   and their stored audit bundle digests.
3. The current qbit chain from every node the frontends use: tip hash, height,
   chainwork, and the confirmations of the named block or fanout.
4. The complete carry-forward integrity report and any reconciliation output.

Query the writer endpoint, not a replica, in a read-only session (for example
`PGOPTIONS='-c default_transaction_read_only=on' psql`). For a fanout, use its
parent block's height as `<affected_height>`.

```sql
SELECT fatal_error, updated_at, payout_revision, best_tip_hash,
       best_tip_height, best_chainwork
FROM qbit_prism_cluster WHERE singleton;

SELECT block_hash, block_height, parent_hash, coinbase_txid, chain_state,
       maturity_state, matured_at, inactive_since, disconnected_at
FROM qbit_pool_blocks WHERE block_hash = '<block_hash>';

SELECT fanout_txid, block_hash, chunk_index, settlement_status,
       confirmed_block_hash, confirmed_block_height, confirmed_depth, updated_at
FROM qbit_ctv_fanout_artifacts WHERE fanout_txid = '<fanout_txid>';

SELECT b.block_hash, b.block_height, b.chain_state, b.maturity_state,
       a.audit_bundle_sha256
FROM qbit_pool_blocks b
LEFT JOIN qbit_pool_audit_bundles a USING (block_hash)
WHERE b.block_height >= <affected_height>
ORDER BY b.block_height, b.block_hash
LIMIT 200;

SELECT fanout_txid, confirmed_block_hash, confirmed_block_height,
       confirmed_depth
FROM qbit_ctv_fanout_artifacts
WHERE settlement_status = 'confirmed' AND confirmed_depth >= 1000
ORDER BY confirmed_block_height DESC, fanout_txid DESC
LIMIT 20;

SELECT qbit_carry_forward_integrity_report();
```

```sh
qbit-cli getblockchaininfo
qbit-cli getblockheader <block_hash>
qbit-cli getrawtransaction <fanout_txid> true <confirmed_block_hash>
```

A header `confirmations` of -1 means the block is not in that node's active
chain.

### 2. Stop every frontend

Stop every `run` frontend gracefully with SIGTERM (bundled stacks:
`docker compose stop --timeout 45 prism-coordinator prism-coordinator-2`). Shutdown closes
admission and drains tasks and sessions for up to 30 seconds. Only then does the
server record `stopped`, and only if no session guard remains. If that marker
fails, the process exits with an error and its row keeps its previous status.
Keep deployment supervisors and automatic restart (Compose restart policies,
systemd units, orchestrators, HA managers) disabled until the final
verification.

Inspect every instance row:

```sql
SELECT instance_id, started_at, heartbeat_at,
       round(extract(epoch FROM clock_timestamp() - heartbeat_at)::numeric, 1)
         AS age_seconds,
       status->>'state' AS state, status->>'schema' AS schema,
       status->'ready' AS ready
FROM qbit_prism_instances
ORDER BY instance_id
LIMIT 200;
```

Every row must show `stopped` or `drained`. A running frontend's row holds its
health payload (`qbit.prism.audit-health.v1`) and no `state`. A `starting` row
never became ready. Stale does not mean stopped: a heartbeat older than the
`self-check` window (`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds) shows
only that reporting stopped. The process may be hung, paused, cut off from
PostgreSQL, or on an unreachable host, and
may resume. `clear` therefore rejects missing, `starting`, unready, unknown,
and old live states, however old the heartbeat.

Resolve each blocker by finding the process for that `instance_id` and stopping
it gracefully so it records its own marker. Do not insert, update, or delete
`qbit_prism_instances` rows, move `heartbeat_at`, or set or clear `fatal_error`
with SQL. The crashed-owner step in
[retained reservations](prism-session-sequence.md#retained-reservations-and-recovery)
is a different procedure and does not authorize a marker for this recovery. If
an instance cannot record `stopped` itself, stop and escalate for a separately
reviewed decision.

### 3. Reconcile before clearing

Establish whether the halt reflects the canonical chain. Compare the evidence
from independent nodes with `best_tip_hash` and `best_chainwork`. A node on a
lower-work or minority branch is repaired at the node, never by clearing.

If the block or fanout really is disconnected, resolve the chain and accounting
discrepancy (affected payouts, carry-forward balances, and fanout settlement)
through a separately reviewed reconciliation before running `clear`. This
runbook provides no SQL for that change. `clear` is not approval to forgive a
still-disconnected mature payout: it refuses while any mature pool block or deep
checkpoint is missing from the chain, and success means only that its checks
passed. Record the review reference, then repeat the evidence queries.

### 4. Clear the fatal state

Run `clear` from a host with the frontends' normal configuration, using the
operator's own database login:

```sh
set -o pipefail
qbit-prism-server fatal-state clear \
  --reason "<incident>: <operator>; <block or fanout> reconciled per <review>; evidence <ref>" \
  | tee fatal-state-clear.json
```

Save the success JSON with the incident record, along with the audit event:

```sql
SELECT cleared_at, operator_identity, database_role, reason, fatal_error,
       fatal_error_set_at, instances, reconciliation
FROM qbit_prism_fatal_state_events
ORDER BY cleared_at DESC
LIMIT 5;
```

A validation or reconciliation failure clears nothing and writes no event. Correct the reported blocker
and rerun deliberately; do not loop. If the connection drops around commit, the
outcome is unknown: check `fatal-state show` and the event table before
rerunning.

### 5. Restart and verify

1. Confirm `qbit-prism-server fatal-state show` exits zero, and save its JSON.
2. Record the ledger head before admitting traffic:

   ```sql
   SELECT share_seq, accepted_at FROM qbit_share_ledger
   WHERE accepted ORDER BY share_seq DESC LIMIT 1;
   ```

3. Re-enable supervisors and start the frontends. Each `/healthz` must return
   200 (see [health checks](#health-diagnostics-and-validation)), and each
   instance row must carry a fresh, ready health payload.
4. Confirm new accepted shares. The step 2 query must return a higher
   `share_seq` with a later `accepted_at`, and each frontend's
   `qbit_prism_accepted_shares_total` and
   `qbit_prism_share_ack_seconds_count{result="accepted"}` must increase.
5. Keep the evidence, reconciliation reference, clear JSON, audit event, and
   these checks together.

If a frontend reports `cluster halted` again, a new fatal state was recorded.
Start again from evidence collection.

## Health, diagnostics, and validation

`/healthz` returns 200 only when the process has fresh work for its observed tip
and current payout revision and job delivery can progress; otherwise it returns
503. The HTTP handler reads a published snapshot and fails closed when that
snapshot becomes stale. Database or node outages therefore cannot keep an old
green response indefinitely. A tracked task polling beyond two seconds, or an
health/metrics publication exceeding the existing freshness budget, also
returns 503 with
`ok=false`, `ready=false`, and `status="runtime-stalled"`; the corresponding
`qbit_prism_runtime_task_stalled{task="..."}` is 1. This live check requires a
surviving runtime worker to serve the probe; a completed long poll retains
metric evidence without keeping readiness failed. The publication progress
guard ends before heartbeat/prune maintenance; their asynchronous waits do not
mark a just-published snapshot runtime-stalled.

The coordinator health payload also supplies three known 2.x compatibility
aliases: `ledger_backend` is `postgres-native` for the PostgreSQL backend,
`accepted_block` is whether `found_block_count` is positive, and
`accepted_block_count` copies that count. `ready_miner_count` and `max_blocks`
remain unmapped because their native sources have not been agreed.

`/metrics` exports native process health, accepted/rejected share and block
counters, runtime workers, connections, pending builds, current-work delivery
coverage, and delivery outcomes. It also includes share ACK latency and reject
reasons, initial-work waits, candidate backlog, collector pool waits and status,
runtime lag, and RSS; the full [native family inventory](prism-native-metrics.md)
identifies the timing families declared without samples. Scrape every instance
with its own label; process counters reset after restart. Dashboard accounting
is read from PostgreSQL across instances. Detailed Python queue, writer lease, watchdog,
and incremental-refresh metrics no longer describe this runtime.

The coordinator (`run`) serves the last complete metrics publication and adds
three gauges at scrape time:

| Gauge | Meaning |
| --- | --- |
| `qbit_prism_metrics_snapshot_available` | `0` before the first publication; `1` afterward, including when stale |
| `qbit_prism_metrics_snapshot_stale` | `1` when missing or older than the freshness budget; otherwise `0` |
| `qbit_prism_metrics_snapshot_age_seconds` | Monotonic age in seconds; `-1` before the first publication |

The coordinator uses the same budget as `/healthz`:
`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds. The setting is read as an
unsigned whole number of seconds from 1 through 86400, defaulting to 2 when
absent; invalid values fail startup. Compose supplies 5 by default. Both values
give a 15-second freshness budget. The publisher ticks at this configured
interval, so publication cadence and the staleness budget stay aligned.
Once the age exceeds that budget, a scrape sets `qbit_prism_health_state` to `0`
while retaining the other cached samples. Collector age/availability and runtime
state are overlaid from memory at scrape time; this does not refresh the cached
body timestamp or a collector's last-success timestamp.
Scraping neither renews the publication age nor queries the database.

Both `run` and `public-api` return HTTP 200 for `/metrics`, including missing
and stale observations. Inspect the freshness signals and `/healthz` rather
than treating a successful scrape as readiness. GET and HEAD responses carry:

- `Cache-Control: no-store`.
- `X-Prism-Metrics-State: fresh`, `stale`, or `unavailable`.
- `Age`: elapsed whole seconds, rounded down, including `0`; omitted when unknown.
- `Warning: 110 qbit-prism "metrics snapshot is stale; serving last complete payload"`
  only when the state is `stale`.

The public role derives these headers from its existing readiness probe age,
also exported as `qbit_prism_public_ledger_probe_age_seconds`. Its budget is
`max(3 * PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS, 15)` seconds; the probe
interval defaults to 5 seconds. Before the first completed probe the state is
`unavailable`. A recent failed probe is still `fresh`, with
`qbit_prism_public_ledger_ready 0`; freshness does not mean the database is ready.
The public role keeps its existing metrics body, without the coordinator's
three snapshot gauges.

Useful checks:

```sh
qbit-prism-server check-config
qbit-prism-server healthcheck --url http://127.0.0.1:3341/healthz
curl --silent --show-error --include --max-time 5 http://127.0.0.1:3341/metrics
qbit-prism-server self-check
bash test/prism-native-tests.sh
QBITD_BIN=/path/to/qbitd bash test/prism-native-tests.sh live
```

The database test wrapper starts a private local cluster unless
`PRISM_TEST_DATABASE_URL` is supplied. Its default mode runs the whole workspace
and the three explicit `--ignored` database targets, as CI does, and checks the
integration gate's manifest so no gated test passes without running (see
[the integration test gate](prism-integration-test-gate.md)). Live tests add
actual qbitd regtest and bounded CPU mining. Use an isolated database for tests. The native builder
benchmark measures CPU build/verify work, not end-to-end accepted-share capacity;
see [measurement](prism-payout-artifact-measurement.md) and
[optional qualification](prism-capacity-readiness.md).

The physical failover test uses disposable PostgreSQL primary/synchronous
standby processes and two ledger clients through a stable TCP endpoint. It
checks survival of acknowledged IDs, deduplication, and resumed writes after
immediate primary loss and promotion:

```sh
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin \
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
```

The test skips unless that server-tool directory is provided. Its test proxy
and promotion sequence do not replace validation of a production HA manager.

The collector failure/recovery test requires a disposable PostgreSQL database
and is ignored by ordinary `--all-targets` runs. Invoke it explicitly when
running without the wrapper:

```sh
PRISM_TEST_DATABASE_URL=postgresql://test_user@127.0.0.1:5432/test_db \
  cargo test --locked -p qbit-prism-server --test observability_database -- --ignored
```

It verifies valid zero values, a blocked query, pool exhaustion, and recovery;
never point test fixtures at a production database.
