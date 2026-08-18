# PRISM Ledger Operations Contract

This contract is the public operating model for the qbit PRISM share ledger.
PRISM means Payouts, Rewards, and Integrity Settlement Manifest.
It is intentionally narrower than a full production deployment guide: it
defines the invariants the implementation and regtest evidence rely on, and it
names the hardening layers that can be added without changing reward semantics.

## Canonical Write Path

Accepted shares must enter exactly one ordered log: `qbit_share_ledger`.
Stratum frontends may scale horizontally, but they do not insert independent
share sequences. The supported topology is:

1. Stratum frontend validates miner identity, share shape, job id, and target.
2. Frontend submits the accepted share to the bounded group-commit writer and
   waits for Postgres to commit it.
3. One logical ledger writer owns the Postgres writer lease and inserts shares
   into `qbit_share_ledger`.
4. PRISM's TIDES-style reward windows, audit exports, payout policy, and reorg
   reversal read from that same canonical log.

The coordinator runs the frontend and writer in one process. Every accepted
share receives one monotonic `share_seq`, and all reward windows are derived
from that ordering. Stratum success is sent only after the transaction commits;
worker counters and vardiff accounting advance at the same boundary. A full
queue, commit error, or commit timeout returns no success. Miners may retry the
same submission safely.

The writer batches up to `PRISM_SHARE_COMMIT_BATCH_SIZE` shares for at most
`PRISM_SHARE_COMMIT_LINGER_MILLISECONDS` before committing. The defaults are 64
shares and 5 ms. `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` bounds queue admission.
Once admitted, a client waits for a definite commit outcome; the watchdog
restarts a wedged writer instead of returning an ambiguous timeout. Tune the linger against measured ACK
latency, but do not weaken Postgres durability settings to reduce it. A batch
flushes immediately when it contains a block candidate so the normal linger
does not consume that candidate's tip-race budget.

## Writer Lease and Replay

The writer lease is stored in `qbit_ledger_writer_lease`. A writer is identified
by `(writer_id, writer_epoch, writer_session_token)`. A process may refresh only
the exact session token it acquired; another process with the same writer id and
epoch is still fenced until the existing lease expires. During startup, a
replacement process with the same writer id and epoch waits and retries until
that predecessor lease expires, then acquires a fresh session token. A different
writer id or epoch is treated as a conflicting active writer and fails fast.

`share_id` is globally unique. Replaying an exact share payload is idempotent
and returns its original sequence without inserting another row. Reusing the ID
with any different payout, difficulty, job, timestamp, nonce, or credit policy
fails the complete batch. After failover, the replacement writer resumes at the
next database sequence value and stale writers are rejected before insert.

The ledger is single-writer, not active-active. A replacement writer takes the
lease after expiry. Active-active insertion would create ambiguous ordering and
is outside the accepted PRISM contract.

Graceful coordinator shutdown closes Stratum admission and all background
writer admission first, then waits for admitted share batches, accepted-block
finalization, CTV status updates, and payout/reorg mutations to finish. It
releases the exact-session writer lease immediately after that writer barrier;
client socket delivery, obsolete job fanout, and executor/thread cleanup drain
after release and therefore cannot delay a replacement writer. SIGTERM only
closes admission and wakes the serve loop; the barrier and database work run
outside the signal handler.

`PRISM_WRITER_QUIESCENCE_TIMEOUT_SECONDS` bounds the writer barrier and defaults
to 15 seconds. This leaves time for conservative synchronous Postgres flushing
while remaining well below the default 60-second lease TTL. If the timeout
expires, the coordinator logs each still-active writer component and
deliberately does not release the lease; process termination and TTL fencing
then preserve the single-writer invariant. Do not shorten this below the
durable flush time of the deployed Postgres system.

Shutdown emits structured JSON log events named `shutdown_start`,
`writer_quiescence`, `lease_release_attempt`, `lease_release`,
`lease_release_withheld`, and `non_writer_drain`. Prometheus exposes the same
path through `qbit_prism_shutdowns_total`,
`qbit_prism_shutdown_writer_quiescence_seconds`,
`qbit_prism_shutdown_writer_quiescence_total`,
`qbit_prism_shutdown_lease_release_attempts_total`,
`qbit_prism_shutdown_lease_release_total`,
`qbit_prism_shutdown_lease_release_seconds`,
`qbit_prism_shutdown_sigterm_to_lease_release_seconds`,
`qbit_prism_shutdown_non_writer_drain_seconds`, and
`qbit_prism_shutdown_release_withheld_total`.

### Orphaned Locks and Bounded Startup Acquisition

A coordinator that vanishes without closing its sockets — network partition,
`SIGSTOP`, VM pause — can leave a Postgres backend idle in transaction, still
holding the `qbit_ledger_writer_lease` row lock its landing CTE took. Without
countermeasures the successor's startup lease upsert queues behind that lock,
inside ledger construction and before the watchdog arms, until kernel TCP
keepalive teardown (hours at OS defaults): a full-pool availability outage.
Settlement correctness is unaffected — the outbox row stays pending and
replays — only availability is at stake.

Two independent layers bound this. First, every Postgres session the
coordinator opens (the pooled native client, the dedicated lease-guard
session, and the psql subprocess backend) carries session guards:
`idle_in_transaction_session_timeout`
(`PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS`, default 15) makes the
server abort an orphaned transaction and release its locks, and the
server-side keepalive GUCs (`PRISM_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS`,
`PRISM_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS`,
`PRISM_POSTGRES_TCP_KEEPALIVES_COUNT`, defaults 30/10/3) bound the server's
teardown of a socket toward a vanished client at 30 + 3x10 = 60 seconds, the
backstop where the idle-in-transaction timer does not apply.

Second, the startup lease upsert and adoption CAS each run under a bounded
lock deadline (`PRISM_LEDGER_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS`, default 5)
with a bounded retry (`PRISM_LEDGER_LEASE_ACQUIRE_ATTEMPTS`, default 5). Each
timed-out attempt is logged with its attempt number and the underlying error.
The retry budget (5x5s = 25s) is sized to outlast the idle-in-transaction
timeout — which runs on the blocking backend's own clock, from when its
transaction went idle rather than from when the successor started retrying —
so an orphaned lock is normally reaped mid-budget and startup self-heals
without operator action. An acquisition that never completes within the budget
fails construction with a `RuntimeError`, exiting the process visibly for the
supervisor to restart. That error names the lock conflict as the likeliest
cause but does not assert it: a connect timeout, an exhausted connection-pool
slot, and a server that is merely overloaded all expire the same deadline, so
read the chained cause it quotes before assuming a stuck transaction. Waiting
for a *live* holder's lease TTL to expire is unchanged — that outer wait is
intended failover behaviour; only the per-statement lock wait is bounded.

The session guards carry three deployment caveats. The guards travel as
libpq startup options, so native connections now always set the `options`
connect parameter: a deployment routing `PRISM_DATABASE_URL` through a
connection pooler that rejects startup options (older PgBouncer builds) will
fail at connect rather than silently drop them, visibly at coordinator
startup. The default compose topology connects directly to `prism-postgres`
and is unaffected. The psql-subprocess backend delivers the same guards
through `PGOPTIONS`, so a wrapper script standing in for `psql`
(`PRISM_POSTGRES_PSQL_COMMAND`) that does not forward its environment drops
them without any error. On a Unix-socket DSN the `tcp_keepalives_*` GUCs are
ignored — accepted and inert — leaving
`PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS` as the guard that still
applies; it is the one that covers the orphaned lease-row lock in any case.

## Block Candidate Outbox

A block-worthy share transaction also inserts an immutable intent into
`qbit_block_candidate_outbox`. The intent contains the complete block, template
context, reward inputs, and extranonce fields required to finish audit and
submission. The share and intent become visible atomically before Stratum
success.

The bounded live-candidate queue is only a wakeup path. Queue saturation
coalesces wakeups; it cannot delete an outbox row. Recovery restores pending
rows in batches into a separate, lower-priority replay queue, without doing
per-row database accounting. Live discoveries therefore always outrank restart
work, while an older replay stalled in accounting cannot hide later durable
rows. The pre-accept startup recovery pass is best-effort under a slow ledger:
if its database budget expires, the coordinator finishes starting and the
block-submitter loop retries every durable pending row with ordinary backoff.
Because job builds stay blocked until every pending candidate is known, the
startup enumeration must be provably untruncated: a full batch re-queries
with a doubled window (capped at 1024 rows) until the outbox returns fewer
rows than requested. If the cap is ever hit, the gate stays closed while the
restored batch drains and the submitter loop re-enumerates the remainder.
Before qbitd can observe a candidate, the coordinator installs a short in-memory
prospective-payout barrier; this prevents startup prewarm from issuing child
work from the old balance base without falsely claiming that the block landed.

Once a durable candidate is dequeued, its qbit `submitblock` RPC is the fast
lane: it runs before the attempt-marker write, accepted-block writer admission,
audit construction, or payout publication. The node result and same-hash lease
then transfer to an independent, height-prioritized accounting lane. A full
primary handoff spills to a result-preserving overflow queue; it never turns an
already-offered block back into a raw-submit retry. `block_submitter` and
`block_accounting` expose independent phase heartbeats, so slow accounting does
not delay later node offers or disguise the phase that stopped progressing.

`PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` bounds the fast-lane RPC (default 1
second). `PRISM_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS` gives each later Postgres
statement and local ledger gate a fresh deadline (default 1 second); direct
outbox reads and mutations additionally use a single-flight wrapper so a
driver that ignores its deadline cannot accumulate retry threads. Timeouts
leave the row pending and enter the ordinary candidate backoff.

Landing-path observability lives on `/metrics`:
`qbit_prism_block_ledger_calls_total` / `_call_timeouts_total` /
`_call_budget_seconds` / `_call_last_duration_seconds` /
`_call_max_duration_seconds` (labelled by `call_class`, `fast` vs
`landing`), `qbit_prism_accepted_parent_unresolved_transitions` and
`_unresolved_oldest_seconds`,
`qbit_prism_accepted_parent_preview_wait_timeouts_total`,
`qbit_prism_prior_balances_reads_total` / `_read_last_seconds` /
`_read_max_seconds`, and `qbit_prism_startup_phase_seconds{phase=...}`.
Alert before the landing deadline is exhausted, not after: page when
`qbit_prism_prior_balances_read_max_seconds` exceeds ~20% of the
landing budget or the poll budget, when any
`qbit_prism_block_ledger_call_timeouts_total{call_class="landing"}`
increment occurs, and when
`qbit_prism_accepted_parent_unresolved_oldest_seconds` exceeds the
preview wait budget. The #188 prior-balances read crossed the one-second
line silently over several weeks; these series exist so that growth is a
ticket, not an outage.

Deadlines are split by call class. The poll-class budget above covers only
cheap outbox polls and fast-lane-adjacent calls. The landing-class
accounting tail — persisting an accepted block, reading prior balances,
confirming it, and rejecting the prepared state of a terminal candidate —
runs each statement under `PRISM_BLOCK_LANDING_DB_TIMEOUT_SECONDS`
(default 30 seconds) starting with the first attempt. After an observed
landing timeout the next attempt for the same block hash doubles its
budget up to `PRISM_BLOCK_LANDING_DB_TIMEOUT_MAX_SECONDS` (default 120
seconds); only ledger-originated deadlines count as landing timeouts —
a node RPC timing out inside the landing tail neither escalates the
next PostgreSQL budget nor increments the landing-timeout series; retries stay paced by the candidate backoff, server-side
cancellation is confirmed by the ledger backends (the pooled session is
rolled back or replaced, never reused mid-cancel), and the stuck-call and
coordination watchdogs remain the overall bound. Startup replay of a
pending accepted candidate re-enters the same landing-class scope, and
the gating startup outbox enumeration both runs with the landing budget
and records under `call_class="landing"`, so an enumeration timeout
fires the landing-timeout alert rather than inflating the fast-call
budget gauge. A
landing-class operation must never start at the poll budget: a
structurally slow landing under a one-second ceiling is statement-canceled
on every attempt and can never converge (issue #188). Contended
submit-path locks are acquired in heartbeat slices and identify the lock in a
periodic diagnostic controlled by `PRISM_BLOCK_SUBMIT_LOCK_WAIT_LOG_SECONDS`
(default 5 seconds). At most two timeout-ignoring RPC workers and two
timeout-ignoring ledger workers may remain detached. If either bounded worker
pool remains exhausted for `PRISM_BLOCK_SUBMIT_STUCK_CALL_EXIT_SECONDS`
(default 30 seconds), the coordinator requests shutdown and exits nonzero so
the supervisor replaces the poisoned process; durable outbox rows remain
pending for replay. One detached call does not interrupt the healthy raw lane
while the other bounded slot can still make progress.

Successful submissions become `submitted`; candidates that definitively lose
their tip race or fail validation become `abandoned`. If the process exits
after `submitblock` but before the attempt marker or terminal outbox update,
restart resubmits the same bytes. qbit's accepted-duplicate response is a
successful landing signal; block-hash-keyed ledger persistence and the
finalize-only registry keep accounting and terminal side effects exactly once.
Restart can also recognize the candidate as the active tip and complete the
same idempotent confirmation path. Exact miner resubmissions observe an
existing terminal outbox state in the same durable pre-submit transaction:
`submitted` coalesces to success and `abandoned` stays rejected before any new
node offer. Exact share replays return the original row as not newly inserted,
so process-local worker and vardiff counters are not credited twice.
Transient RPC, audit, and ledger outcomes remain pending and retry with an
exponential delay starting at 250 milliseconds and capped at 30 seconds. They
do not increment terminal abandonment counters. An abandonment is counted only
after any prepared payout state is rejected and the false disposition is fixed;
if cleanup fails, the candidate remains pending and can still converge to
submitted on later chain evidence. Replay carries the database row's block hash
separately from candidate JSON, so malformed payloads can be quarantined using
the authoritative outbox key instead of replaying forever.
If qbit has already returned the candidate outcome but durable outbox
finalization fails, replay resumes only that finalization step with the same
bounded pacing. It does not call `submitblock` again, recount an accepted block,
rebuild or republish audit evidence, or reacquire a share-writer floor already
released after the known outcome.

The block submitter heartbeat carries its current phase, including replay
query, node RPC, lock admission, audit, persistence, and finalization. A stale
watchdog diagnostic therefore reports a label such as
`block_submitter:replay-outbox-query` instead of only the thread name.

When a network-valid hash is below a listener's advertised share target, the
coordinator first stores a candidate-only intent, submits it synchronously, and
links share credit only if the block lands. This closes the submit-to-credit
crash window without crediting a below-target hash that loses its tip race.
Terminal outbox rows retain the intent digest but clear the large block/template
body, bounding permanent outbox storage while preserving exact-replay checks.

Production deployments must use the Postgres-backed ledger. The in-memory ledger
exists only for local/regtest proof runs and requires an explicit
`PRISM_ALLOW_MEMORY_LEDGER=1` opt-in.

## Reward Query Semantics

`qbit_prism_window(anchor_job_issued_at, window_weight)` returns accepted shares
in descending `share_seq` order until the requested weight is filled. This is
the TIDES-style reward-window primitive inside PRISM. The oldest included share
is partially counted when needed. Eligibility requires both:

- `job_issued_at <= anchor_job_issued_at`
- `accepted_at <= anchor_job_issued_at`

That second condition freezes the block view and prevents an old-job share that
arrives after the found-block anchor from entering the published payout split.

`qbit_audit_share_window(anchor_job_issued_at, network_difficulty)` is the
public audit wrapper. It fixes the TIDES-style window multiplier at 8x network
difficulty and returns the counted difficulty for every included share.

Accepted rows may carry a nullable `credit_policy`. Normal shares leave it
empty; `stale-grace` marks a prior-tip share credited by the coordinator's short
stale-grace policy. Reward-window queries still count these rows because they
are accepted shares, while audits can distinguish them from normal current-tip
shares. Audit bundles containing a credited row use
`qbit.prism.audit-bundle.v1.1`; external auditors must upgrade before operators
enable stale-grace crediting.

Deployments that run with `PRISM_POSTGRES_INIT_SCHEMA=0` must apply
`crates/qbit-prism/sql/001_share_ledger.sql` before starting any upgraded
coordinator. The file is the cumulative, idempotent schema initializer and
migration path, despite its `001` name. Skipping it can break share inserts,
reward-window calls, pool-block confirmation/reactivation, and audit evidence
publication because required columns, functions, and the durable publication
ordinal will be missing.

### Audit publication ordering migration

Existing databases must receive the new `audit_publication_sequence` migration.
It creates and validates a bigint sequence, adds the nullable pool-block column,
deterministically backfills confirmed and inactive rows by `found_at` and
`block_hash`, adds a unique index and state constraint, advances the allocator
beyond every retained ordinal, and replaces confirmation/reactivation functions
so each new durable confirmation receives an ordinal. Exact confirmation replay
preserves its prior ordinal. Historical inactive rows are backfilled so later
reactivation can retain that already-published ordinal without allocating a new
audit publication.

The migration includes `ALTER TABLE` operations that require PostgreSQL's
`ACCESS EXCLUSIVE` table lock. They wait for existing readers and writers and
can interrupt new reads as well as writes while held. A later serialized phase
takes a transaction-scoped advisory lock and a `SHARE ROW EXCLUSIVE` lock on
`qbit_pool_blocks`, and the unique index is built non-concurrently. Treat the
whole migration as read-impacting: stop the old coordinator or use a reviewed
maintenance window, take the normal database backup, and apply the file with
`ON_ERROR_STOP` using the same database role and schema search path as PRISM.
For example:

```sh
psql "$PRISM_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f crates/qbit-prism/sql/001_share_ledger.sql
```

With `PRISM_POSTGRES_INIT_SCHEMA=1`, coordinator construction applies the same
script before listeners open. The migration is rerunnable and tested across
fresh, legacy, partial, concurrent, malformed, and bigint-boundary states; it
fails closed instead of accepting a conflicting sequence, column, index, or
constraint definition.

`qbit_shares_since_template_height(min_template_height)` supports operational
replay and frontend recovery. It returns accepted shares at or above the
template height in ascending `share_seq` order and excludes rejected shares.

## Payout and Reorg State

Accepted pool blocks are persisted in `qbit_pool_blocks`, with payout rows in
`qbit_pool_payout_entries`, carried balances in `qbit_payout_carry_forward`, and
audit bundles in `qbit_pool_audit_bundles`. The direct coordinator durably
persists the compact candidate intent before calling `submitblock`, then builds,
verifies, and persists the full audit and payout state after the block becomes
active. A definitive pre-acceptance rejection terminalizes the outbox row and
creates no prepared payout state.

Immature disconnects are first quarantined as `chain_state='inactive'`, which
removes their carry-forward balances from current owed totals without mutating
historical shares or payout rows. If the block returns to the active chain, the
coordinator reactivates it. The terminal reversal path is the fenced ledger
wrapper `PsqlShareLedger.reverse_immature_block(...)`, backed by
`qbit_reverse_immature_pool_block(...)`; it marks the block, payout entries, and
carried balances as reversed. Mature rows, and rows already height-mature at the
supplied active tip, must not be reversed by that path. qbit coinbase maturity
is 1000 blocks, so operators must not mark pool payouts mature before
`block_height + 1000`.

## Publication Ordinal Rollback And Schema Revert

Schema initialization adds a durable publication ordinal to accepted pool
blocks: `qbit_pool_blocks.audit_publication_sequence`, allocated from
`qbit_audit_publication_sequence_seq` at the durable prepared -> confirmed
boundary, unique across the table, and required by a validated CHECK
constraint on every confirmed row. Historical confirmed and inactive rows are
backfilled deterministically in `(found_at, block_hash)` order the first time
the migration runs.

Rolling back coordinator code does not require reverting this schema. The
`qbit_pool_blocks_assign_publication_ordinal` BEFORE trigger assigns the next
ordinal to any confirming write that omits it, so a pre-ordinal coordinator
that confirms with a plain `chain_state` UPDATE keeps satisfying the
constraint. That trigger is the primary rollback path; prefer it.

`crates/qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql`
is the second path, for the cases the trigger cannot cover: rolling back more
than one release, or aligning a live ledger with a restored pre-migration
base backup. It returns `qbit_pool_blocks` to its pre-ordinal shape by
dropping, in order, the CHECK constraint, the assignment trigger and its
function, the unique index, the column, and the sequence, then restores the
pre-ordinal `qbit_confirm_pool_block` body. The file is one transaction
serialized behind the same advisory lock as the forward migration: any
failure aborts the whole revert, and re-running it after the failure is
resolved -- or on an already-reverted or never-migrated schema -- is safe. An
object the migration did not create, such as an operator view over the
ordinal column, fails the revert loudly instead of being dropped.

Before applying the revert:

1. Stop the coordinator and anything else writing to the ledger. The revert
   takes an ACCESS EXCLUSIVE lock on `qbit_pool_blocks`, so it stalls behind
   a live writer. Worse, a post-migration coordinator left running (or
   restarted afterwards) confirms blocks by assigning the ordinal explicitly:
   every such confirmation fails with `column "audit_publication_sequence"
   does not exist` the moment the revert commits, and a post-migration
   coordinator restarted with schema initialization enabled immediately
   re-applies the forward migration, silently undoing the revert.
2. Take a base backup, or verify the continuous WAL archive covers this
   point. The revert discards every assigned publication ordinal; nothing can
   recover them afterwards except that backup.

What is permanently lost: the recorded confirmed-publication order. The
ordinal is allocated when a block durably reaches `confirmed` and is reused
across exact replay and inactive -> confirmed reactivation, so it is the only
record of the order in which blocks were actually published. After the
revert, publication currency falls back to what pre-ordinal code used to
select current evidence -- `found_at`/`block_height` read order -- which is
not that durable publication order. Re-applying `001_share_ledger.sql` later
re-adds the column and backfills deterministically by
`(found_at, block_hash)`, but that is a fresh assignment: it does not
reproduce the ordinals observed before the revert, and external consumers
that recorded pre-revert ordinals will see the sequence renumbered.

Apply the revert with the ledger role whose `search_path` selects the PRISM
schema, and stop on the first error:

```sh
psql "$PRISM_DATABASE_URL" \
  --set ON_ERROR_STOP=1 \
  -f crates/qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql
```

The full round trip -- migrate, revert, confirm pre-ordinal-style, re-apply,
re-backfill without double assignment -- is exercised by
`tests/prism_postgres_a1_revert_gate.py` inside
`test/test-prism-postgres-ledger.sh`.

## CTV Fanout Artifact Repair

Schema initialization is also the idempotent repair path for deployed PRISM
databases. In particular, it drops the old `NOT NULL` constraint from
`qbit_ctv_fanout_artifacts.anchor_vout` so fee-bearing, anchorless CTV fanouts
can persist with `anchor_vout = NULL`.

The same schema init path also adds bounded broadcast-attempt summaries to
`qbit_ctv_fanout_artifacts`. Operators can tune retained detail rows with
`PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT`; the summary columns retain total
attempt count, latest package/result/error context, and per-status counts after
old detail rows are no longer retained.

If a block was mined while the old constraint was still present, backfill the
missing fanout artifact rows from the persisted audit bundle, a local
`prism-live-audit-bundle-*.json` envelope, or a local audit body file:

```bash
PRISM_DATABASE_URL='postgres://...' \
python3 -m lab.prism.backfill_ctv_fanouts --db-block-height 21883
```

The repair tool also accepts `--db-block-hash <hash>` or local JSON paths. Local
paths may be full v1 bundles, live envelopes, legacy compact audit body refs,
or `qbit.prism.audit-bundle.v2` proof bodies; the tool follows envelope
`body_uri` pointers and reads `bundle_without_shares` from compact bodies
because CTV backfill does not need share rows. For a local candidate/final
audit bundle whose filename does not include the block hash, pass
`--path-block-hash <hash>`. The tool runs schema init by default before
backfilling and then calls the same fenced
`persist_ctv_fanout_manifest_set` path as the coordinator. Stop the active
coordinator or otherwise ensure the repair process can acquire the ledger
writer lease before running a backfill.

## Compaction and Archive Contract

The ledger is append-only for reward correctness. Compaction is allowed only as
an archive-first operation, and only after proving it cannot change any future
window, audit, maturity, or reorg answer.

Before deleting any hot rows, an operator must:

1. Export the candidate `share_seq` prefix to durable archive storage.
2. Record row count, first and last sequence, and a cryptographic hash of the
   exported rows in canonical order.
3. Prove no unresolved pool block, audit bundle, immature payout, or future 8x
   PRISM reward window can reference the candidate rows.
4. Re-run representative `qbit_prism_window`,
   `qbit_audit_share_window`, and `qbit_shares_since_template_height` queries
   before and after the dry run and verify identical results.

No public harness currently deletes ledger rows. Until an archive proof exists,
production deployments should retain the full canonical share log.

For operator disk planning and permanent-vs-ephemeral data categories, see
[`docs/prism-storage-sizing.md`](prism-storage-sizing.md).

## Throughput Evidence

`make test-prism-postgres-throughput` is an opt-in capacity harness. It creates
a temporary Postgres container, bulk-inserts synthetic accepted shares through
the canonical schema, records observed shares/sec, and stores
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` output for the audit-window query.

Useful environment variables:

- `QBIT_PRISM_THROUGHPUT_SHARES`: number of synthetic rows to insert.
- `QBIT_PRISM_MIN_SHARES_PER_SEC`: optional failing threshold.
- `QBIT_PRISM_THROUGHPUT_REPORT`: JSON report path.

The throughput harness measures schema/query capacity. It is separate from the
lab ledger adapter's execution backends described below.

## Hot-Path Execution Backends

The lab ledger adapter (`PsqlShareLedger`) supports two interchangeable
execution backends over the same SQL and the same durability contract:

- **`psycopg-pool` (production default):** a persistent pooled psycopg client.
  The coordinator's share-writer group commits (`PRISM_SHARE_COMMIT_BATCH_SIZE`
  / `PRISM_SHARE_COMMIT_LINGER_MILLISECONDS`) execute as one round trip on a
  long-lived connection instead of one `psql` fork+connect per statement; the
  batch statement's own `set_config('synchronous_commit', 'on', true)` keeps
  the Stratum ACK boundary at the database commit. Reads share a pool bounded
  by `PRISM_POSTGRES_READ_CONCURRENCY` plus one writer slot. Read-only
  statements retry once after a lost connection; mutations do not re-execute
  automatically because a lost response cannot prove that PostgreSQL did not
  commit the first execution.
- **`psql-subprocess` (fallback):** the legacy zero-Python-dependency backend
  that shells out one `psql` per statement. It remains fully supported for
  regtest portability and as the operational escape hatch.

`PRISM_POSTGRES_NATIVE_CLIENT` selects the backend: `auto` (default) uses the
pooled client when psycopg is importable and a `postgres://` DSN is available
(from `PRISM_DATABASE_URL` or inside `PRISM_POSTGRES_PSQL_COMMAND`), `1`
requires it, and `0` forces the subprocess fallback. The coordinator startup
line reports the active backend as `ledger_execution=`.

Readiness and health counters (`accepted_share_stats`) are maintained
incrementally by the single lease-holding writer and reconciled against the
database once per `PRISM_ACCEPTED_STATS_CACHE_SECONDS` (default 60) instead of
running `count(*) / count(DISTINCT miner_id)` aggregates every few seconds.

`make test-prism-postgres-native-ledger` is the opt-in end-to-end validation
for the pooled backend: it provisions a temporary Postgres container and
exercises schema init, lease guards, concurrent batched appends, duplicate
replay, cached stats, and cross-backend read consistency with the `psql`
fallback. Run it on any docker host with `psycopg` installed before rolling
the pooled backend into an environment.

It does not simulate live Stratum miner swarms, reconnect storms, malformed
client messages, or stale-share bursts across job changes. Those remain separate
operator-load concerns if production needs coverage beyond ledger/query
capacity.

## Operator Readiness

Optional capacity qualification and its versioned evidence contract are
documented in [PRISM capacity qualification](prism-capacity-readiness.md).

`make prism-self-check` is the PRISM operator readiness probe. It resolves the
same Compose environment as `make up-prism-pool`, then emits PASS/WARN/FAIL
rows for:

- qbit RPC reachability, chain identity, IBD state, and peer count.
- the configured genesis hash against `getblockhash 0` when
  `QBIT_EXPECTED_GENESIS_HASH` is set.
- PRISM coordinator `/healthz` from inside the coordinator container.
- miner-facing Stratum TCP reachability.
- Postgres readiness for the canonical ledger.
- PRISM signing/key environment and forbidden production bypass flags.
- audit/archive path writability.
- basic mining configuration such as share difficulty, vardiff bounds, pool
  fee configuration, CTV fanout fee sourcing, and minimum ready miners.

The command is safe to run repeatedly. It exits non-zero when any FAIL row is
present. Use `python3 scripts/prism-self-check.py --skip-live` to validate only
static configuration before the profile is running.

On mainnet, an explicit
`QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0` keeps the probe useful before
launch: `qbit.ibd`, a missing initial `stratum.highdiff_floor` difficulty
notification, and a below-threshold `coordinator.ready_miners` result are WARN
rows, while all other checks remain active and fatal. Set the flag to `1` for
launch; an omitted flag is also strict, and a malformed value fails closed.

For operator runs, `make up-prism-pool` requires Postgres plus
`PRISM_MANIFEST_SIGNING_SEED_HEX`,
`PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX`, and the trusted
`PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX`. The two seed values are 32-byte hex
Ed25519 signing seeds. The ledger public key is the verifying key derived from
the ledger attestation seed and must be distributed through an operator-trusted
channel, not copied from the bundle being verified. Keep
`PRISM_ALLOW_MEMORY_LEDGER`, `PRISM_ALLOW_TEST_SIGNING_SEEDS`,
`PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY`, and
`PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN` disabled outside local tests.

Mainnet configuration is always treated as production configuration, even when
the separate production toggle is omitted. It must select the chain explicitly
with `QBIT_CHAIN_FLAG=-chain=main` and pin the final release genesis hash in
`QBIT_EXPECTED_GENESIS_HASH`. The live readiness probe normalizes the configured
`mainnet` name to the `main` name returned by qbit RPC, then verifies height zero
against the pin.

Production builds using the git source provider must set `QBIT_GIT_COMMIT` to a
full 40-character object ID. The environment doctor verifies that the resolved
checkout is at that exact commit instead of trusting a mutable branch or tag.
Production also requires `PRISM_STRATUM_STALE_GRACE_SECONDS=0`; stale-credit
grace should be enabled only after the deployed verifier and accounting release
have an explicit compatibility proof for it.

The parent-chain selector is checked independently: `BITCOIN_CHAIN` and
`BITCOIN_CHAIN_FLAG` must be an exact pair, including `mainnet` with
`-chain=main`. When a production configuration selects a non-regtest parent for
AuxPoW, both `QBIT_MINER_ADDRESS` and `BITCOIN_MINER_ADDRESS` must be explicit;
automatic wallet-derived payout addresses are rejected.

Mainnet CTV settlement requires an operator-reviewed positive
`PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT`. On non-mainnet networks,
if it is omitted, the live readiness probe requires `estimatesmartfee` to return
a positive rate before the pool is considered ready. A new chain, or a chain
producing only empty blocks, does not have the confirmed transaction history
needed for empirical fee estimation. A wallet fallback fee does not populate
that history and is not a substitute for the explicit CTV fanout rate.
