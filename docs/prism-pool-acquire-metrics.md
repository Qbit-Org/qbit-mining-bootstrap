# Pool acquisition timing

`qbit_prism_database_pool_acquire_seconds` measures a connection checkout,
from polling the acquisition to its success, error or cancellation. This
includes waiting for a pool slot, opening or validating a connection, and
checkout hooks. SQL execution and the `BEGIN` round trip occur afterward and
are outside this duration. The unit is seconds.

The `result="success"` label means the checkout succeeded; a later query error
does not change that observation. An acquisition cancelled while waiting
records one `result="failure"` observation with its elapsed duration. Creating
and dropping an acquisition future without polling it records nothing.
An acquisition still waiting has not yet contributed to `_count`.

Ledger, collector, rollup and partition checkouts use `metrics::time_pool_acquire`, alongside
the metrics recording hooks, and Tokio's monotonic clock.
In an unpaused runtime this measures real elapsed time. In a paused test
runtime it follows the runtime's controlled clock; ledger checkouts now share
the collector's existing behavior in those tests. Constructing an unpolled
future or acquiring without a metrics handle reads no observation clock.
Advisory-lock observations retain their separate standard monotonic clock.

## Coverage

In addition to the existing ledger transaction and metrics collector
acquisitions, these paths use the shared checkout timer:

- `payout_revision` and `release_session_owner_reservations`;
- `SessionId::release` and its spawned drop cleanup;
- `worker_difficulty` and `share_accepted_at_ms`;
- `cpfp_package` and `retired_cpfp_funding`;
- `heartbeat` and the attached ledger's `fatal_state` read;
- `audit_bundle`'s initial representation read and its native snapshot/range
  reconstruction reads;
- the existing-audit probe before candidate landing and each page of its
  durable-range proof, released before the page's blocking comparison;
- `landed_audit` and `record_landed_audit_bits`;
- the durable-range proof's inline/bootstrap existence probe, newest eligible
  boundary query, and oldest eligible boundary query for partial history;
- `pool_blocks_for_reconcile`;
- `migration_source`, the one-row pages of `import_legacy_audits` (including
  the final empty read), and `backfill_ctv`'s initial audit list;
- `job`, `compact_prepared`, and `prune_expired_jobs`;
- `WorkLedger::now_ms`, `chain_observation_state`, and `persist_block_only`'s
  initial duplicate probe and credit/disposition polls;
- the server's rollup loop, once per valid `advance` attempt;
- partition attachment at server startup, each maintenance tick, and the fresh
  transaction after an append is refused with SQLSTATE 23514;
- candidate heartbeat's live-token read after renewal contention and its
  terminal-state read after work cancellation;
- `apply_online_migration`'s startup checkout, before the connection is detached.

Each pending online migration records one checkout when metrics are attached.
Timing ends before detach: the runner's session settings, advisory-lock waits, concurrent
index builds and drops, and the detached connection's lifetime are excluded.
An error or cancellation after checkout does not add another observation or
change that checkout's success. The transaction that records the migration
uses the detached connection and is not another pool checkout.

The job readers and expiry statement each record one checkout. The compact
reader releases it before waiting for blocking decoding and hashing. Expiry
releases it before the separate blob cleanup transaction; the expiry cutoff,
bounded selection, renewal recheck, blob lock ordering and cleanup deadline
are unchanged. A missing job or zero deleted rows is a successful checkout;
SQL or decoding failures after acquisition do not relabel that checkout.

The migration-source lookup keeps schema resolution and the provenance read
on one checkout. Acquiring through an existing connection or transaction is
not another pool checkout. Import reads release before filesystem and audit
verification work, then acquire separately for each write transaction.

Recording requires an attached metrics handle. Operator-only connections and
ledgers created without telemetry continue to work without observations.

Session reservations retain their creating ledger's metrics handle. Explicit
release and background drop cleanup each time only their own checkout, before
the unchanged token-fenced DELETE. A successful explicit release disarms drop
cleanup and records once. An error or cancellation retains the existing drop
fallback: if that task runs, it attempts and records a separate checkout.
Dropping an unpolled release future records nothing for that release, but still
spawns its background cleanup when a Tokio runtime is current. Without a current
runtime, drop retains the reservation and records nothing. An unpolled background
task records nothing; runtime shutdown during checkout records one failure. SQL waits and errors
after checkout keep the successful observation. Failed cleanup can retain a
reservation for the existing owner cleanup/reclamation paths; timing adds no
retry, deadline or change to reservation ownership.

Each durable-range boundary statement records one checkout and releases it
before any proof refusal, blocking fold or subsequent query. Bootstrap uses
only its existence probe; a non-inline range uses its existing page checkouts
and the newest boundary query, plus the oldest query only for partial history.
SQL failures and boundary refusals after checkout retain a success observation.
These probes add no transaction and do not time the SQL or the proof itself.

Each landed-audit lookup or bits update records one checkout and releases its
connection at the statement boundary, before decoding the row or returning.
Found and missing rows, a filled legacy NULL, and zero-row/idempotent updates
all retain a successful checkout. SQL errors or cancellation after checkout
do too; cancellation while acquiring records one failure. The read's
authentication contract and the update's `found_block_bits IS NULL` predicate
are unchanged. Failure or cancellation after a write is sent can still leave
its commit outcome unknown; retries retain the same idempotence.

Ledger audit reconstruction records one checkout for the initial representation
read, one for a native snapshot, and one for a non-inline share range. Native
inline snapshots therefore record two successes and range-backed snapshots
three; imported canonical bytes, legacy inline bodies and missing audit rows
record one. Each connection is released at its statement boundary, before row
decoding, blocking reconstruction or the next acquisition. Missing snapshots,
incomplete ranges, SQL errors and digest/decoding refusals retain the successes
already observed. Cancelling a pending checkout records one failure; cancelling
SQL or the later reconstruction does not relabel completed checkouts. The
snapshot authority, anchored range, canonical bytes and digest checks are
unchanged.

The coordinator database clock and chain-observation state each record one
checkout. The latter retains its one-statement revision/epoch/tip snapshot.
Each block-only duplicate probe and each credit/disposition poll records its
own checkout, inside the
original acknowledgement deadline. The two block-only probes are not issued-job
reads. Credit and disposition still share one SQL statement and MVCC snapshot.
Connections are released at the statement boundary, before enqueue or poll
sleep. Missing rows, duplicates and terminal dispositions retain successful
checkout observations; SQL errors or cancellation after acquisition do too.
Cancellation while acquiring records a failure. Before enqueue a read failure
is definite; after enqueue a failed or missing read does not prove whether the
durable candidate earned credit. Timing changes neither that distinction nor
the enqueue's existing handling of an unknown outcome.

The server passes its existing metrics handle to the rollup loop. Each valid
rollup attempt records one checkout before `BEGIN`; `SET LOCAL`, the single
rollup statement and `COMMIT` reuse that connection and add no observations.
A real batch and an empty batch both record success. Invalid batches and
unpolled futures record nothing. Pool errors and cancellation during checkout
record failure; later SQL errors or cancellation retain the completed success
and its duration. Shutdown drops the in-flight attempt using the existing
select. Timing changes no transaction ownership, statement timeout, tick
scheduling or error propagation. Cancellation after `COMMIT` starts can still
be committed: the existing watermark and idempotence semantics reconcile that
outcome. The public pool-only `rollups::advance` and `rollups::run` wrappers
remain compatible and record nothing.

Partition maintenance uses the same boundary: checkout completes before
`BEGIN`, the existing 30-second local statement timeout, the ensure function,
and `COMMIT`. An intact lead and newly attached partitions both record one
success. Pool errors and cancellation during checkout record one failure;
SQL errors, SQL cancellation and shutdown after checkout retain its success
and duration. The public `partitions::ensure` and `partitions::run` wrappers
still pass no metrics. The server passes its registry at startup and on each
tick; an append's SQLSTATE 23514 fallback passes its ledger's metrics handle.
That fallback records three checkouts: the refused append transaction, the
partition transaction, and the one retry. It does not double-count
`Ledger::begin`, restart the enclosing deadline, or change the SQL, rollback,
retry count or share-credit semantics. As before, failure after sending
`COMMIT` can have an unknown commit outcome.

The candidate live-token and terminal-state probes each record their own
checkout inside the original `timeout` future. The first probe still uses
the remaining lease budget; the second still uses the existing reconciliation
timeout after dropping work. SQL predicates, token/expiry decisions and
original renewal errors are unchanged. Acquisition failure or cancellation
records failure; missing/nonterminal rows and SQL errors after acquisition
retain checkout success. A failed probe supplies no new evidence of ownership
or completion. These probes add samples to the same aggregate, without adding
labels or a second observation to the renewal transaction's `Ledger::begin`.

Polls contribute actual acquisition attempts to the same aggregate histogram
as the other covered callers. Expanding coverage can change its percentile:
a pending candidate can produce many fast acquisitions when the pool has spare
capacity, even if settlement is stalled. Each poll waits for its checkout and
SQL before the existing 50 ms sleep, so a saturated pool slows those attempts
and their observed waits increase. The pool-wait alert's five-minute p99 and
ten-attempt minimum describe this combined population, not distinct submissions,
per-caller latency or settlement latency. Adding fast samples can lower the
aggregate p99; this coverage change does not change the alert's grouping,
threshold or minimum count.

Rollup attempts also join this aggregate population, including no-work ticks.
Their checkout counts describe attempts, not folded shares or completed
rollups; fast rollup checkouts can lower the aggregate percentile too.
Partition ticks and fallback attachment attempts join it too, including
no-work ensures. Their samples count checkouts, not created partitions or
successful share appends.

Landed-audit reads and bits writes also join this aggregate population,
including misses and idempotent retries. Their samples describe checkout
attempts, not newly landed blocks or successful SQL statements.

## Source census and exclusions

The census for [#352](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/352)
is checked in CI by the existing Python test shards, which automatically
discover `tests/test_check_prism_pool_acquires.py`. Its baseline test verifies
the repository inventory alongside the negative controls. Locally,
`python3 scripts/check_prism_pool_acquires.py` checks the same inventory;
`--census` prints current call sites and source lines without updating the
reviewed classifications in `scripts/prism_pool_acquires.json`. Each entry
has an exact expected occurrence count and a policy with an owner and reason.
Adding a site, adding another occurrence of an existing site, removing a site,
or removing/disabling its timer fails the guard until reviewed.

The scanner walks the server crate's production module graph, including its
binaries and platform branches, and excludes test-only modules. It tokenizes
comments and SQL strings as opaque text. Direct implicit executors (including
`raw_sql`), explicit `acquire`/`begin` variants, aliases and calls to typed
pool/generic executor/reader helpers are inventoried. Mutable SQLx executor
borrows execute on an existing connection; nested acquisitions remain separate
sites. `Transaction::begin` on a checkout, migration connection reborrows and
window/range readers must not be counted again. The report counts source
syntax, including helper forwarding, rather than runtime attempts; adding its
categories together does not give a number of pool acquisitions.

This is a conservative source review gate, not Rust type analysis or a
whole-program proof. New pooling abstractions, type aliases or generated code
need review of the scanner's coverage as well as classification. Tests mutate
the real source tree to prove detection of new implicit and explicit pool
calls, renamed pool handles, generic/helper calls, stale entries and removed
timers; a borrowed-connection control adds no fresh checkout site.
Imported pool type aliases and ordinary `type` aliases enroll helper callers;
`include!` fails closed until the generated Rust source is explicitly supported.

The following exclusions are intentional and remain visible in the inventory:

| Population | Owner and reason |
| --- | --- |
| Startup schema, capability, provenance and coordinator session-setting checks | Ledger startup/migrations: gates before serving. Existing timed startup transactions and online migration checkouts still contribute. |
| Generic migration `Acquire` helpers | Ledger migrations: a pool argument at startup acquires; a borrowed transaction during migration does not. Their callers are inventoried too. |
| Candidate inventory/recovery reader, fatal-state inspection and self-check | Operator tools: dedicated diagnostic pools or explicit operator calls, outside the frontend work loops. |
| Public HTTP reads and readiness probes | Public API: isolated read pools; the public role's metric export policy requires a separate decision. |
| Private audit HTTP endpoints and shared API read-model helpers | Operator API: private routes can retain the run-role pool, while public dispatch substitutes its isolated read pool. These HTTP queries remain excluded even when served by the run process. |
| Pool-only audit, partition and rollup wrappers, including `lead_rows` | Compatibility APIs: no attached metrics owner. The server uses metrics-aware maintenance; its headroom collector reads within an already timed transaction. |

Pool creation and idle connection replenishment are not acquisition attempts
observed by this helper. Ledgers created without telemetry record nothing.
Accordingly the histogram is the aggregate of **observed checkout attempts at
covered callers**, not all process acquisitions, all use of a database role,
request throughput or an unbiased sample of every pool operation. The metric
type, labels, buckets and result semantics have not changed.

## Adding a caller

Use `Ledger::acquire()` inside the caller's existing deadline, then run the
unchanged query on the returned connection. Release the checkout at the
original statement boundary, before another transaction, page or unrelated
work. For a single statement, a temporary such as
`query.fetch_one(&mut *ledger.acquire().await?).await` keeps that scope small.
Do not wrap a whole query or transaction in the acquisition timer, and do not
add a second observation around `Ledger::begin()`.

The histogram's cumulative buckets describe completed acquisition durations.
For waits below its first finite bucket (10 ms), `_sum / _count` gives an
average, not a percentile. Compare rates over the same interval, handle a
zero observation count, and retain appropriate instance/result grouping.
See [histogram consumer guidance](prism-metrics-histogram-consumers.md) for
query examples and bucket-resolution limits.
