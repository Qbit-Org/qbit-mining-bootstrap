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

Ledger, collector and rollup checkouts use `metrics::time_pool_acquire`, alongside
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
- the durable-range proof's inline/bootstrap existence probe, newest eligible
  boundary query, and oldest eligible boundary query for partial history;
- `pool_blocks_for_reconcile`;
- `migration_source`, the one-row pages of `import_legacy_audits` (including
  the final empty read), and `backfill_ctv`'s initial audit list;
- `job`, `compact_prepared`, and `prune_expired_jobs`;
- `WorkLedger::now_ms`, `chain_observation_state`, and `persist_block_only`'s
  initial duplicate probe and credit/disposition polls;
- the server's rollup loop, once per valid `advance` attempt;
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

This is partial coverage of [#352](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/352).
Other direct coordinator, candidate and startup queries still acquire without
this helper. The pool-only public helpers have no attached metrics owner:
`audit_canonical_bytes`'s representation lookup and its reconstruction, and
direct public calls to `materialize_audit_row`, still record no observations.
Public API read pools and the public role's export
policy require a separate decision. Consequently `_count` is neither a census
of pool acquisitions nor request throughput.

Remaining sites outside this slice include:

- `ledger/connect.rs`: startup schema/capability/provenance checks. The
  reservation write uses `Ledger::begin`; releasing an owner's reservations
  uses `Ledger::acquire` and remains one observation.
- `ledger/candidates.rs`: `landed_audit` and `record_landed_audit_bits`.
- `ledger/audit.rs`: the pool-only public helper reads described above;
  startup/migration validation helpers keep their existing
  connection ownership.
- `partitions.rs`: startup attachment and background partition maintenance.

Coordinator startup checks, candidate lease/terminal reconciliation,
public read pools, operator connections and pool-only rollup wrappers remain
outside this slice.
The other `ledger/window.rs` reads take transactions through `Ledger::begin`
and pass existing connections to range/probe readers; those readers must not
be counted as fresh checkouts.

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
