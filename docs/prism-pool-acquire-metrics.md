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

Ledger and collector checkouts use `metrics::time_pool_acquire`, alongside
the metrics recording hooks, and Tokio's monotonic clock.
In an unpaused runtime this measures real elapsed time. In a paused test
runtime it follows the runtime's controlled clock; ledger checkouts now share
the collector's existing behavior in those tests. Constructing an unpolled
future or acquiring without a metrics handle reads no observation clock.
Advisory-lock observations retain their separate standard monotonic clock.

## Coverage

In addition to the existing ledger transaction and metrics collector
acquisitions, the shared ledger helper covers these direct statements:

- `payout_revision` and `release_session_owner_reservations`;
- `worker_difficulty` and `share_accepted_at_ms`;
- `cpfp_package` and `retired_cpfp_funding`;
- `heartbeat` and the attached ledger's `fatal_state` read;
- `audit_bundle`'s initial representation read;
- the existing-audit probe before candidate landing and each page of its
  durable-range proof, released before the page's blocking comparison;
- `pool_blocks_for_reconcile`;
- `migration_source`, the one-row pages of `import_legacy_audits` (including
  the final empty read), and `backfill_ctv`'s initial audit list.

The migration-source lookup keeps schema resolution and the provenance read
on one checkout. Acquiring through an existing connection or transaction is
not another pool checkout. Import reads release before filesystem and audit
verification work, then acquire separately for each write transaction.

Recording requires an attached metrics handle. Operator-only connections and
ledgers created without telemetry continue to work without observations.

This is partial coverage of [#352](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/352).
Other direct job, coordinator, candidate, startup and session-reservation
cleanup queries still acquire without this helper. Audit reconstruction
(`audit_canonical_bytes`, the snapshot lookup in `materialize_audit_row`, and
its range read) remains untimed, including when `audit_bundle` invokes it.
The independent bootstrap/newest/older boundary probes added by #379 also
remain untimed; only the durable-range page checkouts are observed here.
The separate rollup transaction also remains untimed. Public API read pools and the public role's export
policy require a separate decision. Consequently `_count` is neither a census
of pool acquisitions nor request throughput.

Remaining coordinator/job sites are `job`, `prune_expired_jobs`,
`compact_prepared`, coordinator startup checks, candidate lease/terminal
reconciliation, miner-submit issued-job checks, and `WorkLedger::now_ms`.
At this revision `ledger/window.rs` takes transactions through `Ledger::begin`
and passes existing connections to its range/probe readers; those readers
must not be counted as fresh checkouts.

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
