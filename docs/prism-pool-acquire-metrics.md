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

Ledger and collector checkouts use one observer and Tokio's monotonic clock.
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
- `heartbeat` and the attached ledger's `fatal_state` read.

Recording requires an attached metrics handle. Operator-only connections and
ledgers created without telemetry continue to work without observations.

This is partial coverage of [#352](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/352).
Other direct job, coordinator, audit, startup and session-reservation cleanup
queries still acquire without this helper. The separate rollup transaction
also remains untimed. Public API read pools and the public role's export
policy require a separate decision. Consequently `_count` is neither a census
of pool acquisitions nor request throughput.

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
