# Share append admission

Share append transactions acquire bounded admission **before** checking out a
database connection. For a pool of `N` connections, at most `N - 1` appends can
hold connections, including connections still being cleaned up after an error
or cancellation. Frontend construction supports pools of at least two
connections, so even the smallest frontend pool leaves one connection available
to other work. A caller-supplied one-connection pool still permits one append;
it cannot provide concurrent read headroom.

The cap follows pool identity. Ledger clones and separately constructed ledger
handles assigned the same public pool share one allowance. Separate pools using
the same database URL have separate allowances. A weak registry retains no idle
pool after its outstanding appends and cleanup finish.

This prevents appenders waiting for `ORDER_LOCK` from consuming every frontend
connection needed by refresh. It does not reserve an exclusive refresh connection:
other database users can consume the remaining capacity, and work that needs an
accounting lock must still wait for that lock. Pool size, lock ordering and SQL
authority checks are unchanged.

Candidate preparation precedes admission. The submission's original deadline
still covers admission and persistence; queueing and the existing single retry
for a missing partition create no new deadline. A cancelled queued append has
not started SQL. After checkout, the transaction borrows a connection guard that
keeps admission until SQLx has returned or closed that connection. Dropping a
transaction only queues rollback, so releasing admission at caller cancellation
would be too early. The same rule covers an uncertain COMMIT reply; admission
does not classify that outcome, retry it, or suppress cleanup warnings. An
aborted append keeps its server-side `ORDER_LOCK` queue position until the lock
is granted and the queued rollback runs, exactly as before; abort does not free
the slot promptly. The guard spawns that cleanup on the runtime handle captured
at admission, so a future dropped from a thread outside the runtime context
still cleans up without panicking while the runtime lives; on a shut-down
runtime the cleanup is cancelled and the floated connection and permit are
released by ownership.

One failure mode changes: the pool's `acquire_timeout` used to fail an append
with `PoolTimedOut` when every connection was busy. Admission now waits for a
permit with no bound of its own, so an append that its caller follows rather
than refuses at the acknowledgement deadline (a candidate-bearing share) waits
for its turn instead of failing after that timeout. Checkout after admission is
still bounded by `acquire_timeout`.

The missing-partition retry releases the first attempt through that cleanup,
attaches the partition lead outside append admission, and reacquires for its
single retry. Revision and pre-COMMIT fences still run inside the transaction.
An exact match to an already durable share keeps its existing duplicate outcome.
Miner rejection identifiers and successful-credit deduplication are unchanged.

`append_admission` integration tests hold the actual advisory key on durable
PostgreSQL and queue more appends than the frontend pool can hold. They check
control-read progress, cancellation on and off the runtime, shared-pool handles,
revision/gate changes, lost COMMIT replies, durable replay and missing-partition
recovery. Pool checkout telemetry continues to measure actual checkout; it does
not include the new admission wait, so checkout saturation under-reports append
queueing.

External-tip latency (node tip observation through automatic refresh, issuance,
persistence and first client delivery under concurrent share load) is a separate
measurement with its own warm-up, coverage and reconciliation rules; it is not
the explicit-refresh delivery benchmark.
