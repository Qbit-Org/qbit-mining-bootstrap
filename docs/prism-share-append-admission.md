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
does not classify that outcome, retry it, or suppress cleanup warnings.

The missing-partition retry releases the first attempt through that cleanup,
attaches the partition lead outside append admission, and reacquires for its
single retry. Revision and pre-COMMIT fences still run inside the transaction.
An exact match to an already durable share keeps its existing duplicate outcome.
Miner rejection identifiers and successful-credit deduplication are unchanged.

`append_admission` integration tests hold the actual advisory key on durable
PostgreSQL and queue more appends than the frontend pool can hold. They check
control-read progress, cancellation, shared-pool handles, revision/gate changes,
lost COMMIT replies, durable replay and missing-partition recovery. Pool checkout
telemetry continues to measure actual checkout; it does not include the new
admission wait.

External-tip latency is a separate measurement: node tip observation through
automatic refresh, issuance, persistence and the client's first matching decoded
notification before replacement. Warm-up and steady-state observations, partial
coverage, unknown delivery and ACK/row reconciliation must remain separate. The
explicit-refresh delivery benchmark does not measure that full boundary under
concurrent share load.
