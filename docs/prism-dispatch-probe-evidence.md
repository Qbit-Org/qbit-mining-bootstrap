# Candidate dispatch probe (#432)

The dispatch probe now selects at most one due row from an ordered derived
query before allocating a sequence slot. It keeps both advancing
`clock_timestamp()` predicates and the existing unfinished partial index.
The probe grants no candidate ownership; the atomic claim, fresh/oldest lane
policy, parameters, result shape and schema are unchanged.

The motivating PG16.14 synthetic investigation pinned base
`b3832e9c877703712fbf33a71bec3c297817358a` and executed 456 plans. At 3,120
backed-off unfinished rows, growing retained history from 50,000 to 100,000
made the original probe reject 53,120 and 103,120 visible rows, respectively.
The ordered query rejected 3,120 in both fixtures. Scan buffer accesses were
1,601/2,953 for the original and 213/214 for the ordered query. Those measured
accesses were shared hits, not physical disk reads. Live-claimed and
sparse-last layouts sometimes cost the ordered query more buffer accesses.

The separate `candidate_dispatch_probe` regression target executes the
canonical Rust builder against real PostgreSQL. It records the query and its
SHA-256, server/planner settings, exact counts, relation sizes, and
`EXPLAIN (ANALYZE, BUFFERS, SETTINGS, TIMING OFF, FORMAT JSON)` output:

- Exactly 24, 100 and 3,120 total unfinished rows, each including all four
  unfinished states, alongside 50,000 and 100,000 retained rows.
- Busy, backed-off, live-claimed, expired and sparse-last populations, using
  default planner methods/costs and direct plus parameter-free prepared SQL.
- Churn with an old snapshot retaining obsolete tuple/index versions, 100,000
  rows moved into and out of the unfinished set, delete/reinsert scattering,
  stale statistics, then ordinary `VACUUM (ANALYZE)`.
- Empty, terminal and positive-infinity polls consume no slots; each positive
  probe allocates one slot and leaves candidate records unchanged.
- A clock-substituted canonical-query model checks exact due/expiry
  microsecond boundaries, separately from actual advancing-clock execution
  in an old transaction. Sequence allocation survives transaction rollback.
- Native `claim_candidate` timeout, cancellation and connection-loss paths
  preserve failure and release resources; they do not return false idle.

The first qualification on PG16.14 executed 72 plans (36 direct and 36
prepared), plus six preparatory executions per prepared case. All used the
unfinished index without a Sort. With an old snapshot and stale statistics,
3,120 visible unfinished rows still incurred about 11,800 buffer accesses.
Thus the assertions cover these plan shapes and visible rows; they impose no
latency/page threshold or universal physical-work bound. Shared reads can
be served by the operating-system cache and are not proof of disk I/O.

Reproduce on a **disposable** PG16 database whose role can create databases:

```sh
export CARGO_BUILD_JOBS=2
export PRISM_TEST_DATABASE_URL=postgresql://USER@127.0.0.1:PORT/postgres
export PRISM_TEST_REQUIRE_INTEGRATION=1
export PRISM_TEST_GATE_MANIFEST=/tmp/dispatch-probe-gate.txt
cargo test --locked -p qbit-prism-server --test candidate_dispatch_probe -- --test-threads=1 --nocapture
```

Record `git rev-parse HEAD` with the output. Each test owns and drops its own
fixture database. The normal CI shard discovery includes this target, and
its four gate identities are registered in `test/prism-gated-tests.txt`.
The existing admission, lifecycle, claim fairness/owner-loss and storm/restart
targets provide native runtime qualification separately; storm sizes 100 and
3,120 refer to their existing configured sibling counts, not the exact total
unfinished counts of this probe matrix. The merged storm tests and assertions
are unchanged. No cold-cache latency, universal optimizer choice, or exact
concurrent clock trace equivalence is claimed.
