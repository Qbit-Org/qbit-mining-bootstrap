# Job persistence measurement harness (#275)

**Performance acceptance is not yet met.** This PR supplies measurement and
baseline contention tests; it does not optimize locks or batch inserts. The
one-second delivery target remains a criterion, not a promise. Refs #275.

## Baseline and environment

- Runtime baseline: frozen PR #397, commit
  `1b0d409344c1b99f5f4bd04970890b9b034a9eeb`.
- Measurement date: 2026-09-15 UTC.
- Host: Apple M4 Max, 14 logical CPUs, 36 GiB memory, macOS 26.5.1.
- Rust 1.98.1, Cargo test profile (unoptimized, debug information), two build
  jobs; each test runtime has two worker threads and each frontend two build
  workers. Both frontends and all clients share that runtime/process/host.
- Disposable local PostgreSQL 16.14, separate primary on port 55475/database
  `b275_measure`. `fsync`, `full_page_writes`, and `synchronous_commit` are all
  checked to be `on`. Four pool connections per frontend; separate two-connection
  fixture and administration pools. Each case creates and drops a UUID schema.
- Sixteen deterministic accepted shares from the existing window fixture, five
  payout recipients, empty transaction template, fake node RPC over local HTTP.
  Coordinator publication, durable prepared/issued records, codec and public
  Stratum listeners are the actual runtime. No SQL proxy or write trigger.

The smoke observations below were taken on a shared development host. They are
fixture validation, not an uncontended benchmark or evidence at 2,000 sessions.

## What is timed

1. Start one or two independent coordinators sharing the disposable database.
   Seed the window and refresh both before opening clients.
2. Subscribe and authorize every TCP client, with at most 32 concurrent logins.
   Receive initial work on all clients before starting the clock. Connections
   remain open. Admission is explicitly 2,000 per frontend; vardiff is disabled.
3. Change the fake node to a new parent. Start one monotonic clock immediately
   before polling all `refresh_once` calls concurrently. This includes chain/RPC
   checks, refresh, job construction, persistence, scheduling and TCP delivery.
   It excludes setup/login and automatic node polling intervals.
4. Every client timestamps the first decoded `mining.notify` for that parent,
   requiring a fresh job ID and `clean_jobs=true`. All refreshes and client reads
   share the same absolute 120-second deadline; receiving another frame never
   resets it. Failed reads and missing clients remain explicit in the JSON.
5. Report refresh return times, received count, median and maximum observed
   delivery time. `all_sessions_within_one_second` also requires complete delivery
   and successful refreshes. A slower complete run still passes the measurement
   test; an incomplete run fails. `B275_DURABILITY` is printed only after checking
   every delivered ID has a live durable row naming its frontend's prepared
   dependency, new parent and original payout revision.

The 2,000-session case uses **2,000 total** in both topologies: 2,000 on one
frontend, then 1,000 on each of two frontends, with fresh schemas and complete
cleanup between runs. It is an explicit ignored test. One paired observation
does not establish a stable speedup or satisfy the second-frontend non-regression
criterion without controlled repetitions and a stated comparison tolerance.

Existing Prometheus histogram count/sum deltas separate pool checkout from
advisory-lock waits, including each lock kind and outcome. They cover all
instrumented frontend operations during the bracket, including refresh and
fanout, **not only job SQL**. Their sums overlap across concurrent sessions and
cannot be subtracted from elapsed time to infer commit cost. The baseline has no
commit-timing metric, so `commit_seconds` is `null`. No production instrumentation
was added. Lock metrics measure the awaited lock query, including its round trip.

## Smoke results

| Case | Sessions | Frontends | Maximum client delivery | Refresh return | Durable IDs |
| --- | ---: | ---: | ---: | --- | ---: |
| Small smoke | 8 | 1 | 0.029793 s | 0.020519 s | 8/8 |
| Small smoke | 8 | 2 | 0.031811 s | 0.024691 / 0.024230 s | 8/8 |
| Required scale | 2,000 | 1 | **Not measured** | Not measured | Not measured |
| Required scale | 2,000 | 2 | **Not measured** | Not measured | Not measured |

The first smoke bracket counted 53 successful checkout observations (sum
0.021456 s) and 15 settlement-lock observations (sum 0.015923 s). The second
counted 33 successful checkouts on each frontend (sums 0.002454 / 0.002656 s)
and 11 settlement locks each (sums 0.020793 / 0.008625 s). These sums are not
serial wall time or a per-job breakdown.

### Controlled landing-lock baseline

Build real runtime work first, then acquire the database-wide settlement advisory
transaction lock on a fixture connection. Start `persist_issued_job` and confirm
an ungranted advisory lock in `pg_locks` for this database. Hold the stub until
three seconds after starting persistence, release it, and verify the job row.
The stub holds the lock only; it does not simulate landing CPU, share reads,
balance writes or revision changes.

Observed persistence was **3.003617 s**, release acknowledgement **3.001119 s**,
and the runtime's single settlement-lock sample **3.000660 s**. This is evidence
of baseline contention, not lock-free persistence. The test requires an observed
waiter and successful durable persistence; it deliberately does not assert
latency below the hold duration. A separately interleaved revision bump verifies
the exact original `payout revision changed during job construction` fence and
that neither a generic stale row nor the stale runtime issued row is persisted.

## Reproduction

Use a dedicated PostgreSQL 16 primary/database with all three durability settings
on. Coordinate the full measurement so other load, scale and global-WAL runs are
idle. Set `PRISM_TEST_DATABASE_URL` to that disposable database; the tests create
and drop schemas. Use fresh manifest paths for each command:

```sh
PRISM_TEST_REQUIRE_INTEGRATION=1 \
PRISM_TEST_GATE_MANIFEST=/tmp/b275-smoke-manifest.txt \
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server \
  --test b275_persistence_measure -j 2 -- --test-threads=2 --nocapture

PRISM_TEST_REQUIRE_INTEGRATION=1 \
PRISM_TEST_GATE_MANIFEST=/tmp/b275-scale-manifest.txt \
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server \
  --test b275_persistence_measure measure_2000_sessions_one_and_two_frontends \
  -j 2 -- --ignored --exact --test-threads=2 --nocapture
```

The ordinary gate registers three tests in `test/prism-gated-tests.txt`; the
explicit scale test calls the required integration gate and fails if the database
input is missing. Preserve its separate manifest and `B275_MEASUREMENT` JSON
lines with the tested commit and host/configuration. Roughly 4,000 client/server
TCP file descriptors are needed in the single process; check the host limit.
The harness does not issue `CHECKPOINT` or sample cluster-wide WAL counters.

## Verification

- Final small suite: six passed, zero failed, one explicitly ignored scale test
  (three integration cases and three existing fixture unit tests).
- Required integration manifest: all three registered integration cases executed;
  scoped manifest checker passed with no skips or failures.
- Unchanged `readiness_rpc` test
  `another_frontend_payout_revision_retires_same_parent_work_and_preserves_parent_grace`:
  passed against the same disposable database.
- Selecting the ignored scale case without the database input: failed as required
  (exit 101), before opening a fixture; this is gate validation, not a scale run.
- `cargo fmt --all -- --check` and target-scoped Clippy with `-D warnings`: passed.

## Limits and outstanding acceptance

The 2,000-session measurement slot and external review are pending. Neither the
one-second target nor the two-frontend non-regression criterion is established.
Commit duration, CPU attribution, peak RSS, WAL, remote-network latency, multiple
hosts/processes, automatic poll detection delay, full payout-window scale,
transaction-heavy templates and miner submissions are unmeasured. This fixture
uses synthetic node responses and does not measure a live node or production.
Issue #275 must remain open for measurement interpretation and any separately
authorized optimization.
