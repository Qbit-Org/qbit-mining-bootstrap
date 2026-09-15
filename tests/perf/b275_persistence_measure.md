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
5. Report refresh return times, received count, lower median and maximum observed
   delivery time. `all_sessions_within_one_second` also requires complete delivery
   and successful refreshes. Since review hardening, completion additionally
   requires zero failed listener delivery attempts; their deltas are reported.
   A slower complete run still passes the measurement
   test; an incomplete run fails. `B275_DURABILITY` is printed only after checking
   every delivered ID has a live durable row naming its frontend's prepared
   dependency, new parent and original payout revision, and the total issued-row
   count for that frontend/new parent equals the delivered count.

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
| Required scale | 2,000 | 1 | 1.428–1.649 s (three runs below) | 0.020–0.024 s | 2,000/2,000 each |
| Required scale | 2,000 | 2 | 1.359–1.710 s (three runs below) | 0.024–0.047 s | 2,000/2,000 each |

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
the compact-issued transaction's exact original
`payout revision changed while observing chain state` fence after its own lock
wait, and that neither its stale row nor the public runtime issued row is
persisted. The frozen-head version exercised the generic save's revision error;
the compact-issued strengthening is a separately validated review fix.

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

The ordinary gate registers four tests in `test/prism-gated-tests.txt`; the
explicit scale test calls the required integration gate and fails if the database
input is missing. Preserve its separate manifest and `B275_MEASUREMENT` JSON
lines with the tested commit and host/configuration. Roughly 4,000 client/server
TCP file descriptors are needed in the single process; check the host limit.
The harness does not issue `CHECKPOINT` or sample cluster-wide WAL counters.

## Verification

- Revised small suite: seven passed, zero failed, one explicitly ignored scale
  test (four integration cases and three existing fixture unit tests).
- Required integration manifest: all four registered integration cases executed;
  scoped manifest checker passed with no skips or failures.
- Unchanged `readiness_rpc` test
  `another_frontend_payout_revision_retires_same_parent_work_and_preserves_parent_grace`:
  passed against the same disposable database.
- Selecting the ignored scale case without the database input: failed as required
  (exit 101), before opening a fixture; this is gate validation, not a scale run.
- `cargo fmt --all -- --check` and target-scoped Clippy with `-D warnings`: passed.

## Limits and outstanding acceptance

Three paired 2,000-session runs completed, and four independent source-review
reports arrived; see the appendices below. The one-second target was missed in
all six cases, and two-frontend non-regression is not established.
Commit duration, component CPU attribution, WAL, remote-network latency, multiple
hosts/processes, automatic poll detection delay, full payout-window scale,
transaction-heavy templates and miner submissions are unmeasured. This fixture
uses synthetic node responses and does not measure a live node or production.
Issue #275 must remain open for measurement interpretation and any separately
authorized optimization.

## Frozen-head 2,000-session results — 2026-09-15

Tested harness commit: `3d4775cacc8c6f8376df0ce028d1083bda738746`; runtime
baseline remains `1b0d409344c1b99f5f4bd04970890b9b034a9eeb`. The existing
unoptimized test binary was invoked directly, with no rebuild, three times from
22:03:44 through 22:04:16 UTC. Each invocation explicitly selected
`measure_2000_sessions_one_and_two_frontends --ignored --exact --test-threads=2
--nocapture`, always one frontend first and two second. All three exited zero;
each emitted two measurements and two durability confirmations. Each manifest
contains the executed explicit scale gate. Cleanup left no measurement schemas;
the dedicated database was stopped afterward.

| Pair | Frontends | Sessions received / durable | Lower median delivery (s) | Maximum delivery (s) | Refresh returns (s) | Within 1 s |
| --- | ---: | --- | ---: | ---: | --- | --- |
| 1 | 1 | 2,000 / 2,000 | 0.829421 | 1.499653 | 0.020061 | No |
| 1 | 2 | 2,000 / 2,000 | 1.187969 | 1.710496 | 0.039934 / 0.047239 | No |
| 2 | 1 | 2,000 / 2,000 | 0.815554 | 1.428409 | 0.020274 | No |
| 2 | 2 | 2,000 / 2,000 | 0.881717 | 1.358960 | 0.023731 / 0.024136 | No |
| 3 | 1 | 2,000 / 2,000 | 1.029605 | 1.648645 | 0.023670 | No |
| 3 | 2 | 2,000 / 2,000 | 0.942327 | 1.412927 | 0.025471 / 0.029112 | No |

The paired two/one ratios for maximum delivery were 1.141, 0.951 and 0.857.
Median maxima were 1.500 s and 1.413 s respectively. These mixed observations,
fixed ordering, three pairs and absence of a predeclared comparison tolerance do
not establish non-regression or a stable speedup. The original one-second target
and deadlines were unchanged. There were no incomplete or failed cases.

### Host and configuration evidence

- Apple M4 Max / Mac16,6, 14 logical CPUs, 36 GiB, macOS 26.5.1, Rust 1.98.1.
  PG16.14 used the existing disposable primary at local port 55475, database
  `b275_measure`; all three durability settings were verified `on` before running
  and by each case. No production or testnet endpoint was involved.
- Runtime author confirmed its heavy commands had finished and held new ones;
  refresh preparation was complete. A separate compiler observed during early
  preflight was allowed to finish. Process snapshots before each repetition
  found no other compiler/test process. CPU idle in the second pre-run sample
  was 74.15%, 75.66%, and 77.68%. This was a development Mac with background apps,
  not an isolated benchmark machine.
- Process descriptor limit was set to 16,384, above the roughly 4,000 TCP
  descriptors plus headroom; kernel per-process ceiling was 138,240. Memory
  pressure reported 39–46% free during the window, with about 17.4 GiB of
  existing swap use. Snapshots preserve those qualifications instead of calling
  the machine uncontended. Per-pair test-process peak RSS was 104,742,912,
  104,579,072 and 103,120,896 bytes; these include client and server work and
  exclude PostgreSQL. User/system CPU seconds are preserved in the resource logs.
- Each frontend had two build workers, four database connections, an independent
  128-permit job-build admission semaphore, a 30 s timeout for each build and
  persist phase, and a 20 s write timeout. Thus two frontends had 256 total build
  admission permits and eight connections. The 2,000 connection ceiling is a
  different limit. Two Tokio workers serve the entire process; client receive
  futures are polled by one test task, so serialized client JSON decoding is
  included in these delivery times.

### Interpretation and reproducibility

This is a **debug-profile, 16-share synthetic-node fanout baseline**. It measures
the stated decoded-client boundary and durable runtime path, not production-shaped
qualification. Full payout-window scale, real-node/network latency, miner
submissions, automatic polling delay and transaction-heavy templates remain
unmeasured. Fsync being on is necessary but does not make this a production load.

The frozen harness did not observe listener delivery-failure/retry counters or
count committed issued rows that were never delivered. Therefore `complete=true`
means all clients eventually received work and refresh returned successfully;
it does not prove zero internal failed attempts or zero unmatched rows. These
unknowns are retained for these original measurements; later harness fixes must
not retroactively populate them. All six maxima are below the 30 s phase timeout,
but that alone cannot rule out earlier failures and retries.

Raw JSONL, full test/resource logs, gate manifests, binary SHA-256, Cargo
fingerprint, tested tree/SHA, hardware/profile, database settings and per-run host
snapshots are preserved in [the result bundle](results/b275-2026-09-15/metadata.json).
The four independent reviews are alongside it as `review-ctx_*.md`; they reviewed
the frozen source and did not independently run tests. Their findings and
dispositions are tracked in [the review ledger](b275-review-ledger.json).

## Review fixes and validation

Append-only commit `c4019a4` changes only the harness and its required-test
inventory. JSON schema `b275.delivery.v2` records effective configuration and
listener success/failure deltas; unsuccessful attempts cannot become a clean
result after a retry. An additional regression temporarily rejects new-parent
issued rows with a CHECK constraint in its private schema, then removes it after
observing failures. All eight clients recovered, but the report retained eight
failed attempts and `complete=false`; the regression passed. This fault is never
installed in a measurement topology.

The compact-issued fence test now waits in `save_issued_job_compact` itself and
asserts the precise post-wait revision error. Contention probes are scoped to
the fixture's unique PostgreSQL application name and persistence must last at
least the three-second hold. Cleanup uses one path for setup failures and normal
completion. The case deadline reaches login and delivery, with one second
reserved for evidence and explicit login-failure output; deadlines were not
increased. Metrics parsing selects histogram counts/sums explicitly.

The revised disposable-PG16 suite passed **7 tests, 0 failures, 1 intentionally
ignored scale test**, including **4/4 registered integration gates**. Ordinary
small deliveries reported zero failed attempts and zero unmatched issued rows.
Formatting, diff whitespace checks and target-scoped Clippy with warnings denied
passed. [The final smoke log](results/b275-2026-09-15/review-fixes-smoke.log) and
manifest are preserved. These checks validate the review fixes; the six scale
observations above remain measurements of the original frozen commit only.
