# Release-profile public Stratum delivery (#275)

**Five of six 2,000-session cases missed the unchanged one-second target.**
All 12,000 timed deliveries matched durable issued rows, with zero failed
delivery attempts and zero unmatched rows. Two-frontend non-regression and
production-shaped qualification remain unestablished; #275 remains open.
This report adds release evidence to the [historical debug measurements](b275_persistence_measure.md).

## Revision and conditions

- Actual tested source: `d5d14ce5e828375c8964db21b8c756dcf2f41d9f`, the fetched
  `3.x.x` tip including #402. No source edits preceded the measurements.
- Fresh `cargo test --release --locked --no-run` build, Rust/Cargo 1.98.1,
  `aarch64-apple-darwin`: optimization level 3, debug information 0, debug
  assertions and overflow checks disabled, thin LTO, one codegen unit.
  Cargo reported the test artifact as newly built, not fresh from cache.
  Test binary SHA-256:
  `47ae2c1e5d98f4ad43992795b7d91d8495f25c573aeee3ee655123b272c172c9`.
- Paired runs: 2026-09-16, 17:41:40–17:42:11 UTC. Apple M4 Max (Mac16,6),
  14 logical CPUs, 36 GiB RAM, macOS 26.5.1 (25F80).
- Fresh dedicated disposable PostgreSQL 16.14 primary on the same host:
  `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`,
  `wal_sync_method=open_datasync`, `shared_buffers=128MB`,
  `max_connections=100`; no recovery/replica. The harness verifies PG16 and
  all three durability settings for each topology.
- Other workers held new heavy tests after their active suite exited.
  Process checks before/after each pair and every 0.5 s during execution found
  no competing Cargo, rustc, Clippy, pytest, nextest or test-binary process.
  This does **not** establish a quiet host: background applications remained,
  pre-pair one-minute load was 12.29/13.64/12.46, and the second one-second
  CPU samples were only 48.28%/55.83%/45.58% idle. Memory free was 40–42%,
  existing swap use was 18,295.69 MiB, and disk free was 5.3 GiB. Subsecond
  interference and background activity are not excluded.
- Descriptor limit 16,384 (kernel process ceiling 138,240). Test-process
  peak RSS per pair: 96,010,240 / 97,206,272 / 97,304,576 bytes, excluding PG.

## Effective configuration and boundary

The fixture sets configuration directly; shell production settings do not
override it. Both topologies share two Tokio workers and one test process.
Each frontend has two build workers, four database connections, a connection
limit of 2,000 and 128 job-build admission permits. Build and persistence each
have a 30 s phase timeout, writes 20 s, and issued-job retention 300 s;
vardiff is disabled. Two frontends therefore provide eight aggregate database
connections and 256 admission permits. Template/submit freshness, snapshot
interval and health timeout are 600 s; automatic background polling is absent.

The fixture contains **16 deterministic accepted shares, five payout recipients,
an empty transaction template and a synthetic local node**. Public TCP Stratum,
coordinators, prepared records and issued-job persistence use the real runtime.
This is a synthetic fanout measurement, not a production-sized payout-window test.

Every session subscribes, authorizes and receives initial work before timing.
The common monotonic interval starts immediately before concurrent
`refresh_once` polling after changing the synthetic parent, and ends per client
when it decodes fresh new-parent `mining.notify` with `clean_jobs=true`.
The boundary includes refresh, construction, persistence, scheduling and TCP
delivery, including the single test task's client decoding. It excludes login
and automatic tip polling delay. All clients share a 120 s delivery deadline;
each topology has the original 240 s body budget. These are completion bounds,
not substitutes for the **one-second performance target**.

## Three paired observations

Each pair runs one frontend with 2,000 sessions, then two with 1,000 each,
using fresh schemas and closed listeners/pools between topologies.

| Pair | Frontends | Lower median (s) | Maximum (s) | Refresh returns (s) | Within 1 s |
| --- | ---: | ---: | ---: | --- | --- |
| 1 | 1 | 0.639068 | 1.198136 | 0.009681 | No |
| 1 | 2 | 0.906132 | 1.374454 | 0.014736 / 0.012828 | No |
| 2 | 1 | 0.695557 | 1.160832 | 0.010847 | No |
| 2 | 2 | 0.874736 | 1.481149 | 0.007875 / 0.009870 | No |
| 3 | 1 | 0.765952 | 1.301847 | 0.009791 | No |
| 3 | 2 | 0.645584 | 0.970648 | 0.013121 / 0.014339 | Yes |

Every row received and durably verified exactly 2,000 jobs, reported
`complete=true`, zero refresh/delivery errors, zero failed delivery attempts
and zero unmatched issued rows. Success counters were 2,000 for one frontend
or 1,000 each for two. Durable validation also checked prepared identity,
payout revision, expiry and unchanged prepared bytes. The three invocations
exited zero and each recorded its required integration gate.

Two/one maximum-delivery ratios were **1.147, 1.276 and 0.746**. Fixed ordering,
three repetitions, mixed direction, background load and no predeclared
non-regression tolerance prevent a speedup/non-regression conclusion.
The older debug results used a different revision and measurement window;
this is not a controlled debug-versus-release speed comparison.

## Where the observed delay accumulates

These are successful-call histogram deltas over refresh plus fanout, summed
across frontends. Counts are 10,013 pool checkouts and 2,007 settlement-lock
acquisitions for one frontend, or 10,026 and 2,014 for two.

| Pair | Frontends | Pool wait sum (s) | Pool mean (ms) | Settlement wait sum (s) | Settlement mean (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 333.524 | 33.309 | 2.299 | 1.145 |
| 1 | 2 | 720.512 | 71.864 | 7.503 | 3.726 |
| 2 | 1 | 331.274 | 33.084 | 2.156 | 1.074 |
| 2 | 2 | 746.210 | 74.428 | 8.246 | 4.094 |
| 3 | 1 | 371.612 | 37.113 | 2.480 | 1.236 |
| 3 | 2 | 497.757 | 49.647 | 5.049 | 2.507 |

Refresh returned within 15 ms, while delivery continued for roughly 1 s or more.
The measured queueing is on the database checkout/settlement path during fanout;
doubling frontend resources did not consistently reduce delivery time. **These
overlapping sums are not additive wall time, per-job latency, lock hold time or
commit duration.** Build, query execution, commit/WAL, scheduling and socket
costs remain unsplit. In particular, the data does not establish that removing
the settlement lock would meet the target.

The next bounded diagnostic is per-job build/persist timing in the test harness,
paired with a separate PG transaction/commit-wait observation. That should
separate admission/pool queueing from time executing and committing each issued
row before choosing a runtime change. No production lock or batching change
is proposed for landing by this report.

## Regression evidence and remaining limits

The same release binary then passed **7 tests, 0 failures, 1 intentionally
ignored scale test**, with **4/4 database gates executed**. The retry injection
recovered all eight clients after six failed attempts and correctly emitted
`complete=false`; eventual delivery did not masquerade as failure-free delivery.
Its durable-row check is not reached on that intentional incomplete result,
so unmatched rows for this retry control remain unknown. Both ordinary smoke
topologies reported zero failed attempts and unmatched rows. The original
revision-fence regression passed. The separate three-second settlement-lock
stub observed a waiter and 3.004581 s persistence; it is not actual landing work
and does not establish lock-free persistence. Commit timing remains unknown.

Cleanup found zero measurement schemas and stopped the disposable primary.
No production or testnet service was used. Full payout windows, transaction-heavy
templates, miner submissions, real-node latency, remote networks, separate
frontend hosts/processes and automatic polling delay remain unmeasured.
The prior independent 500k-window qualification is separate evidence, not a
qualification of this 2,000-session workload.

## Reproduction and publication revision

Use a clean checkout of the actual tested SHA above, coordinate heavy work,
and set `PRISM_TEST_DATABASE_URL` to a disposable durable PG16 primary.
Build before timing. Run this command three times with a fresh gate-manifest
path each time, recording effective configuration and machine conditions:

```sh
ulimit -n 16384
PRISM_TEST_REQUIRE_INTEGRATION=1 \
PRISM_TEST_GATE_MANIFEST="$manifest" \
cargo test --release --locked -p qbit-prism-server \
  --test b275_persistence_measure \
  measure_2000_sessions_one_and_two_frontends \
  -- --ignored --exact --test-threads=2 --nocapture
```

The observed runner invoked the freshly compiled release test binary directly
with those test arguments; compilation was outside the measured window.
For the seven regressions, omit the test name, `--ignored` and `--exact`.

Publication re-fetched `3.x.x` at `a828c21cb9cb8c854d5148ceb6b1678debf90d22`
(#422) and advanced this documentation branch. That delta changes a separate
recovery fixture, its regression and the gate inventory; it changes none of
the measured runtime or harness/support files. The six observations remain
attributed solely to **`d5d14ce`**, not to the later documentation commit.
