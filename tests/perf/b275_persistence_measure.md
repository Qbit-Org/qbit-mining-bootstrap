# Job persistence measurements (#275)

**The one-second target was missed in all six 2,000-session cases.**
Two-frontend non-regression and production-shaped qualification are not established.
Issue #275 remains open; this PR adds measurement and regressions, not runtime
optimization.

## Tested revision and environment

- Measured harness: `3d4775cacc8c6f8376df0ce028d1083bda738746`.
  Runtime baseline: frozen PR #397, `1b0d409344c1b99f5f4bd04970890b9b034a9eeb`.
- Three paired runs on 2026-09-15, 22:03:44–22:04:16 UTC, using the existing
  compiled Cargo test binary: unoptimized with debug information, Rust 1.98.1.
- Apple M4 Max (Mac16,6), 14 logical CPUs, 36 GiB, macOS 26.5.1.
- Dedicated disposable PostgreSQL 16.14 primary: `fsync=on`,
  `full_page_writes=on`, `synchronous_commit=on`, checked before and during
  each case. No production or testnet endpoint was used.
- Two Tokio workers for all frontends and clients in one process. Each frontend
  had two build workers, four database connections and its own 128-permit
  job-build admission semaphore. Build and persistence each had a 30 s timeout;
  writes had a 20 s timeout. Thus two frontends had 256 aggregate admission
  permits and eight database connections. Vardiff was disabled.
- Process descriptor limit: 16,384; kernel per-process ceiling: 138,240.
  Roughly 4,000 TCP descriptors plus headroom are needed.
- Runtime/refresh authors coordinated the window; a separate compiler was
  allowed to finish. No compiler or test process was found before any repetition.
  Pre-run CPU idle was 74.15%, 75.66%, 77.68%. Background apps remained; memory
  pressure reported 39–46% free with about 17.4 GiB of existing swap use.
  This was a shared development Mac, not an isolated benchmark machine.
- Sixteen deterministic accepted shares, five payout recipients, empty transaction
  template and a synthetic local node. Coordinators, public TCP Stratum,
  prepared records and durable issued-job persistence were the real runtime.

## Boundary and results

Every client subscribed, authorized and received initial work before timing.
The common monotonic clock started immediately before concurrent
`refresh_once` polling after changing the synthetic parent, and stopped for
each client when it decoded a fresh new-parent `mining.notify` with
`clean_jobs=true`. The bracket includes refresh, construction, persistence,
scheduling and TCP delivery; it excludes login and automatic polling delay.
All clients shared one 120 s delivery deadline; each topology had a 240 s
case budget. Client decoding ran through one test task and is included.

Each pair ran 2,000 sessions on one frontend first, then 1,000 on each of two
frontends, with fresh schemas and full cleanup between topologies.

| Pair | Frontends | Lower median (s) | Maximum (s) | Refresh returns (s) | Received / durable |
| --- | ---: | ---: | ---: | --- | --- |
| 1 | 1 | 0.829421 | 1.499653 | 0.020061 | 2,000 / 2,000 |
| 1 | 2 | 1.187969 | 1.710496 | 0.039934 / 0.047239 | 2,000 / 2,000 |
| 2 | 1 | 0.815554 | 1.428409 | 0.020274 | 2,000 / 2,000 |
| 2 | 2 | 0.881717 | 1.358960 | 0.023731 / 0.024136 | 2,000 / 2,000 |
| 3 | 1 | 1.029605 | 1.648645 | 0.023670 | 2,000 / 2,000 |
| 3 | 2 | 0.942327 | 1.412927 | 0.025471 / 0.029112 | 2,000 / 2,000 |

All three invocations exited zero, emitted both required-scale measurements and
durability confirmations, and recorded the explicit integration gate. There were
no incomplete cases or test failures. Cleanup left no measurement schemas and the
disposable primary was stopped. Test-process peak RSS per pair was 104,742,912,
104,579,072 and 103,120,896 bytes, excluding PostgreSQL.

Two/one ratios of maximum delivery were 1.141, 0.951 and 0.857; median maxima were
1.500 s and 1.413 s. Mixed observations, fixed ordering, three pairs and no
predeclared tolerance do not establish a speedup or non-regression. No target
or deadline was relaxed.

## Limits

This is a **debug-profile, 16-share synthetic-node fanout baseline**.
Full payout windows, transaction-heavy templates, miner submissions, real-node
latency, remote networks, separate frontend hosts/processes and automatic tip
polling delay remain unmeasured.

Frozen v1 recorded eventual delivery but did not record internal failed/retried
attempts or committed rows never delivered. Both remain **unknown** for these
six observations; later harness fixes do not retroactively establish zero.
Commit duration is also unknown (`commit_seconds=null`). Pool and advisory-lock
histogram sums overlap across concurrent work and are not additive wall time
or a way to infer commit cost. A separate three-second lock stub measured
3.003617 s persistence on the original smoke run; it does not establish
lock-free persistence or simulate actual landing work.

## Reproduction

Use a clean checkout of the measured harness SHA. Set `PRISM_TEST_DATABASE_URL` to a dedicated
disposable PG16 primary with the three durability settings above; coordinate
other builds/tests and inspect resource limits first. For each of three
repetitions, set `manifest` to a fresh output file, then run:

```sh
ulimit -n 16384
PRISM_TEST_REQUIRE_INTEGRATION=1 \
PRISM_TEST_GATE_MANIFEST="$manifest" \
cargo test --locked -p qbit-prism-server --test b275_persistence_measure \
  measure_2000_sessions_one_and_two_frontends \
  -- --ignored --exact --test-threads=2 --nocapture
```

The original runner directly invoked the existing test-profile binary
`target/debug/deps/b275_persistence_measure-696abc204732aa70` with the same test
arguments and inherited disposable database URL and descriptor limit. That
hash-suffixed path is evidence of the original invocation, not a portable path.
Preserve stdout JSON, exit status, manifests, tested SHA and host/configuration
for any new measurement. Original evidence is in
[PR #402's commit history](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/402/commits),
at `32f13fe766542155aa4ae7722d6f7ce34ddff4b8`; the current tree contains this
standalone report and reusable tests.

## Integration after the historical run

The branch incorporates PR #397 through
`67e24b0dc26e8982b6d8d0b4925d8b82436701bd`, including explicit `ANALYZE` after
bulk fixture loading for the separate 500k qualification. Its 25 s resume budget
and correctness assertions remain unchanged. The six measurements above still
belong only to `3d4775c`; they were not rerun or reattributed to this integration.
New delivery output uses schema `b275.delivery.v3` and names the old reference
`historical_baseline_sha`, so it cannot be mistaken for the tested runtime.
Record the actual build revision in each new run's manifest. Timing boundaries,
session counts, deadlines and acceptance checks are unchanged.

On 2026-09-16 the integrated source passed 23 reconciliation tests (all 11
database gates executed) and seven harness tests (all four gates executed) on
disposable PG16. The retry control recovered eight clients after two failed
attempts and reported `complete=false`. The release-profile 500k qualification
also passed: refresh 5.251 s, shared resume 4.848 s, four waiters reading exactly
500,000 rows across 123 pages, against the unchanged 25 s resume budget.
These are separate integration checks, not new 2,000-session measurements or
production qualification.

## Revised-harness validation

Four independent source-review lanes identified reporting and test-strength
gaps. Follow-up harness changes report effective configuration and listener
attempt counters, require no failed attempts for a clean result, count unmatched
issued rows, exercise the compact-issued revision fence during its own lock wait,
scope waiter attribution, share the case deadline and unify fixture cleanup.
These are separate from the frozen v1 scale results above.

Before that upstream integration, the revised disposable-PG16 suite passed
**7 tests, 0 failures, 1 intentionally
ignored scale test**, including **4/4 required integration gates**. A transient
issued-row rejection regression recovered all eight clients after eight failed
attempts and correctly emitted `complete=false`. Ordinary small deliveries had
zero failed attempts and unmatched rows. Formatting and target-scoped Clippy
with warnings denied passed.

Final CI exposed an unchanged commit-reconciliation test clock anchored before
proof construction/submission. A controlled 250 ms setup delay reproduced the
same late-confirmation count failure (zero instead of one). Anchoring the same
350 ms hold after observed append entry passed all **12 reconciliation tests**,
with the 200 ms share bound, 1,000 ms candidate bound, late-ACK and candidate
retention assertions unchanged. This is a test-only correction; payout/runtime
behavior is unchanged. Independent delta review also passed all 23 tests matching
`commit_reconcile`, reproduced the failing old-clock control, and verified that
the six historical rows and unknown outcomes survived report pruning.
Current CI and sanitized review dispositions are tracked
on [PR #402](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/402).
