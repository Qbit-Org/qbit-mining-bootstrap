# PRISM window ownership and isolated verification (#332 / #254)

This change repairs the reproduced failure-retention paths and moves production
full-window verification out of the lease-bearing Python interpreter. It does
not change lease terms, alert thresholds, GC settings, or deployment policy.
It does not close either incident without the operational evidence and soak
required by #332 and #254.

## Ownership and error boundaries

Fanout deliveries and reconciliation prefetches publish a detached failure
value. The value retains exception type, arguments, attributes, notes and
detached cause/context diagnostics. Each consumer raises a private copy.
The worker releases locals only from completed frames belonging to that
invocation, before publishing the result; running frames, foreign traceback
frames, and traceback links are not changed. A blocked fanout stores another
detached value for its eventual retry exception.

The prefetch join waits with `Future.exception(timeout=...)`. Only that wait
can raise a join-expiry timeout. Worker failures are raised afterwards, so a
worker-raised `TimeoutError` cannot race a `done()` check and be mislabeled.
Cancellation and the single outstanding prefetch slot keep their existing
semantics. A true join expiry leaves the work available for a later retry.

## Independent oracle and snapshot contract

The production `PsqlShareLedger` uses the existing full-window SELECT, read
semaphore, MVCC snapshot, deadline and retry contract. A row sink writes its
bounded decode batches to an anonymous spool instead of accumulating records.
Retry resets the spool before reading the replacement snapshot. No partial
read is handed to a helper. The native client's raw PostgreSQL result buffer
still exists; this change removes the accumulated Python record/dictionary
graphs, not libpq's underlying result buffer.
The oracle also arms a nested ledger operation deadline from its remaining
120-second budget, so a stalled SQL read cannot outlive the oracle budget
before child supervision begins. An earlier caller deadline remains earlier.

The helper starts via exec of `python -I`, with no forked Python state and no
inherited database, signing or SSH environment. Only anonymous standard-stream
descriptors are passed. It has no ledger handle, writer lease, signing seed or
external-effect callback. It independently runs the Python
`IncrementalShareWindow.from_full_snapshot` oracle, not the Rust computation
being checked. Oversize recentering compares at the cached weight and adopts
at the live weight from the same read.

The output is a bounded metadata line followed by canonical share items.
Metadata binds the anchor, comparison/adopted weights, append epoch, count and
digests. The parent validates framing, metadata identity, canonical digest and
record count. Its structural walker retains one record at a time; it does not
repeat the sort, payout-window fold or page construction. Matching self-checks
reuse the existing canonical buffer. Daemon preparation streams these bytes
without parsing the full array. The installed share view always belongs to
the adopted mirror.

Cold starts, daemon-loss/busy/out-of-range full rescans and synchronous ready
fallbacks all use this production spool/helper contract. Ready-build artifact
seeding preserves the canonical view. A daemon decline keeps the independently
verified mirror and its reason; a subsequent failed advance performs another
bounded isolated rescan. Disabling the Rust pipeline can therefore increase
full-rescan frequency: it does not restore an in-process production fold.
Legacy custom/embedded ledgers without the spool API retain their existing
compatibility full-read behavior; they are not the shipped PostgreSQL path.

The existing inflight scan-anchor exposure covers the read and verification.
Publication/build fences still reject obsolete generation, append epoch,
accepted-parent and payout-state identities. Full cache installation rechecks
generation/epoch after helper and daemon work. Shutdown/cancellation kills and
reaps a running helper; all anonymous descriptors close on every exit.

Helper failure never switches to an in-process production oracle. A full build
fails and follows the existing retry path. A failed periodic check retains the
already validated delta and records/spaces the failed attempt under the existing
self-check policy. A concurrent invalidation is propagated, not reported as a
successful check or installed as a current window.

## Declared resource bounds

| Resource | Bound |
| --- | ---: |
| Concurrent helpers, including input preparation | 1 per coordinator process |
| Input spool / canonical result payload | 512 MiB each |
| Input records / encoded input record | 1,000,000 / 1 MiB |
| Result metadata | 64 KiB |
| Helper address space | 4 GiB (`RLIMIT_AS`) |
| Helper CPU / complete operation wall budget | 120 seconds each |
| Admission / child supervision poll | 50 ms |
| Weakly tracked mirror, sequence and page owners | 16,384 |

Linux enforces the helper address-space limit. The repository's existing
`helper_limits` policy logs a macOS refusal and continues uncapped there;
macOS results cannot qualify Linux limit enforcement. Each child output file
is capped at 512 MiB + 64 KiB, including stderr. Input, output and diagnostic
spools together therefore have a conservative approximately 1.5 GiB ceiling
for the one admitted operation. Parent native-driver buffers, legitimate job
history, the psql backend's pre-decode raw result spool and other coordinator
components are separate from that helper reservation.

The `qbit_prism_window_ownership_*` gauges track live canonical buffers/bytes,
page records and parsed mirror records by weak ownership. Mirror/sequence/page
aliases of the same byte object count once. Counters update at construction,
lazy parsing and retirement; scrapes take an O(1) snapshot without walking the
heap or job graph. `observations_dropped_total > 0` explicitly invalidates
complete-coverage claims. Plain custom share lists, serialization spools,
libpq allocations and allocator retention are outside this family and retain
their existing component/allocator metrics. Component page metrics also count
pages still reachable through `cached_window.shares_json` beside a mirror.

## Repeatable local validation

```sh
python3 -m unittest tests.test_prism_async_failure_ownership tests.test_prism_window_oracle
GIT_CONFIG_GLOBAL=/dev/null python3 -m unittest discover -s tests -p 'test_*.py'
PRISM_TOOL_BIN_DIR="$PWD/target/debug" QBIT_WINDOW_PIPELINE_PARITY_ADAPTER=rust-daemon \
  python3 -m tests.window_pipeline_parity_gate
python3 tests/perf/window_oracle_retirement.py --records 228397 400000 --cycles 6
```

The replay declares at most three distinct buffers during replacement and two
after each cycle (at most 1.5 GiB and 1 GiB canonical bytes respectively under
the payload cap). After all legitimate owners retire, buffer/page/parsed
counts must return to baseline without forced GC. Its 50 ms monitor reports
wakes at the unchanged 400 ms warning boundary. GC stays enabled. This is an
allocation/retirement replay, not a simulation of production SQL, Stratum,
host pressure or an enforced proof of the lease monitor's exit timing.

## Local results, September 14, 2026

Runtime: Python 3.14.6, macOS 26.6.2 arm64. Disposable PostgreSQL 16.15
(`postgres:16-alpine`, image digest
`sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685`),
with psycopg 3.2.13 in a workspace-only virtual environment. These are local
test versions, not a fresh observation of Union's running versions.

| Replay | Payload bytes at last cycle | Cycles | Peak monitor lateness | Wakes ≥400 ms |
| --- | ---: | ---: | ---: | ---: |
| 228,397 records | 143,322,235 | 6 | 11.307 ms | 0 |
| 400,000 records | 251,088,919 | 6 | 11.307 ms | 0 |

Both runs returned weak ownership to baseline after drain, without forced GC.
There were no parent generation-2 collections during either measured run.
After warmup, two retained historical buffers accounted for approximately
286.6 MB and 502.2 MB respectively; changes between cycles were only the
fixture's changing encoded sequence numbers. Process RSS high-water readings
were 465,223,680 and 1,218,625,536 bytes; these include allocator retention and
the second reading includes the earlier run. They are corroboration, not the
ownership assertion. Total replay wall times were 14.02 and 25.16 seconds.

Validation passed: the full Python suite (3,697 tests, 44 environment-gated
skips), 309 final targeted ledger/helper/ownership tests, 302 tests with the native
driver installed, both real PostgreSQL integration suites (including native
recovery and statement-spool checks), and the real Rust daemon's frozen parity
gate (7 tests). The new live integration assertions compare the isolated
oracle with both PostgreSQL backends at exact crossing-row weights. The native
integration used the cached PostgreSQL image as a task-local `psql` client
because this Mac has no host `psql`; no global package was installed.

Before production qualification, freshly record running source/image hashes
and Python/PostgreSQL/qbitd versions; obtain the scoped September 13–14 exit
logs; establish the actual dominant owners and a workload-specific bound for
permitted live job history. After separately approved deployment, run the
issue's minimum two-hour soak with multiple self-checks, daemon recovery,
ready fallback, client churn, both Stratum lanes, accepted-share progress and
unchanged restart/warning/breach counters. A recurrence or unexplained ownership
growth keeps the incident open. The in-process watchdog can still be delayed
by other Python/runtime work; this change does not make its scheduling a
mathematical guarantee.
