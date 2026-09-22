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
lazy parsing and retirement; scrapes take an O(1) snapshot of the scalar
family and one pass over the bounded registry for the labelled family, without
walking the heap or job graph. `observations_dropped_total > 0` explicitly
invalidates complete-coverage claims. Plain custom share lists, serialization
spools, libpq allocations and allocator retention are outside this family and
retain their existing component/allocator metrics. Component page metrics also
count pages still reachable through `cached_window.shares_json` beside a mirror.

## Owners that outlive their mirror (#332, defect 4)

Production at `e76234aa` showed window owners growing linearly (about 5% of
advances) after defects 1–3 were fixed, each sole owner of a distinct
historical buffer, and about 70 of them still holding a parsed window of
dicts. Two changes address what can be bounded without knowing the holder,
and make the holder attributable from metrics alone.

**Parsed representation.** `DaemonShareJsonSequence` no longer owns its
parsed tuple. The tuple lives in a holder the sequence references weakly:
iteration keeps it alive through the generator frame, indexing and slicing
hold it for the call, and `retained()` pins it for one operation that reads
the window repeatedly. Concurrent readers share the live parse; the last
reader releases it by reference count, with no cycle through the release
callback. `parsed_records` therefore reports rows a consumer is holding right
now, never rows cached by a sequence nobody is reading. The routine build
path stays lazy, the durable candidate intent and the one-shot builder input
still splice canonical bytes without parsing, canonical digests are unchanged,
and a count the bytes refute still raises with nothing published.

**Attribution.** Every owner registers with its `kind` (`mirror`, `sequence`,
`page`) and a bounded creation-site label, `module.function` of the first
frame outside the constructor passthroughs (`json_records`, `advanced`,
`from_full_items`, dataclass `__init__`/`replace`). Admission is capped at 64
labels ever seen, not labels concurrently live: a retired site keeps its
admission, so an unseen label folds into `other` once the cap is reached and
the series population Prometheus retains is bounded. The registry keeps
insertion order, so
each kind's oldest owner is its first entry. Exported beside the scalar
family:

| Series | Labels | Meaning |
| --- | --- | --- |
| `qbit_prism_window_ownership_owners_by_site` | `kind`, `site` | Live owners minted at that site |
| `qbit_prism_window_ownership_parsed_records_by_site` | `kind`, `site` | Parsed rows currently held by owners from that site |
| `qbit_prism_window_ownership_oldest_owner_age_seconds` | `kind` | Age of the oldest live owner of that kind |

A retained owner shows up as one `(kind, site)` series that rises with the
advance rate while the others stay flat, and as an `oldest_owner_age_seconds`
value that keeps growing across advances instead of resetting when the window
is replaced. On the production target:

```promql
sum by (kind, site) (qbit_prism_window_ownership_owners_by_site{job="qbit-prism",instance="100.70.93.39:9828"})
qbit_prism_window_ownership_oldest_owner_age_seconds{job="qbit-prism",instance="100.70.93.39:9828"}
sum by (kind, site) (qbit_prism_window_ownership_parsed_records_by_site{job="qbit-prism",instance="100.70.93.39:9828"})
```

The leaking site names the `json_records()` caller (for sequences) or the
mirror constructor's caller; the holder is then one of the containers that
receive that site's artifact, bundle or job context. Nothing here adds an
owning reference or a heap walk to the lease-bearing interpreter.

**Replay.** `tests/perf/window_owner_attribution.py` drives the real
coordinator through byte-changing and anchor-only advances, periodic
self-checks, template changes, shared bundle builds, first-job delivery to
fake clients with connect/disconnect churn and cancelled first-job requests,
and the parse-forcing consumers (durable intent staging, compact audit walk),
with GC enabled and never forced. It arms artifacts through the production
preparation entry. Cycles are paced against the replay's job-retention TTL
because retention is a wall-clock term. The declared bound is the three live
owners plus permitted job history, `(retention + graveyard prune interval) /
cycle pacing`, plus four for the in-flight bundle and armed artifact. The run
fails if owners or distinct buffers exceed the bound, and prints the kind,
site, age and referrer chain of any owner that outlives its mirror by more
than `--grace` cycles. It has no PostgreSQL, Rust daemon, Stratum sockets,
block landings, reorg reconciliation or block accounting, so a holder on
those paths cannot show here.

## Repeatable local validation

```sh
python3 -m unittest tests.test_prism_async_failure_ownership tests.test_prism_window_oracle
GIT_CONFIG_GLOBAL=/dev/null python3 -m unittest discover -s tests -p 'test_*.py'
PRISM_TOOL_BIN_DIR="$PWD/target/debug" QBIT_WINDOW_PIPELINE_PARITY_ADAPTER=rust-daemon \
  python3 -m tests.window_pipeline_parity_gate
python3 tests/perf/window_oracle_retirement.py --records 228397 400000 --cycles 6
python3 -m unittest tests.test_prism_window_owner_retention
python3 tests/perf/window_owner_attribution.py --cycles 1500 --grace 70
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

## Local results, September 21, 2026 (defect 4)

Runtime: Python 3.14.4, Linux 7.0.0 x86_64, no PostgreSQL, Rust daemon or
docker on the host. The 1,500-advance attribution replay (4 clients, 0.2 s
retention, 0.02 s pacing, declared bound 68 owners / 67 buffers) completed
in 45.2 s with GC enabled: peak 37 owners and 36 distinct buffers, both
oscillating with the graveyard prune cadence and returning to the live set
after every prune; `parsed_records` was 0 at every sampled cycle although the
compact audit walk parsed every fifth cycle; no owner outlived its mirror
beyond the retention window. Owners were attributed to
`payout_state._incremental_payout_window_materialization` (sequences),
`payout_state._daemon_adopted_window` (the mirror after a self-check) and the
fake daemon's own `share_ledger.from_records` page. The affected unit modules
(166 tests) passed. The full Python suite (3,752 tests, 54 environment-gated
skips) passed apart from `test_prism_public_dashboard_api`, which also fails
on the unchanged `2.x.x` base in this environment, and the
`malloc_trim` in-use assertion of `test_prism_allocator_experiment`, which
is heap-layout sensitive: the unchanged base reports the same 80-byte delta
in this checkout path while a fresh checkout of this change reports zero.

**Not established here.** The production holder was not reproduced: every
in-process path the replay exercises plateaus. The remaining candidates are
the paths the replay cannot run (real daemon uploads and one-shot fallback,
block landing and accounting, reorg reconciliation, real Stratum sessions)
or a race between them. The labelled series above are the instrument for
that: after separately approved deployment, the per-site owner counts and the
oldest-owner age identify the minting site and the retention lifetime from
metrics alone, and the referrer inspection in the replay applies to a
recording of that path. Acceptance stays as amended in #332: at least 1,500
advances or one production day asserting on owners, distinct buffers and
parsed records, and a soak of at least 24 hours in which owners, distinct
buffers and parsed records plateau and the mean generation-2 pause stops
rising.

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
