# Optional PRISM Capacity Qualification

This document defines an optional operator load-test record for a future public
promotion decision. A capacity artifact is not required for PRISM startup,
restart, mainnet readiness, or CI. Compose, `make doctor`, `prism-self-check`,
and the coordinator do not consume an artifact or any `PRISM_CAPACITY_*`
environment variables.

The bootstrap repository ships the strict
`qbit-prism-capacity-evidence/v2` validator, but it does not ship a production
qualification runner. An operator may use the format after capturing the
complete miner-facing path: valid Stratum submission, share validation, ACK,
and durable Postgres commit. Process health and a schema-only database benchmark
are not capacity evidence. Paid rented hash is not required; owned miners or a
controlled load generator may be used if they exercise the real path.

## Qualification Policy

Choose the forecast peak, ACK limit, maximum evidence age, coordinator revision
and image digest, exact Postgres version, and database-profile digest outside the
artifact. Pass them to the standalone validator so the artifact cannot choose
its own passing threshold.

The database-profile SHA-256 is the digest of the reviewed database profile used
for the run. Keep the source profile with qualification records. It should
identify the storage class, CPU and memory allocation, Postgres configuration,
connection path, replica policy, and resource limits that can change commit
latency. A changed profile digest requires a new run.

The artifact separately records and requires these live Postgres settings:

- `fsync=on`
- `full_page_writes=on`
- `synchronous_commit=on`

Do not improve benchmark numbers by weakening durability.

## Bound PRISM Configuration

Qualification schema `qbit-prism-capacity-evidence/v2` binds:

- share difficulty and vardiff enablement
- vardiff minimum, start, maximum, target, retarget, step, EWMA, tolerance, and
  idle-sweep values
- share commit batch size, linger, and timeout
- Stratum send timeout

The validator enforces `minimum <= start <= maximum`. Changing any bound value
requires a new qualification run. The exact `1e-9` local-lab difficulty is
rejected.

## Load-Run Contract

A qualification run must satisfy all of the following:

1. Use a non-zero run UUID and finish within the configured evidence age. A
   timestamp more than five minutes in the future is rejected.
2. Exercise steady-state, miner-reconnect, and slow-database phases. Every phase
   lasts at least 60 seconds, and phase durations reconcile with total duration.
3. Sustain at least twice the externally reviewed forecast peak in aggregate and
   independently during every phase.
4. Keep aggregate and per-phase ACK p99 within the externally reviewed limit.
5. Acknowledge every offered valid share and reject none of them.
6. Reconcile ACK identifiers against unique Postgres ledger identifiers with no
   missing or unexpected rows. Counts and canonical identifier-set SHA-256
   digests must agree in aggregate and for every phase.
7. Record at least ten reconnect events and at least 10 milliseconds of injected
   database delay, so the fault phases cannot be satisfied by token events.

Use a run identifier in every load-generator correlation ID and in the ledger
query predicate. Canonically sort the unique correlation IDs before hashing so
the ACK and Postgres digests are comparable. Query Postgres only after the
writer has drained. Background pool traffic must not enter either set.

The evidence file is an operator-controlled attestation, not a trust boundary.
Generate it from the load runner and reconciliation query rather than editing
measurements by hand. Preserve the raw load-run output, database query output,
and database-profile source beside the release record.

## Example Artifact

`tests/fixtures/prism-capacity-evidence.json` documents the complete JSON shape.
It is marked `artifact_kind=example`, contains synthetic values, and is rejected
by normal standalone validation. The CLI test-only override exists solely so automated
tests can validate the example's structure:

```bash
python3 scripts/prism_capacity_evidence.py \
  tests/fixtures/prism-capacity-evidence.json \
  --allow-example-evidence-for-tests
```

Never use that override in a deployment command or runtime environment.

## Validate Qualification Evidence

Pass the independently configured policy and subject together with every bound
runtime value. The shell names below are local validator inputs, not coordinator
environment variables:

```bash
python3 scripts/prism_capacity_evidence.py /path/to/capacity-evidence.json \
  --forecast-peak-shares-per-second "$CAPACITY_FORECAST_SHARES_PER_SECOND" \
  --ack-p99-limit-milliseconds "$CAPACITY_ACK_P99_LIMIT_MILLISECONDS" \
  --max-age-seconds "$CAPACITY_EVIDENCE_MAX_AGE_SECONDS" \
  --expect-coordinator-revision "$COORDINATOR_REVISION" \
  --expect-coordinator-image-digest "$COORDINATOR_IMAGE_DIGEST" \
  --expect-postgres-server-version "$POSTGRES_SERVER_VERSION" \
  --expect-database-profile-sha256 "$DATABASE_PROFILE_SHA256" \
  --expect PRISM_STRATUM_SHARE_DIFF="$PRISM_STRATUM_SHARE_DIFF" \
  --expect PRISM_STRATUM_VARDIFF="$PRISM_STRATUM_VARDIFF" \
  --expect PRISM_STRATUM_VARDIFF_TARGET_SECONDS="$PRISM_STRATUM_VARDIFF_TARGET_SECONDS" \
  --expect PRISM_STRATUM_VARDIFF_MIN_DIFF="$PRISM_STRATUM_VARDIFF_MIN_DIFF" \
  --expect PRISM_STRATUM_VARDIFF_START_DIFF="$PRISM_STRATUM_VARDIFF_START_DIFF" \
  --expect PRISM_STRATUM_VARDIFF_MAX_DIFF="$PRISM_STRATUM_VARDIFF_MAX_DIFF" \
  --expect PRISM_STRATUM_VARDIFF_RETARGET_SECONDS="$PRISM_STRATUM_VARDIFF_RETARGET_SECONDS" \
  --expect PRISM_STRATUM_VARDIFF_MAX_STEP_UP="$PRISM_STRATUM_VARDIFF_MAX_STEP_UP" \
  --expect PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN="$PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN" \
  --expect PRISM_STRATUM_VARDIFF_EWMA_ALPHA="$PRISM_STRATUM_VARDIFF_EWMA_ALPHA" \
  --expect PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE="$PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE" \
  --expect PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS="$PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS" \
  --expect PRISM_SHARE_COMMIT_BATCH_SIZE="$PRISM_SHARE_COMMIT_BATCH_SIZE" \
  --expect PRISM_SHARE_COMMIT_LINGER_MILLISECONDS="$PRISM_SHARE_COMMIT_LINGER_MILLISECONDS" \
  --expect PRISM_SHARE_COMMIT_TIMEOUT_SECONDS="$PRISM_SHARE_COMMIT_TIMEOUT_SECONDS" \
  --expect PRISM_STRATUM_SEND_TIMEOUT_SECONDS="$PRISM_STRATUM_SEND_TIMEOUT_SECONDS"
```

No startup or deployment path runs this validator automatically. If an operator
later adopts capacity qualification as public-routing policy, deployment
orchestration should call it explicitly before opening public routes and archive
the artifact and raw results. Private canaries and routine restarts do not
require it. Re-run only when the qualified image, bound load configuration,
database/hardware profile, forecast, or ACK objective changes.

## Heap and Component-Cardinality Telemetry

Issue #226 recorded a coordinator whose resident set grew by roughly 390 MB per
hour over 45 hours of uptime with no restart, no OOM kill, and a memory map made
of hundreds of 4-64 MiB anonymous regions -- the shape of glibc per-thread
malloc arenas across the process's 64 threads. Nothing on `/metrics` could say
whether that growth was live Python objects, pymalloc fragmentation, or glibc
arena fragmentation. The families below are the always-on instrument for that
question. They are cheap (every reading is a counter load, a `len()`, or one
`mallinfo2` call; never a heap walk, never `gc.get_objects()`, never
`tracemalloc`) and fixed-cardinality (no label carries a job id, tip hash,
generation, worker name, or any other value that grows with traffic). They are
not the census, the allocator experiment, or the fix; those are #226's second
part.

### Interpreter and allocator families

Rendered by `lab/prism/metrics.py` from `lab/prism/process_telemetry.py`, in
this order, after the progress-health block:

| Family | Type | Meaning |
| --- | --- | --- |
| `qbit_prism_process_allocated_blocks` | gauge | `sys.getallocatedblocks()`: memory blocks the CPython allocator currently holds. A live-object proxy: it rises with retention and stays flat under pure allocator fragmentation. |
| `qbit_prism_process_gc_trigger_count{generation}` | gauge | `gc.get_count()`: the collector's per-generation trigger counters, compared against `gc.get_threshold()` to decide when to collect. Generation 0 is allocations minus deallocations. **Not** a count of retained objects. CPython 3.13 made the collector incremental, so the third entry is unused and reads `0`. |
| `qbit_prism_process_gc_collections_total{generation}` | counter | `gc.get_stats()`: collector passes per generation since start. |
| `qbit_prism_process_gc_collected_objects_total{generation}` | counter | Objects the collector freed per generation since start. |
| `qbit_prism_process_gc_uncollectable_objects_total{generation}` | counter | Objects found unreachable but not freeable. Growth here is a leak the collector already knows about. |
| `qbit_prism_process_threads` | gauge | `threading.active_count()`: each live thread is a candidate glibc arena. |
| `qbit_prism_process_malloc_info_available` | gauge | 1 when glibc `mallinfo2` is bound and `PRISM_MALLOC_TELEMETRY` is on; 0 otherwise. |
| `qbit_prism_process_malloc_arena_bytes` | gauge | `mallinfo2.arena`: heap space glibc obtained from the kernel for its arenas, main heap plus every per-thread arena heap, excluding mmapped chunks. |
| `qbit_prism_process_malloc_in_use_bytes` | gauge | `mallinfo2.uordblks`: bytes in allocated chunks across every arena. |
| `qbit_prism_process_malloc_free_bytes` | gauge | `mallinfo2.fordblks`: bytes in free chunks glibc retains rather than returning. |
| `qbit_prism_process_malloc_mmapped_bytes` | gauge | `mallinfo2.hblkhd`: bytes glibc served directly through `mmap`. |

The `generation` label is the closed set `0`, `1`, `2`. An interpreter that
reports fewer generations renders `-1` for the missing one rather than
shrinking the series set.

**`mallinfo2` is glibc-only.** The symbol exists in glibc 2.33 and later. It is
absent on musl, on macOS, and on older glibc; the older int-typed `mallinfo` is
deliberately not used as a fallback because its 32-bit fields wrap on a
multi-gigabyte heap. On a platform without it the collector binds once, remembers
the absence, never raises and never logs on a scrape, and renders
`qbit_prism_process_malloc_info_available 0` with all four byte gauges at `-1`.
It never renders zero bytes: a zero arena on a 17 GB process is a lie an operator
would act on. The family set is identical on every platform, so a dashboard built
against one reads the same series on another; only the availability gauge and
the sentinel values differ.

`PRISM_MALLOC_TELEMETRY` (default `1`, strict boolean, validated at startup by the
coordinator config loader) switches the `mallinfo2` call off. The call walks
glibc's free lists under each arena lock; that is negligible on a healthy heap,
but it is the one reading here that is not a plain counter load, so an operator
can stop it without a redeploy. Off renders exactly like an absent symbol. The
interpreter families have no switch: they are pure counter loads and are always
on.

The switch is passed through in `compose.yaml` and documented in `.env.example`,
so a restart with a changed `.env` is enough on the compose stack. The mainnet
deployment sets the coordinator's environment from its own configuration rather
than from this file, so turning the switch off there needs the same variable
added on that side. The renderer resolves the switch once, at construction, and
falls back to the shipped default if the value has become invalid since startup
-- a telemetry knob must not be able to take the coordinator down from a metrics
thread.

### Component-cardinality families

Two families with a closed `component` label set, rendered after the heap
block:

| Family | Type | Meaning |
| --- | --- | --- |
| `qbit_prism_component_entries{component}` | gauge | Entries retained by each in-process component: one `len()` or `Queue.qsize()` over the owning structure. |
| `qbit_prism_component_bytes{component}` | gauge | Bytes retained by each byte-sized payload: the cached payout window's canonical JSON, the daemon window mirror, and the share-window serialization compact strings. Every value is resident bytes except `share_window_serialization_spool`, which is an on-disk temporary file and is *not* part of RSS. |

The `component` values are fixed by `PRISM_COMPONENT_ENTRY_KINDS` and
`PRISM_COMPONENT_BYTE_KINDS` in `lab/prism/metrics.py` and pinned by the family
test. They cover the payout window (pages, records, the armed artifact's shares,
the preview and invalidation maps, the append-invalidation stamps, the in-flight
scan anchors), the single-slot share-window serialization cache, job contexts
and the bundle cache, the evicted-job graveyard and its same-tip index, the
candidate lanes and every hash-keyed candidate registry (outstanding,
tip-observed, counted abandonments, preview stamps, ancestor re-drives, terminal
outcomes, disposition flights, waiting retries, dequeued hashes, accounted
blocks), reconcile flights and the trusted-tip memo, the pending-share commit
floor, and the daemon mirror (records in a mirror-backed window and the
compiler's mirror of the daemon's uploaded-window LRU).

The cached payout window is accounted exactly once, so the byte family can be
summed for attribution. Its two backings are mutually exclusive: on the Python
path the window reports under `payout_window_pages`, `payout_window_records`,
and `payout_window_canonical_json` with the daemon-mirror components at zero;
on the Rust path (`PRISM_WINDOW_PIPELINE_RUST=1`, which is what mainnet runs)
the mirror *is* the cached window, so it reports under
`daemon_window_mirror_records` and `daemon_window_mirror_canonical_items` with
all three `payout_window_*` components at zero. Which pair is non-zero is how
you read the active backing off `/metrics`. `daemon_uploaded_windows` is a
separate structure -- the compiler's LRU of windows already uploaded to the
daemon -- and is never part of that either/or.

There is no hashrate-rollup gauge because there is no in-process rollup buffer:
the rollup maintenance loop folds shares in PostgreSQL through
`advance_hashrate_rollups` and retains nothing between passes.

### What to look at first when RSS climbs

1. **Is it retention or fragmentation?** Plot
   `qbit_prism_process_allocated_blocks` and
   `qbit_prism_process_malloc_in_use_bytes` against
   `qbit_prism_process_resident_memory_bytes`. All three rising together is
   retention: something is holding objects. RSS rising while allocated blocks
   and in-use bytes stay flat is fragmentation *or* an mmapped retention -- see
   step 2 before deciding.
2. **If fragmentation, whose?** `qbit_prism_process_malloc_arena_bytes minus
   qbit_prism_process_malloc_in_use_bytes` (equivalently the free-bytes gauge)
   growing with `qbit_prism_process_threads` high is glibc arena fragmentation,
   the shape #226 captured. Arena flat while RSS rises points at pymalloc or at
   the Rust daemon child, which this process's gauges do not see.

   **Check `qbit_prism_process_malloc_mmapped_bytes` before concluding
   fragmentation.** glibc serves allocations above its mmap threshold with
   `mmap` and records them in `hblkhd`, which this gauge exports — they are
   *not* in `uordblks`/`..._in_use_bytes`. So a large retained native buffer
   makes both step 1's in-use gauge and this step's arena figure look flat while
   RSS climbs, which reads exactly like fragmentation and is in fact retention.
   Rising mmapped bytes means step 1's answer was retention after all.
3. **If retention, which component?** Read `qbit_prism_component_entries` and
   `qbit_prism_component_bytes` on one panel. The component whose slope matches
   RSS is the one to census. A registry keyed by block hash that only ever grows
   (terminal outcomes, preview stamps, accounted blocks) is the first suspect
   after a candidate storm; the payout window and serialization bytes are the
   first suspects after a run of full rescans.
4. **Is the collector keeping up?** A rising
   `qbit_prism_process_gc_uncollectable_objects_total` is a reference cycle with
   a finalizer the collector cannot break — that is retention the collector
   cannot fix on its own, and it is the one GC series that counts objects.
   `qbit_prism_process_gc_collections_total` flattening while allocation
   continues means collections have stopped happening at all. Do **not** read
   `qbit_prism_process_gc_trigger_count` as a backlog of retained objects: it is
   the counter CPython compares against its thresholds, it resets on every
   collection, and its third entry is unused on the shipped interpreter.

Every gauge here is a scrape-time snapshot of the same process; read the
`X-Prism-Metrics-State` header before trusting a stale document.

## Heap Census, Allocator Control, and the Resident-Set Bound

Issue #226's second part. The always-on families above say *whether* the
resident set is growing through retention or through fragmentation; this
section is the instrument that says *what* is retained and *which allocator
setting* bounds the fragmentation, and the automated check that a fix has to
pass. Everything here is off by default, operator-triggered, bounded, and
unreachable from any request path: no HTTP route, no Stratum message, and no
`/metrics` scrape can start a heap walk or a trim. `tests/test_prism_heap_census.py`
pins each of those properties.

### Running a census

The census is `lab/prism/process_telemetry.py`'s `HeapCensus`. It is armed only
when the coordinator starts with `PRISM_HEAP_CENSUS=1`; without that the signal
is not registered, no worker thread exists, and the census entry point is inert
even if called. Armed, it responds to `SIGUSR1`:

```sh
# Compose stack
docker kill --signal=SIGUSR1 <prism-coordinator-container>
# Bare process
kill -USR1 <pid>
```

The signal handler writes one byte to a pipe and returns; a dedicated thread
(`prism-heap-census`) does the walk. That is the shape `SIGUSR2`'s
`faulthandler` dump follows, with the work moved off the handler so a second
signal cannot re-enter a lock the handler holds. Signals that arrive within
`PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS` (default `60`) of the last census are
logged as suppressed, so a repeated `kill` cannot stack walks.

What the walk costs, and why it is never automatic: it is one
`gc.get_objects()` call followed by one pass over every tracked object taking
its type and `sys.getsizeof()`. The `gc.get_objects()` call allocates one
pointer per tracked object and holds the GIL until it returns; nothing can
interrupt it, and on a heap of tens of millions of objects it is a stall of
seconds on every thread. The pass that follows is what the walk cap bounds.
Take a census in a low-load window, and expect a share-ack latency blip in
`qbit_prism_share_ack_seconds` when you do.

The switches and bounds, all validated at startup (a value outside its range
refuses startup rather than promising a bound it does not keep). Each passes
through `compose.yaml` to the `prism-coordinator` service and is listed in
`.env.example`, so a value in the deploy dotenv reaches the process; compose
has no `env_file:` for the service, and a variable without a passthrough line
would silently never arrive:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PRISM_HEAP_CENSUS` | `0` | Arm `SIGUSR1`. Strict boolean. |
| `PRISM_HEAP_CENSUS_DIR` | `$PRISM_AUDIT_DIR/heap-census` | Where reports are written. Created on first census. |
| `PRISM_HEAP_CENSUS_TOP_N` | `40` (max `200`) | Entries in each of the four lists: types by count, types by shallow bytes, `tracemalloc` sites by line, sites by file. |
| `PRISM_HEAP_CENSUS_MAX_BYTES` | `262144` (`4096` to `4194304`) | Output ceiling per report. Met by halving top-N until the JSON fits, never by cutting the document. |
| `PRISM_HEAP_CENSUS_MAX_SECONDS` | `10` (max `120`) | Walk cap for the per-object pass; the report says when it was hit. |
| `PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS` | `60` | Signals closer together than this coalesce. |
| `PRISM_HEAP_CENSUS_TRACEMALLOC` | `0` | Start `tracemalloc` (one frame) at startup so the census can name allocation sites. Requires `PRISM_HEAP_CENSUS=1`. |

`tracemalloc` is the second, separate opt-in because its cost is standing, not
per census: every allocation pays for the trace for as long as it runs, and the
trace itself is resident memory proportional to live blocks. Use it for a
diagnosis window, not steady state. Without it the census still names the
retained *types*; with it the census also names the *sites*.

Reports land as `heap-census-<UTC stamp>-<pid>.json`, written to a temporary
name and renamed into place. At most 32 are kept per directory (oldest removed
first), so with the default byte ceiling the census can occupy at most 8 MiB
of the audit volume.

### Reading a census

A report is one JSON document:

- `process`: the same readings `/metrics` exports (`resident_memory_bytes`,
  `allocated_blocks`, `threads`, the GC counters, the `mallinfo2` bytes) plus
  what the scrape cannot afford: `malloc_arena_count` and the split of glibc's
  free bytes into `malloc_free_bin_bytes` (interior free chunks) and
  `malloc_free_top_bytes` (the free space at the end of each arena's heaps),
  and `anonymous_map`, the private-anonymous region histogram in the same
  4-64 MiB bands the mainnet capture was reported in.
- `walk`: `tracked_objects`, `objects_walked`, `distinct_types`, `truncated`,
  and the seconds spent in `gc.get_objects()` and in the whole pass.
- `types_by_count` and `types_by_bytes`: top-N `{type, count, shallow_bytes}`.
  Shallow bytes are `sys.getsizeof()`; a `dict` of 60k share records is one
  large dict plus 60k small dicts plus their strings, so read the two lists
  together.
- `tracemalloc`: `{"tracing": false}` or the top-N sites by line and by file
  with bytes and block counts, plus the traced total and peak.

Read it in this order:

1. **Retention or fragmentation?** Compare `process.resident_memory_bytes`
   with `process.malloc_in_use_bytes` and `process.allocated_blocks` against
   an earlier census (or against the `/metrics` history). In-use bytes and
   allocated blocks rising with RSS is retention; RSS rising over a flat
   in-use is fragmentation. On the mainnet capture the map alone said
   fragmentation, but only a census on that process can prove there is no
   retention underneath it.
2. **If retention, of what?** `types_by_count` names the type; the first
   census after startup is the baseline and the type whose count grows across
   censuses is the leak. With tracing on, `tracemalloc.by_line` names the
   allocation site. A site inside the payout-window materialization is an
   issue #228 finding; a registry keyed by block hash is a candidate-lane
   finding.
3. **If fragmentation, can a trim reach it?** `malloc_free_bin_bytes` is what
   `malloc_trim` returns to the kernel, in every arena. `malloc_free_top_bytes`
   across `malloc_arena_count` arenas is what it cannot: glibc trims a
   per-thread heap's top only when the owning thread frees a chunk of 64 KiB
   or more while the top exceeds `M_TRIM_THRESHOLD`, and that threshold
   floats up to 64 MiB as large chunks are freed. Free bytes parked in the
   tops of many arenas are the case `MALLOC_ARENA_MAX` fixes and a trim does
   not; free bytes in bins are the case a trim fixes.
4. **Was the walk complete?** `walk.truncated` true means the type lists are
   over a prefix of the heap. Raise `PRISM_HEAP_CENSUS_MAX_SECONDS` and take
   another census in a quieter window rather than reading a partial census
   as a whole one.

### `malloc_trim`

`MallocTrimmer` calls glibc `malloc_trim(0)` and reports the readings around
it through the same collector `/metrics` uses: RSS, arena, in-use and free
bytes before and after, the deltas, glibc's own released flag, and the call's
duration, on one log line (`prism malloc_trim ...`). A trim that works shows
`in_use_delta=0` (it frees nothing the program holds), a negative
`resident_delta`, and `released=True`.

Two ways to run it, both off by default:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PRISM_MALLOC_TRIM_SIGNAL` | `0` | Arm `SIGRTMIN+1` (`docker kill --signal=RTMIN+1`, `kill -RTMIN+1`). Strict boolean. |
| `PRISM_MALLOC_TRIM_INTERVAL_SECONDS` | `0` (off; minimum `60`) | Periodic trim on the census thread, paced by this interval. |

Both go through the same worker thread and the same
`PRISM_HEAP_CENSUS_MIN_INTERVAL_SECONDS` gate, so signals cannot stack trims
either. The design call is to offer both: the signal is the instrument (does
RSS return when asked?), and the paced periodic action is the candidate fix
for interior fragmentation once the soak shows it returns memory. The floor
exists because of what the call does: `malloc_trim` takes every arena's lock
in turn while it consolidates and walks that arena's free lists, and a thread
allocating in that arena at that moment waits. On a fragmented multi-gigabyte
heap that is a stall of tens to hundreds of milliseconds across whichever of
the 64 threads happen to allocate, not a wedge; it is not something to run
every second on a latency-sensitive process. Run the signal form in a
low-load window first and read the logged duration before choosing an
interval.

What it cannot do: shrink the tops of per-thread heaps (see "Reading a
census"). With many arenas, `malloc_trim` consolidates each arena's free
chunks *into* its top and then cannot return that top, so RSS does not move;
with `MALLOC_ARENA_MAX=2` the same free chunks stay interleaved with other
threads' live chunks in two arenas, and the trim returns them. That is
measured, not inferred: the storm instrument below shows it on any glibc host.

### Allocator settings and the storm instrument

The image sets `MALLOC_ARENA_MAX=2`; `docs/mainnet-deployment.md` records the
decision, the rationale, and how to override it per deployment. The
experiment behind it is `tests/prism_candidate_storm.py` with three additions:

- `--heap-report` reads the resident set, `allocated_blocks`, thread count,
  the `mallinfo2` bytes, the arena count, the free-bytes split (tops versus
  bins), and the anonymous-map histogram at every phase boundary of the
  storm, then after the storm's objects are released and collected, then
  after one `malloc_trim`. The release-and-collect reading is #185's
  post-storm drain measurement, re-runnable on any host.
- `--arena-probe-threads N` (with `--arena-probe-rounds`) runs `N` threads that
  each allocate medium buffers (4-120 KiB: over pymalloc's 512-byte ceiling,
  under glibc's initial 128 KiB `mmap` threshold) and meet at a barrier before
  exiting, which is how glibc gives each of them an arena. It reproduces the
  coordinator's long-lived-thread shape; the storm rig itself is single-
  threaded.
- `--allocator-experiment default,2,4` runs the instrument once per
  `MALLOC_ARENA_MAX` setting in a child process (glibc reads the variable once
  at start) and prints every child's heap section side by side;
  `--allocator-mmap-threshold BYTES` pins `MALLOC_MMAP_THRESHOLD_` in the
  children as an extra axis.

```sh
python3 tests/prism_candidate_storm.py --decide --drain-per-row \
  --arena-probe-threads 64 --allocator-experiment default,2,4 > arena-experiment.json
```

Read the mechanism, not the absolutes: memory absolutes vary by up to an order
of magnitude across hosts, and the instrument prints `python`, `libc`,
`machine` and `cpu_count` beside every number for that reason. What should
transfer: `malloc_arena_count` after the probe reaches the thread count at
the default and stops at the cap otherwise; `anonymous_regions_4mib_to_64mib`
climbs into the mainnet shape at the default; `allocated_blocks` at
`after_release_gc` returns to `start` (no Python retention from the storm);
and `malloc_trim` returns memory at the cap and not at the default. The drain's
`wall_seconds` and the `phase_seconds` are the latency comparison across
settings.

### The resident-set bound

The stated bound: **after a one-hour warm-up, the resident set must stay within
2.0x the warm-up peak for the rest of a 24 h soak at ordinary load.** The
warm-up peak is the baseline because the first hour materializes the payout
window and runs the first full rescan; the #226 slope (390 MB/h from a 125-145
MiB start) breaches this bound in its third hour, and a post-storm excursion
like #185's 613 MiB drains back under it. The check is automated, not a graph
someone reads:

```sh
python3 -m lab.prism.process_telemetry rss-bound --samples soak-rss.csv \
  --warmup-seconds 3600 --multiple 2.0 --min-span-seconds 82800
```

The input is one `seconds,rss_bytes` line per sample (absolute epoch seconds
are fine; `#` comments are skipped; `-1` samples are ignored). The command
prints the verdict as JSON (baseline, bound, peak and its time, the first
breach time, and the least-squares slope over the post-warm-up samples) and
exits `0` on pass, `1` on fail, `2` on unusable input. `--max-slope-bytes-per-hour`
adds a slope guard for a leak slow enough to stay under the multiple inside 24
hours; `--min-span-seconds` refuses a series shorter than the soak.

When it fails: the first breach time says whether the growth is the steady
slope (breach hours in) or an excursion (breach right after a storm or a
rescan burst). Take a census at the breach and read it as above; if the
census names a retained type, that type's owner is the fix; if it names free
bytes parked in arena tops, the arena cap is the fix and the soak is rerun at
the other setting. Do not raise the multiple to make a soak pass.

### Testnet 24 h soak runbook (deferred to the operator)

This soak has not been run. Nothing here was verified against a live host;
the instrument, the thresholds and the procedure are what this package
delivers, and the numbers are what the soak produces.

1. **Build** the release image (which now carries `MALLOC_ARENA_MAX=2`) and
   record its digest.
2. **Run three soaks in sequence**, each a fresh coordinator process for at
   least 24 h at ordinary testnet load with the same miner population, with
   these values in the deploy dotenv (each passes through `compose.yaml` to
   the `prism-coordinator` service; a deployment that sets the coordinator's
   environment another way sets them there):
   - `MALLOC_ARENA_MAX=2` (the image default),
   - `MALLOC_ARENA_MAX=4`,
   - `MALLOC_ARENA_MAX=2` with `PRISM_MALLOC_TRIM_INTERVAL_SECONDS=900`.

   Set `PRISM_HEAP_CENSUS=1` and `PRISM_MALLOC_TRIM_SIGNAL=1` on all three;
   leave `PRISM_HEAP_CENSUS_TRACEMALLOC` off unless a census names growth
   without naming a site. Before trusting a run, confirm the setting reached
   the process and the census is armed:

   ```sh
   c=<prism-coordinator-container>
   docker exec "$c" sh -c 'echo MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX PRISM_HEAP_CENSUS=$PRISM_HEAP_CENSUS'
   docker logs "$c" 2>&1 | grep 'prism heap census armed'
   ```

   Record the exact environment beside each run.
3. **Capture every 5 minutes** for the whole soak. The audit port is bound to
   the container's loopback and compose does not publish it, so both reads
   run inside the container, where the coordinator is PID 1 and the `lab`
   package is on the path (`3341` is the default `PRISM_AUDIT_PORT`;
   substitute the deployment's value):

   ```sh
   c=<prism-coordinator-container>
   while true; do
     docker exec "$c" python3 -m lab.prism.process_telemetry rss-sample --pid 1 >> soak-rss.csv
     docker exec "$c" python3 -c 'import sys, urllib.request; sys.stdout.write(urllib.request.urlopen("http://127.0.0.1:3341/metrics", timeout=5).read().decode())' \
       | grep -E '^qbit_prism_process_(resident_memory_bytes|allocated_blocks|threads|malloc_(arena|in_use|free)_bytes) ' \
       | sed "s/^/$(date +%s) /" >> soak-metrics.log
     sleep 300
   done
   ```

   Also keep the share-ack histogram (`qbit_prism_share_ack_seconds`) at
   hours 1, 12 and 24 for the latency comparison between settings.
4. **Census** at hour 1 (the baseline) and hour 24 with
   `docker kill --signal=SIGUSR1 "$c"`, and once more at any breach. The
   coordinator logs `prism heap census written path=...`; the report lands on
   the audit volume inside the container, so copy it out with
   `docker cp "$c":/var/lib/qbit-prism/audit/heap-census ./heap-census-<run>`
   (substitute `PRISM_AUDIT_DIR` if it was changed). Docker accepts both
   `SIGUSR1` and `RTMIN+1` as `--signal` values and delivers them; both were
   checked against a container on the development host.
5. **Trim** once at hour 23 with `docker kill --signal=RTMIN+1 "$c"` on the
   runs without the periodic trim, and record the logged `prism malloc_trim`
   line (`docker logs "$c" 2>&1 | grep 'prism malloc_trim'`):
   `resident_delta`, `free_delta`, `seconds`.
6. **Judge** each run:

   ```sh
   python3 -m lab.prism.process_telemetry rss-bound --samples soak-rss.csv \
     --warmup-seconds 3600 --multiple 2.0 --min-span-seconds 82800
   ```

   Pass: exit `0` on every run you intend to ship, and share-ack p99 at
   hour 24 within the alert threshold already configured for the deployment,
   with no `prism malloc_trim` duration above 500 ms. Fail: any exit `1`, or a
   latency regression between the arena settings that the operator would
   alert on. Between 2 and 4, ship the one that passes with the larger margin
   under the bound; if both pass and the latency is indistinguishable, keep
   the image default of 2.
7. **Record** on issue #226: the three verdict JSON documents, the census
   reports, the trim lines, the share-ack histograms, the image digest, and
   the glibc version inside the image (`ldd --version`), because the trim
   behaviour above was measured on glibc 2.39 and the image carries a newer
   one.
8. **Re-run #185's drain** on the fixed build:

   ```sh
   python3 tests/prism_candidate_storm.py --decide --drain-per-row --heap-report
   ```

   and record `start` against `after_release_gc` and `after_malloc_trim`,
   with the platform line, on #185.
