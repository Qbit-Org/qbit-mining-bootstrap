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
