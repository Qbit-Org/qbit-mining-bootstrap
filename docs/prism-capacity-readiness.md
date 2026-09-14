# Optional PRISM Capacity Qualification

This document defines an optional operator load-test record for a future public
promotion decision. A capacity artifact is not required for PRISM startup,
restart, mainnet readiness, or CI. Compose, `make doctor`, `prism-self-check`,
and the coordinator do not consume an artifact or any `PRISM_CAPACITY_*`
environment variables.

The bootstrap repository ships the native `qbit-prism-server
capacity-evidence` command for the strict `qbit-prism-capacity-evidence/v3`
format, but it does not ship a production
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

Schema `qbit-prism-capacity-evidence/v3` binds only settings the native server
reads at startup, so the artifact names the values the measured binary ran
with. Record each value from the measured coordinator's environment:

- `PRISM_STRATUM_SHARE_DIFF` and `PRISM_STRATUM_VARDIFF` (`0` or `1`)
- vardiff minimum, start, maximum, target, retarget, step up, step down, EWMA,
  and tolerance
- `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` and `PRISM_STRATUM_SEND_TIMEOUT_SECONDS`
- `PRISM_RUNTIME_WORKERS`: Tokio worker threads, `1..=1024`
- `PRISM_DATABASE_MAX_CONNECTIONS`: the PostgreSQL pool that share commits
  draw from, `4..=1024`
- `PRISM_STRATUM_MAX_CONNECTIONS`: accepted miner connections, from `1` to the
  platform's Tokio semaphore permit limit
- `PRISM_JOB_BUILD_EXECUTOR_WORKERS`: blocking workers that build job bundles,
  `1..=PRISM_RUNTIME_WORKERS + 8`

The validator requires exactly this set, enforces `minimum <= start <= maximum`
and the native startup ranges above, and rejects the exact `1e-9` local-lab
difficulty. Changing a bound value requires a new qualification run. The set
does not describe frontend/resource topology or prove multi-instance scaling;
retain the complete measured environment and topology beside the image and
revision, database profile, raw miner output, and reconciliation results.

Version 1 and 2 artifacts are rejected with an error naming the retired schema,
and a pre-rewrite artifact does not qualify the native binary. Any other
`qbit-prism-capacity-evidence/` version is rejected as unsupported. Version 2
bound Python-runtime knobs the native server never read. Evidence or an
`--expect` flag that names one is rejected with the key named:

- `PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS` <!-- retired-setting: PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS -->
- `PRISM_SHARE_COMMIT_BATCH_SIZE` <!-- retired-setting: PRISM_SHARE_COMMIT_BATCH_SIZE -->
- `PRISM_SHARE_COMMIT_LINGER_MILLISECONDS` <!-- retired-setting: PRISM_SHARE_COMMIT_LINGER_MILLISECONDS -->

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
qbit-prism-server capacity-evidence \
  tests/fixtures/prism-capacity-evidence.json \
  --allow-example-evidence-for-tests
```

Never use that override in a deployment command or runtime environment.

## Validate Qualification Evidence

Pass the independently configured policy and subject together with every bound
value. The `CAPACITY_*`, subject, and profile shell names are local validator
inputs; each `--expect` value is the measured coordinator's native setting:

```bash
qbit-prism-server capacity-evidence /path/to/capacity-evidence.json \
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
  --expect PRISM_SHARE_COMMIT_TIMEOUT_SECONDS="$PRISM_SHARE_COMMIT_TIMEOUT_SECONDS" \
  --expect PRISM_STRATUM_SEND_TIMEOUT_SECONDS="$PRISM_STRATUM_SEND_TIMEOUT_SECONDS" \
  --expect PRISM_RUNTIME_WORKERS="$PRISM_RUNTIME_WORKERS" \
  --expect PRISM_DATABASE_MAX_CONNECTIONS="$PRISM_DATABASE_MAX_CONNECTIONS" \
  --expect PRISM_STRATUM_MAX_CONNECTIONS="$PRISM_STRATUM_MAX_CONNECTIONS" \
  --expect PRISM_JOB_BUILD_EXECUTOR_WORKERS="$PRISM_JOB_BUILD_EXECUTOR_WORKERS"
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
malloc arenas across the process's 64 threads. The families this section used
to describe were the always-on instrument for that question on the Python
coordinator. That runtime was removed in #244 and the families went with it;
what the native server exports in their place is the table further down.

### Interpreter and allocator families

Retired. The `qbit_prism_process_allocated_blocks`, `qbit_prism_process_gc_*`,
`qbit_prism_process_threads` and `qbit_prism_process_malloc_*` families, with
the `PRISM_MALLOC_TELEMETRY` switch, read CPython allocator and collector <!-- retired-setting: PRISM_MALLOC_TELEMETRY -->
state and glibc `mallinfo2` from the Python coordinator's process-telemetry
module. The native server has no interpreter, no cycle collector, and does not
bind `mallinfo2`, so none of those readings has a Rust analogue and none is
planned. `PRISM_MALLOC_TELEMETRY` is not read by the native server and is no <!-- retired-setting: PRISM_MALLOC_TELEMETRY -->
longer in `compose.yaml` or `.env.example`.

### Component-cardinality families

Retired. `qbit_prism_component_entries{component}` and
`qbit_prism_component_bytes{component}` were `len()` and byte readings over
the Python coordinator's in-process structures, with the `component` label set
pinned by two constants in its metrics module. The payout window, candidate
registries and job caches they counted now live in PostgreSQL or in Rust
structures the registry does not yet size. #278's cutover-minimum scope does
not size them either, and defers its per-worker series to P2 (#262); #279 owns
the inventory that decides which component gauges the native server carries. A `qbit_prism_component_*` series
does not appear in a native `/metrics` body.

### Process telemetry on the native server

The registry landed by #308 exports one process reading and the state needed
to trust it. Names are the ones in `docs/prism-native-metrics.md`; every family
carries the `qbit_prism_` prefix.

| Family | Type | Meaning |
| --- | --- | --- |
| `process_resident_memory_bytes` | gauge | `VmRSS` from `/proc/self/status`, in bytes, refreshed by the process collector every ten seconds. `-1` when the read failed, the platform is unsupported, or the observation is older than 30 seconds. |
| `collector_available{collector="process"}` | gauge | 1 only while the latest completed process collection succeeded and is at most 30 seconds old. Read RSS only when this is 1. |
| `collector_success{collector="process"}` | gauge | Latest attempt succeeded (1), failed (0), or none completed yet (-1). |
| `collector_age_seconds{collector="process"}` | gauge | Monotonic age of the last successful process observation; `-1` before the first success. A failure does not refresh it. |
| `runtime_workers` | gauge | Configured Tokio worker threads (`PRISM_RUNTIME_WORKERS`; empty chooses the available parallelism). |
| `runtime_lag_seconds` | gauge | Latest wake lateness of the 100 ms runtime sampler; `-1` before its first tick. |
| `runtime_poll_lag_seconds{task}` | gauge | Largest active poll, or a completed poll retained for 60 to 61 seconds, per monitored task. |
| `runtime_progress_age_seconds{task}` | gauge | Oldest active explicit operation's time since progress; zero when idle. |
| `runtime_task_stalled{task}` | gauge | 1 while a poll exceeds two seconds or an explicit operation exceeds its budget. |
| `metrics_snapshot_available`, `metrics_snapshot_stale`, `metrics_snapshot_age_seconds` | gauge | The #277 freshness block for the cached body itself. |

Every gauge here is a scrape-time render of cached observations; the scrape
performs no I/O. Read the `x-prism-metrics-state` header (`fresh`, `stale` or
`unavailable`) before trusting a body, as before. What the native block does
not have, and who owns it: thread and open-file-descriptor gauges were on
#278's original list but fall outside its cutover-minimum scope, which keeps
RSS alone from the process block; allocator arena,
in-use, free and mmapped byte gauges are in no open issue, so a soak that
needs the retention-versus-fragmentation split has to raise it on #279 rather
than expect it.

### What to look at first when RSS climbs

1. **Is the reading real?** `qbit_prism_collector_available{collector="process"}`
   must be 1 and the `x-prism-metrics-state` header `fresh`. RSS renders `-1`
   when the collector failed or its observation aged out; a drop to `-1` is a
   collection failure, not a release of memory.
2. **Is it load or time?** Plot `qbit_prism_process_resident_memory_bytes`
   against `qbit_prism_connections`, `qbit_prism_authorized_clients`, the rate
   of `qbit_prism_accepted_shares_total`, and
   `qbit_prism_block_candidates_pending`. RSS that moves with connections or
   the candidate backlog and returns when they do is working set; RSS that
   climbs at flat load is the #226 shape and is what the bound below catches.
3. **Is the runtime keeping up?** `qbit_prism_runtime_task_stalled{task}` at 1,
   `qbit_prism_runtime_poll_lag_seconds{task}` in whole seconds, or
   `qbit_prism_database_pool_acquire_seconds` shifting into its upper buckets
   during the climb points at a blocked worker holding buffers rather than at
   a leak. The incident-6 lesson is that such a process still answers
   `/healthz` from a surviving worker, so the runtime gauges are the tell.
4. **Retention or fragmentation?** The native registry cannot say. The Python
   lane split RSS into live objects, allocator in-use bytes, free bytes and
   mmapped bytes; the native server exports RSS alone. The binary uses the
   system allocator, glibc malloc in the shipped image, so arena mechanics
   still apply to its worker and blocking-pool threads, but nothing on this
   branch measures them. Record the RSS series and the correlated series
   above and attach them to the finding; do not infer an allocator setting
   from RSS alone.

## Heap Census, Allocator Control, and the Resident-Set Bound

Issue #226's second part. On the Python coordinator this section was the
instrument that said *what* was retained (a `gc.get_objects()` census on
`SIGUSR1`) and *which allocator setting* bounded the fragmentation
(`malloc_trim` on `SIGRTMIN+1`, the `MALLOC_ARENA_MAX` experiment), plus the
automated bound a fix had to pass. The census and the allocator controls were
CPython and glibc instruments wired into the Python process and left with it
in #244. The bound and the soak that produces its input are
runtime-independent and are re-anchored below.

### Running a census

Retired. The heap census, its `SIGUSR1` arming and the `PRISM_HEAP_CENSUS*` <!-- retired-setting: PRISM_HEAP_CENSUS -->
settings applied to the retired Python coordinator (#244). The native server
registers no census signal and reads none of those variables, `compose.yaml`
and `.env.example` no longer pass them, and #288's acceptance includes a CI
check that fails if one reappears in `docs/` as live. The native server
installs handlers for `SIGTERM` and `SIGINT` only. In the shipped container it
is PID 1 with no init, so the kernel drops `SIGUSR1` and the signal does
nothing; run as a bare process or under an init, the default action
terminates it. Do not send it.

### Reading a census

Retired with the census. The report format (`process`, `walk`,
`types_by_count`, `types_by_bytes`, `tracemalloc`) described CPython heap
state and has no native producer.

### `malloc_trim`

Retired. The trimmer, `PRISM_MALLOC_TRIM_SIGNAL` and <!-- retired-setting: PRISM_MALLOC_TRIM_SIGNAL -->
`PRISM_MALLOC_TRIM_INTERVAL_SECONDS` were a Python-process instrument (#244). <!-- retired-setting: PRISM_MALLOC_TRIM_INTERVAL_SECONDS -->
The native server exposes no trim hook, and `SIGRTMIN+1` is unhandled there,
so the same warning as for `SIGUSR1` applies.

### Allocator settings and the storm instrument

Retired. The image no longer sets `MALLOC_ARENA_MAX`, `compose.yaml` and
`.env.example` carry no allocator variable, and the candidate-storm rig under
`tests/` that produced the arena experiment was deleted with the Python lane
(#244). The `MALLOC_ARENA_MAX=1` setting in
`docs/prism-payout-artifact-measurement.md` applies to that test process on
the operator's host, not to the coordinator image. No native allocator
experiment exists on this branch; if the soak below fails at flat load, the
allocator is one candidate cause among others and gets its own issue with the
soak evidence attached.

### The resident-set bound

The stated bound is unchanged: **after a one-hour warm-up, the resident set
must stay within 2.0x the warm-up peak for the rest of a 24 h soak at ordinary
load.** The warm-up peak is the baseline because the first hour materializes
the payout window and runs the first full rescan; the #226 slope (390 MB/h from
a 125-145 MiB start) breaches this bound in its third hour, and a post-storm
excursion like #185's 613 MiB drains back under it. The check is automated,
not a graph someone reads. The Python tool that computed the verdict left with
#244; the same verdict is one `awk` pass over the sample file:

```sh
sort -t, -k1,1n soak-rss.csv | awk -F, -v warmup=3600 -v multiple=2.0 -v min_span=82800 -v max_gap=360 '
  function numeric(s) { return s ~ /^[ \t]*-?([0-9]+\.?[0-9]*|\.[0-9]+)[ \t]*$/ }
  /^[ \t]*(#|$)/ { next }
  NF != 2 || !numeric($1) || !numeric($2) { bad = $0; unusable = 1; exit }
  $2 < 0 { next }
  prev != "" && $1 - prev > max_gap { gap = $1 - prev; gap_at = $1; exit }
  { prev = $1
    if (t0 == "") t0 = $1
    if ($1 - t0 <= warmup) { if ($2 > base) base = $2; next }
    post++; last = $1
    if ($2 > peak) { peak = $2; peak_at = $1 }
    if (!breach && $2 > base * multiple) breach = $1 }
  END {
    if (unusable) { print "unusable input: row \"" bad "\" is not seconds,rss_bytes"; exit 2 }
    if (gap) { printf "unusable input: %d s between the samples at %d and %d, soak samples every 300 s\n", gap, prev, gap_at; exit 2 }
    if (base == "" || !post) { print "unusable input: no warm-up or no post-warm-up samples"; exit 2 }
    if (last - t0 < min_span) { printf "unusable input: series spans %d s, soak needs %d s\n", last - t0, min_span; exit 2 }
    printf "baseline=%d bound=%d peak=%d peak_at=%d first_breach_at=%s\n", base, base * multiple, peak, peak_at, breach ? breach : "none"
    exit breach ? 1 : 0 }'
```

The input is one `seconds,rss_bytes` line per sample (absolute epoch seconds
are fine). `#` comments and blank lines are skipped and `-1` samples are
ignored; any other row that is not exactly two numeric fields is unusable
input (exit `2`), so a truncated row during an excursion cannot pass as a
skipped one, as it could not in the Python tool. The field count is exact,
which is stricter than the Python tool: it read the first two fields and
ignored the rest, so `82800,300,000` judged as 300 bytes and `0,100,garbage`
set the baseline. The capture loop below writes exactly two fields, and a row
with a third was corrupted somewhere between the loop and the judge, so it is
refused rather than read. The samples are sorted by timestamp before they
are judged, as the Python tool sorted them, so concatenated partial logs of
one run judge the same as a single file; comment and blank lines are still
skipped wherever they sort, and a malformed row still exits `2`. The same sort
is why two runs must never share a file, which step 3 below prevents with a
directory per run. The warm-up runs from the earliest sample and includes a
sample taken exactly one hour after it, as it did in the Python tool; only
later samples are judged against the bound. The command prints the baseline,
the bound, the post-warm-up peak and its time, and the first breach time, and
exits `0` on pass, `1` on fail, `2` on unusable input. The span floor is the
Python tool's default: `min_span=82800` refuses a series that spans less than
23 hours from its first sample to its last, not one shorter than the soak. The
tool's source recorded the hour of slack as tolerance for a late first sample;
the run itself is still the 24 h that step 2 below asks for, and step 3's loop
ends the run only when its samples span that, by writing a marker that step 6
requires before this check is run, so the hour of slack does not pass a run
that was cut short in its last hour, however it was cut. The span floor
sees only the first and last samples, so it cannot see a hole between them:
two samples 23 hours apart satisfy it, and so does a series with an hour
missing after the warm-up. `max_gap=360` refuses a series in which two
consecutive samples are more than 360 seconds apart, as unusable input (exit
`2`) naming the interval and both timestamps, because an excursion that rose
and drained inside such a hole is not in the file, and a verdict over the
rest would be a pass over evidence the run never took. The tolerance is the
five-minute cadence plus a minute of slack for the three reads an iteration
of the capture loop makes before it sleeps, of which only the `curl` is
bounded; a series taken in cadence has no interval near it. A `-1` row is
skipped before the interval is measured, as it is skipped everywhere else, so
a hole bridged by `-1` rows every five minutes is still a hole: those rows
are samples without a value and say nothing about what RSS did between the
usable ones. By the same rule a single `-1` row between two five-minute
samples opens a 600 s gap; the capture loop below never writes one, it stops
the run instead, so a file refused on that account was not the loop's. The
Python tool had no gap rule, so this is a tightening over it of the same
kind as the exact field count: its verdict passed a series with an
hour-long hole after the warm-up, and a faithful port would have too. The
slope guard the Python tool offered (a leak slow enough to stay under the
multiple inside 24 hours) has no replacement in the runbook; take it from
the RSS series on the deployment's dashboard, whose rules #279 owns.

When it fails: the first breach time says whether the growth is the steady
slope (breach hours in) or an excursion (breach right after a candidate storm
or a rescan burst). There is no census to take at the breach; the evidence is
the RSS series, the correlated series from "What to look at first when RSS
climbs" at the breach time, and the full `/metrics` body captured there. Do
not raise the multiple to make a soak pass.

### Testnet 24 h soak runbook (deferred to the operator)

This soak has not been run against the native server. Nothing here was
verified against a live host; the thresholds and the procedure are what this
document delivers, and the numbers are what the soak produces. It is the
memory-bound evidence for the promotion decision and is distinct from #291's
two-hour cutover soak, which reads its own criteria from the same registry.

1. **Build** the coordinator image the deployment will run and record its
   image ID. Substitute the deployment's env files for the repository
   examples once, in the function; the same Compose invocation then builds
   the image and resolves its name, so a `PRISM_COORDINATOR_IMAGE` set only
   in an env file is the image inspected, not the default. The name is read
   from the resolved service record because `config --images` also lists the
   images of the services the coordinator depends on; the read needs
   `python3` on the operator host:

   ```sh
   compose() {
     docker compose --env-file config/upstream.env.example --env-file .env.example \
       -f compose.yaml --profile prism "$@"
   }
   compose build prism-coordinator
   image=$(compose config --format json \
     | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["prism-coordinator"]["image"])')
   docker image inspect --format '{{.Id}}' "$image"
   ```

2. **Run** one fresh coordinator process for at least 24 h at ordinary testnet
   load with the same miner population throughout. The three
   `MALLOC_ARENA_MAX` runs the Python lane called for are retired with the
   allocator experiment above; there is one configuration to soak, the one
   the deployment ships. Before trusting a run, confirm the process collector
   is publishing and the body is fresh. The audit port is bound to the
   container's loopback and compose publishes only the Stratum port, so the
   read runs inside the container, where the server is PID 1 and the image
   carries `curl` (`3341` is the default `PRISM_AUDIT_PORT`; substitute the
   deployment's value):

   ```sh
   c=<prism-coordinator-container>
   docker exec "$c" curl -sS --max-time 5 -D - http://127.0.0.1:3341/metrics \
     | tr -d '\r' \
     | awk '
       $0 == "x-prism-metrics-state: fresh" { fresh = 1 }
       $0 == "qbit_prism_collector_available{collector=\"process\"} 1" { up = 1 }
       END {
         if (fresh && up) { print "ready"; exit 0 }
         if (!fresh) why = "x-prism-metrics-state is not fresh"
         if (!up) why = why (why ? ", " : "") "process collector gauge is not 1"
         print "not ready: " why; exit 1 }'
   ```

   Both lines are required, `x-prism-metrics-state: fresh` and the process
   collector gauge at 1: a `stale` or `unavailable` body is a cached render
   whose process gauge can still read 1, so the gauge alone proves nothing.
   Do not start the run, and do not trust its RSS series, until the check
   prints `ready` and exits `0`. Record the deploy dotenv (secrets redacted)
   and the image ID beside each run.
3. **Capture every 5 minutes** for the whole soak: RSS from `/proc/1/status`
   for the bound, and the correlated series for the reading order above. Both
   reads run inside the container; the parsing runs on the host. The fence
   defines `capture`, which takes the samples in a loop; the end of step 4
   calls it, once the two snapshot functions it also calls are defined.
   Before every sample it checks that the clock has not moved backward since
   the previous sample ended, that no more than 360 s have passed since
   the previous one started and that it is still reading the process the run
   started with, after the sample's reads it reads the identity again and
   then the clock, which must not precede this sample's start or be more than
   360 s past the time the previous sample's reads ended, and it stops the run
   as invalid when any of these checks fails or one of its appends to the run's files
   does. The first samples taken 3,600 s, 43,200 s and 86,400 s or more after
   the first also take that hour's snapshots, below and in step 4, between the
   sample's appends and its second identity read, and a snapshot that fails
   stops the run the same way. It ends the run itself, as complete, after the
   first sample taken 86,400 s or more after the first, once that sample has
   passed every check, by writing `soak-complete` and returning `0`.

   Set `expected_authorized_clients` to the positive number of authorized
   clients in the ordinary-load population. Every sample must match it and,
   after the first establishes a baseline, show at least one new accepted
   share ACK since the previous sample. Keep ordinary load producing accepted
   shares in each five-minute interval; a cumulative histogram left by earlier
   traffic or rejected-only traffic cannot qualify an idle run. The count
   checks population size, so the operator must also keep the individual
   miners unchanged.

   ```sh
   c=<prism-coordinator-container>
   expected_authorized_clients=<ordinary-load-authorized-client-count>
   run=soak-$(date -u +%Y%m%dT%H%M%SZ)
   process() {
     docker inspect --format '{{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}' "$c"
   }
   invalid() {
     tee -a "$run/soak-invalid" >&2
   }
   capture() {
     prev=
     prev_end=
     start=
     prev_accepted=
     snapped=0
     mkdir "$run" || return
     if ! awk -v expected="$expected_authorized_clients" 'BEGIN { exit !(expected ~ /^[0-9]+$/ && expected + 0 > 0) }'; then
       echo "$(date -u +%FT%TZ): soak invalid, expected_authorized_clients must be a positive integer" | invalid
       return 1
     fi
     if ! first=$(process) || [ -z "$first" ]; then
       echo "$(date -u +%FT%TZ): soak invalid, could not read the coordinator process identity at the start of the run" | invalid
       return 1
     fi
     while true; do
       now=$(date +%s)
       if [ -n "$prev_end" ] && [ "$now" -lt "$prev_end" ]; then
         echo "$(date -u +%FT%TZ): soak invalid, the clock moved backward from the previous sample's end at $prev_end to $now" | invalid
         return 1
       fi
       if [ -n "$prev" ] && [ $((now - prev)) -gt 360 ]; then
         echo "$(date -u +%FT%TZ): soak invalid, no sample for $((now - prev)) s at $now, the last one was at $prev" | invalid
         return 1
       fi
       prev=$now
       if [ -z "$start" ]; then start=$now; fi
       if ! current=$(process) || [ -z "$current" ]; then
         echo "$(date -u +%FT%TZ): soak invalid, could not read the coordinator process identity before the sample at $now" | invalid
         return 1
       fi
       if ! echo "$now $current" >> "$run/soak-process.log"; then
         echo "$(date -u +%FT%TZ): soak invalid, could not append the reading at $now to $run/soak-process.log" | invalid
         return 1
       fi
       if [ "$current" != "$first" ]; then
         {
           echo "$(date -u +%FT%TZ): soak invalid, the coordinator is not the process the run started with"
           echo "  at start: $first"
           echo "  now:      $current"
         } | invalid
         return 1
       fi
       body=$(docker exec "$c" cat /proc/1/status)
       rc=$?
       rss=$(printf '%s\n' "$body" | awk -v now="$now" '
         /^VmRSS:/ && $2 ~ /^[0-9]+$/ && $3 == "kB" { n++; row = now "," $2 * 1024 }
         END { if (n == 1) print row }')
       case $rc:$rss in
         0:"$now",[0-9]*)
           echo "$rss" >> "$run/soak-rss.csv" || {
             echo "$(date -u +%FT%TZ): soak invalid, could not append the sample at $now to $run/soak-rss.csv" | invalid
             return 1
           } ;;
         *)
           {
             echo "$(date -u +%FT%TZ): soak invalid, no RSS sample at $now"
             if [ "$rc" -ne 0 ]; then echo "  docker exec exited $rc"; fi
             echo "  read from /proc/1/status:"
             printf '%s\n' "$body" | sed 's/^/    /'
           } | invalid
           return 1 ;;
       esac
       metrics=$(docker exec "$c" curl -sS --max-time 5 -D - http://127.0.0.1:3341/metrics)
       rc=$?
       metrics=$(printf '%s\n' "$metrics" | tr -d '\r')
       why=$(printf '%s\n' "$metrics" | awk -v rc="$rc" -v previous="$prev_accepted" -v expected="$expected_authorized_clients" '
         function canonical(n) { sub(/^0+/, "", n); return n == "" ? "0" : n }
         function increased(n, p) {
           n = canonical(n); p = canonical(p)
           return length(n) > length(p) || (length(n) == length(p) && ("n" n) > ("n" p))
         }
         /^x-prism-metrics-state:/ { state = $0 }
         $0 == "x-prism-metrics-state: fresh" { fresh = 1 }
         /^qbit_prism_collector_available\{collector="process"\} / { up = $0 }
         $0 == "qbit_prism_collector_available{collector=\"process\"} 1" { up_ok = 1 }
         /^qbit_prism_process_resident_memory_bytes[ {]/ { rss = $0; if ($NF ~ /^[0-9]+$/) rss_ok = 1 }
         $1 == "qbit_prism_share_ack_seconds_count{result=\"accepted\"}" {
           accepted_n++; accepted = $2; accepted_ok = NF == 2 && $2 ~ /^[0-9]+$/
         }
         $1 == "qbit_prism_authorized_clients" {
           clients_n++; clients = $2; clients_ok = NF == 2 && $2 ~ /^[0-9]+$/
         }
         END {
           if (rc != 0) print "docker exec exited " rc
           else if (!fresh) print (state ? "state header read \"" state "\"" : "no x-prism-metrics-state header")
           else if (!up_ok) print (up ? "process collector gauge read \"" up "\"" : "no qbit_prism_collector_available{collector=\"process\"} sample")
           else if (!rss_ok) print (rss ? "RSS gauge read \"" rss "\"" : "no qbit_prism_process_resident_memory_bytes sample")
           else if (accepted_n != 1 || !accepted_ok) print "qbit_prism_share_ack_seconds_count{result=\"accepted\"} must have one nonnegative integer sample"
           else if (previous != "" && !increased(accepted, previous)) print "accepted share ACK count did not increase from " previous " to " accepted
           else if (clients_n != 1 || !clients_ok) print "qbit_prism_authorized_clients must have one nonnegative integer sample"
           else if (canonical(clients) != canonical(expected)) print "authorized client count " clients " does not match expected " expected }')
       if [ -n "$why" ]; then
         {
           echo "$(date -u +%FT%TZ): soak invalid, no metrics sample at $now"
           echo "  $why"
           echo "  headers read from /metrics:"
           printf '%s\n' "$metrics" | awk 'NF == 0 { exit } { print "    " $0 }'
         } | invalid
         return 1
       fi
       prev_accepted=$(printf '%s\n' "$metrics" | awk '$1 == "qbit_prism_share_ack_seconds_count{result=\"accepted\"}" { print $2 }')
       lines=$(printf '%s\n' "$metrics" \
         | grep -E '^(x-prism-metrics-state:|qbit_prism_(share_ack_seconds(_bucket|_sum|_count)|process_resident_memory_bytes|collector_available|collector_age_seconds|runtime_lag_seconds|runtime_task_stalled|runtime_poll_lag_seconds|database_pool_acquire_seconds(_bucket|_sum|_count)|connections|authorized_clients|accepted_shares_total|block_candidates_pending)[ {])' \
         | sed "s/^/$now /")
       printf '%s\n' "$lines" >> "$run/soak-metrics.log" || {
         echo "$(date -u +%FT%TZ): soak invalid, could not append the sample at $now to $run/soak-metrics.log" | invalid
         return 1
       }
       for hour in 1 12 24; do
         if [ "$hour" -gt "$snapped" ] && [ $((now - start)) -ge $((hour * 3600)) ]; then
           snapped=$hour
           name=h$(printf '%02d' "$hour").txt
           if ! why=$(share_ack_snapshot "$run/share-ack-$name" 2>&1); then
             {
               echo "$(date -u +%FT%TZ): soak invalid, no hour-$hour share-ack snapshot at $now"
               printf '%s\n' "$why" | sed 's/^/  /'
             } | invalid
             return 1
           fi
           if [ "$hour" -ne 12 ] && ! why=$(full_metrics_snapshot "$run/metrics-$name" 2>&1); then
             {
               echo "$(date -u +%FT%TZ): soak invalid, no hour-$hour full metrics snapshot at $now"
               printf '%s\n' "$why" | sed 's/^/  /'
             } | invalid
             return 1
           fi
         fi
       done
       if ! after=$(process) || [ -z "$after" ]; then
         echo "$(date -u +%FT%TZ): soak invalid, could not read the coordinator process identity after the sample at $now" | invalid
         return 1
       fi
       if [ "$after" != "$first" ]; then
         {
           echo "$(date -u +%FT%TZ): soak invalid, the coordinator changed while the sample at $now was read"
           echo "  at start:   $first"
           echo "  after read: $after"
         } | invalid
         return 1
       fi
       end=$(date +%s)
       if [ "$end" -lt "$now" ]; then
         echo "$(date -u +%FT%TZ): soak invalid, the clock moved backward during the sample from $now to $end" | invalid
         return 1
       fi
       if [ -n "$prev_end" ] && [ $((end - prev_end)) -gt 360 ]; then
         echo "$(date -u +%FT%TZ): soak invalid, the reads for the sample at $now ended at $end, $((end - prev_end)) s after the previous sample's ended at $prev_end" | invalid
         return 1
       fi
       prev_end=$end
       if [ $((now - start)) -ge 86400 ]; then
         printf '%s\n' "$(date -u +%FT%TZ): soak complete, the samples from $start to $now span $((now - start)) s" > "$run/soak-complete.tmp" \
           && mv "$run/soak-complete.tmp" "$run/soak-complete" || {
           echo "$(date -u +%FT%TZ): soak invalid, could not write $run/soak-complete after the sample at $now" | invalid
           return 1
         }
         return 0
       fi
       sleep 300
     done
   }
   ```

   The bound is evidence about one process. `compose.yaml` gives
   `prism-coordinator` `restart: on-failure`, so a coordinator that exits
   mid-soak is started again by Compose inside the same container, and a loop
   that only read `/proc/1/status` would go on sampling the new PID 1 without
   a word: the CSV would splice several short lifetimes into one 23 h span,
   each restart's low RSS and reset share counters would hide the growth, and
   the bound, which assumes one process, could pass. The container is the
   same across a Compose restart, so `docker inspect` on `$c` is the right
   probe: `StartedAt` moves on any restart, `RestartCount` counts the ones the
   policy made, and `Status` catches a process that exited and was not
   restarted. Every identity probe must exit successfully and return a
   nonempty value; a failed or empty probe invalidates the run, including
   the initial probe before any samples are taken.
   `soak-process.log` keeps one reading per sample, so a later
   reader can show the run was one process. That reading is taken before the
   sample's two reads, so it catches a restart since the previous sample; a
   restart between the check and the reads would record the replacement's
   RSS and metrics under the original identity in `soak-process.log`. Every
   later iteration's check would catch that, but the last iteration of a
   valid run has no later iteration, because `capture` ends the run after
   the sample that completes the 24 h, so a restart in that window on the
   last sample would leave a directory with no `soak-invalid` that reads as
   a single-process run. So the identity is read again after the two reads,
   and a change ends the run as invalid; that sample's rows are already in
   the files, which is fine, because the marker keeps the directory from
   being judged, as step 6 says. The completion check comes after this
   one, so `soak-complete` is written only when the sample that completes
   the 24 h has passed it.
   Any change during the soak invalidates the run: keep the run directory,
   whose `soak-invalid` holds the message, attach `docker logs "$c"` (the
   container keeps the exited process's output), and start over from step 2.

   A run that starts over must not write into the files of the run it
   replaces. The judge sorts the samples by timestamp before it reads them,
   so a `soak-rss.csv` shared by two runs would take its first hour, and so
   its baseline, from the earlier run, judge the later run against that
   baseline, and satisfy the span floor with the earlier run's first sample
   and the later run's last; the two logs would splice the same way. So
   every run writes into its own directory, named for its UTC start time,
   and `capture` returns on the `mkdir` that creates it: `mkdir` without
   `-p` refuses a name that already exists, and on that refusal `capture`
   returns with `mkdir`'s status before its loop has run, rather than
   append to whatever the name holds. The earlier run's directory stays as
   it was, which is the record the restart rule asks to keep.

   A missing sample invalidates the run for the same reason. The bound assumes
   an unbroken five-minute series: its span floor only checks the first and
   last timestamps, so two samples 23 h apart satisfy it, and a gap between
   them can hide an excursion that drained back before the next sample. A
   loop that wrote nothing on a failed read would leave exactly that gap, and
   the judge's malformed-row rule cannot catch it because no row is written
   at all; its gap rule would, but a day later, without the failed read's
   output. So each iteration writes exactly one `seconds,bytes` row or stops:
   the body is read into a variable first, because the exit status of a
   pipeline is its last command's without `pipefail`, which not every
   operator shell sets; the row is printed only when the body carries one
   `VmRSS:` line in kB; and an empty or malformed result, whether from a
   `docker exec` that failed (its exit status is printed) or a body without
   `VmRSS:` (the body is printed), ends the run. A transient `docker exec`
   failure is therefore not skipped: the run is invalid and starts over from
   step 2.

   An append that fails is a missing sample the loop itself caused, and it
   invalidates the run for the same reason. The run directory can fill, or
   the filesystem under it can return an I/O error, at any hour of the soak,
   and a shell that is not running under `set -e` goes on past a failed
   redirection as if it had succeeded, so a loop that did not look at the
   status of its appends would keep reading and keep failing to write. Once
   the rows already on disk spanned the 82,800 s the judge's floor asks for,
   they would pass the judge, and the hours after the disk filled, with the
   process readings and metrics lines that should explain them, would not
   be in the record. So the loop checks the status of each of its three
   appends, to `soak-process.log`, `soak-rss.csv` and `soak-metrics.log`,
   and stops the run as invalid on the first that fails, naming the file
   and the sample time. The metrics lines are read into a variable before
   they are appended, for the reason the body and the response are: the
   status of a pipeline is its last command's, which here would be `sed`,
   and what a failed write does to `sed`'s exit status is each
   implementation's own affair, whereas the shell builtin `printf` reports
   one as `1` in `sh`, `bash` and `zsh` alike, so the same guard reads all
   three appends in the shell's own terms. The run is invalid and starts
   over from step 2.

   A gap between samples invalidates the run for the same reason, and it is
   the hole no read can report. A host that is suspended for an hour, or a
   `docker inspect` or `docker exec ... cat` that stalls for one, leaves the
   series the same hole a missing sample would; neither call is bounded, only
   the `curl` carries `--max-time`. Until now the iteration after the stall
   wrote a valid row and the loop went on, the judge's span floor saw the
   first and last timestamps and nothing between, and an excursion that rose
   and drained inside the hole was gone from the record. So the loop keeps
   the previous iteration's `now` and compares each new one with it before
   anything is written for the iteration: past 360 s it prints the interval,
   its own timestamp and the last sample's to stderr and stops, so the run's
   files end at the last sample taken in cadence. The tolerance is the 300 s of sleep plus a
   minute for the three reads, and it is the same `max_gap=360` the judge
   above holds the file to, so a run judged from its file alone, or a file
   whose loop was ended some other way, is held to the same interval. The
   run is invalid and starts over from step 2.

   The last sample's reads are the hole that check cannot see. A row is
   stamped with the `now` read before the sample's reads, so a
   `docker inspect` or `docker exec ... cat` that stalls inside them shows
   only in the next iteration's `now`, which is what the check above
   compares, and the last iteration of a valid run has no next iteration,
   because `capture` ends the run after the sample that completes the 24 h.
   Until now a stall of an hour in that sample's reads passed both identity
   checks, because the process had not changed, and the completion check
   read the sample's stale `now`: the run ended with a row stamped 86,400 s
   after the first that described the process an hour later, a marker that
   gave that stamp as the end of the span, and nothing in the directory to
   tell it from a run in cadence. So the clock is read again after the
   post-read identity check, and each sample's end is held to the previous
   sample's end as its start is held to the previous start: past 360 s it
   prints the sample's time, when its reads ended and how long after the
   previous sample's that was, to stderr, and stops, before the completion
   check can run. The 300 s of sleep lie inside that interval, so the check
   leaves the reads the same minute the tolerance above does, and holds the
   last sample's reads to it with no next iteration needed; a hole in the
   sleep itself is still seen first by the check above, before anything is
   read for the iteration. The run is invalid and starts over from step 2.

   Every consecutive clock reading must also be nondecreasing: the next
   sample's start cannot precede the previous sample's end, and a sample's
   end cannot precede its own start, including the first and final samples.
   A backward step invalidates the run before another sample or completion
   marker can be written. Keep the invalid directory for diagnosis and
   restart from step 2; sorting its CSV cannot make the timing trustworthy.

   The metrics sample is held to the same rule. The correlated series is what
   reads an RSS excursion back at its own five-minute sample, so a scrape
   that fails, or whose header says `stale` or `unavailable`, would leave
   that series with a hole at the very interval the RSS series may need
   explained. The response is read into a variable the same way, and the
   filtered lines are appended only when the exec succeeded, the body carries
   the line `x-prism-metrics-state: fresh` and the line
   `qbit_prism_collector_available{collector="process"} 1`, and the RSS gauge
   family, `qbit_prism_process_resident_memory_bytes`, the series the reading
   order starts from, carries a nonnegative integer value; anything else
   prints the time, the first condition that failed (the exit status, the
   state header actually seen or its absence, the process collector gauge
   line actually seen or its absence, or the RSS gauge line actually seen or
   its absence) and the header lines to stderr, and ends the run. The header
   alone does not make the RSS value usable: it describes the age of the last
   snapshot publication, and a fresh snapshot can carry a failed process
   collection. The collector families are overlaid at scrape time, and when
   the last successful process collection is absent or older than 30
   seconds, the server renders the process collector gauge at 0 and
   `qbit_prism_process_resident_memory_bytes` at `-1` under a header that
   still says `fresh`. That sample has no RSS value for the reading order to
   start from, so it is unusable and the run is invalid. The same fresh scrape
   must contain exactly one nonnegative integer accepted ACK count and
   authorized-client gauge. The accepted count must increase after the first
   sample, and the authorized count must match `expected_authorized_clients`;
   missing, malformed, frozen or decreasing evidence invalidates the run
   before completion can be published. Both counts are retained in
   `soak-metrics.log`, together with every share-ACK bucket, sum and count for
   both results from that same scrape. The other families
   in the filter are not required, because a histogram bucket or a
   `runtime_task_stalled{task}` series can legitimately be absent from a
   given scrape. The step-2 gate proves the body fresh and the collector
   publishing once, at the start; this check proves both, a usable RSS value,
   the configured population size and accepted-share progress at every later
   sample.

   Every stop path writes its message the same way, through `invalid`,
   which copies what it is given to `$run/soak-invalid` and to stderr, and
   then returns `1` from `capture`. The message on the terminal serves the
   operator who is watching it and nobody else. A loop left with `break`
   ends with status `0`, which is what a caller reads as success, and the
   iteration that finds a bad metrics sample has already written its RSS
   row, so a stop after 23 hours used to leave a directory whose CSV
   satisfies the span floor and nothing, in the directory or in the status,
   to say the run was cut short; a wrapper that ran the fence as a script,
   or an operator who kept the directory and lost the shell's scrollback,
   would have judged it and passed it. So the directory records why it is
   invalid on its own, and the status says that it is: `capture` returns
   `1` only through `invalid`, and `0` only once it has written the
   completion marker the next paragraph describes, `soak-complete`. The fence
   defines `capture`, and step 4 calls it, in the operator's interactive shell,
   which is why the stop paths `return` rather than `exit`, which would end
   the shell that steps 4 to 7 read `$run` from, and why `run=` is set
   outside the function. When the fault is the disk itself the marker may
   not be written; `tee` says so on stderr and still copies the message
   there, so it reaches the operator either way, and the run starts over
   from step 2 as before.

   A valid run ends with a marker too, for the same reason. The judge's
   span floor is 82,800 s, an hour short of the soak, and a loop that ran
   until something outside it ended the run left that hour to whatever
   did: a `SIGHUP` from a closed terminal, a `SIGTERM` from a host going
   down or an operator's interrupt in the 24th hour runs none of the stop
   paths, writes no `soak-invalid`, and leaves a CSV whose first and last
   samples are 23 h apart, which the floor accepts. Nothing in that
   directory told it apart from a run the operator had ended after 24 h,
   and a directory judged on its files alone passed. So the loop keeps the
   time of its first sample, the first row of the CSV, and ends the run
   itself: after the first sample taken 86,400 s or more after it, once
   that sample's reads, appends, hour-24 snapshots, both identity checks and
   the check on the time its reads ended have passed, it writes one line, the time and the
   span the samples cover, to `$run/soak-complete.tmp`, renames that to
   `$run/soak-complete`, and returns `0`. The marker is published by the
   rename, so it appears whole or not at all. A redirect straight to the
   marker's name would create the file before the first byte is written,
   and a write that then failed, on a full disk, could keep `invalid` from
   writing `soak-invalid` on the same disk in the same moment, leaving an
   empty `soak-complete` and no `soak-invalid`, which is the one directory
   the gate must never accept. The rename is a single operation inside the
   run directory: a write or rename that fails, or an interruption between
   the two, leaves at most `soak-complete.tmp`, which step 6 does not read,
   and a write or rename that fails also ends the run as invalid through
   `invalid`, like a failed append, so the run is invalid whether or not
   that message reaches the disk. Step 6 reads the two markers before the
   CSV: `soak-invalid` makes the run invalid whatever else the directory
   holds, neither marker is a run cut short before it was complete, and
   only `soak-complete` with no `soak-invalid` reaches the bound check. The
   marker is written by the loop that took every sample in the directory
   and by nothing else: `capture` returns on the `mkdir` that finds the
   directory already there, so a marker an earlier run left cannot be
   found by a later one, and an operator does not write one by hand. A run
   cut short is invalid and starts over from step 2, keeping its directory
   as the restart rule asks.

   `VmRSS` in `/proc/1/status` is the field the registry's process collector
   reads, so the CSV and the gauge agree up to collector cadence. The log also
   carries the runtime and pool series item 3 of the reading order cites, so a
   breach found after the run can be read back at its own five-minute sample
   instead of from the hour-1 or hour-24 snapshot. The per-cadence share-ACK
   histograms support interval latency comparisons. `capture` also keeps
   cumulative share-ACK snapshots at hours 1, 12 and 24 for inspection,
   through this function:

   ```sh
   share_ack_snapshot() (
     snapshot=$1
     metrics=$(docker exec "$c" curl -fsS --max-time 5 -D - http://127.0.0.1:3341/metrics) || {
       echo "share-ack snapshot failed: metrics scrape failed" >&2
       exit 1
     }
     lines=$(printf '%s\n' "$metrics" | awk '
       BEGIN { in_headers = 1 }
       { sub(/\r$/, "") }
       in_headers {
         headers = headers $0 "\n"
         if (tolower($0) == "x-prism-metrics-state: fresh") fresh = 1
         if ($0 == "") in_headers = 0
         next
       }
       /^qbit_prism_share_ack_seconds_(bucket|sum|count)[{ ]/ { histogram = histogram $0 "\n" }
       $1 == "qbit_prism_share_ack_seconds_count{result=\"accepted\"}" {
         accepted_n++; accepted_ok = NF == 2 && $2 ~ /^[0-9]+$/ && $2 + 0 > 0
       }
       END {
         if (!fresh || histogram == "" || accepted_n != 1 || !accepted_ok) exit 1
         printf "%s%s", headers, histogram
       }') || {
       echo "share-ack snapshot failed: fresh state header and positive accepted ACK count required" >&2
       exit 1
     }
     if ! printf '%s\n' "$lines" > "$snapshot.tmp" || ! mv "$snapshot.tmp" "$snapshot"; then
       echo "share-ack snapshot failed: could not publish $snapshot" >&2
       exit 1
     fi
   )
   ```

   `capture` calls it with `"$run/share-ack-h01.txt"`,
   `"$run/share-ack-h12.txt"` and `"$run/share-ack-h24.txt"` in the first
   samples taken 3,600 s, 43,200 s and 86,400 s or more after its first.
   Each file keeps the response headers and the histogram from the same
   successful scrape. A cached response can return HTTP 200 with a `stale`
   or `unavailable` state, so the function requires the `fresh` header and a
   positive integer accepted ACK count before writing anything. A rejected-only
   histogram is not accepted-share latency evidence; the capture loop separately
   checks that accepted ACKs keep progressing. It publishes the snapshot by
   renaming a completed temporary file; a failed write leaves no partial snapshot at the final
   name. Use only the final `.txt` files for snapshot inspection. If any
   call fails, the run lacks usable latency evidence, so `capture` ends it
   as invalid through `invalid`: keep its directory and repeat the soak from
   step 2.

4. **Snapshot** the full `/metrics` body, headers included, at hour 1 (the
   baseline), hour 24, and at any breach:

   ```sh
   full_metrics_snapshot() (
     snapshot=$1
     if ! docker exec "$c" curl -fsS --max-time 5 -D - http://127.0.0.1:3341/metrics > "$snapshot.tmp"; then
       echo "full metrics snapshot failed: metrics scrape or temporary-file write failed" >&2
       exit 1
     fi
     if ! awk '
       { sub(/\r$/, "") }
       $0 == "" { exit }
       tolower($0) == "x-prism-metrics-state: fresh" { fresh = 1 }
       END { exit !fresh }
     ' "$snapshot.tmp"; then
       echo "full metrics snapshot failed: fresh state header required" >&2
       exit 1
     fi
     if ! mv "$snapshot.tmp" "$snapshot"; then
       echo "full metrics snapshot failed: could not publish $snapshot" >&2
       exit 1
     fi
   )
   ```

   `capture` calls it with `"$run/metrics-h01.txt"` and
   `"$run/metrics-h24.txt"` in the samples that take the hour-1 and hour-24
   share-ack snapshots, after them; at each breach call it with a distinct
   name such as `"$run/metrics-breach-$(date +%s).txt"`.
   For a manual breach snapshot while `capture` is running, use a second
   shell, set `c` to the same container and `run` to the absolute path of the
   existing run directory, and define `full_metrics_snapshot` there before
   calling it.
   The function requires a successful scrape and a `fresh` response header,
   then renames the temporary file to publish the complete headers and body
   unchanged. HTTP errors, transport failures, cached responses and failed
   writes or renames return nonzero and publish no new final snapshot. Use
   only the final `.txt` files as evidence; a `.tmp` file may be partial or
   stale. If a scheduled snapshot fails, `capture` ends the run as invalid
   through `invalid`; keep the run directory and repeat the soak from step 2.
   If a manual breach snapshot fails, record the failure in
   `$run/soak-invalid`, keep the directory, and repeat the soak from step 2.

   This replaces the census step: there is no heap walk on the native server,
   and the body at the breach is what the correlated reading works from.

   The loop schedules the hour snapshots against its first sample's
   timestamp. It takes them after appending the sample and before checking
   process identity and read timing again, so a restart or delay during a
   snapshot can invalidate the run. A failed scheduled snapshot writes
   `soak-invalid` with the snapshot function's diagnostic; `soak-complete`
   follows successful hour-24 snapshots. Define `capture`,
   `share_ack_snapshot` and `full_metrics_snapshot` in the same shell before
   starting the run:

   ```sh
   capture
   ```
5. **Trim** is retired with `malloc_trim`; there is nothing to send at hour 23.
6. **Judge** each run with the `awk` bound check above against its own
   `$run/soak-rss.csv`, and only once this gate has accepted the run
   directory:

   ```sh
   completed() {
     if [ -e "$run/soak-invalid" ]; then
       echo "not judged: $run/soak-invalid says why the run is invalid" >&2
       return 1
     fi
     if [ ! -f "$run/soak-complete" ]; then
       echo "not judged: $run has no soak-complete, capture did not end the run after 24 h of samples" >&2
       return 1
     fi
     cat "$run/soak-complete"
   }
   completed
   ```

   The gate prints the completion line and exits `0` for a directory that
   holds `soak-complete` and no `soak-invalid`; then run the bound check
   inside the run directory, or substitute the path, since the check names
   `soak-rss.csv`. It exits `1`, and the run is not judged whatever its CSV
   would say, when the directory holds `soak-invalid`, because `capture`
   stopped the run on a gap between samples, a process change, a missing
   RSS sample, an append that failed, a metrics scrape that failed, was not
   fresh, had its process collector unavailable or carried no usable RSS
   value, an hour snapshot that failed, or a completion marker it could not
   write, and the marker says
   which; and when the directory holds no `soak-complete`, because then
   `capture` did not end the run itself after 24 h of samples, so whatever
   did, a signal, a reboot, a closed terminal or an interrupt, did so before
   the run was complete, however far its CSV reaches. Either run is invalid
   and is run again from step 2. The marker is `capture`'s to write; do not
   write one by hand.
   For latency, use `soak-metrics.log`: subtract the previous cadence's
   bucket and count values from the hour-1 cadence and, separately, from the
   hour-24 cadence. Match series by their labels and buckets by `le`, never
   line order; require complete, unchanged bucket boundaries, including
   `+Inf`, and counts that agree with `+Inf`. Compute p99 from each interval's
   cumulative bucket deltas, summing accepted and rejected results before
   taking the quantile. A single hour snapshot is cumulative since process
   start; its p99 can hide a slow final interval behind earlier fast ACKs.
   Missing or malformed evidence, counter decreases/resets, inconsistent
   bucket deltas or fewer than 100 interval observations make latency
   **inconclusive**, so the run cannot pass the combined verdict.

   These are adjacent-sample intervals, nominally 300 s with up to 60 s of
   read overhead and scrape-time variation, not exact Prometheus `[5m]`
   windows. The shipped `PrismShareAckP99High` rule in
   `docs/prism-native-alert-rules.json` takes p99 over `rate(..._bucket[5m])`,
   combines both results per target, requires `increase(..._count[5m]) >= 100`
   and a fresh, available snapshot, and fires only after p99 stays **above
   1 second** for `3m`. Use the deployment's actual rule and archived
   Prometheus evaluations to establish that alert verdict; two soak samples
   cannot establish the exact five-minute estimate or its dwell.

   Pass: RSS check exit `0` and usable hour-1 and hour-24 interval evidence,
   with hour-24 p99 within the deployment's configured ACK threshold.
   Fail: RSS check exit `1` or hour-24 interval p99 above that threshold.
   Record both interval p99 values, observation counts, elapsed times and the
   threshold; report any alert firing separately from this interval verdict.
   Record every
   `qbit_prism_runtime_task_stalled` sample at 1 with its timestamp; a stall
   that coincides with an RSS excursion is the first thing to explain.
7. **Record** on issue #291, which owns cutover qualification: the verdict
   line, the run directory (`soak-process.log`, `soak-rss.csv`,
   `soak-metrics.log`, `soak-complete`, the three share-ack histograms, and
   the hour-1, hour-24 and breach snapshots), the image ID, and the redacted
   deploy dotenv. The glibc version inside the image
   (`docker exec "$c" ldd --version`) still belongs in the record, because
   the process uses it as its allocator.
8. **The post-storm drain re-run** on #185 is retired; the storm rig was a
   Python test deleted in #244, and #185's drain measurement is the Python
   lane's record.

## Payout-window JSONB storage ceiling

Independent of any load-test record, the payout window has a hard storage
ceiling. PostgreSQL refuses a JSONB container whose elements exceed 268,435,455
bytes, and three native writes still embed the whole window, so each grows
linearly with the share count. `cargo test -p qbit-prism-server --test
jsonb_ceiling_gate` measures them against 25% of that limit (67,108,863 bytes)
and fails when the set of crossing writes changes in either direction.

| Path | Column written | Window copies | At 400,000 shares | Removed by |
| --- | --- | --- | --- | --- |
| refresh | `qbit_prism_jobs.payload` | 3 | refused by PostgreSQL (measured) | #273 |
| enqueue | `qbit_block_candidate_outbox.candidate` | 2 | refused by PostgreSQL (measured) | #265 |
| landing | `qbit_pool_audit_bundles.audit_bundle` | 1 | 235 MB, 3.5x the gate threshold (measured) | #267 |

The legacy audit import (`import-audits`) was a fourth such write, refused by
PostgreSQL at 400,000 shares. #265 removed it. The import now stores only
`canonical_audit_bytes`, which is `bytea`, so PostgreSQL's 1 GiB value limit
bounds it rather than the JSONB ceiling. It writes no JSONB value.

The host, gate commit, PostgreSQL version (16.15) and build mode (debug) behind
these measurements are recorded under "Baseline, measured at the base commit
plus the gate's own test files" and "At 400,000 shares" in
`docs/prism-payout-artifact-measurement.md`.

Sizes are uncompressed JSONB containers. A refused write is reported as refused,
never with a size. After the enqueue write is refused, the gate writes a
window-free substitute outbox row itself so the later phases stay measurable;
the report labels that row `SUBSTITUTE` and never counts it as the enqueue write.
See "JSONB ceiling gate and 400k-share
baselines" in `docs/prism-payout-artifact-measurement.md` for the 50,000-,
100,000-, 200,000- and 400,000-share measurements and how to reproduce them. A
capacity qualification run should record the payout window size it exercised,
because a window near 400,000 shares reaches this ceiling before it reaches any
throughput limit.
