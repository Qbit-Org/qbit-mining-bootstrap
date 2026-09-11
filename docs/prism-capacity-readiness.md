# Optional PRISM Capacity Qualification

This document defines an optional operator load-test record for a future public
promotion decision. A capacity artifact is not required for PRISM startup,
restart, mainnet readiness, or CI. Compose, `make doctor`, `prism-self-check`,
and the coordinator do not consume an artifact or any `PRISM_CAPACITY_*`
environment variables.

The bootstrap repository ships the strict
native `qbit-prism-server capacity-evidence` command for the strict
`qbit-prism-capacity-evidence/v2` format, but it does not ship a production
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

The v2 format is retained for compatibility with existing qualification records.
Its batch/linger/timeout and idle-sweep fields describe the historical Python
profile; those variables do not configure the native Rust server. Keep their
declared legacy metadata separate from the actual native environment. The
validator still requires the exact v2 keys and bindings and does not reinterpret
them as native tuning controls.

A pre-rewrite artifact does not qualify the new binary. For a native run, retain
the complete measured environment and frontend/resource topology alongside the
image/revision, database profile, raw miner output, and reconciliation results.
The v2 validator alone does not bind newly introduced CPU/connection settings or
prove multi-instance scaling. Measure those explicitly and include them in the
reviewed release evidence.

Qualification schema `qbit-prism-capacity-evidence/v2` binds:

- share difficulty and vardiff enablement
- vardiff minimum, start, maximum, target, retarget, step, EWMA, tolerance, and
  idle-sweep values
- share commit batch size, linger, and timeout
- Stratum send timeout

The validator enforces `minimum <= start <= maximum`. Changing a bound value or
the measured native environment requires a new qualification run. The exact
`1e-9` local-lab difficulty is rejected.

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
profile value. The shell names below are local validator inputs. In particular,
the `QUALIFICATION_LEGACY_*` values must come from independently reviewed v2
metadata; they are not coordinator environment settings:

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
  --expect PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS="$QUALIFICATION_LEGACY_IDLE_SWEEP_SECONDS" \
  --expect PRISM_SHARE_COMMIT_BATCH_SIZE="$QUALIFICATION_LEGACY_COMMIT_BATCH_SIZE" \
  --expect PRISM_SHARE_COMMIT_LINGER_MILLISECONDS="$QUALIFICATION_LEGACY_COMMIT_LINGER_MILLISECONDS" \
  --expect PRISM_SHARE_COMMIT_TIMEOUT_SECONDS="$QUALIFICATION_LEGACY_COMMIT_TIMEOUT_SECONDS" \
  --expect PRISM_STRATUM_SEND_TIMEOUT_SECONDS="$PRISM_STRATUM_SEND_TIMEOUT_SECONDS"
```

No startup or deployment path runs this validator automatically. If an operator
later adopts capacity qualification as public-routing policy, deployment
orchestration should call it explicitly before opening public routes and archive
the artifact and raw results. Private canaries and routine restarts do not
require it. Re-run only when the qualified image, bound load configuration,
database/hardware profile, forecast, or ACK objective changes.

**Status on this branch: deferred.** The validator is native
(`qbit-prism-server capacity-evidence`) and the command above parses against
its CLI, but nothing on this line produces the file it validates. The
repository ships no load runner, and the strict v2 key set is checked as an
exact set that still requires `PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS`,
`PRISM_SHARE_COMMIT_BATCH_SIZE` and `PRISM_SHARE_COMMIT_LINGER_MILLISECONDS`,
which the native server never reads, so an artifact that records only the
settings the native runtime reads is rejected. Issue #288 owns re-keying the
validator to the native capacity settings and bumping the artifact schema
version; issue #291 produces the first native artifact during cutover
qualification and is the gate that consumes this section. Until #288 lands,
the command documents the contract; it is not a check anything on this branch
can pass with honest native evidence.

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
the `PRISM_MALLOC_TELEMETRY` switch, read CPython allocator and collector
state and glibc `mallinfo2` from the Python coordinator's process-telemetry
module. The native server has no interpreter, no cycle collector, and does not
bind `mallinfo2`, so none of those readings has a Rust analogue and none is
planned. `PRISM_MALLOC_TELEMETRY` is not read by the native server and is no
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

Retired. The heap census, its `SIGUSR1` arming and the `PRISM_HEAP_CENSUS*`
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

Retired. The trimmer, `PRISM_MALLOC_TRIM_SIGNAL` and
`PRISM_MALLOC_TRIM_INTERVAL_SECONDS` were a Python-process instrument (#244).
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
sort -t, -k1,1n soak-rss.csv | awk -F, -v warmup=3600 -v multiple=2.0 -v min_span=82800 '
  function numeric(s) { return s ~ /^[ \t]*-?([0-9]+\.?[0-9]*|\.[0-9]+)[ \t]*$/ }
  /^[ \t]*(#|$)/ { next }
  NF < 2 || !numeric($1) || !numeric($2) { bad = $0; unusable = 1; exit }
  $2 < 0 { next }
  { if (t0 == "") t0 = $1
    if ($1 - t0 <= warmup) { if ($2 > base) base = $2; next }
    post++; last = $1
    if ($2 > peak) { peak = $2; peak_at = $1 }
    if (!breach && $2 > base * multiple) breach = $1 }
  END {
    if (unusable) { print "unusable input: row \"" bad "\" is not seconds,rss_bytes"; exit 2 }
    if (base == "" || !post) { print "unusable input: no warm-up or no post-warm-up samples"; exit 2 }
    if (last - t0 < min_span) { printf "unusable input: series spans %d s, soak needs %d s\n", last - t0, min_span; exit 2 }
    printf "baseline=%d bound=%d peak=%d peak_at=%d first_breach_at=%s\n", base, base * multiple, peak, peak_at, breach ? breach : "none"
    exit breach ? 1 : 0 }'
```

The input is one `seconds,rss_bytes` line per sample (absolute epoch seconds
are fine). `#` comments and blank lines are skipped and `-1` samples are
ignored; any other row that is not `seconds,rss_bytes` is unusable input (exit
`2`), as it was in the Python tool, so a truncated row during an excursion
cannot pass as a skipped one. The samples are sorted by timestamp before they
are judged, as the Python tool sorted them, so concatenated partial logs judge
the same as a single file; comment and blank lines are still skipped wherever
they sort, and a malformed row still exits `2`. The warm-up runs from the
earliest sample and includes a sample taken exactly one hour after it, as it
did in the Python tool; only later samples are judged against the bound. The
command prints the baseline, the bound, the post-warm-up peak and its time, and
the first breach time, and exits `0` on pass, `1` on fail, `2` on unusable
input. The span floor is the Python tool's default: `min_span=82800` refuses a
series that spans less than 23 hours from its first sample to its last, not one
shorter than the soak. The tool's source recorded the hour of slack as
tolerance for a late first sample; the run itself is still the 24 h that step 2
below asks for. The slope guard the Python tool offered (a leak slow enough to
stay under the multiple inside 24 hours) has no replacement in the runbook;
take it from the RSS series on the deployment's dashboard, whose rules #279
owns.

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
   reads run inside the container; the parsing runs on the host:

   ```sh
   c=<prism-coordinator-container>
   while true; do
     now=$(date +%s)
     docker exec "$c" cat /proc/1/status \
       | awk -v now="$now" '/^VmRSS:/ { printf "%s,%d\n", now, $2 * 1024 }' >> soak-rss.csv
     docker exec "$c" curl -sS --max-time 5 -D - http://127.0.0.1:3341/metrics \
       | tr -d '\r' \
       | grep -E '^(x-prism-metrics-state:|qbit_prism_(process_resident_memory_bytes|collector_available|collector_age_seconds|runtime_lag_seconds|runtime_task_stalled|runtime_poll_lag_seconds|database_pool_acquire_seconds(_bucket|_sum|_count)|connections|authorized_clients|accepted_shares_total|block_candidates_pending)[ {])' \
       | sed "s/^/$now /" >> soak-metrics.log
     sleep 300
   done
   ```

   `VmRSS` in `/proc/1/status` is the field the registry's process collector
   reads, so the CSV and the gauge agree up to collector cadence. The log also
   carries the runtime and pool series item 3 of the reading order cites, so a
   breach found after the run can be read back at its own five-minute sample
   instead of from the hour-1 or hour-24 snapshot. Also keep the share-ack
   histogram at hours 1, 12 and 24 for the latency comparison:

   ```sh
   docker exec "$c" curl -sS --max-time 5 http://127.0.0.1:3341/metrics \
     | grep -E '^qbit_prism_share_ack_seconds' > share-ack-h01.txt
   ```

4. **Snapshot** the full `/metrics` body, headers included, at hour 1 (the
   baseline), hour 24, and at any breach:

   ```sh
   docker exec "$c" curl -sS --max-time 5 -D - http://127.0.0.1:3341/metrics > metrics-h01.txt
   ```

   This replaces the census step: there is no heap walk on the native server,
   and the body at the breach is what the correlated reading works from.
5. **Trim** is retired with `malloc_trim`; there is nothing to send at hour 23.
6. **Judge** each run with the `awk` bound check above against
   `soak-rss.csv`. Pass: exit `0`, and share-ack p99 at hour 24 within the
   alert threshold configured for the deployment (the native rules are
   #279's). Fail: exit `1`, or a share-ack regression between hour 1 and hour
   24 that the operator would alert on. Record every
   `qbit_prism_runtime_task_stalled` sample at 1 with its timestamp; a stall
   that coincides with an RSS excursion is the first thing to explain.
7. **Record** on issue #291, which owns cutover qualification: the verdict
   line, `soak-rss.csv`, `soak-metrics.log`, the three share-ack histograms,
   the hour-1, hour-24 and breach snapshots, the image ID, and the redacted
   deploy dotenv. The glibc version inside the image
   (`docker exec "$c" ldd --version`) still belongs in the record, because
   the process uses it as its allocator.
8. **The post-storm drain re-run** on #185 is retired; the storm rig was a
   Python test deleted in #244, and #185's drain measurement is the Python
   lane's record.

## Payout-window JSONB storage ceiling

Independent of any load-test record, the payout window has a hard storage
ceiling. PostgreSQL refuses a JSONB container whose elements exceed 268,435,455
bytes, and four native writes still embed the whole window, so each grows
linearly with the share count. `cargo test -p qbit-prism-server --test
jsonb_ceiling_gate` measures them against 25% of that limit (67,108,863 bytes)
and fails when the set of crossing writes changes in either direction.

| Path | Column written | Window copies | At 400,000 shares | Removed by |
| --- | --- | --- | --- | --- |
| refresh | `qbit_prism_jobs.payload` | 3 | refused by PostgreSQL (measured) | #273 |
| enqueue | `qbit_block_candidate_outbox.candidate` | 2 | refused by PostgreSQL (measured) | #265 |
| landing | `qbit_pool_audit_bundles.audit_bundle` | 1 | 235 MB, 3.5x the gate threshold (measured) | #267 |
| import | `qbit_pool_audit_bundles.audit_bundle` | 2 | refused by PostgreSQL (measured) | #265 |

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
