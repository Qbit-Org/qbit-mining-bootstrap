# Measure native Prism performance

Measure the Rust deployment with the intended payout window, miner population,
database, and frontend count. The removed Python incremental-window scheduler,
subprocess builder, and its event names are not measurement targets for the
native server. A faster synthetic builder result alone does not demonstrate
miner-facing capacity or HA durability.

## Native builder benchmark

The built-in benchmark builds and verifies actual signed audit bundles in
process using synthetic shares:

```sh
cargo run --locked --release -p qbit-prism-server -- benchmark \
  --shares 100000 --miners 100 --iterations 20 \
  --output-json /tmp/prism-native-builder.json
```

Record the commit, binary/image digest, compiler/build mode, CPU allocation,
memory limit, and exact dimensions. Output schema
`qbit.prism.native-builder-benchmark.v1` reports:

- `build_and_verify_p50_ms` and `build_and_verify_p99_ms`
- `canonical_audit_bytes`
- `shares`, `miners`, `iterations`, and `engine`

The timings include native construction and verification. They exclude Stratum
networking, PostgreSQL commits, node RPC, multi-instance contention, and durable
CTV broadcasting. The benchmark uses direct settlement with synthetic data;
measure the configured CTV path in a real integration run as well. A percentile
from few iterations is descriptive, not a well-sampled latency tail.

## Miner-facing load measurement

Use controlled miners or a load generator that submits valid work to the actual
Stratum listeners. Keep load bounded and run against isolated test infrastructure
before a production canary. Measure at least:

| Measurement | Why it matters |
| --- | --- |
| Valid submissions, successful ACKs, unique committed proofs | Establishes throughput and accounting reconciliation |
| ACK p50/p95/p99 and maximum | Captures database, target validation, and routing latency |
| Subscribe/authorize to initial difficulty/job | Detects reconnect admission or delivery stalls |
| Tip observation to fresh job delivery | Measures usable mining work under refresh load |
| RSS and CPU per frontend, peak concurrent builders | Shows actual resource use and window memory cost |
| Database lock waits, pool utilization, statement latency | Reveals the shared database bottleneck |
| WAL bytes and table/index growth | Quantifies durability/storage cost |
| Candidate submit-to-active and active-to-accounted latency | Separates node acceptance from durable settlement |
| Reclaimed candidate/CTV work after interruption | Exercises recovery rather than process uptime alone |

Capture steady state, reconnect bursts, slow database service, a frontend
restart, and actual HA database failover if zero acknowledged-share loss is a
requirement. Reconcile unique ACKed proofs against database identifiers after
in-flight transactions have resolved. Do not count exact duplicate resubmissions
as additional accepted work.

Record the same workload against one and several frontends, with total miner
load and total CPU allocation stated separately. More frontends provide routing
and process redundancy, but every share still crosses one PostgreSQL commit and
ordering boundary. Report measured scaling; do not infer it from thread count.

The retained [optional v2 qualification validator](prism-capacity-readiness.md)
checks a strict evidence record. The repository's synthetic builder benchmark
does not generate a qualification artifact or replace the complete load runner.

## Compare database query costs

If `pg_stat_statements` is enabled in the reviewed database profile, capture
cumulative counters at the start and end of matching observation intervals.
Do not reset shared production statistics. A reset invalidates the subtraction:

```sql
SELECT now() AT TIME ZONE 'UTC' AS captured_at_utc, stats_reset
FROM pg_stat_statements_info;

SELECT queryid, calls, total_exec_time, rows, shared_blks_hit,
       shared_blks_read, temp_blks_read, temp_blks_written, query
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY total_exec_time DESC;
```

Capture every query row or explicitly retain all relevant ledger, snapshot,
carry-forward, audit, and outbox queries at both boundaries. Absence from a top-N
list does not mean a zero counter. For matching query IDs, compute differences
in calls, execution time, reads, and temporary I/O; interval mean execution time
is `total_exec_time_delta / calls_delta`.

Use the actual native query with representative arguments when inspecting a
plan. `EXPLAIN ANALYZE` executes the query, so use an isolated restored database
for expensive window/audit probes. Keep a statement timeout and read-only
transaction. Native reward windows use eight times network difficulty; the old
Python prefetch's sixteen-times scan is not the current reward policy.

## Storage and audit response measurement

Record `pg_total_relation_size` deltas for the ledger, proof registry, audit
snapshots, non-share audit bodies, job records, and payout/CTV tables. Count
native snapshot references separately from imported legacy inline bundles.
Measure full public audit serialization time and response bytes for large
windows; normalization reduces persistent duplication but the compatible public
response still contains the logical share array.

Verify sampled reconstructed bundles against their canonical SHA, the trusted
ledger public key, and recorded coinbase. A reduced storage footprint is useful
only when old and new audits remain independently reproducible.

## Comparison record

Retain raw measurements and a concise record containing revision/image,
configuration, database profile and durability settings, workload, observation
start/end, frontend/resource counts, latency distributions, unique-commit
reconciliation, storage/WAL growth, and recovery outcomes. State omissions and
sample sizes. Compare matched workloads and label predictions separately from
observed results.

## JSONB ceiling gate and 400k-share baselines

PostgreSQL refuses a JSONB container whose elements exceed 268,435,455 bytes.
Four native writes still embed the whole payout window and therefore grow
linearly with the share count, so the payout window has a hard storage ceiling
that arrives well before any capacity limit. The gate at
`crates/qbit-prism-server/tests/jsonb_ceiling_gate.rs` measures those writes and
holds the line while they are being removed.

### Running it

Reduced sizes, which is what `cargo test --locked -p qbit-prism-server
--all-targets` and the `prism-native-postgres` CI job already run. The
database-free `rust-tests` job skips it by name
(`--skip jsonb_ceiling_ratchet_at_reduced_sizes`), because with `CI` set and no
database the gate fails instead of skipping:

```sh
PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
  cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate
```

Full size (400,000 shares) against a disposable cluster the repository script
starts and cleans up itself:

```sh
test/prism-native-tests.sh cargo-args \
  --locked -p qbit-prism-server --test jsonb_ceiling_gate \
  -- --ignored --nocapture --exact jsonb_ceiling_ratchet_at_full_size
```

Always name the test: `--ignored` without a filter also starts
`jsonb_ceiling_baseline_sweep` in the same process, against the same cluster.

The full-size run measures the reduced pair first, then 400,000 shares. It needs
the reduced pair's projections to attribute the writes PostgreSQL refuses at
full size, so budget its wall clock on top.

Full size against a PostgreSQL you already have:

```sh
PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
  cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate \
  -- --ignored --nocapture jsonb_ceiling_ratchet_at_full_size
```

The baseline sweep that produced the table below. It also runs the reduced pair,
5,000 and 20,000, for the CI projection it checks its own sizes against, and
skips a size the list already contains:

```sh
PRISM_JSONB_GATE_BASELINE_SIZES=50000,100000,200000 \
PRISM_TEST_DATABASE_URL=postgresql://user@127.0.0.1:5432/postgres \
  cargo test --locked -p qbit-prism-server --test jsonb_ceiling_gate \
  -- --ignored --nocapture jsonb_ceiling_baseline_sweep
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `PRISM_TEST_DATABASE_URL` | none | PostgreSQL to test against. Missing while `CI` is set is a failure, never a skip. |
| `PRISM_JSONB_GATE_TARGET_SHARES` | `400000` | share count the reduced run projects to |
| `PRISM_JSONB_GATE_N1` | `5000` | smaller reduced size |
| `PRISM_JSONB_GATE_N2` | `20000` | larger reduced size |
| `PRISM_JSONB_GATE_STATEMENT_TIMEOUT_MS` | `600000` | exported as `PRISM_DATABASE_STATEMENT_TIMEOUT_MS` before the first `Ledger::connect` |
| `PRISM_JSONB_GATE_BASELINE_SIZES` | `50000,100000,200000` | sizes for the baseline sweep: at least two, strictly increasing, each dividing the window weight; checked before anything runs |

Every share count must divide the window weight (8,000,000) exactly, and
`0 < n1 < n2 <= target` must hold. A missing variable takes the default; an
empty, malformed, zero, negative or out-of-range value fails loudly and is never
replaced by the default.

### What it measures

The gate drives five phases against a real PostgreSQL 16 with an in-process fake
node. No phase submits a block, so none of them needs `QBITD_BIN`.

| Phase | Call | Write |
| --- | --- | --- |
| refresh | `Coordinator::refresh_once` | `qbit_prism_jobs.payload` |
| enqueue | `Ledger::append(share, Some(candidate))` | `qbit_block_candidate_outbox.candidate` |
| claim | `Ledger::claim_candidate` | lease columns only |
| landing | `Ledger::land_candidate` then `finish_candidate` | `qbit_pool_audit_bundles.audit_bundle` |
| import | `Ledger::import_legacy_audits` | `qbit_pool_audit_bundles.audit_bundle` and `canonical_audit_bytes` |

After every phase it discovers every JSONB column through
`information_schema.columns` (17 at this base commit, all present only after
`Ledger::connect(.., true)` has applied `001_share_ledger.sql` and migrations
002-005) and records, per row:

- `pg_column_size(col)`, the TOAST-compressed stored size;
- `octet_length(col::text)`, the JSON text form;
- `pg_column_size(col::text::jsonb)`, the **uncompressed** JSONB container. The
  268,435,455-byte ceiling applies to the uncompressed container, so the
  compressed on-disk size must not be what a threshold compares against. A
  computed datum is never TOAST-compressed, so the round trip through `text`
  reports the real container size. Measured, it comes out at 1.07 times
  `octet_length(col::text)` and about 27 to 35 times `pg_column_size(col)` for
  share-heavy documents on the synthetic fixture: 27.4 for the 50,000-share
  refresh payload, 34.7 for the 400,000-share landing body.

A value is attributed to the phase whose write created its row or changed its
value, compared by `md5(col::text)`. Attributing by `xmin` alone would be wrong:
the claim UPDATE touches only the lease columns, but the new tuple version
carries the unchanged TOAST pointer forward, so `xmin` alone blames claim for
the candidate that enqueue wrote. The gate prints every such tuple rewrite as an
`attribution:` line so the distinction stays visible.

### How the ratchet works

A write **crosses** when its size at the target exceeds 67,108,863 bytes, 25% of
the hard limit, or when PostgreSQL refuses it outright. The reduced run fits
`size(n) = a + b*n` through its two sizes and projects to the target. A negative
slope fails the gate instead of quietly passing, and so does an intercept more
negative than 5% of the `n1` measurement: that write grew faster than linearly
between the two sizes, and a straight line would understate it. The measured
window-carrying writes sit under 0.3%. A positive intercept, up to the `n1` size
itself for a constant-size write, is a fixed per-write overhead and is allowed.
The full-size run compares the measured size directly.

A refusal is a result rather than a crash, but PostgreSQL's ceiling error names
neither the table nor the column, and the server adds no context that would.
The gate therefore attributes every refusal by projection: among the columns the
refused phase writes, exactly one has to project past 90% of the hard limit at
the refused share count, and it has to be the write the gate expected there. No
candidate, two candidates, or a single candidate that is not the expected write
each fail the gate, naming what was found and what was expected, so a second
oversized write in the same phase can never be recorded as the one already
known. The projections come from the reduced pair, which is why the full-size
run and the baseline sweep both run that pair first; at the reduced sizes
themselves a refusal at `n2` is attributed from the `n1` measurement, and that scaled measurement stays
the write's projection for the full-size run and the sweep, so a violation
already refused at `n2` is still explained at the target. A refusal at `n1`
fails the gate, since nothing smaller was accepted to attribute it from. The ratchet row says how: `refused at n=400000 (attributed by
projection)`, and never a byte count.
Until a refusal is attributed, the measurement table shows it by phase
only, under `(not attributed)`, never under the column its call site
assumed; each run attributes before it prints.

The gate passes only when the set of crossing writes equals `KNOWN_VIOLATIONS`
exactly. An unlisted write that crosses fails it as a new violation. A listed
write that stops crossing also fails it, with a message naming the entry to
delete, so #265, #267 and #273 are forced to shrink the list as they land. The
full table is printed on every run, passing or failing.

### Baseline, measured at the base commit plus the gate's own test files

All numbers below are **measured**, unless a cell says "projected". Recorded on
2026-09-10 by the gate as committed in `0c62751`, which is base commit
`1398bbc1f6e00972c53595bc6f27a6e57adf386c` plus only the three new test files,
with `cargo test` in **debug** (rustc 1.97.1) against a local disposable
PostgreSQL **16.15** cluster with stock settings, on a single host: 8 vCPU
(Intel Core, Haswell-class) and 22 GiB RAM, no swap. One host, one table; do not
mix in numbers from another machine.

Later commits changed how the gate reports and validates: refusals, the
substitute row, units, unknown values, the fit checks and the settings checks.
They did not change what the pipeline writes or how a JSONB size is measured,
so the sizes here stand. The one measurement they did change is the claim
phase's peak RSS, now reset before claim; see the note below the
per-phase table.

Largest JSONB write per path. "Uncompressed" is `pg_column_size(col::text::jsonb)`,
the size the 268,435,455-byte ceiling applies to; "stored" is the
TOAST-compressed `pg_column_size(col)`. The stored size varies by a few bytes
from run to run: the payload embeds a random `storage_key` and schema UUID, so
the pglz output differs. The gate thresholds on the uncompressed size, which is
byte-identical across runs and hosts.

| Shares | Path | Column | Uncompressed B | Text B | Stored B |
| ---: | --- | --- | ---: | ---: | ---: |
| 50,000 | refresh | `qbit_prism_jobs.payload` | 94,694,334 | 88,230,523 | 3,460,299 |
| 50,000 | enqueue | `qbit_block_candidate_outbox.candidate` | 61,934,708 | 57,742,014 | 2,160,865 |
| 50,000 | claim | none | - | - | - |
| 50,000 | landing | `qbit_pool_audit_bundles.audit_bundle` | 29,173,666 | 27,252,045 | 861,409 |
| 50,000 | import | `qbit_pool_audit_bundles.audit_bundle` | 61,933,626 | 57,740,951 | 2,160,601 |
| 100,000 | refresh | `qbit_prism_jobs.payload` | 189,894,214 | 176,380,550 | 6,923,912 |
| 100,000 | enqueue | `qbit_block_candidate_outbox.candidate` | 124,334,648 | 115,392,039 | 4,315,575 |
| 100,000 | claim | none | - | - | - |
| 100,000 | landing | `qbit_pool_audit_bundles.audit_bundle` | 58,773,666 | 54,402,069 | 1,707,202 |
| 100,000 | import | `qbit_pool_audit_bundles.audit_bundle` | 124,333,566 | 115,390,976 | 4,315,311 |
| 200,000 | refresh | `qbit_prism_jobs.payload` | **refused** | - | - |
| 200,000 | enqueue | `qbit_block_candidate_outbox.candidate` | 248,734,408 | 230,992,039 | 8,626,824 |
| 200,000 | claim | none | - | - | - |
| 200,000 | landing | `qbit_pool_audit_bundles.audit_bundle` | 117,573,546 | 108,902,069 | 3,399,850 |
| 200,000 | import | `qbit_pool_audit_bundles.audit_bundle` | 248,733,326 | 230,990,976 | 8,626,557 |

The claim phase writes no JSONB value at any size; it updates only the lease
columns. That row is a measured zero-copy result, not a missing measurement.

At 200,000 shares the prepared job is already refused:

```text
error returned from database: total size of jsonb object elements exceeds the
maximum of 268435455 bytes
```

The enqueued candidate and the imported audit body are at 248.7 MB there, inside
the limit by 7%. One more doubling puts both over it.

Wall time, WAL and peak resident set, per phase. WAL is
`pg_wal_lsn_diff` around the one operation and includes full-page writes, so it
moves with checkpoint timing rather than purely with the payload. Peak RSS is
`VmHWM` of the **test process** (the gate plus the native library, not the
PostgreSQL backend), with `/proc/self/clear_refs` resetting the high-water mark
at the start of each phase. On an OS without a resettable `VmHWM` (macOS) the
gate prints `not measured on this OS` for every phase instead of a number. When
these tables and the 400,000-share table below were recorded, the claim phase
did not reset the mark, so its peak RSS spans enqueue and claim together; the
gate now resets it before claim too.

**WAL and stored sizes here are measured on synthetic shares and are
optimistic.** The fixture repeats five miner identities and a padded `miner_id`,
so the window compresses about 29:1 in TOAST
(`uncompressed / pg_column_size(col)` = 27.4 to 34.7 across these runs), while
production data is assumed to compress about 3:1. That 3:1 is the assumption
stated in issue #273, not a measurement. Do not carry these WAL or stored figures into
a production estimate without restating that ratio. The gate prints the same
caveat on every run. The uncompressed sizes, which are what the JSONB ceiling
applies to and what the gate thresholds on, are unaffected.

| Shares | Phase | Result | Seconds | WAL B | Peak RSS KiB |
| ---: | --- | --- | ---: | ---: | ---: |
| 50,000 | refresh | ran | 17.66 | 3,704,648 | 655,244 |
| 50,000 | enqueue | ran | 14.91 | 2,501,664 | 1,028,772 |
| 50,000 | claim | ran | 11.25 | 896 | 1,101,840 |
| 50,000 | landing | ran | 33.05 | 1,040,592 | 1,082,872 |
| 50,000 | import | ran | 50.25 | 4,617,296 | 1,248,072 |
| 100,000 | refresh | ran | 35.73 | 7,419,792 | 1,638,612 |
| 100,000 | enqueue | ran | 30.06 | 4,638,512 | 1,924,400 |
| 100,000 | claim | ran | 22.13 | 896 | 2,149,764 |
| 100,000 | landing | ran | 67.16 | 2,023,304 | 2,088,428 |
| 100,000 | import | ran | 101.06 | 9,385,664 | 2,293,936 |
| 200,000 | refresh | refused | 74.10 | 1,145,896 | 3,230,884 |
| 200,000 | enqueue | ran | 60.03 | 9,252,776 | 3,984,264 |
| 200,000 | claim | ran | 44.70 | 78,856 | 4,435,528 |
| 200,000 | landing | ran | 133.45 | 12,776,016 | 4,312,596 |
| 200,000 | import | ran | 203.34 | 18,528,496 | 4,862,096 |

Whole-pipeline wall clock: 177.6 s at 50,000, 357.6 s at 100,000 and 702.9 s at
200,000. The fixture load itself is a small part of that: 15,000 rows/s, so
3.3 s, 6.5 s and 13.5 s respectively.

`qbit_pool_audit_bundles.canonical_audit_bytes` is bytea, so the JSONB ceiling
does not apply to it; it is reported only. After import it holds 55,640,585 B at
50,000 shares, 111,190,610 B at 100,000 and 222,590,610 B at 200,000, against
268,435,456 B, which is 25% of the 1 GiB varlena limit. Projected to 400,000
shares it is ~445 MB, over that reporting line but still inside the varlena
limit.

#### At 400,000 shares

Measured on the same 8 vCPU / 22 GiB host as the table above, which is close to
its limit at this size (see the allocator note at the end of this subsection).
A run on a larger, dedicated host should be recorded in its own host-labelled
table rather than merged into this one.

The full-size run completes and the ratchet holds: every phase is reached, and
the crossing set is exactly the four known violations. Three of the four writes
are refused by PostgreSQL outright, each with

```text
error returned from database: total size of jsonb object elements exceeds the
maximum of 268435455 bytes
```

| Path | Result | Uncompressed B | Text B | Stored B | Seconds | WAL B | Peak RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| refresh, `qbit_prism_jobs.payload` | refused | - | - | - | 128.28 | 692,280 | 5,094,428 |
| enqueue, `qbit_block_candidate_outbox.candidate` | refused | - | - | - | 111.91 | 23,264 | 4,149,832 |
| claim, of the gate-written substitute row | ran, writes no JSONB | - | - | - | 0.01 | 808 | 4,150,152 |
| landing, `qbit_pool_audit_bundles.audit_bundle` | ran | 235,173,306 | 217,902,069 | 6,785,157 | 266.81 | 7,305,224 | 4,584,516 |
| import, `qbit_pool_audit_bundles.audit_bundle` | refused | - | - | - | 399.19 | 166,864 | 5,610,676 |

Whole pipeline: 1,222.9 s, of which 27.7 s is the fixture load (400,000 rows at
14,465 rows/s, 585.7 B per serialized share, 223.4 MiB of window). Peak RSS of
the test process was 5.6 GiB. `canonical_audit_bytes` is never written at this
size because the import that would write it is refused first.

Two notes on reading the 400,000-share table. The enqueue write is refused, and
the gate reports it only as refused: `rejected` in its size columns,
`refused at n=400000 (attributed by projection)` in the ratchet, and
PostgreSQL's error text underneath.
After the refusal the gate writes a window-free **substitute** outbox row itself,
so that the claim, landing and import writes stay measurable instead of being
reported as unreached. The substitute is not the enqueue write and never enters
the ratchet; the report lists it on its own line labelled `SUBSTITUTE`. In the
run recorded here it measured 14,880 B uncompressed, a figure that run's report,
produced before the substitute was labelled, printed in the enqueue row. The
claim row above claimed that substitute, so its seconds, WAL and peak RSS
describe claiming a window-free row, not a production candidate. And the landing
write survives at 400,000 shares only because it carries
one window copy rather than two or three: at 235 MB it is inside the hard limit
but 3.5 times over the gate threshold, and one more doubling of the window puts
it over the ceiling as well.

The measured landing size confirms the projections: 235,173,306 B measured
against 233,066,529 B projected from the CI pair (0.90% low) and 236,373,666 B
projected from 50,000 and 100,000 shares (0.51% high).

**Run the full-size test on a host with memory to spare.** At 400,000 shares the
test process needs several GiB and PostgreSQL needs several more at the same
time. Without
`MALLOC_ARENA_MAX=1 MALLOC_MMAP_THRESHOLD_=65536 MALLOC_TRIM_THRESHOLD_=65536`
the test process retained about 7 GiB of freed allocations by the landing phase;
with them it peaked at 5.6 GiB and finished. Set those for the full-size run on
any machine that is not dedicated to it, and budget for PostgreSQL alongside.
Nothing in CI is affected: CI runs only the reduced sizes.

The reduced-size gate itself, at its default 5,000 and 20,000 shares, pinned to
two cores with `taskset -c 0,1` and `CARGO_BUILD_JOBS=2` on the same host, took
**88.3 s** of test time (17.8 s at 5,000, 70.4 s at 20,000). In CI it runs in
the `prism-native-postgres` job, a 2 vCPU runner with `timeout-minutes: 20`
(`.github/workflows/ci.yml`). That job's test step took about 4m41s at `3.x.x`
`f316eb9`, before this gate existed. The 88.3 s comes from the recording host,
not a CI runner, so treat the sum as an estimate of the headroom, not a
measurement of it.

Fit stability. The baseline sweep computes and checks this itself, per write. It
runs the reduced CI pair (5,000 and 20,000) alongside its own sizes, projects
every write to 400,000 shares from that pair and again from the largest pair of
sweep sizes at which **that write** was accepted, and fails when the two differ
by more than 5% of the larger-pair projection. The pair has to be chosen per
write because the writes are refused at different sizes - refresh is already
refused at 200,000 while the other three are not - so no single pair fits all
four. A write accepted at fewer than two sweep sizes fails the sweep by name
rather than going unchecked, and a CI projection below 1 MiB is reported as not
compared, because a relative difference that small is fixed per-write overhead
and not the window's growth. The table below was recorded before the check was
per write, against 50,000 and 100,000 - the largest pair at which all four
writes were accepted - and the two projections agree to well inside the
tolerance. Re-running the same sweep now pairs enqueue, import and landing at
100,000 and 200,000, where the measurements above show them still accepted;
refresh, refused at 200,000, keeps 50,000 and 100,000:

| Path | From 5,000 and 20,000 | From 50,000 and 100,000 | Difference |
| --- | ---: | ---: | ---: |
| refresh | 756,772,922 B | 761,093,494 B | 0.57% |
| enqueue | 494,920,433 B | 498,734,288 B | 0.76% |
| import | 494,919,351 B | 498,733,206 B | 0.76% |
| landing | 233,066,529 B | 236,373,666 B | 1.40% |

### Fixture

The window is loaded by `crates/qbit-prism-server/tests/support/window_fixture.rs`
with one server-side `INSERT ... SELECT FROM generate_series` per 50,000 rows,
then the `qbit_prism_share_hashes` backfill migration 002 performs. Shares are a
pure function of the share index: no wall clock, no OS randomness, and the same
parameters always produce byte-identical shares. The row shape follows the #258
measurement fixture; `miner_id` is padded so one serialized `AcceptedShare` is
exactly 581 bytes at share index 1 and averages 583.7 to 584.9 bytes over a
window, against the ~581 bytes per share measured on the production window in
#254 (372,257 shares, ~232 MB canonical).

Two deviations from #258 are deliberate. `share_difficulty` is scaled to
`8,000,000 / n` so exactly `n` shares fill the payout window, instead of #258's
21-digit value; keeping that value would require a network difficulty of ~5e24
at 400,000 shares, which the 24-bit compact-bits mantissa cannot express
exactly, so the window length would stop being an exact function of `n`. And the
share index inside `share_id` is zero-padded to a fixed width. Both cost bytes
that the `miner_id` padding gives back, which is why the serialized share size
is the same at every window size and the two-point linear fit is sound.
