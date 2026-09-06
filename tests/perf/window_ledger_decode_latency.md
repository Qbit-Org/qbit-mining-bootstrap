# Payout-window database decode latency (#236, PR 1)

Companion to `tests/perf/window_ledger_decode_latency.py`. Measures the
database/decode phase of the coordinator's payout-window reads --
`snapshot_at_job_issue` (bounded and unbounded) and
`snapshot_between_job_issues` -- before and after the per-row decoding change
in PR 1 of the #236 plan, on both ledger backends, at the incident window size
(210k shares) and the stress size (400k), with a wake-lateness monitor thread
beside every call.

**Scope of the claim.** This is the ledger read only: admission, statement
execution, result receipt, row decoding and record conversion, returned as
the same `list[AcceptedShareRecord]`. It says nothing about the Rust window
builder, the serialization paths (PR 2), the pipe transport (PR 3), the lease
heartbeat itself, or the 24-hour production soak. The monitor thread is a
proxy for the lease monitor's wake lateness -- it measures whether another
Python thread can get the interpreter while the read runs -- and its numbers
are *scheduling symptoms*, not a proof of which C call held the GIL. The GC
probe in section 4 is the only attribution in this document that separates
one cause from another.

**Provenance.** Measured 2026-09-06 on `alexdevbox1`, a **shared host**:
three Claude agent sessions for the #236 lanes plus other long-lived
sessions were resident, and the 1-minute load average sat between 1.5 and
3.8 (8 CPUs) during the runs, most of it this benchmark and its PostgreSQL
backend. Nothing here was measured in isolation. Baseline and patched
passes ran back to back in the same environment on the same fixture; treat
wall-time differences below ~10% as noise and read the lateness columns as
the signal.

- Baseline: `lab.prism.share_ledger` from a detached checkout of
  `e59bda34016f6dc0a6bc39f9b879b8df64f51f60` (the plan's baseline and the
  branch tip when this lane started).
- Patched: the working tree of this PR before commit (the JSON therefore
  also records `e59bda3` as the tree's HEAD). The only change made after the
  run was the final-partial-batch deadline recheck (two extra clock reads
  per read), which cannot move these figures.
- Python 3.12.3 (system), psycopg 3.2.13 (`psycopg[binary]`) in a scratch
  venv for the native backend, `/usr/bin/psql` 16 for the subprocess
  backend, PostgreSQL 16.15 over a Unix socket on a local cluster
  (`port 55436`), not the deployment's container image or CPython 3.14.
- Fixture: generated, production-shaped rows (`username.rig:block_hash_hex`
  share ids, 62-character payout addresses as `miner_id`/`order_key`,
  32-byte P2MR programs, `share_difficulty` 16384, `network_difficulty`
  226646186, one job id per 64 shares, `stale-grace` on ~1% of rows, eight
  miners with the scale gate's skew). `--reps 3`, one warm-up per read
  shape. Command lines are in the driver's docstring; the two JSON captures
  and this comparison were produced by `--json` and `--compare`.

## 1. Headline

**The whole-window decode call is gone from both backends, and the worst
monitor wake lateness during a read drops by roughly 3-4x at both sizes.**
No wake-up later than 500 ms was observed with the patch, against three per
read (one per rep, i.e. every rep) at the baseline. At 210k shares the
patched worst case is 176-260 ms; at 400k it is 342-484 ms.

**The residual is CPython's cyclic GC, not the decode.** With automatic GC
disabled around the same patched read, the worst wake-up is 11-12 ms
(section 4). The remaining 100-500 ms stalls are generation-2 collections
triggered by allocating the 210k-400k record objects the read returns;
they exist at the baseline too, underneath its single decode call. Fixing
them is a process-wide GC policy decision outside this lane (section 6).

Server-side time did not regress: every per-row statement is as fast or
faster than its `json_agg` predecessor (the delta halves), with the same
index scans and the aggregate node replaced by a sort. Peak RSS of the
benchmark process fell by ~43%, from 1.0 GB to 0.57 GB at 210k and from
1.87 GB to 1.05 GB at 400k, because no whole-window JSON text and no
whole-window list of dicts exist at once. Wall time is mixed: the psql
backend and the delta read get faster (10-25%), while the native bounded and
unbounded reads get 8-11% slower and burn ~30% more caller-thread CPU,
because psycopg now runs its JSON loader once per row through Python instead
of one C decode of the aggregate. That trade is deliberate: the extra CPU is
spread across 400+ GIL-switchable batches instead of one uninterruptible
call.

## 2. Results

### 210,000 shares

| backend | read | records | wall median (ms) base -> patch | wall max (ms) base -> patch | monitor max lateness (ms) base -> patch | wake-ups > 100 ms base -> patch | wake-ups > 250 ms base -> patch | wake-ups > 500 ms base -> patch |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| native | bounded | 210,000 | 6207 -> 6695 | 6470 -> 6761 | 949 -> 255 | 15 -> 8 | 3 -> 1 | 3 -> 0 |
| native | delta | 210,000 | 5536 -> 4598 | 5576 -> 4643 | 830 -> 260 | 15 -> 7 | 3 -> 1 | 3 -> 0 |
| native | unbounded | 210,000 | 5357 -> 5607 | 5522 -> 5794 | 827 -> 222 | 14 -> 7 | 3 -> 0 | 3 -> 0 |
| psql | bounded | 210,000 | 6985 -> 6161 | 7248 -> 6337 | 852 -> 204 | 17 -> 6 | 3 -> 0 | 3 -> 0 |
| psql | delta | 210,000 | 6032 -> 4460 | 6500 -> 4491 | 752 -> 200 | 17 -> 6 | 3 -> 0 | 3 -> 0 |
| psql | unbounded | 210,000 | 6448 -> 5315 | 6570 -> 5627 | 794 -> 176 | 18 -> 6 | 3 -> 0 | 3 -> 0 |

| read | server planning+execution (ms) base -> patch | statement uses json_agg base -> patch |
|---|---:|---|
| bounded | 2859 -> 2708 | True -> False |
| delta | 2179 -> 884 | True -> False |
| unbounded | 2058 -> 1807 | True -> False |

| backend | read | caller-thread CPU median (ms) base -> patch | process RSS high-water (MB) base -> patch |
|---|---|---:|---:|
| native | bounded | 3122 -> 4054 | 994 -> 571 |
| native | delta | 3047 -> 3832 | 1001 -> 571 |
| native | unbounded | 3039 -> 4017 | 1000 -> 571 |
| psql | bounded | 3439 -> 3088 | 1001 -> 571 |
| psql | delta | 3267 -> 3024 | 1001 -> 571 |
| psql | unbounded | 3446 -> 2956 | 1001 -> 571 |

### 400,000 shares

| backend | read | records | wall median (ms) base -> patch | wall max (ms) base -> patch | monitor max lateness (ms) base -> patch | wake-ups > 100 ms base -> patch | wake-ups > 250 ms base -> patch | wake-ups > 500 ms base -> patch |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| native | bounded | 400,000 | 11892 -> 13156 | 12249 -> 15200 | 1643 -> 484 | 19 -> 9 | 11 -> 9 | 3 -> 0 |
| native | delta | 400,000 | 10403 -> 8844 | 10710 -> 9017 | 1586 -> 429 | 19 -> 8 | 10 -> 8 | 3 -> 0 |
| native | unbounded | 400,000 | 10210 -> 11108 | 10331 -> 11451 | 1516 -> 465 | 20 -> 9 | 10 -> 9 | 3 -> 0 |
| psql | bounded | 400,000 | 13039 -> 12403 | 13050 -> 12539 | 1500 -> 380 | 26 -> 7 | 13 -> 7 | 3 -> 0 |
| psql | delta | 400,000 | 11514 -> 8587 | 11820 -> 8928 | 1420 -> 342 | 25 -> 7 | 10 -> 6 | 3 -> 0 |
| psql | unbounded | 400,000 | 11277 -> 10734 | 11452 -> 10885 | 1479 -> 406 | 26 -> 7 | 13 -> 6 | 3 -> 0 |

| read | server planning+execution (ms) base -> patch | statement uses json_agg base -> patch |
|---|---:|---|
| bounded | 5555 -> 5179 | True -> False |
| delta | 4167 -> 1766 | True -> False |
| unbounded | 4120 -> 3364 | True -> False |

| backend | read | caller-thread CPU median (ms) base -> patch | process RSS high-water (MB) base -> patch |
|---|---|---:|---:|
| native | bounded | 5809 -> 7934 | 1856 -> 1043 |
| native | delta | 5806 -> 7259 | 1865 -> 1047 |
| native | unbounded | 5664 -> 7779 | 1864 -> 1044 |
| psql | bounded | 6288 -> 5987 | 1865 -> 1046 |
| psql | delta | 5999 -> 5865 | 1865 -> 1046 |
| psql | unbounded | 6055 -> 6128 | 1865 -> 1046 |

Columns: *wall* is the call as the coordinator sees it (admission included;
the gate was uncontended); *monitor max lateness* is the worst single
wake-up of the 10 ms sleeper across the three reps; the *wake-ups* columns
count wake-ups later than the threshold summed over the three reps (each rep
takes ~450-1300 samples); *server* is `EXPLAIN (ANALYZE)` planning plus
execution of the statement the ledger actually sent, so *wall - server* is
the client-side share (transfer, decode, conversion); *caller-thread CPU* is
`time.thread_time()` of the reading thread; *RSS high-water* is `VmHWM` of
the benchmark process after the read (monotonic across a run, so the 400k
figure includes the 210k pass).

## 3. Query plans

Captured from the statements the ledger sent (`statement_sha256_prefix` in
the JSON identifies each). The selection CTEs are byte-identical between
baseline and patch; only the outer projection changed.

- *bounded*: `Aggregate -> Recursive Union -> ... Index Scan[qbit_share_ledger_pkey] (LIMIT 4096 pages)`
  became `Sort -> Recursive Union -> ...`. Same 52 (210k) / 98 (400k) page
  steps, same index. 2859 -> 2708 ms at 210k, 5555 -> 5179 ms at 400k.
- *unbounded*: `Aggregate -> Index Scan[pkey]` became `Index Scan[pkey]`
  (the ORDER BY is satisfied by the index). 2058 -> 1807 ms, 4120 -> 3364 ms.
- *delta*: `Aggregate -> Gather Merge -> Sort -> Append(...accepted_recent_idx...)`
  became `Gather Merge -> Sort -> Result -> Append(...)`, same parallel
  branches. 2179 -> 884 ms, 4167 -> 1766 ms: the aggregate previously
  serialized the whole result on the leader.

## 4. Attributing the residual stalls

`gc_probe.py` (kept in the session scratchpad, not committed; it reuses the
driver's fixture and monitor) ran the patched native bounded read at 210k
three ways, two reps each, recording every collection through
`gc.callbacks`:

| GC mode during the read | wall (ms) | monitor max lateness (ms) | collections | gen-2 collections | worst single collection (ms) | GC total (ms) |
|---|---:|---:|---:|---:|---:|---:|
| default | 6185, 6120 | 108, 114 | 412 | 4 | 111, 118 | 457, 431 |
| `gc.freeze()` before the read | 6372, 5994 | 104, 92 | 412 | 4 | 103, 98 | 387, 374 |
| `gc.disable()` during the read | 5458, 5664 | 11, 12 | 2 | 1 | 26, 17 | 26, 17 |

Every wake-up later than 100 ms in the default rows coincides with a gen-2
collection of the same duration; with collection disabled the same code
path, same fixture and same batch size shows nothing above 12 ms and runs
~10% faster. `gc.freeze()` helps little because the objects being traversed
are the freshly built records themselves, not the pre-existing heap. The
benchmark's higher patched maxima (255 ms at 210k, 484 ms at 400k) are the
same mechanism in a process that had already accumulated more live objects
(both backends, both sizes, three reps each) and on a busier host; the
count of gen-2 collections per read grows with the number of live tracked
objects, and each one traverses all of them.

This is measurement, not a fix. Changing collection policy is process-wide
and affects every other allocation-heavy path in the coordinator (PR 2's
serialization included), so it is recorded here as the residual for the
lease-latency gate rather than changed in this lane.

## 5. What this does and does not establish

- Established: neither production backend builds a whole-window JSON value
  for these two reads; individual decode calls are bounded (512 rows); the
  worst observed wake lateness during the read fell 3-4x; server time and
  peak RSS fell; results are byte-identical to the `json_agg` oracle on both
  backends (the PostgreSQL gates).
- Not established: the 250 ms engineering target at 400k (worst observed
  342-484 ms, all GC); any figure on the deployment image, CPython 3.14 or
  psycopg pinned by the deployment; the lease heartbeat's own behaviour;
  first-usable-work time; anything about the Rust builder or the
  serialization/transport lanes; the 24-hour soak. The dedicated-environment
  release gates in the plan remain to be run with all three PRs applied.
- Caveats: shared host (above); three reps; the native backend's fixed
  ~30% extra caller-thread CPU is a real cost the coordinator pays on the
  reading thread; the psql backend still spools the whole result to a
  temporary file (page cache, not heap) and psycopg still buffers the raw
  result in libpq, exactly as before.

## 6. Follow-ups suggested by the data

1. Decide GC policy at the coordinator level with all three lanes in view:
   candidates are `gc.freeze()` after startup plus raised thresholds, or
   suspending automatic collection around the bulk window read/build with an
   explicit collection at a point where the lease monitor is not waiting.
   Measure with the same monitor; the probe above is the template.
2. If native-backend CPU matters more than the psql path, register a
   psycopg loader for the `json` OID that returns the raw text and decode
   with `json.loads` directly, removing one Python-level indirection per row.
3. Re-run this driver in the dedicated production-image environment with
   the plan's 210k/400k gates, alongside the real heartbeat, once PRs 2 and
   3 land.
