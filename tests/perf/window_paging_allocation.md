# Rust payout-window paging allocation: builder RSS and timing at 210k and 400k shares (#240)

Issue #240 (incident #236): `PayoutWindow::from_full_snapshot` in
`crates/qbit-prism/src/window.rs` paged the retained records with
`remaining.split_off(page_size)` in a loop and stored the head as each page.
`Vec::split_off` keeps the original vector's allocation, so page *k* retained
backing capacity for every record still unpaged when it was cut (N, N−P, N−2P,
…) and the tail was copied again on every iteration: O(N²/P) retained memory
and copying.

What that cost had been measured at, before this PR: a **prior synthetic
reproduction on this development host** (`tests/perf/window_pipeline_gil_scaling.md`
§9, same fixture and page size as below, single run) recorded 9,613 MiB peak
builder RSS and a 12.2 s response for a 210k-share preparation, and 15,623 MiB
followed by SIGKILL at 400k. Production (#236) separately observed roughly
9.69 GiB builder RSS at its ~210k-share window; those production figures are
cited from the incident, not reproduced here.

This report measures the fix in this PR (every retained record moves exactly
once into a page allocated for exactly the records it holds; the `advance`
delta loop, which had the same `split_off` shape, is paged the same way)
against the real `qbit-prism-build-audit-bundle --serve` daemon, before and
after, on this host and inside a locally built copy of the production
coordinator image, with every experiment bounded so it cannot exhaust the
host.

Driver: `tests/perf/window_paging_allocation.py`. Standard library plus
read-only imports from `lab/`; no database, network or coordinator. It is an
**on-demand instrument, not a test**: it asserts no thresholds and is not
named `test_*` (#160); its bounded safety behaviour (exchange deadlines,
child reaping) is covered by `tests/test_window_paging_allocation_harness.py`.
Every number a reviewer needs is in this file; the structured JSON captures
behind the tables (`--json` output of the runs below) are retained in the
worker's workspace outside the repository and are not committed.

---

## Headline

**The allocation defect is gone and paging is linear in the retained count.**
Peak resident set of the daemon across a cold full preparation, two advances,
a re-center (second full preparation held beside the first) and a further
advance:

| retained shares | old paging (baseline binary) | fixed paging (this PR) | fixed, single window only |
|---:|---:|---:|---:|
| 52,000 | **1,028 MiB**, full response 1.05 s | **163 MiB**, 0.33 s | 110 MiB |
| 105,000 | **3,884 MiB**, 3.78 s | **325 MiB**, 0.63 s | 219 MiB |
| 210,000 | 9,613 MiB, 12.2 s *(prior synthetic measurement, §5)* | **601 MiB**, 1.29 s | 435 MiB |
| 400,000 | 15,623 MiB then SIGKILL *(prior synthetic measurement, §5)* | **1,142 MiB**, 2.60 s | 826 MiB |

Old-paging memory per 1k shares doubles when the window doubles (19.8 →
37.0 MiB per 1k from 52k to 105k), consistent with the quadratic record-vector
capacity model (§4). Fixed-paging memory is flat at **2.9–3.1 MiB per
1k shares** from 52k to 400k. The 400k preparation completes in 2.6 s with a
clean daemon exit; no daemon fell back, died, timed out or declined.

**The same binary built inside the production coordinator image gives the
same numbers** (§7): 601 MiB / 1.46 s at 210k and 1,142 MiB / 2.60 s at 400k
under CPython 3.14.7 with `MALLOC_ARENA_MAX=2`.

**Every digest agreed.** All 40 phase outcomes (host: two binaries × sizes ×
five phases; image: one binary × two sizes × five phases) returned
`prepared`; every returned digest re-hashed from the daemon's own canonical
bytes (with the coordinator's drop-prefix/append-suffix mirror surgery
replayed for advances), and every one matched the shipped Python fold run on
the same inputs as an independent oracle — under CPython 3.12 on the host and
CPython 3.14 in the image. The frozen parity corpus passed against the
rebuilt binary (§6).

**Transport and Rust processing are separated by measurement, not
inference:** the fixed daemon reports its own JSON parse, fold and canonical
serialization times in the `prepare_window` envelope (an additive `metrics`
field the coordinator ignores). At 210k the 1.29 s response wait is 0.27 s
parse + 0.38 s fold + 0.63 s canonical digest/items; the blocking-pipe
transport is 0.15 s to write the 91 MiB request and 0.12 s to read the 91 MiB
response. The paging fold is now the *smallest* of the three daemon terms.

---

## 1. Source, binaries and verification

| | |
|---|---|
| Branch | `fix/prism-240-rust-window-paging-20260906`, base `0bc7fa6c023082df81d23137ebabf4ff96f24683` (`2.x.x` with #237/#238/#239 merged) |
| Rust fix commit | `527b3acb5e25148ec6e6a71141818ddbaa06866a` — the only commit on this branch touching `crates/` |
| Fixed binary | `target/release/qbit-prism-build-audit-bundle`, `cargo build --release --locked -p qbit-prism --bin qbit-prism-build-audit-bundle`, sha256 `46faaa9547c39960441be84fc85268789d5d55f99a0ab70a214b643ca10293ce`, 2,241,216 bytes |
| Verification of the fixed binary | built from a tree whose `crates/` content is byte-identical to `527b3ac`; re-running the same build command after the commit was a cargo fingerprint no-op (nothing recompiled, same sha256). The two touched sources at `527b3ac` hash to `d1529288d117af52…` (`window.rs`) and `68ab080a746e3d18…` (`qbit-prism-build-audit-bundle.rs`), and the copies inside the image (§7) hash identically |
| Baseline binary | same command from the unmodified base `0bc7fa6`, sha256 `e7f5ca0cccf31b6a3045a3b898ef53b0e2857d7cf276cadf0530147e3c298e2d`, 2,244,296 bytes |
| Harness | the `tests/perf/window_paging_allocation.py` committed beside this report, byte for byte (the host sweep recorded its tree as `527b3ac` plus the then-uncommitted harness, report and harness tests that the next commit on this branch adds; no `crates/` change) |
| Toolchain (host) | cargo/rustc 1.97.1 (c980f4866 2026-06-30), stable x86_64-unknown-linux-gnu |
| Host | Linux 6.8.0-106-generic x86_64, glibc 2.39, KVM guest, 8 vCPU, 23,464 MiB RAM, no swap; shared development host |
| Available memory | 17,436 MiB at the start of the host sweep; recorded before every daemon run (16,425–17,281 MiB) |
| Load (1 m) | 0.78 at start, 1.13–1.69 before the larger runs (other worktrees on the host were quiet but not idle) |
| Python (driver + oracle, host) | CPython 3.12.3, `MALLOC_ARENA_MAX` unset |
| Page size | 512 (`DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE`) |
| Bounding | every daemon under `RLIMIT_AS` = 6,144 MiB; a run is skipped unless `MemAvailable` ≥ 8,192 MiB; one daemon at a time; every exchange (handshake included) under a 600 s deadline that kills and reaps the daemon |

### Fixture

`build_records` from `tests/perf/window_pipeline_gil_scaling.py`, the fixture
the #236 probe and the prior synthetic baseline used: production-shaped
`share_id` (`bench-miner-N.rigM:<64 hex>`), 200 identities, difficulty
16,384, one share per second, all eligible at the anchor. `window_weight` is
the fixture's total difficulty so every record is retained. Request sizes:
22.5 MiB (52k), 45.4 MiB (105k), 91.0 MiB (210k), 173.6 MiB (400k).

### Phases, per binary and size, on one daemon

| phase | request | what it exercises |
|---|---|---|
| `full` | `mode=full`, all records | the cold fold: filter, sort, validate, cutoff, **paging** |
| `advance_small` | `mode=advance`, 16 appended records | the boundary-page merge |
| `advance_large` | `mode=advance`, 1,043 appended records | the delta paging loop (two full pages plus a partial, same `split_off` shape as the fold) |
| `recenter` | `mode=full`, same records at ¾ of the weight | what the coordinator's self-check re-center sends while the previous window is still held: a second prepared window in the two-entry cache |
| `recenter_advance` | `mode=advance` against the re-centered digest, 16 records | advance from the re-centered generation |

### What the columns mean

- **write** — elapsed until the blocking pipe accepted the last request byte
  (the daemon has consumed all but at most one pipe capacity). Transport.
- **wait** — from then until the envelope line arrived. Contains the daemon's
  remaining parse, the fold or advance, canonical serialization and response
  readiness.
- **read** — the raw canonical-items section after the envelope. Transport.
- **parse / fold / serialize** — the daemon's own `metrics`: `serde_json`
  parse of the request line; `from_full_snapshot` or `advance`; canonical
  digest plus (for `full`) the items stream. Measured inside the process with
  `Instant`, so this is Rust processing, not pipe time.
- **residual (approx.)** — wait − (parse + fold + serialize). The client's
  wall interval and the daemon's internal timers are independent intervals on
  either side of a pipe (the daemon may still be reading and parsing when the
  client clock starts; the envelope write and scheduling are on neither
  timer), so this is an approximation of scheduling plus envelope handling,
  **not** a measured transport term. It is clamped at zero and blank when a
  binary reports no metrics.
- **RSS after** — `VmRSS` from `/proc/<pid>/status` right after the phase.
  **peak so far / peak** — `VmHWM`, the kernel's lifetime high-water mark,
  read before the daemon is reaped (not a sampling artefact; the 20 ms
  sampler's maximum agreed within 22 MiB in every run).
- **self-check** — `sha256("[" + items + "]")` of the daemon's bytes equals
  its `share_snapshot_sha256`; for advances the coordinator's mirror surgery
  (drop `retained_drop_bytes`, append the returned suffix) is replayed first.
- **oracle** — the digest equals `IncrementalShareWindow` (the shipped Python
  fold, `lab/prism/share_ledger.py`) on the same records, weight, anchor and
  page size, including its `advance` for the advance phases.

---

## 2. Host results

All peak figures below are observed high-water marks before the daemon exits,
including the clean runs. They are lower bounds on the final lifetime peak:
a child can allocate between a live `/proc` read and normal exit or delivery
of `SIGKILL`. The harness now renders `≥` for every such observation, including
watchdog pre-kill reads, with the source recorded separately. The tables retain
the captured numeric observations; they do not establish a lifetime memory
ceiling. Structural page-capacity tests establish the allocation bound.

### Summary per daemon (observed before exit)

| binary | shares | peak RSS MiB (VmHWM) | peak VSZ MiB | MiB per 1k shares | full wait s | full fold s | daemon exit | available MiB before |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| baseline | 52,000 | 1,028 | 1,042 | 19.76 | 1.05 | – | clean (0) | 17,281 |
| fixed | 52,000 | 163 | 177 | 3.14 | 0.33 | 0.085 | clean (0) | 16,903 |
| baseline | 105,000 | 3,884 | 3,910 | 36.99 | 3.78 | – | clean (0) | 16,861 |
| fixed | 105,000 | 325 | 351 | 3.10 | 0.63 | 0.184 | clean (0) | 16,823 |
| fixed | 210,000 | 601 | 640 | 2.86 | 1.29 | 0.377 | clean (0) | 16,510 |
| fixed | 400,000 | 1,142 | 1,226 | 2.85 | 2.60 | 0.779 | clean (0) | 16,425 |

The baseline binary has no `metrics` field (blank columns); its wait is
whole-daemon time.

### Every phase

| binary | shares | phase | records | request MiB | write s | wait s | read s | parse s | fold s | serialize s | residual s (approx.) | RSS after MiB | peak so far MiB | status | self-check | oracle |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| baseline | 52,000 | full | 52,000 | 22.5 | 0.03 | 1.05 | 0.02 | – | – | – | – | 623 | 668 | prepared | yes | yes |
| baseline | 52,000 | advance_small | 16 | 0.0 | 0.00 | 0.14 | 0.00 | – | – | – | – | 625 | 668 | prepared | yes | yes |
| baseline | 52,000 | advance_large | 1,043 | 0.5 | 0.00 | 0.14 | 0.00 | – | – | – | – | 616 | 668 | prepared | yes | yes |
| baseline | 52,000 | recenter | 52,000 | 22.5 | 0.04 | 0.71 | 0.02 | – | – | – | – | 983 | 1,028 | prepared | yes | yes |
| baseline | 52,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.15 | 0.00 | – | – | – | – | 715 | 1,028 | prepared | yes | yes |
| fixed | 52,000 | full | 52,000 | 22.5 | 0.05 | 0.33 | 0.03 | 0.085 | 0.085 | 0.155 | 0.003 | 65 | 110 | prepared | yes | yes |
| fixed | 52,000 | advance_small | 16 | 0.0 | 0.00 | 0.14 | 0.00 | 0.000 | 0.001 | 0.141 | 0.001 | 66 | 110 | prepared | yes | yes |
| fixed | 52,000 | advance_large | 1,043 | 0.5 | 0.00 | 0.15 | 0.00 | 0.002 | 0.005 | 0.140 | 0.000 | 69 | 110 | prepared | yes | yes |
| fixed | 52,000 | recenter | 52,000 | 22.5 | 0.05 | 0.24 | 0.01 | 0.058 | 0.063 | 0.114 | 0.004 | 141 | 163 | prepared | yes | yes |
| fixed | 52,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.12 | 0.00 | 0.000 | 0.001 | 0.103 | 0.012 | 141 | 163 | prepared | yes | yes |
| baseline | 105,000 | full | 105,000 | 45.4 | 0.06 | 3.78 | 0.06 | – | – | – | – | 2,417 | 2,508 | prepared | yes | yes |
| baseline | 105,000 | advance_small | 16 | 0.0 | 0.00 | 0.29 | 0.00 | – | – | – | – | 2,418 | 2,508 | prepared | yes | yes |
| baseline | 105,000 | advance_large | 1,043 | 0.5 | 0.00 | 0.29 | 0.00 | – | – | – | – | 2,399 | 2,508 | prepared | yes | yes |
| baseline | 105,000 | recenter | 105,000 | 45.4 | 0.10 | 2.36 | 0.04 | – | – | – | – | 3,760 | 3,884 | prepared | yes | yes |
| baseline | 105,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.37 | 0.00 | – | – | – | – | 2,091 | 3,884 | prepared | yes | yes |
| fixed | 105,000 | full | 105,000 | 45.4 | 0.06 | 0.63 | 0.06 | 0.137 | 0.184 | 0.308 | 0.005 | 128 | 219 | prepared | yes | yes |
| fixed | 105,000 | advance_small | 16 | 0.0 | 0.00 | 0.27 | 0.00 | 0.000 | 0.002 | 0.273 | 0.000 | 129 | 219 | prepared | yes | yes |
| fixed | 105,000 | advance_large | 1,043 | 0.5 | 0.00 | 0.29 | 0.00 | 0.002 | 0.004 | 0.281 | 0.000 | 132 | 219 | prepared | yes | yes |
| fixed | 105,000 | recenter | 105,000 | 45.4 | 0.08 | 0.53 | 0.04 | 0.138 | 0.139 | 0.252 | 0.006 | 246 | 325 | prepared | yes | yes |
| fixed | 105,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.25 | 0.00 | 0.000 | 0.001 | 0.221 | 0.026 | 246 | 325 | prepared | yes | yes |
| fixed | 210,000 | full | 210,000 | 91.0 | 0.15 | 1.29 | 0.12 | 0.270 | 0.377 | 0.634 | 0.012 | 253 | 435 | prepared | yes | yes |
| fixed | 210,000 | advance_small | 16 | 0.0 | 0.00 | 0.55 | 0.00 | 0.000 | 0.003 | 0.548 | 0.000 | 255 | 435 | prepared | yes | yes |
| fixed | 210,000 | advance_large | 1,043 | 0.5 | 0.00 | 0.58 | 0.00 | 0.002 | 0.006 | 0.575 | 0.000 | 258 | 435 | prepared | yes | yes |
| fixed | 210,000 | recenter | 210,000 | 91.0 | 0.17 | 1.04 | 0.11 | 0.281 | 0.282 | 0.465 | 0.013 | 442 | 601 | prepared | yes | yes |
| fixed | 210,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.48 | 0.00 | 0.000 | 0.002 | 0.419 | 0.054 | 443 | 601 | prepared | yes | yes |
| fixed | 400,000 | full | 400,000 | 173.6 | 0.28 | 2.60 | 0.29 | 0.574 | 0.779 | 1.231 | 0.020 | 479 | 826 | prepared | yes | yes |
| fixed | 400,000 | advance_small | 16 | 0.0 | 0.00 | 1.10 | 0.00 | 0.000 | 0.011 | 1.093 | 0.000 | 483 | 826 | prepared | yes | yes |
| fixed | 400,000 | advance_large | 1,043 | 0.5 | 0.00 | 1.08 | 0.00 | 0.001 | 0.007 | 1.067 | 0.001 | 486 | 826 | prepared | yes | yes |
| fixed | 400,000 | recenter | 400,000 | 173.6 | 0.26 | 2.13 | 0.17 | 0.578 | 0.611 | 0.918 | 0.021 | 838 | 1,142 | prepared | yes | yes |
| fixed | 400,000 | recenter_advance | 16 | 0.0 | 0.00 | 1.03 | 0.00 | 0.000 | 0.004 | 0.858 | 0.168 | 840 | 1,142 | prepared | yes | yes |

Advance stats returned by both binaries at every size: `advance_small` added
16 / expired 16 / touched 2 pages; `advance_large` added 1,043 / expired 1,043
/ touched 2; the re-center retained exactly ¾ of the records (39,000; 78,750;
157,500; 300,000), matching the oracle's counts. An earlier host sweep with
the pre-hardening harness (same binaries) gave the same peak figures when
rounded to MiB and comparable timings.

### The Python oracle beside it (host, CPython 3.12.3)

| shares | full fold s | re-center fold s | records (full / re-center) | full digest |
|---:|---:|---:|---:|---|
| 52,000 | 0.41 | 0.31 | 52,000 / 39,000 | `cff331dccb93a7ff…` |
| 105,000 | 0.82 | 0.65 | 105,000 / 78,750 | `cfdf4a0ed72673b7…` |
| 210,000 | 1.58 | 1.26 | 210,000 / 157,500 | `e38ca96a869cee23…` |
| 400,000 | 3.27 | 2.73 | 400,000 / 300,000 | `30b53c3312705f2c…` |

For scale only: the Rust fold at 210k is 0.38 s against 1.58 s in-process
Python, and the Python fold holds the GIL (`window_pipeline_gil_scaling.md`).

---

## 3. Where the daemon's time goes now

Fixed binary, `full` phase, host:

| shares | write (transport) | parse | fold | serialize | residual (approx.) | read (transport) | Rust processing / round trip |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 52,000 | 0.05 | 0.09 | 0.09 | 0.16 | 0.00 | 0.03 | 0.33 / 0.41 s |
| 105,000 | 0.06 | 0.14 | 0.18 | 0.31 | 0.01 | 0.06 | 0.63 / 0.75 s |
| 210,000 | 0.15 | 0.27 | 0.38 | 0.63 | 0.01 | 0.12 | 1.28 / 1.56 s |
| 400,000 | 0.28 | 0.57 | 0.78 | 1.23 | 0.02 | 0.29 | 2.58 / 3.17 s |

Between 52k and 400k every measured term grows in proportion to the window
(a 7.7× window gives 6.8× parse, 9.2× fold, 7.9× serialize). The claim this
PR makes is about **paging and page materialization**: they are now linear
in the retained count. The fold as a whole still sorts the eligible records
(`sort_by_key`, O(N log N)) and validates them in one pass; the parse and the
canonical serialization are proportional to the request and response bytes.
After the fix the fold is ~29% of daemon time; canonical serialization
(per-record fragment concatenation plus a SHA-256 over 91–174 MiB) is ~49% and
the request parse ~21%. The blocking-pipe transport measured here is under
0.3 s per direction at 400k; the coordinator's own transport after #237 is
readiness-driven and was measured in `window_builder_pipe_latency.md`.

Two residual daemon costs are visible and are **not** addressed by this PR:

- **Every advance re-hashes the whole window.** `serialize` for a 16-record
  advance is 0.55 s at 210k and 1.09 s at 400k, because `advance` recomputes
  `canonical_digest_hex()` over every retained fragment to name the new
  generation. The advance itself (`fold`) is 3–11 ms. This is daemon-side
  latency per window generation, off the coordinator's GIL, and proportional
  to the window; an incremental digest would be separate work.
- **The request parse** (`serde_json` into `Vec<AcceptedShare>`) is 0.27 s at
  210k. It is proportional to the request's own bytes; a more compact wire
  encoding would be a protocol change outside this lane.

On the old binary's time: at 105k the baseline answers in 3.78 s against the
fixed 0.63 s. No profiler was attached, so the 3.15 s difference is not
attributed to any one operation by measurement; the inference is that the
repeated tail copying and allocation the old loop performed (about 10.8 M
record moves of 224 bytes each, and the same volume of allocation and
release, §4) accounts for it, since the fold's other work is unchanged
between the two binaries.

---

## 4. Why it was quadratic, and why the numbers say the paging fix is complete

`AcceptedShare` is 224 bytes (`size_of`, x86_64). With N retained records and
P = 512, the old loop made ⌈N/P⌉ pages whose record vectors kept capacities
N, N−P, N−2P, …; aggregate capacity Σ(N−kP) records × 224 B:

| N | pages | old aggregate capacity | old record-vector storage (MiB) | measured RSS after `full` (baseline) | new record-vector storage (MiB) |
|---:|---:|---:|---:|---:|---:|
| 52,000 | 102 | 2,666,688 records | 569.7 | 623 MiB | 11.1 |
| 105,000 | 206 | 10,819,120 | 2,311.2 | 2,417 MiB | 22.4 |
| 210,000 | 411 | 43,171,440 | 9,222.4 | 9,613 MiB peak (prior synthetic measurement, §5) | 44.9 |
| 400,000 | 782 | 156,450,048 | 33,421.3 | SIGKILL at 15,623 MiB before completion (prior synthetic measurement) | 85.4 |

The capacity model counts only record-vector backing storage, in MiB
(bytes / 2**20). RSS also includes heap-owned record strings, canonical
fragments, input/output buffers, and allocator overhead, so it is not an
exact RSS prediction. The smaller completed baseline runs and the structural
capacity tests establish the quadratic paging defect without requiring a
leak or a second generation. The 400k run was killed before allocating the
full window: its observed RSS is a lower bound on that run's memory demand,
not a completed measurement validating the 33,421.3 MiB storage estimate.

The fragment vectors were never affected (`collect` from an exact-size
iterator). After the fix, record-vector storage and the other window buffers
are proportional to the window; the measured 2.9–3.1 MiB per 1k shares from
52k to 400k is consistent with that bound.

The structural regression tests in `window.rs`
(`full_snapshot_paging_keeps_aggregate_capacity_linear_across_sizes`,
`full_snapshot_paging_after_the_cutoff_allocates_only_retained_records`,
`advance_shares_untouched_pages_and_pages_large_deltas_linearly`,
`digest_items_and_payout_totals_are_independent_of_page_size`,
`empty_snapshots_and_page_size_validation`) assert exactly this bound — no
page vector's capacity above `page_size`, aggregate at most one page's worth
per page — across page sizes 1, 3, 7 and 512 at empty input, one record, one
short of a page, exact single and multiple boundaries, one past a boundary
and long partial tails, plus the cutoff, unsorted input, page-size and weight
validation, immutable page sharing across an advance and a full-rebuild
digest match. Run against the unmodified base implementation (same tests
appended to `0bc7fa6`'s `window.rs`), four of them fail at the capacity
assertion, e.g. *"page 0 of 3 retains capacity 234 for 100 records
(page_size 100)"*; the other 17 window tests pass on both.

---

## 5. The 210k/400k baseline is a prior synthetic measurement, cited not repeated

The old binary was measured here only at 52k and 105k, sizes whose predicted
footprint (§4) fits under the 6 GiB address-space limit with margin on a
shared host. The 210k and 400k old-paging figures in the headline are the
single-run blocking-pipe measurements recorded in
`tests/perf/window_pipeline_gil_scaling.md` §9 on the same host, same fixture
and same page size, made while investigating #236 and before this PR: peak
RSS 9,613 MiB with a 12.2 s response at 210k; 15,623 MiB then SIGKILL after
~24 s at 400k, with ~15 GB available. They are synthetic development-host
measurements, not production observations. Re-running the 400k case would
have risked exhausting the host again. The completed smaller runs and the
structural capacity tests suffice to reproduce the defect; the killed 400k
run does not measure its full allocation demand.

---

## 6. Correctness gates run with the rebuilt binary

- `cargo test --locked -p qbit-prism --lib window`: 21 tests, ok (16
  pre-existing plus the five above).
- `cargo test --locked --workspace --all-targets` (the CI gate): 241 tests
  across the workspace, ok.
- `QBIT_WINDOW_PIPELINE_PARITY_ADAPTER=rust-daemon PRISM_TOOL_BIN_DIR=$PWD/target/release python3 -m tests.window_pipeline_parity_gate`
  against the fixed binary: 7 tests, ok — 35 full preparations and 16
  `advance()` calls across 15 frozen cases, covering canonical bytes, digests,
  spool tails, advance stats, every rejection category and the declared
  integer domain.
- `python3 -m unittest tests.test_window_pipeline_parity_gate tests.test_prism_window_pipeline_rust tests.test_prism_payout_window_daemon_recenter`:
  43 tests, ok (coordinator-side contracts, including the #207 re-center
  behaviour, against the fake daemon; these do not spawn the binary).
- `python3 -m unittest tests.test_window_paging_allocation_harness`: 5
  tests, ok — the driver's exchange deadline kills and reaps a daemon that
  never handshakes, one that stalls after reading the request, and one that
  never reads it (request writer blocked on a full pipe), with pipes and the
  stderr capture closed on every path, and the run recorded as a timeout.
- This driver: 40/40 phases prepared, 40/40 self-consistent, 40/40 oracle
  matches (host and image), the two host binaries agreeing with each other
  and with Python at 52k and 105k.

---

## 7. Production coordinator image, built and measured locally

`lab/prism/Dockerfile` (the coordinator's production image recipe:
`python:3.14-slim-trixie`, Debian's rustc/cargo, `cargo build --locked
--release -p qbit-prism --bins`, `MALLOC_ARENA_MAX=2`) was built on this host
from a disposable context holding only `Cargo.toml`, `Cargo.lock`, `crates/`,
`lab/auxpow/` and `lab/prism/` copied from the tree at `527b3ac`:

| | |
|---|---|
| Image | `prism-240-local:test`, id `f36022841b35`, 1.36 GB on disk; never pushed |
| Interpreter / toolchain inside | CPython 3.14.7; rustc 1.85.0 (4d91de4e4 2025-02-17), cargo 1.85.0; Debian GNU/Linux 13 (trixie), glibc 2.41 |
| Source verification | `/app/crates/qbit-prism/src/window.rs` sha256 `d1529288d117af52…` and `/app/crates/qbit-prism/src/bin/qbit-prism-build-audit-bundle.rs` `68ab080a746e3d18…` — identical to `git show 527b3ac:<path>` |
| Image binary | `/app/target/release/qbit-prism-build-audit-bundle` sha256 `50fda6ac0f8e44450545f3b16592b96622ab0c7bab4c7b80fed827d32eabab33`, 2,143,032 bytes (different compiler, so a different binary from the host build of the same source) |
| Run | `docker run --rm --memory 8g -v $PWD/tests:/app/tests:ro … prism-240-local:test python3 tests/perf/window_paging_allocation.py --daemon-binary /app/target/release --sizes 210000,400000`; driver and oracle under the image's CPython 3.14.7 with `MALLOC_ARENA_MAX=2`; daemon under the same `RLIMIT_AS` and deadline as on the host |

| binary | shares | peak RSS MiB (VmHWM) | peak VSZ MiB | MiB per 1k shares | full wait s | full fold s | daemon exit | available MiB before |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| image | 210,000 | 601 | 640 | 2.86 | 1.46 | 0.473 | clean (0) | 16,682 |
| image | 400,000 | 1,142 | 1,226 | 2.85 | 2.60 | 0.847 | clean (0) | 16,429 |

| binary | shares | phase | records | request MiB | write s | wait s | read s | parse s | fold s | serialize s | residual s (approx.) | RSS after MiB | peak so far MiB | status | self-check | oracle |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| image | 210,000 | full | 210,000 | 91.0 | 0.17 | 1.46 | 0.11 | 0.334 | 0.473 | 0.646 | 0.012 | 253 | 435 | prepared | yes | yes |
| image | 210,000 | advance_small | 16 | 0.0 | 0.00 | 0.60 | 0.00 | 0.000 | 0.004 | 0.595 | 0.000 | 255 | 435 | prepared | yes | yes |
| image | 210,000 | advance_large | 1,043 | 0.5 | 0.00 | 0.56 | 0.00 | 0.001 | 0.006 | 0.556 | 0.000 | 259 | 435 | prepared | yes | yes |
| image | 210,000 | recenter | 210,000 | 91.0 | 0.16 | 1.17 | 0.09 | 0.282 | 0.391 | 0.480 | 0.013 | 442 | 601 | prepared | yes | yes |
| image | 210,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.49 | 0.00 | 0.000 | 0.002 | 0.430 | 0.059 | 444 | 601 | prepared | yes | yes |
| image | 400,000 | full | 400,000 | 173.6 | 0.23 | 2.60 | 0.30 | 0.529 | 0.847 | 1.202 | 0.021 | 479 | 826 | prepared | yes | yes |
| image | 400,000 | advance_small | 16 | 0.0 | 0.00 | 1.05 | 0.00 | 0.000 | 0.005 | 1.040 | 0.000 | 483 | 826 | prepared | yes | yes |
| image | 400,000 | advance_large | 1,043 | 0.5 | 0.00 | 1.09 | 0.00 | 0.001 | 0.006 | 1.078 | 0.000 | 486 | 826 | prepared | yes | yes |
| image | 400,000 | recenter | 400,000 | 173.6 | 0.31 | 2.12 | 0.16 | 0.517 | 0.679 | 0.900 | 0.021 | 838 | 1,142 | prepared | yes | yes |
| image | 400,000 | recenter_advance | 16 | 0.0 | 0.00 | 0.90 | 0.00 | 0.000 | 0.004 | 0.784 | 0.117 | 840 | 1,142 | prepared | yes | yes |

The image's peak and after-phase resident sizes are identical to the host
build's at both sizes — the daemon is single-threaded and its footprint is
page vectors and fragment buffers, so neither the arena policy nor the
compiler moved it. Its fold is 0.1 s slower at 210k (rustc 1.85 codegen
against 1.97 on the host), still under half a second. The CPython 3.14.7
oracle produced the same digests as CPython 3.12.3 on the host
(`e38ca96a869cee23…` at 210k, `30b53c3312705f2c…` at 400k), and the image's
in-process fold at 210k took 2.11 s against 1.58 s on the host interpreter.

This is the production image's interpreter, allocator setting and binary
exercising the daemon protocol; it is **not** a coordinator run (§8).

---

## 8. Limits

- Single run per configuration on a shared KVM development host. Timings are
  wall-clock with load 0.8–1.7; the memory figures are kernel high-water
  marks and are not sensitive to load. The earlier pre-hardening host sweep
  reproduced every peak figure when rounded to MiB.
- Synthetic fixture: uniform difficulty, one share per second, 200
  identities. Production windows have mixed difficulties and credit policies;
  neither changes the paging arithmetic, which depends only on record count
  and page size.
- Not a coordinator run, on the host or in the image: no ledger read, record
  conversion, mirror validation, lease heartbeat, Stratum job build or
  restart loop. The #236 gates — cold-start time to first usable Stratum
  work at the production window with the heartbeat running, builder RSS in
  the deployed container, and the testnet/mainnet soak — remain operator
  gates and are not claimed here.
- The image was built on this host from the branch's sources; it is not the
  registry image the deployment pulls, though it is the same Dockerfile.
- `RLIMIT_AS` bounds virtual size, which for this daemon tracked resident
  size within 1–8% (peak VSZ column); it is a safety net, not a measurement.

---

## Re-running

```
cargo build --release --locked -p qbit-prism --bin qbit-prism-build-audit-bundle
# an old binary for the baseline rows, built from the base commit into its own target dir:
git worktree add /tmp/base 0bc7fa6c023082df81d23137ebabf4ff96f24683 && \
  (cd /tmp/base && CARGO_TARGET_DIR=/tmp/base-target cargo build --release --locked -p qbit-prism --bin qbit-prism-build-audit-bundle)

python3 tests/perf/window_paging_allocation.py \
    --daemon-binary target/release/qbit-prism-build-audit-bundle \
    --baseline-binary /tmp/base-target/release/qbit-prism-build-audit-bundle \
    --sizes 52000,105000,210000,400000 --baseline-sizes 52000,105000 \
    --json window_paging_allocation.json

python3 tests/perf/window_paging_allocation.py --sizes 210000 --skip-oracle   # fixed binary only, quick

# inside a locally built coordinator image (context: Cargo.toml, Cargo.lock, crates/, lab/auxpow/, lab/prism/):
docker build -f lab/prism/Dockerfile -t prism-240-local:test <context-dir>
docker run --rm --memory 8g -v "$PWD/tests:/app/tests:ro" -v "$PWD/out:/out" prism-240-local:test \
    python3 tests/perf/window_paging_allocation.py --daemon-binary /app/target/release --daemon-label image \
    --sizes 210000,400000 --stderr-dir /tmp/stderr --json /out/container.json
```

Check `MemAvailable` first; the driver refuses a run below the limit plus
margin (`--daemon-memory-limit-mb`, `--memory-margin-mb`) and records the
skip, and kills a daemon that stalls past `--exchange-timeout`. Never point
`--baseline-binary` at sizes above ~105k on a host without tens of gigabytes
to spare.
