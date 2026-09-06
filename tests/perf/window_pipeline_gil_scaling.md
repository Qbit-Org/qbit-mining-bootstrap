# GIL scaling of the PRISM payout-window materialization pipeline

Closes the Known gap #162 recorded on #143: *"GIL behaviour at megabyte buffer sizes is
unmeasured."* Issue #131 attributes ~135 ms per window materialization to **GIL-held**
Python at today's 21,868-share window (~555 ms at 100k, ~2.2 s at 400k). That attribution
was asserted for this pipeline, never measured on it — and it matters, because #143 §1
bracketed CPython's GIL-release threshold at **1–2 KiB** while every buffer here is
megabytes.

Driver: `tests/perf/window_pipeline_gil_scaling.py`. No database, no network, no
coordinator, standard library only. `python3 tests/perf/window_pipeline_gil_scaling.py`
runs the sweep and prints these tables; `--json` captures the same data structured, and
`--render FILE` re-prints the tables from a captured JSON.

This is an **on-demand instrument, not a test.** It asserts no thresholds and is
deliberately not named `test_*` so the discovery run never executes it (#160: a threshold
assertion on a shared runner is a flaky test in waiting). Nothing under `lab/` was
modified; it is imported read-only.

**Measured at `4c8ef4c`**, not the `2aa63cd` this task's contract names — the `2.x.x` tip
advanced by one commit during launch. That commit ("Make schema applies atomic…") touches
only `PsqlShareLedger`'s psql subprocess backend; `lab/prism/bundle_compiler.py` is
byte-identical between the two revisions and the `share_ledger.py` delta does not reach any
callable measured here. Every line number below was re-derived at `4c8ef4c`.

---

## Headline

**The premise weakens substantially. It does not fail, and it does not hold as stated.**

- The **canonical-JSON digest is already fully parallel** — 7.57 cores across 8 threads,
  statistically indistinguishable from the 1 MiB `hashlib.sha256` positive control (7.23)
  measured beside it. That row is *not* GIL time. Lead 1 is confirmed.
- The **fold is genuinely GIL-held** — 1.02 cores flat from 1→8 threads, at every window
  size. Lead 2 is confirmed, and the paging half (`json.dumps` per record) is 84% of it.
- The **spool term is mixed, but the mix does not rescue it**: 90–96% of it is
  `json.dumps` at 1.02 cores, and only the `os.write` half (1.0 ms of 37.3 ms) runs
  parallel at 7.01 cores. Lead 3 is confirmed in structure and refuted in significance —
  splitting the term buys ~1%.

Applying the measured per-stage release fractions to #131's own published milliseconds,
**23–25% of the profiled time is already running in parallel** (32 ms of 135 ms; 141 of
555; 537 of 2,158), essentially all of it the digest. The remaining ~75% is real GIL time.

That percentage is a weighted average carrying #131's own stage proportions, which this
host does not reproduce — weighted by *this* machine's costs the parallel share is 8.5%.
The per-stage verdicts transfer; the aggregate percentage should not be quoted without
§4's caveat.

Two attribution defects in #131's table, both material and both stated with the evidence in
§5: the four rows **double-count** ~5 ms because record→JSON runs *inside* the fold, and the
spool row's **byte annotations are canonical-JSON sizes, not spool sizes** — the real spool
payload is 5.5× smaller at every window size (1.49 MB, not 8 MB, today).

---

## 1. Machine, metric, and controls

### Machine

| | |
|---|---|
| CPU | Intel Core (Haswell, no TSX), 8 vCPU — KVM guest, 1 core/socket × 8 sockets, no SMT |
| SHA acceleration | **none** (`sha_ni` absent), so `hashlib.sha256` is software |
| Platform | Linux 6.8.0-106-generic, x86_64, glibc 2.39 |
| Python | 3.12.3 (main, Jun 19 2026) [GCC 13.3.0], CPython |
| GIL | standard build (`Py_GIL_DISABLED` = False) |
| Memory | 22 GB total, ~16 GB available; peak RSS during the sweep **7.6 GB** (400k × 8 threads) |
| Load | 0.27 at start, 2.10 at end; per-table ranges reported beside each table |
| Page size | 512 records (`DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE`) |

### Metric

**cores-used = `time.process_time_ns()` / `time.perf_counter_ns()`**, with the stage
running in N threads over N **independent inputs**. `process_time` sums CPU across every
thread in the process, which is exactly what separates "N threads made progress together"
from "N threads took turns under the GIL". 1.00 flat from 1→8 means fully GIL-held;
scaling toward N means the stage releases it.

`thread_time` — the headline clock in the sibling instrument
`tests/perf/serialize_block_share_cost.py` — is deliberately *not* used here: it is
per-thread and structurally cannot see the concurrency that is the whole question.

### Statistic and alternation

Median of **3 repetitions**, taken **alternating** (outer loop repetition, inner loop
configuration) rather than in blocks, so host drift lands on every configuration equally
instead of on whichever ran last. Run-to-run spread was very tight — every stage's
min-to-max across the 3 reps sits within 0.01 cores except `spool_write` at 400k
(6.71–7.53) and the digest at 100k (7.25–7.57).

### The wall-clock floor — a real trap, measured not assumed

Thread create/join is charged to wall clock but contributes almost no process CPU, so a
measurement whose wall time is close to the thread-start cost reads **low** on cores-used
regardless of GIL behaviour. On this rig, the *same* 1 MiB sha256 positive control reads:

| wall | cores-used, 8 threads |
|---|---|
| 0.25 s | 6.43 |
| 0.26 s | 5.33 |
| 0.90 s | 7.42 |
| 3.71 s | 7.42 |

This bias is **one-sided**: it pushes every stage toward 1.00, i.e. toward falsely
confirming "GIL-held". The driver therefore grows every configuration until its wall clock
clears `--min-wall-seconds` (default 1.0 s). My first-cut probe of this pipeline, before the
floor was enforced, reported the positive control at 6.43 — had I reported stage numbers
from that rig, the digest's parallelism would have been understated.

### Controls

Both controls run **in the same process, at the same thread counts, in the same
alternation** as the stages.

| control | N=1 | N=2 | N=4 | N=8 |
|---|---|---|---|---|
| **positive** — 1 MiB `hashlib.sha256` | 1.00 | 1.98 | 3.94 | **7.23** |
| **negative** — 20,000-iteration pure-Python loop | 1.00 | 1.00 | 1.01 | **1.02** |

Load average (1 m) during the controls: 0.71–1.00.

The positive control reaches **7.23 of #143's published 7.89 — a ratio of 0.916** — on an
8-vCPU KVM guest whose 8th core also carries the main thread and the OS. The negative
control pins at 1.02. **The rig reproduces #143 §1 and the stage numbers below are
reportable.**

The driver enforces this as a gate, not a footnote: `--control-floor-fraction` (default
0.75 of the nominal thread count — 6.0 cores on an 8-thread sweep) aborts the run and
withholds every stage number if the positive control does not reach it. That
gate fired for real during development — an early version built each thread's control
buffer with `bytes(n)` instead of `bytes([n])`, giving every thread a differently sized
buffer; the control collapsed to 3.71 and the run refused to report stages.

**Cores-used is normalized against the positive control measured on this host at the same
thread count**, not against the nominal thread count: an 8-vCPU guest cannot reach 8.00 even
on perfectly parallel work, so dividing by 8 would understate every stage. The reported
`released` fraction is `(cores − 1) / (control − 1)`, clamped to [0, 1].

---

## 2. Cores-used: stage × thread count × window size

Indented rows are sub-terms of the row above, not additional profile rows. `CPU ms` is
per-call interpreter CPU at N=1. `released` is at the highest thread count.

### 21,868 shares (today's window)

canonical JSON 8.34 MB (381 B/record, 43 pages, **189 KiB/page**) · spool payload 1.49 MB
(68 B/record) · input 35.8 MB/thread

| stage | N=1 | N=2 | N=4 | N=8 | CPU ms | released |
|---|---|---|---|---|---|---|
| `fold` | 1.00 | 1.00 | 1.01 | **1.02** | 220.8 | 0% |
| &nbsp;&nbsp;`fold_pages` | 1.00 | 1.00 | 1.01 | 1.02 | 186.1 | 0% |
| `digest` | 1.00 | 1.97 | 3.91 | **7.57** | 22.8 | **100%** |
| `to_prism_json` | 1.00 | 1.00 | 1.01 | **1.02** | 15.3 | 0% |
| `spool_acquire` | 1.00 | 1.03 | 1.05 | **1.06** | 37.3 | 1% |
| &nbsp;&nbsp;`spool_compact` | 1.00 | 1.00 | 1.01 | 1.02 | 33.6 | 0% |
| &nbsp;&nbsp;`spool_encode` | 1.00 | 1.00 | 1.01 | 1.02 | 0.1 | 0% |
| &nbsp;&nbsp;`spool_write` | 1.00 | 1.98 | 3.85 | **7.01** | 1.0 | 96% |
| *positive control* | 1.00 | 1.98 | 3.94 | 7.23 | | |
| *negative control* | 1.00 | 1.00 | 1.01 | 1.02 | | |

Load average (1 m) during this table: 1.32–1.62.

### 100,000 shares

canonical JSON 38.27 MB (383 B/record, 196 pages, 191 KiB/page) · spool payload 6.88 MB
(69 B/record) · input 87.3 MB/thread

| stage | N=1 | N=2 | N=4 | N=8 | CPU ms | released |
|---|---|---|---|---|---|---|
| `fold` | 1.00 | 1.01 | 1.01 | **1.02** | 1,067.6 | 0% |
| &nbsp;&nbsp;`fold_pages` | 1.00 | 1.01 | 1.01 | 1.02 | 896.0 | 0% |
| `digest` | 1.00 | 1.99 | 3.95 | **7.41** | 101.2 | **100%** |
| `to_prism_json` | 1.00 | 1.00 | 1.01 | **1.02** | 89.5 | 0% |
| `spool_acquire` | 1.00 | 1.02 | 1.04 | **1.05** | 176.1 | 1% |
| &nbsp;&nbsp;`spool_compact` | 1.00 | 1.00 | 1.01 | 1.02 | 157.0 | 0% |
| &nbsp;&nbsp;`spool_encode` | 1.00 | 1.01 | 1.01 | 1.02 | 0.9 | 0% |
| &nbsp;&nbsp;`spool_write` | 1.00 | 1.98 | 3.94 | **7.14** | 5.4 | 99% |
| *positive control* | 1.00 | 1.98 | 3.94 | 7.23 | | |
| *negative control* | 1.00 | 1.00 | 1.01 | 1.02 | | |

Load average (1 m) during this table: 1.40–1.82.

### 400,000 shares — **N=1 and N=8 only**

Swept at the endpoints only, as the task permits: eight independent 400k inputs cost
373 MB each and the sweep already peaked at 7.6 GB RSS. N=2 and N=4 were not measured at
this size. Given 1.02 at N=2 and N=4 at both smaller sizes and 1.02 at N=8 here, no
interior behaviour is in question — but it is not measured, and I am not claiming it.

canonical JSON 154.07 MB (385 B/record, 782 pages, 192 KiB/page) · spool payload 28.18 MB
(70 B/record) · input 373.4 MB/thread

| stage | N=1 | N=8 | CPU ms | released |
|---|---|---|---|---|
| `fold` | 1.00 | **1.02** | 4,260.4 | 0% |
| &nbsp;&nbsp;`fold_pages` | 1.00 | 1.02 | 3,727.0 | 0% |
| `digest` | 1.00 | **7.64** | 408.3 | **100%** |
| `to_prism_json` | 1.00 | **1.02** | 414.8 | 0% |
| `spool_acquire` | 1.00 | **1.04** | 710.1 | 1% |
| &nbsp;&nbsp;`spool_compact` | 1.00 | 1.01 | 679.7 | 0% |
| &nbsp;&nbsp;`spool_encode` | 1.00 | 1.02 | 5.5 | 0% |
| &nbsp;&nbsp;`spool_write` | 1.00 | **6.98** | 23.8 | 96% |
| *positive control* | 1.00 | 7.23 | | |
| *negative control* | 1.00 | 1.02 | | |

Load average (1 m) during this table: 1.11–1.66.

**The verdicts are flat in window size.** Nothing crosses a threshold between 8 MB and
154 MB of canonical JSON: the digest is parallel at all three sizes, the fold is GIL-held at
all three.

---

## 3. Stage-internal decomposition

### The fold splits into a cheap prefix and an expensive paging half

`IncrementalShareWindow.from_full_snapshot` (`lab/prism/share_ledger.py:364-430`) is a
`sorted()` plus a per-record validation loop, then `_IncrementalShareWindowPage.from_records`
(`:274-293`) per page.

| sub-term | 21,868 | 100k | 400k | cores @ N=8 |
|---|---|---|---|---|
| `fold` total | 220.8 ms | 1,067.6 ms | 4,260.4 ms | 1.02 |
| `fold_pages` (paging) | 186.1 ms | 896.0 ms | 3,727.0 ms | 1.02 |
| prefix (`sorted` + validation), by difference | 34.7 ms | 171.6 ms | 533.3 ms | — |

The paging half is **84–87%** of the fold and is where `to_prism_json`, the per-record
`json.dumps`, and `b",".join` live. Both halves are GIL-held; there is no parallel
sub-term to recover here.

### The spool splits into `json.dumps` and `os.write` — and `os.write` is 3%

`_ShareWindowSerialization.acquire_spooled_tail` (`lab/prism/bundle_compiler.py:250-293`)
calls `compact_fragments` (`:222-248`), encodes both fragments to UTF-8, then writes them to
a `tempfile.TemporaryFile`. Separated by driving each shipped sub-step against pre-built
inputs held constant, so each measurement isolates one primitive:

| sub-term | what it is | 21,868 | share of term | cores @ N=8 | verdict |
|---|---|---|---|---|---|
| `spool_compact` | `_compact_share_payload` + 2 × `json.dumps` | 33.6 ms | 90% | 1.02 | **GIL-held** |
| `spool_encode` | `str.encode("utf-8")` ×2 | 0.1 ms | 0.3% | 1.02 | GIL-held |
| `spool_write` | `TemporaryFile` write/flush/seek | 1.0 ms | 2.7% | **7.01** | **GIL-released** |
| residual | instance construction, lock, teardown | 2.6 ms | 7% | — | — |
| **`spool_acquire`** | **all of the above** | **37.3 ms** | 100% | **1.06** | **GIL-held** |

At 400k the same split holds: `spool_compact` 679.7 ms (96% of the 710.1 ms term) at 1.01
cores, `spool_write` 23.8 ms (3.3%) at 6.98 cores.

**Lead 3 is structurally right and practically wrong.** The term *is* mixed, and `os.write`
*does* release the GIL exactly as predicted — but it is 3% of the term. Splitting it changes
the spool row's parallel fraction from 0% to about 1%.

---

## 4. Splitting #131's profile into GIL-held and GIL-released ms

**Method for the split.** For each profile row, `released_fraction = (cores_used − 1) /
(positive_control_cores − 1)` measured at 8 threads on this host and clamped to [0, 1], then
`parallel_ms = published_ms × released_fraction` and `held_ms = published_ms − parallel_ms`.
The fraction — not the millisecond — is the portable quantity; see the caveat below, which
is important.

| profile row | #131 ms | released | **GIL-held ms** | **parallel ms** |
|---|---|---|---|---|
| **21,868 shares** | | | | |
| Window fold | 71 | 0% | **71** | 0 |
| Canonical-JSON digest | 31 | 100% | **0** | **31** |
| Spool serialization | 28 | 1% | **28** | 0 |
| Record→JSON conversion | 5 | 0% | **5** | 0 |
| **total** | **135** | | **103** | **32 (23%)** |
| **100,000 shares** | | | | |
| Window fold | 271 | 0% | **270** | 1 |
| Canonical-JSON digest | 139 | 100% | **0** | **139** |
| Spool serialization | 122 | 1% | **121** | 1 |
| Record→JSON conversion | 23 | 0% | **23** | 0 |
| **total** | **555** | | **414** | **141 (25%)** |
| **400,000 shares** | | | | |
| Window fold | 1,057 | 0% | **1,054** | 3 |
| Canonical-JSON digest | 530 | 100% | **0** | **530** |
| Spool serialization | 476 | 1% | **473** | 3 |
| Record→JSON conversion | 95 | 0% | **95** | 0 |
| **total** | **2,158** | | **1,621** | **537 (25%)** |

### The caveat that governs how much of that 23–25% transfers

**The percentage depends on #131's host balance, which this host does not reproduce.** The
per-stage *verdicts* are host-independent and robust. The *aggregate percentage* is not,
because it is a weighted average and the weights are #131's.

On #131's workstation the fold:digest ratio is 71:31 = **2.3**. On this VM it is
220.8:22.8 = **9.7** — this host is disproportionately slow at pure-Python bytecode relative
to software SHA-256. Running the identical split on *this host's own* milliseconds gives:

| | this host, 21,868 shares |
|---|---|
| GIL-held | 257.0 ms |
| parallel | 23.9 ms |
| **parallel share** | **8.5%** |

So: "the digest is fully parallel" is a measured fact that transfers. "23% of the 135 ms is
already parallel" is that fact re-weighted by #131's published proportions, and lands
anywhere between ~8% (this host's balance) and ~23% (#131's balance) depending on whose
CPU you weight it with. Both are reported; neither should be quoted without the other.

---

## 5. Attribution — what I charged where, and two defects in the profile

### Which callable I charged to each row

| #131 row | callable driven | location at `4c8ef4c` |
|---|---|---|
| Window fold | `IncrementalShareWindow.from_full_snapshot` | `lab/prism/share_ledger.py:364-430` |
| Canonical-JSON digest | `IncrementalShareJsonSequence.canonical_json_sha256` | `lab/prism/share_ledger.py:333-345` |
| Spool serialization | `_ShareWindowSerialization.acquire_spooled_tail` (cold, fresh instance per call) | `lab/prism/bundle_compiler.py:250-293` |
| Record→JSON conversion | `AcceptedShareRecord.to_prism_json` over every record | `lab/prism/share_ledger.py:212-229` |

Each is the shipped callable, driven directly — nothing is reimplemented or stubbed. The
spool stage constructs a **fresh** `_ShareWindowSerialization` per call because
`acquire_spooled_tail` memoizes; a reused instance would measure a dict lookup after the
first call. Its descriptor is closed through the shipped `retire_spool` /
`release_spooled_tail` teardown rather than left to the GC.

### Defect 1 — the four rows double-count record→JSON conversion

`_IncrementalShareWindowPage.from_records` opens with
`prism_json_records = tuple(record.to_prism_json() for record in records)`
(`lab/prism/share_ledger.py:279`). The per-record JSON conversion therefore happens
**inside** the fold. Measured on this host at 21,868 shares, `to_prism_json` over all
records is 15.3 ms and sits within `fold_pages`' 186.1 ms — it is 8% of the paging half, and
11% at 400k.

Summing all four of #131's rows to a "Total GIL-held" therefore counts that work twice. On
#131's own numbers the overstatement is the full 5 ms row at 21,868 (135 → ~130), 23 ms at
100k, and 95 ms at 400k. It does not change any verdict — the term is GIL-held either way —
but the totals are ~4% high, and the "Record→JSON conversion" row is not an independent term
that could be removed or migrated separately from the fold.

### Defect 2 — the spool row's byte annotations are canonical-JSON sizes, not spool sizes

#131 annotates the spool row "28 ms (8 MB)", "122 ms (37 MB)", "476 ms (151 MB)". Measured:

| window | #131's spool-row bytes | **actual spool payload** | **canonical JSON** | canonical ÷ #131 |
|---|---|---|---|---|
| 21,868 | 8 MB | **1.49 MB** | 8.34 MB | **1.043** |
| 100,000 | 37 MB | **6.88 MB** | 38.27 MB | **1.034** |
| 400,000 | 151 MB | **28.18 MB** | 154.07 MB | **1.020** |

#131's figures match the **canonical-JSON** size to within 2–4% at all three points, and
overstate the **actual spool payload** by 5.5× at all three. The spool writes the
*compact* form — `_compact_share_payload` (`lab/prism/bundle_compiler.py:123-153`)
deduplicates the (`miner_id`, `order_key`, `p2mr_program_hex`) identity triple into an index
and emits positional tuples — which is 68–70 B/record against canonical JSON's
381–385 B/record.

This does not change the GIL verdict (1.49 MB is still ~750× the 1–2 KiB release threshold),
but it matters for anyone sizing this work off #131's table: the premise's own framing —
"the spool is 8 MB today and 151 MB at 400k shares" — is off by 5.5×, and the multi-megabyte
buffer whose GIL behaviour was in question is the *digest's* input, not the spool's.

### Sensitivity: a realistic `share_id` moves the bytes, not the verdicts

Re-run at 21,868 shares with `--share-id-shape production`, which swaps the benchmark's
`bench-share-N` for the `username:block_hash_hex` form `lab/prism/share_writer.py:284`
actually builds (2 repetitions, N=1 and N=8, same controls: positive 7.23, negative 1.02):

| | benchmark shape | production shape |
|---|---|---|
| canonical JSON | 8.34 MB (381 B/record) | **9.84 MB** (450 B/record) |
| spool payload | 1.49 MB (68 B/record) | **2.98 MB** (136 B/record) |
| `fold` @ N=8 | 1.02 | **1.02** |
| `digest` @ N=8 | 7.57 | **7.43** |
| `spool_acquire` @ N=8 | 1.06 | **1.08** |
| `spool_write` @ N=8 | 7.01 | **7.31** |

The spool payload doubles — the `share_id` is per-share and is not deduplicated by the
identity index — and every GIL verdict is unchanged. Note this also *widens* the gap in
Defect 2 rather than closing it: with a realistic `share_id` the spool payload is 2.98 MB
against #131's 8 MB, and the canonical JSON overshoots to 9.84 MB. The benchmark shape is
the closer match to #131's byte annotations, which is consistent with #131 having profiled a
benchmark-shaped window.

### Not a defect, but worth stating

I could not reproduce #131's *absolute* milliseconds and did not try to: this VM is ~3.1×
slower than that workstation on the fold and ~0.74× as slow on the digest. The instrument
measures concurrency behaviour, which is what was unmeasured; #131's per-stage costs on its
own host are not in dispute here.

---

## 6. My reading

**The premise weakens substantially. It does not fail.**

Stated plainly, because a negative result is the useful outcome here: **the
canonical-JSON digest is not GIL-held, and #131 counts it as GIL-held.** At 8 threads it
runs at 7.57 cores against a 7.23-core positive control measured beside it — it is, to the
resolution this host offers, perfectly parallel, and it is the second-largest row in the
profile at every window size. Lead 1 was right. `hashlib` releases the GIL around
189 KiB-per-`update()` page buffers exactly as #143 §1's 1–2 KiB threshold predicts, and
the paged design of `canonical_json_sha256` — feeding pre-encoded per-page buffers into one
`sha256` — is what makes that possible.

But the majority of the profile survives the measurement intact. The fold is GIL-held at
1.02 cores at every size, and it is the largest row. The spool is GIL-held at 1.06, and the
`os.write` escape hatch that lead 3 predicted is real but worth 3% of the term. Between
them, fold + spool are 99 ms of the 135 ms, and they are genuinely serialized.

So the honest summary is: **~75% of #131's ~135 ms is real GIL time; ~25% of it (the digest)
was never GIL time and should not have been counted.** On this host's own cost balance the
parallel share is smaller — 8.5% — because the fold dominates here far more than it does on
#131's workstation; the fraction is weight-dependent, the per-stage verdicts are not.

One thing I want to flag as a judgment rather than a measurement: the profile's *shape* is
now different from what it looks like in #131. Three of the four rows are GIL-held and one
is not, and the one that is not is the one whose "megabyte buffer" framing motivated the
doubt in the first place. That is worth knowing before anyone reasons further from that
table. I am not drawing a conclusion about #131 itself, or about any migration — out of
scope for this task, and the next question (what the GIL-held remainder would cost to
change) is not one these numbers answer.

### What is noisy, and what is not

- **Not noisy.** Every verdict here is separated from its alternative by a factor of ~7.
  Run-to-run spread across 3 alternating repetitions is ≤0.01 cores for every GIL-held stage
  and ≤0.1 for the parallel ones. Nothing is close to a threshold.
- **The 100% figures are clamped.** The digest measured 7.57 / 7.41 / 7.64 against a
  7.23-core control, i.e. slightly *above* the ceiling; the released fraction saturates at
  1.0. Read it as "indistinguishable from fully parallel", not as a claim of exactly 100%.
  The digest and the control are both `hashlib.sha256`, so this is unsurprising — the digest
  does less per-byte Python work than the control's loop overhead.
- **Load was not zero.** 1.1–1.8 during the stage tables, most of which is the driver
  itself. The controls were re-measured in the same process under the same conditions and
  bracket the stages, so drift is visible rather than assumed.

---

## 7. Limits — stated as gates, not guesses

- **This is a developer KVM VM, not production.** 8 vCPUs, no SMT, **no SHA-NI**. A
  production host with SHA extensions would make the digest cheaper in absolute ms without
  changing its GIL behaviour; the parallel *fraction* of the profile would fall, not rise.
- **Nobody on this task has production access.** Every number here is synthetic-input,
  single-host. Nothing about real production window sizes, real miner counts, real
  concurrency, or real contention with the rest of the coordinator is measured or implied.
- **Records are synthetic**, shaped like `lab/prism/job_build_benchmark.py`'s defaults
  (`--shares 21868 --miners 2`), whose values encode a live-host measurement. Two known
  distortions: `--miners 2` makes identity deduplication maximally effective, and the
  benchmark's `bench-share-N` `share_id` is far shorter than the real
  `username:block_hash_hex` form built at `lab/prism/share_writer.py:284` (the ledger's
  `length(share_id) >= 65` index predicate corroborates the length). `--share-id-shape
  production` re-runs the sweep with a realistic `share_id` — measured, and it moves the byte
  accounting without moving any verdict (§5).
- **400k was swept at N=1 and N=8 only** — N=2 and N=4 are unmeasured at that size.
- **`cores-used` measures GIL release, not goodness.** A stage reading 1.02 is serialized;
  that is all it says. It says nothing about whether that stage should change, or how.
- The **positive control lands at 0.916× #143's published 7.89** on this smaller host. All
  released-fractions are normalized against the control measured here, not against 8.00.

---

## 8. Re-running

```
python3 tests/perf/window_pipeline_gil_scaling.py                      # full sweep
python3 tests/perf/window_pipeline_gil_scaling.py --json > run.json    # capture
python3 tests/perf/window_pipeline_gil_scaling.py --render run.json    # re-print
python3 tests/perf/window_pipeline_gil_scaling.py --sizes 21868 --threads 1,8 --reps 1
```

**Run it once.** Two concurrent sweeps contend for the same cores and corrupt both — which
is why `--render` exists rather than a second measuring pass for the text format.

Useful flags: `--min-wall-seconds` (the amortization floor; lowering it biases every stage
toward 1.00), `--control-floor-fraction` (abort threshold for the positive control, as a
fraction of the nominal thread count so the gate stays valid at any `--threads`), `--reps`,
`--miners`, `--share-id-shape`, `--large-size-threads`. The driver skips a window size
outright, with a printed reason, if eight independent inputs would exceed 80% of available
memory.

The full sweep takes ~35 minutes and peaks at 7.6 GB RSS on this host.

---

## 9. #236 follow-up: bounded serialization and the monitor-lateness probe

**Context.** Issue #236's incident thread samples pointed at whole-window JSON encoding and
decoding on the coordinator's payout-window paths (~210k shares) while the writer-lease
monitor thread was late by 0.6–1.05 s. PR 2 of the #236 plan bounds every such call: the
daemon `prepare_window` request streams in record batches, the audit-builder compact tail is
encoded batch by batch into bounded chunks (spool, in-memory fallback and one-shot alike),
plain share lists digest through a streamed SHA-256, the daemon mirror's lazy parse walks one
record at a time through a chunked UTF-8 decoder, the found-block candidate identity digest
streams its `shares_json`, the one-shot canonical payload streams its share array, and the
transient per-share lists on the daemon prepare paths are released in bounded slices. All of
it is byte-identical to the historical output (pinned by the parity tests in
`test_prism_incremental_payout_window`, `test_prism_job_builder`,
`test_prism_window_pipeline_rust` and `test_prism_share_ledger`; the Rust `rust-daemon`
parity gate is unchanged and passes).

**Instrument.** `python3 tests/perf/window_pipeline_gil_scaling.py --latency-probe
[--daemon-binary PATH]` runs each phase once per window size in the main thread while a
monitor thread wakes every `WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS` (50 ms) and records
`wake - due`. Every bounded phase is paired with the historical whole-window call it
replaced, on the same input in the same process, and `gc.callbacks` attributes cyclic-GC
pauses to the phase they landed in. Two controls bracket the host: `idle_control` (main
thread asleep) is the monitor's own jitter and `busy_python_control` (pure-Python loop) is
the interpreter's switch interval. See `LATENCY_PROBE_RATIONALE`.

**Read this before the numbers.** Every figure below was taken on `alexdevbox1`, a shared
8-vCPU KVM guest on which two other #236 implementation agents were running their own tests
and benchmarks at the same time (load average 1.7–3.5 at the start of the runs). Nothing here
is an isolated or production measurement. Monitor lateness is what a 50 ms-cadence thread
observed, whatever the cause; it is **not** by itself a proof of GIL attribution. The
attribution argument is structural and comparative: a historical row's lateness equals that
row's own wall time to within a few percent, while the bounded row over the same bytes, in the
same process and under the same host load, stays at the controls' single-digit milliseconds.
That pattern is what a GIL-held C call produces and what host contention does not. One run
per phase (`--probe-reps 1`); a few percent of run-to-run drift is visible between the three
captures cited below.

### Final capture (bounded and historical phases, production share_id, 200 identities)

`max late`/`p99 late` in ms; `>250ms`/`>500ms` count monitor wakes later than the plan's
strict target and the configured scheduler slack; `gc full` counts generation-2
collections in the phase and `gc max` is the longest single collection pause. `residual`
rows isolate one deallocation cascade (a `del` of a whole parsed tuple or window), measured
on its own.

#### 210,000 shares (210,000 retained) -- canonical items 95.5 MB, compact tail 29.5 MB

| phase | kind | wall ms | wakes | max late ms | p99 late ms | >250ms | >500ms | gc full | gc max ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| idle_control | control | 1000 | 19 | 0.2 | 0.2 | 0 | 0 | 0 | 0.0 |
| busy_python_control | control | 1000 | 18 | 5.3 | 5.3 | 0 | 0 | 0 | 0.0 |
| record_conversion | bounded | 243 | 4 | 5.2 | 5.2 | 0 | 0 | 0 | 0.1 |
| fold_in_process | bounded | 1757 | 31 | 9.5 | 9.5 | 0 | 0 | 0 | 23.9 |
| fold_in_process_keep | bounded | 1704 | 29 | 20.7 | 20.7 | 0 | 0 | 0 | 23.1 |
| fold_release | residual | 38 | 0 | n/a | n/a | 0 | 0 | 0 | 0.0 |
| canonical_encode_stream | bounded | 993 | 17 | 7.4 | 7.4 | 0 | 0 | 0 | 0.0 |
| canonical_encode_whole | historical | 1131 | 1 | **1057.6** | 1057.6 | 1 | 1 | 0 | 0.0 |
| array_digest_stream | bounded | 1273 | 24 | 4.5 | 4.5 | 0 | 0 | 0 | 0.0 |
| array_digest_whole | historical | 1355 | 6 | **1030.4** | 1030.4 | 1 | 1 | 0 | 0.0 |
| compact_tail_stream | bounded | 425 | 7 | 6.0 | 6.0 | 0 | 0 | 0 | 0.1 |
| compact_tail_whole | historical | 427 | 3 | **249.6** | 249.6 | 0 | 0 | 0 | 0.2 |
| spool_write_stream | bounded | 427 | 7 | 6.8 | 6.8 | 0 | 0 | 0 | 0.1 |
| prepare_request_stream | bounded | 735 | 13 | 8.2 | 8.2 | 0 | 0 | 0 | 0.0 |
| prepare_request_whole | historical | 882 | 2 | **722.0** | 722.0 | 1 | 1 | 0 | 0.0 |
| mirror_validate | bounded | 1210 | 22 | 6.0 | 6.0 | 0 | 0 | 0 | 0.0 |
| mirror_parse_stream | bounded | 1408 | 25 | 5.7 | 5.7 | 0 | 0 | 0 | 0.4 |
| mirror_parse_release | residual | 66 | 1 | 16.5 | 16.5 | 0 | 0 | 0 | 0.0 |
| mirror_parse_whole | historical | 949 | 3 | **573.2** | 573.2 | 1 | 1 | 0 | 14.2 |
| mirror_parse_whole_release | residual | 57 | 1 | 7.3 | 7.3 | 0 | 0 | 0 | 0.0 |
| candidate_identity_stream | bounded | 1261 | 24 | 2.1 | 2.1 | 0 | 0 | 0 | 0.0 |
| candidate_identity_whole | historical | 1420 | 6 | **1020.3** | 1020.3 | 1 | 1 | 0 | 0.0 |
| oneshot_payload_stream | bounded | 740 | 13 | 7.8 | 7.8 | 0 | 0 | 0 | 0.0 |
| oneshot_payload_whole | historical | 771 | 1 | **714.7** | 714.7 | 1 | 1 | 0 | 0.0 |

#### 400,000 shares (400,000 retained) -- canonical items 182.1 MB, compact tail 56.2 MB

| phase | kind | wall ms | wakes | max late ms | p99 late ms | >250ms | >500ms | gc full | gc max ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| idle_control | control | 1000 | 19 | 0.2 | 0.2 | 0 | 0 | 0 | 0.0 |
| busy_python_control | control | 1000 | 18 | 7.7 | 7.7 | 0 | 0 | 0 | 0.0 |
| record_conversion | bounded | 294 | 5 | 5.2 | 5.2 | 0 | 0 | 0 | 0.1 |
| fold_in_process | bounded | 3239 | 56 | 46.8 | 40.5 | 0 | 0 | 0 | 49.5 |
| fold_in_process_keep | bounded | 3537 | 62 | 38.1 | 17.7 | 0 | 0 | 0 | 60.5 |
| fold_release | residual | 80 | 1 | 29.9 | 29.9 | 0 | 0 | 0 | 0.0 |
| canonical_encode_stream | bounded | 1951 | 34 | 8.4 | 8.4 | 0 | 0 | 0 | 0.0 |
| canonical_encode_whole | historical | 2384 | 2 | **2040.0** | 2040.0 | 1 | 1 | 0 | 0.0 |
| array_digest_stream | bounded | 2400 | 46 | 4.3 | 4.3 | 0 | 0 | 0 | 0.0 |
| array_digest_whole | historical | 2650 | 11 | **1942.9** | 1942.9 | 1 | 1 | 0 | 0.0 |
| compact_tail_stream | bounded | 709 | 12 | 5.8 | 5.8 | 0 | 0 | 0 | 0.1 |
| compact_tail_whole | historical | 737 | 5 | **438.2** | 438.2 | 1 | 0 | 0 | 0.2 |
| spool_write_stream | bounded | 760 | 13 | 5.8 | 5.8 | 0 | 0 | 0 | 0.1 |
| prepare_request_stream | bounded | 1452 | 25 | 7.6 | 7.6 | 0 | 0 | 0 | 0.0 |
| prepare_request_whole | historical | 1797 | 2 | **1488.9** | 1488.9 | 1 | 1 | 0 | 0.0 |
| mirror_validate | bounded | 2214 | 40 | 7.4 | 7.4 | 0 | 0 | 0 | 0.0 |
| mirror_parse_stream | bounded | 2718 | 49 | 9.4 | 9.4 | 0 | 0 | 0 | 0.4 |
| mirror_parse_release | residual | 113 | 1 | 62.9 | 62.9 | 0 | 0 | 0 | 0.0 |
| mirror_parse_whole | historical | 1813 | 3 | **1180.9** | 1180.9 | 2 | 1 | 0 | 21.8 |
| mirror_parse_whole_release | residual | 105 | 1 | 55.3 | 55.3 | 0 | 0 | 0 | 0.0 |
| candidate_identity_stream | bounded | 2496 | 48 | 4.8 | 4.8 | 0 | 0 | 0 | 0.0 |
| candidate_identity_whole | historical | 2704 | 11 | **1986.5** | 1986.5 | 1 | 1 | 0 | 0.0 |
| oneshot_payload_stream | bounded | 1419 | 25 | 6.9 | 6.9 | 0 | 0 | 0 | 0.0 |
| oneshot_payload_whole | historical | 1510 | 1 | **1449.9** | 1449.9 | 1 | 1 | 0 | 0.0 |


### Reading

- **Every historical whole-window call delays the monitor by about its own duration**:
  0.24–1.06 s at 210k and 0.5–2.2 s at 400k, past the 500 ms scheduler slack in all but one
  case. Those are the calls the incident's thread samples caught, and every one of them
  is gone from the coordinator's paths.
- **Every bounded replacement stays at the controls' level** (≤ 10 ms) at both sizes, with
  the fold as the one exception at 17–46 ms: its `gc max` column shows that is a cyclic-GC
  pause (generation 0/1 collections over the hundreds of thousands of new records and
  dicts), not a JSON call.
- **The remaining single stretches are deallocation cascades, not parsing.** Before the
  key-sharing change, an attribution capture on the same fixture had the streamed mirror
  parse itself late by at most 5.5 ms (210k) and 8.2 ms (400k) while dropping the parsed
  tuple afterwards took 140 ms wall / 90 ms late and 264 ms / 214 ms respectively: CPython
  freeing a few hundred thousand dicts and their private key strings in one refcount
  cascade. Two mitigations went into this PR from that measurement. The walker now shares
  one string object per key across records (`object_hook`), exactly as a whole-array
  `json.loads` does through its scanner memo, which returns the parsed representation to
  the whole-array footprint (105 MB vs 169 MB retained at 100k in a `tracemalloc` check)
  and brings the release down to the `mirror_parse_release` rows above (66 ms / 17 ms late
  at 210k, 113 ms / 63 ms at 400k, on par with `json.loads`' own release). And the
  transient `records`/`records_json` lists on the daemon prepare paths are emptied in
  2,048-entry slices (`release_share_list_incrementally`) instead of one cascade. What
  remains unbounded is the release of a long-lived parsed daemon sequence or in-process
  window when the artifact rotates (`fold_release`, `mirror_parse_release`); both are
  measured above and stay under the strict target at both sizes.
- **CPU cost is unchanged or lower.** Batched `json.dumps` over 512 records costs less than
  one call over the whole array (the canonical encode, digest, request and candidate rows),
  because the batch strings stay cache-resident. The streamed mirror parse costs ~30% more
  than one `json.loads` (`raw_decode` per record); that path only runs for found-block
  consumers.

### The daemon round trip (first capture, `probe-236.json`, `--daemon-binary` set)

| size | phase | wall ms | Python CPU ms | monitor max late | write | read_line | read_exact |
|---|---|---:|---:|---:|---:|---:|---:|
| 210k | daemon_prepare_full | 39,832 | 1,547 | 62.5 ms | 24,570 | 13,833 | 415 |
| 210k | daemon_prepare_advance (16 records) | 571 | 4 | 1.1 ms | 0 | 571 | 0 |
| 400k | daemon_prepare_full | 60,683 | 2,404 | 62.5 ms | 38,416 | 20,346 | — (daemon exited) |

The round trip is transport- and daemon-bound, not encode-bound: at 210k the streamed
request encode is 0.83 s of Python CPU, but the shipped write helper needs 24.6 s to move
95.5 MB because it sleeps 20 ms whenever the 64 KiB pipe is full (PR 3's lane), and
`read_line` then waits 13.8 s for the daemon. The monitor stayed within 63 ms throughout,
because the writer spends its time asleep. Writing the same request through blocking pipes
outside the coordinator (`repro_400k_daemon.py`, same fixture) took 1.0 s and the daemon
answered 12.2 s later, so ~12 s of the 210k cold preparation is daemon-side work —
consistent with the incident's 47.5–48.8 s `daemon_prepare` being transport plus daemon
time rather than Python encoding. At 400k the daemon exited mid-request; see next.

### Rust builder memory at the incident size (outside this PR's lane; reported for #236)

Sending the 210k prepare request to the prebuilt `qbit-prism-build-audit-bundle --serve`
through blocking pipes: **peak daemon RSS 9,613 MB**, response after 12.2 s. At 400k:
**peak RSS 15,623 MB, then SIGKILL** (exit −9, no stderr) 24 s in, on a host with ~15 GB
available. The incident's "roughly 9.7 GiB builder RSS" is therefore reproducible from the
window size alone and is not a leak: `PayoutWindow::from_full_snapshot`
(`crates/qbit-prism/src/window.rs`) pages the retained records with
`remaining.split_off(page_size)` in a loop, and `Vec::split_off` leaves the original
vector's capacity unchanged, so each 512-record page keeps a backing allocation sized for
every record still remaining when it was cut — capacity 210k, then 209.5k, … — and the
tail is copied on every iteration. Summed over 410 pages that is ~9.5 GB of `AcceptedShare`
capacity (~220 B each) and gigabytes of copying.

A scratch experiment (applied, built into a separate target directory, measured, then
reverted; nothing under `crates/` is part of this PR) replaced the loop with

```rust
let mut records = retained.into_iter();
loop {
    let page: Vec<AcceptedShare> = records.by_ref().take(page_size).collect();
    if page.is_empty() {
        break;
    }
    pages.push(Rc::new(WindowPage::from_records(page)));
}
```

and measured, same fixture and same host: **210k: 422 MB peak RSS, response 1.4 s after
the request** (was 9,613 MB / 12.2 s); **400k: 826 MB, 2.8 s** (was SIGKILL at 15.6 GB),
with digests identical to the shipped daemon's. That is the first thing to land for cold-start
recovery, in a Rust-crate PR gated by `tests.window_pipeline_parity_gate` (`rust-daemon`
adapter); with it, the 210k round trip becomes transport-bound outright.

### Limits

- Shared host, single run per phase, synthetic fixture (`build_records`, production
  share_id shape, 200 identities, difficulty 16384). Not production, not isolated, no
  24-hour soak, no Docker or production-image run, no concurrent Stratum submissions.
- Monitor lateness is observed scheduling on this host. The GIL reading rests on the
  historical-vs-bounded contrast under identical conditions and on the GC attribution, not
  on a profiler.
- The daemon phases include the shipped 20 ms polling transport; they are reported to
  attribute, not to claim, and PR 3 changes those helpers.
- Nothing here measures first-usable-Stratum-work time or the combined release gates; the
  `#236` recovery target remains unmeasured.

### Re-running

```
python3 tests/perf/window_pipeline_gil_scaling.py --latency-probe                      # 210k + 400k
python3 tests/perf/window_pipeline_gil_scaling.py --latency-probe --json > probe.json   # capture
python3 tests/perf/window_pipeline_gil_scaling.py --render probe.json                  # re-print
python3 tests/perf/window_pipeline_gil_scaling.py --latency-probe --probe-sizes 52000,210000 \
    --probe-reps 2 --daemon-binary target/release/qbit-prism-build-audit-bundle
```

Run it alone if the point is the lateness numbers; other load on the host lands in the
`idle_control` row first, which is the row to compare against.
