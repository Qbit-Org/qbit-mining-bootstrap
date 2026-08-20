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
