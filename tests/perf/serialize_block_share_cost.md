# Per-share CPU cost of `serialize_block` across template sizes

Closes the "largest open number" named at the end of section 1 of the issue #143 findings
comment: `serialize_block` is O(block bytes) per accepted share, and the composite it measured
came from a 0-transaction template.

Driver: `tests/perf/serialize_block_share_cost.py`. No database, no network, no coordinator,
standard library only. `python3 tests/perf/serialize_block_share_cost.py` prints these tables;
`--json` emits the same data structured.

**Sections 1-6 measure the code as it stood at `413f51e`**, before the fix that shipped with
this file; no `lab/` change was in the tree while they were taken. **Section 7 is the after-fix
delta**, measured against the same driver on the same machine. Read 1-6 as the diagnosis and 7
as the outcome.

**Headline.** The block-serialization term is linear in block bytes at **2.43 µs of interpreter
CPU per KB of block**, with an essentially zero intercept (R² = 0.99996 over eight points from
158 B to the 2,000,000 B consensus ceiling). At a 1 MB template an accepted share costs
**~2,470 µs** of interpreter CPU, of which **~2,440 µs (98.8%)** is block serialization, and
single-core share capacity is **~405 shares/s**. At the 2 MB consensus ceiling it is
**~5,096 µs / ~5,090 µs (99.9%) / ~196 shares/s**.

---

## 1. Machine, clock, and anchors

### Machine

| | |
|---|---|
| CPU | Apple M5 Max, 18 logical cores (12 performance / 6 efficiency) |
| Platform | macOS 26.4, arm64 |
| Python | 3.14.3 (main, Feb 3 2026, 15:32:20) [Clang 17.0.0 (clang-1700.6.3.2)], CPython |
| GIL | standard build (`Py_GIL_DISABLED` = False), GIL enabled at runtime |
| Idle? | **No.** Load average ranged 5.2–7.7 across the ten runs (the driver itself contributes ~1.0). See §6. |

### Clock

**`time.thread_time_ns()`** is the headline clock — interpreter CPU, not wall clock. Reasons:

- the driver is strictly single-threaded, so per-thread CPU *is* the interpreter CPU this path
  burns;
- it excludes time the OS deschedules us, which matters because the host was not idle and
  `perf_counter` would charge that to the measurement;
- on this platform its resolution is 42 ns against `process_time`'s 1 µs
  (`clock_gettime(CLOCK_THREAD_CPUTIME_ID)` vs `clock_gettime(CLOCK_PROCESS_CPUTIME_ID)`).

`process_time` and `perf_counter` were captured over the same batches and **do not diverge**:
at the 0-tx point the composite is 9.22 µs thread CPU / 9.22 µs process CPU / 9.25 µs wall; at
2 MB it is 5,093.1 / 5,092.8 / 5,098.5 µs. Wall exceeds thread CPU by 0.3% and 0.1%
respectively, so there is no hidden kernel or I/O time even at multi-megabyte allocation
sizes, and every number below can be read as either.

### Statistic

Min-of-7 repetitions, matching the findings comment. Each repetition is an auto-sized batch of
calls (target 50 ms per repetition) reported per call, so sub-microsecond primitives stay clear
of clock granularity. The driver's default is min-of-7; every headline number below is the
**median of 7 independent default runs**. A **control** column reports the median of 3 runs at
`--reps 25 --min-batch-seconds 0.02`, used only to show that min-of-7 is not biased — it is
not, the two agree within the run-to-run spread (§6).

### Anchors

| anchor | published (#143) | this machine, min-of-7 | run range | control (min-of-25) | ratio mine/published |
|---|---|---|---|---|---|
| primitives floor, per accepted share | 12.4 µs | **10.85 µs** | 10.50–11.65 | 10.41 | **0.875** |
| 0-tx-template composite `assemble_submission` | 9.0 µs | **9.22 µs** | 8.93–9.87 | 8.92 | **1.025** |
| composite ÷ primitives inflation ratio | 1.266 (implied by 15.7 ÷ 12.4) | 1.095 | 1.059–1.129 | 1.108 | 0.865 |

The 0-tx composite anchor — the one the findings comment specifies unambiguously — reproduces
at **1.02× published**. This machine is, for this path, the same speed as the machine the
findings comment ran on, so its absolute numbers carry over almost unchanged.

The primitives floor anchor is **a reconstruction, not a replay**: the findings comment reports
"12.4 µs (primitives, min-of-7)" but does not enumerate which primitives it summed. The set
below was reconstructed from the accept path and lands 12.5% under the published figure, which
is consistent with the original set including per-share work this one omits (§6).

Primitive breakdown at the 0-transaction baseline, min-of-7, median of 7 runs:

| primitive | group | µs | note |
|---|---|---|---|
| `validate_hex(extranonce2)` | assemble | 0.114 | |
| `apply_version_bits` | assemble | 0.161 | no version rolling on this share |
| `list(job.merkle_branch)` | assemble | 0.034 | 0 branch levels |
| `assemble_coinbase` | assemble | 0.582 | |
| `compute_merkle_root_from_branch_hex` | assemble | 0.466 | 0 fold levels |
| `serialize_header_from_stratum_fields` | assemble | 3.353 | largest single primitive on the path |
| `full_coinbase_hex` concat | assemble | 0.069 | |
| `strip_witness_transaction(coinbase)` + compare | assemble | 1.267 | |
| `header_hash_int` | assemble | 0.495 | |
| `header_hash_hex` | assemble | 0.482 | |
| `coinbase_without_witness.hex()` | assemble | 0.049 | |
| `header.hex()` | assemble | 0.052 | |
| `serialize_block(...).hex()` | assemble | 0.653 | **0 tx, 158 block bytes — the term, at the baseline** |
| `DirectQbitSubmission(...)` | assemble | 0.640 | |
| `json.loads(mining.submit line)` | accept_path | 0.669 | |
| `parse_submit_request` | accept_path | 0.668 | |
| `validate_submit_request` | accept_path | 0.079 | |
| `RecentShareIndex.reserve` | accept_path | 0.332 | fresh key per call, includes its own lock |
| `json.dumps(submit ack)` | accept_path | 0.712 | |
| **sum, assemble group** | | **8.43** | |
| **sum, accept-path group** | | **2.45** | |
| **primitives floor** | | **10.85** | |
| *(memo)* harness floor, `lambda: None` | harness | 0.014 | included in every row above |

---

## 2. Axis 1 — total block bytes

The ceiling is tree-derived, not guessed: `crates/qbit-pool-builder/src/lib.rs:16-17` sets
`MAX_BLOCK_WEIGHT = 2_000_000` and comments it as qbit `consensus.h` with
`WITNESS_SCALE_FACTOR = 1`, so weight units are bytes; `compose.yaml:114` pins
`QBIT_EXPECTED_MAX_BLOCK_WEIGHT: 2000000`; and `docker/ckpool/qbit-ckpool-preflight.py:494-544`
hard-fails unless that expectation and live `getblocktemplate.weightlimit` are both exactly
2000000. Max block = **2,000,000 bytes**. The 0-transaction baseline is the repo's own
(`lab/prism/job_build_benchmark.py:208-217`, `"transactions": []`).

**How each point was produced.** Real `DirectQbitStratumJob`s built through
`direct_stratum.make_job_from_builder_manifest` (route 1). Transactions are synthetic
1-in/1-out non-witness transactions, uniform size, padded in the output scriptPubKey, sized so
the total lands on the target byte count exactly; each is structurally valid and round-trips
through `strip_witness_transaction`, which is what `merkle_branch_for_coinbase` runs on every
one of them at job build. Route 1 was chosen over a hand-frozen job because merkle-branch depth
is itself a per-share cost inside `assemble_submission` — a frozen job with an empty branch
would understate the composite at large transaction counts. The coinbase is the repo
benchmark's synthetic one, 77 bytes. Block bytes = 80 header + transaction-count compact size +
coinbase + transactions; the "block bytes" column is the count actually produced, verified
against `len(submission.block_hex) // 2`.

Transaction size is held at ~499 B on this axis (a plausible average), so transaction count
scales with block bytes; Axis 2 separates the two.

| block bytes | tx count | tx size | merkle levels | composite `assemble_submission` µs/share | `serialize_block(...).hex()` µs/share | term % of composite |
|---:|---:|---:|---:|---:|---:|---:|
| 158 | 0 | — | 0 | 9.2 | 0.7 | 7.1% |
| 50,000 | 100 | 498 B | 7 | 137.2 | 122.0 | 88.9% |
| 100,000 | 200 | 499 B | 8 | 258.2 | 243.6 | 94.3% |
| 250,000 | 500 | 499 B | 9 | 623.0 | 615.7 | 98.8% |
| 500,000 | 1,000 | 499 B | 10 | 1,242.3 | 1,213.9 | 97.7% |
| 1,000,000 | 2,000 | 499 B | 11 | 2,469.2 | 2,438.5 | 98.8% |
| 1,500,000 | 3,000 | 499 B | 12 | 3,802.5 | 3,731.6 | 98.1% |
| **2,000,000** (ceiling) | 4,000 | 499 B | 12 | **5,093.1** | **5,090.0** | **99.9%** |

Control run (min-of-25, median of 3), for the same points:

| block bytes | composite µs | term µs | composite − term µs |
|---:|---:|---:|---:|
| 158 | 8.9 | 0.6 | 8.3 |
| 50,000 | 134.2 | 119.7 | 14.5 |
| 100,000 | 255.3 | 240.2 | 15.0 |
| 250,000 | 618.4 | 602.0 | 16.4 |
| 500,000 | 1,228.3 | 1,210.3 | 18.0 |
| 1,000,000 | 2,444.7 | 2,421.4 | 23.4 |
| 1,500,000 | 3,676.7 | 3,668.9 | 7.9 |
| 2,000,000 | 4,889.5 | 4,848.2 | 41.3 |

**The law.** Least squares of the term against block bytes: **2.4318 µs/KB**, intercept
−2.6 µs, **R² = 0.99996** (control). On the min-of-7 medians: 2.5254 µs/KB, intercept
−22.7 µs, R² = 0.9996. The curve is clean; no extra points were needed.

**Everything that is not serialization is a small constant.** The `composite − term` column is
the rest of `assemble_submission`: 8.3 µs at the 0-tx baseline, rising to ~18 µs at 10 merkle
levels — roughly **0.9 µs per merkle branch level** (each level is a `validate_hash_hex` +
`bytes.fromhex` + 64-byte `double_sha256`). The 1.5 MB (7.9 µs) and 2 MB (41.3 µs) entries are
noise: at those magnitudes a 1% error on either measurement is ±40 µs, which swamps a ~20 µs
quantity. Read that column as "8–20 µs, growing logarithmically with transaction count", not as
the printed values.

**Split of the term.** `serialize_block` (produces the bytes) vs the caller's `.hex()`
re-encode, control run:

| block bytes | `serialize_block` µs | `.hex()` re-encode µs | sum | term measured |
|---:|---:|---:|---:|---:|
| 158 | 0.5 | 0.1 | 0.6 | 0.6 |
| 100,000 | 193.7 | 46.1 | 239.8 | 240.2 |
| 500,000 | 978.4 | 229.9 | 1,208.3 | 1,210.3 |
| 1,000,000 | 1,953.9 | 459.6 | 2,413.5 | 2,421.4 |
| 2,000,000 | 3,925.0 | 922.3 | 4,847.3 | 4,848.2 |

The caller's `.hex()` is a flat ~19% of the term at every size.

---

## 3. Axis 2 — transaction count at fixed total bytes

Total block bytes held at exactly 1,000,000 while transaction count varies 500×. Per-byte work
is therefore constant across the rows and the difference is the per-transaction constant.

| tx count | block bytes | tx size | merkle levels | composite µs/share | term µs/share | term % of composite |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 1,000,000 | 99,984 B | 4 | 1,793.8 | 1,786.6 | 99.6% |
| 100 | 1,000,000 | 9,998 B | 7 | 1,881.7 | 1,880.8 | 100.0% |
| 500 | 1,000,000 | 1,999 B | 9 | 2,040.8 | 2,067.9 | 101.3% |
| 1,000 | 1,000,000 | 999 B | 10 | 2,265.4 | 2,213.5 | 97.7% |
| 2,000 | 1,000,000 | 499 B | 11 | 2,566.0 | 2,526.7 | 98.5% |
| 5,000 | 1,000,000 | 199 B | 13 | 3,106.3 | 3,078.9 | 99.1% |

(Percentages slightly over 100% are measurement noise — the composite and the term are timed in
separate batches, and at ~2 ms a 1–2% error exceeds the ~20 µs that genuinely separates them.)

**Per-transaction constant: ~0.25 µs/tx.**

| derivation | min-of-7 | control (min-of-25) |
|---|---:|---:|
| least-squares slope over all six points | 0.250 µs/tx | 0.240 µs/tx |
| endpoint slope, 10 → 5,000 tx | 0.261 µs/tx | 0.248 µs/tx |
| R² of the linear fit | — | 0.95–0.97 |

R² of 0.95–0.97 rather than 0.999 reflects that the relationship is not perfectly linear —
part of the growth is the merkle branch deepening from 4 to 13 levels, which is logarithmic in
count and sits in the composite, not the term. Take **0.25 µs/tx ± 0.02** as the constant.

At 4,000 transactions (a 2 MB block at 500 B/tx) that constant is ~1.0 ms/share — **20% of the
5.1 ms total** — and it is the part that a byte-count-only model would miss.

---

## 4. Decomposition of the term

`serialize_block` calls `stratum_codec.validate_hex` on the coinbase and on every transaction.
`validate_hex` builds an f-string field name, length-checks, runs `bytes.fromhex` **and
discards the result**, then returns `value.lower()` — a full fresh copy of the hex string, and
unconditionally so: `str.lower()` on already-lowercase ASCII still allocates (verified:
`s.lower() is s` → `False`). The caller then `bytes.fromhex`-decodes that copy for real, joins,
and `.hex()`-re-encodes the whole block.

All rows are control-run (min-of-25) medians. Rows sum to the measured term.

**At the 2,000,000-byte ceiling (4,000 tx), term = 4,848.2 µs**

| part | µs | % of term |
|---|---:|---:|
| (a) `validate_hex`'s `bytes.fromhex` probe (decoded, discarded) | 1,009.3 | 20.8% |
| (a′) `validate_hex`'s `value.lower()` copy | 1,098.4 | 22.7% |
| (a″) `validate_hex` call + per-tx f-string field name | 544.1 | 11.2% |
| (b) the real `bytes.fromhex` decode + list build | 1,061.3 | 21.9% |
| (c) `b"".join` concatenation | 58.2 | 1.2% |
| (d) the caller's `.hex()` re-encode | 932.1 | 19.2% |
| residual (generator step, `extend`, `compact_size`, header concat) | 144.8 | 3.0% |

**At 1,000,000 bytes (2,000 tx), term = 2,421.4 µs**

| part | µs | % of term |
|---|---:|---:|
| (a) `validate_hex`'s `bytes.fromhex` probe | 501.6 | 20.7% |
| (a′) `validate_hex`'s `value.lower()` copy | 548.8 | 22.7% |
| (a″) `validate_hex` call + f-string | 270.9 | 11.2% |
| (b) the real `bytes.fromhex` decode + list build | 517.5 | 21.4% |
| (c) `b"".join` | 28.5 | 1.2% |
| (d) the caller's `.hex()` re-encode | 461.0 | 19.0% |
| residual | 93.1 | 3.8% |

**At the 158-byte baseline (0 tx), term = 0.63 µs**

| part | µs | % of term |
|---|---:|---:|
| (a) probe | 0.077 | 12.2% |
| (a′) `.lower()` copy | 0.087 | 13.8% |
| (a″) call + f-string | 0.111 | 17.6% |
| (b) real decode + list build | 0.091 | 14.5% |
| (c) `b"".join` | 0.022 | 3.5% |
| (d) `.hex()` re-encode | 0.060 | 9.6% |
| residual | 0.182 | 28.9% |

At the baseline the term is call overhead; at MB scale it is buffer work, in the proportions
above.

### Counterfactual floors

Measured on the same material, in the same run. These are *floors for alternative code shapes*,
recorded so the reviewer can see what is reducible and what is memcpy. **No recommendation is
implied or made here.**

| variant | 2 MB | 1 MB | 158 B |
|---|---:|---:|---:|
| as shipped (`serialize_block(...).hex()`) | 4,848.2 µs (100%) | 2,421.4 µs (100%) | 0.63 µs (100%) |
| A: one decode, no `validate_hex`, bytes returned, no caller re-encode | 1,154.2 µs (**23.8%**) | 564.6 µs (**23.3%**) | 0.133 µs (21.1%) |
| B: transactions already held as bytes, bytes returned | 86.8 µs (**1.8%**) | 41.5 µs (**1.7%**) | 0.057 µs (9.0%) |
| memo: `validate_hex` full calls (a + a′ + a″) | 2,651.8 µs (54.7%) | 1,321.3 µs (54.6%) | 0.275 µs (43.6%) |

Reading of those three numbers, as measurement: `validate_hex` accounts for **55% of the term**
at MB scale. The irreducible memcpy — joining bytes that already exist — is **1.8%**. Between
them sits the hex representation itself: keeping hex inputs but decoding once and returning
bytes still costs ~24% of the current term, because two full hex↔bytes conversions (one in, one
out) remain. The cost is not "irreducibly the memcpy"; it is dominated by redundant validation
and by hex being the carrying format.

---

## 5. Capacity restatement

The findings comment's single-core capacity was **64k–81k shares/s** at the 0-tx point, against
the deployed design point of ~4,096 connections at `PRISM_STRATUM_VARDIFF_TARGET_SECONDS=15`,
i.e. **~273 shares/s** steady state.

"Accept-path µs/share" below is this machine's composite at that template size plus the
measured accept-path primitives (2.45 µs) inflated by the composite/primitives ratio (1.095) —
the same construction the findings comment used to turn 12.4 µs into 15.7 µs. It gives 11.9 µs
at the 0-tx point, against the comment's 15.7 µs. "Published-scale shares/s" rescales the
comment's own 64k–81k band by the relative collapse measured here, so it can be dropped
straight into that report without a machine correction.

| block bytes | tx count | accept-path µs/share (this machine) | shares/s (this machine) | published-scale shares/s | % of one core at ~273 shares/s |
|---:|---:|---:|---:|---:|---:|
| 158 (0-tx baseline) | 0 | 11.9 | 84,015 | 64,000–81,000 | **0.33%** |
| 50,000 | 100 | 139.9 | 7,147 | 5,444–6,891 | 3.82% |
| 100,000 | 200 | 260.9 | 3,833 | 2,920–3,695 | 7.12% |
| 250,000 | 500 | 625.7 | 1,598 | 1,218–1,541 | 17.09% |
| 500,000 | 1,000 | 1,245.0 | 803 | 612–774 | 34.00% |
| 1,000,000 | 2,000 | 2,471.9 | 405 | 308–390 | **67.50%** |
| 1,500,000 | 3,000 | 3,805.2 | 263 | 200–253 | 103.91% |
| **2,000,000** (ceiling) | 4,000 | 5,095.7 | 196 | 149–189 | **139.15%** |

The design point's ~273 shares/s is a **non-problem at 0 transactions** (0.33% of one core,
which is what "230–300× headroom" meant) and is **at or past one core somewhere between 1.0 and
1.5 MB of template**: 67.5% of a core at 1 MB, 104% at 1.5 MB, 139% at the 2 MB ceiling. The
crossover — where the accept path alone needs a full core to keep up with the design point — is
at **~1.45 MB of block** on this machine (3,662 µs/share), interpolated between the measured
1.0 MB and 1.5 MB points.

---

## 6. Reading, noise, and limits

### What the numbers show

1. **The term is linear in block bytes at 2.43 µs/KB with a zero intercept.** One number
   predicts the whole curve. Multiply template size in KB by 2.43 to get microseconds of
   interpreter CPU per accepted share, on this machine, at this Python.

2. **Above ~100 KB of template, the accepted-share path *is* block serialization.** The term is
   89% of the composite at 50 KB and ≥94% everywhere above 100 KB. Everything the findings
   comment measured at the 0-tx point — the header assembly, the two `double_sha256`s, the
   coinbase strip, the JSON — collapses to a rounding error: a fixed 8–20 µs against a term
   that reaches 5,090 µs.

3. **The 0-tx anchor understated the real per-share cost by two to three orders of magnitude.**
   On this machine's own scale, 9.22 µs → 2,469 µs at 1 MB is 268×; → 5,093 µs at 2 MB is
   552×. The findings comment's "~230–300× headroom over the design point" was computed at
   the 0-tx point, and the headroom it
   describes is consumed almost exactly by moving to a full template.

4. **There are two independent scaling terms, and both matter.** Per byte: 2.43 µs/KB. Per
   transaction: 0.25 µs/tx. At 4,000 transactions the per-transaction term alone is ~1.0 ms, a
   fifth of the 2 MB total. A model keyed only on block size would miss it.

5. **Fifty-five percent of the term is `validate_hex`**, split roughly evenly between a decode
   that is thrown away and a `.lower()` copy that allocates even when the string is already
   lowercase. The irreducible join of already-decoded bytes is 1.8%.

6. **At the deployed design point the term crosses one core inside the consensus range.** ~273
   shares/s costs 0.33% of a core at 0 tx and 139% at the 2 MB ceiling; the crossover is at
   ~1.45 MB.

### What is noisy

- **The host was not idle.** Load average 5.2–7.7 across all ten runs (~1.0 of which is the
  driver). `thread_time` excludes descheduled time, but it cannot exclude DVFS or
  memory-bandwidth contention from other processes — CPU time is still charged while running
  slower. This is the dominant error source.
- **Run-to-run spread at min-of-7 is 4–12%** at MB scale, 2–7% below 100 KB. Seven independent
  runs were taken and the median reported for that reason.
- **`composite − term` is not resolvable at MB scale.** It is a genuine 8–20 µs quantity sitting
  inside two ~5 ms measurements taken in separate batches; individual runs produce values from
  −176 µs to +49 µs at 2 MB. Only the control run (min-of-25) resolves it consistently
  positive. Do not read that column point by point.
- **Min-of-7 is not biased**, checked directly: three control runs at min-of-25 with 20 ms
  batches land within 1–4% of the min-of-7 medians at every point, always on the low side, as
  a longer min-search should. An independent stopwatch cross-check (arithmetic mean, not min,
  over 200 calls at 1 MB, outside the harness) gives 2,525 µs against the harness's 2,438–2,469
  µs — a mean sitting ~3% above a min is exactly right.

### What I could not measure, and why

- **The primitives-floor anchor is a reconstruction.** The findings comment does not enumerate
  the primitives behind its 12.4 µs. The set here — 14 primitives inside `assemble_submission`
  plus 5 on the accept path around it — sums to 10.85 µs, 12.5% under published. Plausible
  omissions: the ten process-global lock acquisitions the comment counts, vardiff accounting,
  `pending_share_from_submission` and its ledger row, and socket read/write framing. The
  precisely-specified anchor (the 0-tx composite) reproduces at 1.02×, so I would treat the
  machine as matched and this gap as a difference in what was counted, not in how fast the
  machine is. Every sweep number in this report is from this machine in the same set of runs,
  so shapes and deltas are internally consistent regardless.
- **Coinbase size was not swept.** All points use the repo benchmark's 77-byte synthetic
  coinbase. A production PRISM coinbase with many payout outputs is larger, and it enters the
  term exactly as a transaction does (so, 2.43 µs/KB) *and* is parsed once more per share by
  `strip_witness_transaction` in pure Python — a term measured here only at 77 bytes
  (1.27 µs). For a KB-scale coinbase that second cost is unmeasured and is not covered by the
  2.43 µs/KB law. The driver has a `coinbase_outputs` parameter that would support the sweep;
  it was not run.
- **Real transaction size distribution.** Transactions here are uniform-size, 1-in/1-out,
  witness-free. `serialize_block` cost depends only on hex length and transaction count, so
  this is faithful **for the term under measurement**; it is not faithful for anything that
  parses transaction structure. Real templates have a size distribution and real witness data;
  apply 2.43 µs/KB to actual bytes and 0.25 µs/tx to actual count rather than assuming uniform
  transactions.
- **Threading behaviour at these buffer sizes — the most important gap.** The findings comment
  established that this path is GIL-bound *at 0-tx buffer sizes*, and separately found the GIL
  release threshold brackets 1–2 KiB with a 1 MiB positive control reaching 7.89 cores across 8
  threads. Every buffer in this sweep above the baseline is far past that threshold. Whether
  CPython's `bytes.fromhex`, `str.lower`, `bytes.hex` and `b"".join` release the GIL at
  megabyte sizes decides whether this term is 5 ms of *serialized* interpreter time per share
  or 5 ms of work that partly parallelises — and it could cut either way for the sharding
  argument in #143. I did not measure it: the brief scopes this to single-threaded per-share
  cost, and I am not willing to assert a GIL-release claim from reading alone. **It should be
  measured before these numbers are used to argue anything about concurrency.**
- **Downstream cost of `block_hex`.** `DirectQbitSubmission` carries the full hex block string
  for **every** accepted share, block-worthy or not — 4 MB of live string at a 2 MB template.
  I measured the CPU to produce it, not the allocator/GC pressure or peak RSS from holding it
  across the rest of the accept path. That is a separate and unmeasured cost.
- **Everything outside `assemble_submission` at large template sizes.** The accept-path
  primitives were measured at the 0-tx baseline only. They are template-size-independent as far
  as I can tell from the code, but I did not re-measure them per point; the capacity table adds
  a constant 2.68 µs at every size on that assumption, which is stated rather than verified.

### Re-running

```
python3 tests/perf/serialize_block_share_cost.py            # tables
python3 tests/perf/serialize_block_share_cost.py --json     # same data, structured
python3 tests/perf/serialize_block_share_cost.py --reps 25 --min-batch-seconds 0.02   # control
```

One default run takes ~55 s. For an after-fix delta, compare the Axis 1 `serialize_term_us`
column and the §4 decomposition; the `--json` output carries `thread_cpu_us`, `process_cpu_us`
and `wall_us` for every measurement plus the batch size and repetition count behind it.

**No verdict on whether to change anything is offered here** — that call belongs to the
reviewer.

---

## 7. After the fix — measured delta

`assemble_submission` now serializes the block only when `block_pass` is set, so an ordinary
accepted share never builds it. Same driver, same machine, same session; three `before` and three
`after` runs taken **alternating** so any machine drift falls on both sides equally. Medians of
three. `before` is `413f51e` unmodified; `after` is that tree plus the one-line gate.

The driver's jobs use `bits = "1b00ffff"` (difficulty 65,536), so its submissions are ordinary
shares — `block_pass` is false — which is exactly the path under test.

| block bytes | tx | composite before µs | composite after µs | speedup |
|---:|---:|---:|---:|---:|
| 158 | 0 | 9.0 | 8.3 | **1×** |
| 50,000 | 100 | 134.5 | 13.1 | **10×** |
| 100,000 | 200 | 255.6 | 13.8 | **19×** |
| 250,000 | 500 | 620.8 | 14.5 | **43×** |
| 500,000 | 1,000 | 1,235.2 | 15.1 | **82×** |
| 1,000,000 | 2,000 | 2,447.4 | 15.9 | **154×** |
| 1,500,000 | 3,000 | 3,677.0 | 16.5 | **222×** |
| 2,000,000 | 4,000 | 4,888.6 | 16.5 | **296×** |

**The after column is flat.** It rises 8.3 → 16.5 µs across a 12,600× range of block size, and
that residual is not serialization: it is the per-share merkle-branch fold, ~0.9 µs per level,
deepening from 0 to 12 levels as transaction count grows. Block bytes no longer enter the
share path at all.

Single-core share capacity, computed as in §5 (composite plus the 2.69 µs accept-path constant):

| block bytes | shares/s before | shares/s after | % of one core at ~273 shares/s, before | after |
|---:|---:|---:|---:|---:|
| 158 | 85,889 | 91,261 | 0.32% | **0.30%** |
| 50,000 | 7,291 | 63,416 | 3.74% | **0.43%** |
| 100,000 | 3,872 | 60,714 | 7.05% | **0.45%** |
| 250,000 | 1,604 | 58,171 | 17.02% | **0.47%** |
| 500,000 | 808 | 56,189 | 33.79% | **0.49%** |
| 1,000,000 | 408 | 53,912 | 66.89% | **0.51%** |
| 1,500,000 | 272 | 51,995 | 100.45% | **0.53%** |
| 2,000,000 | 204 | 52,060 | 133.53% | **0.52%** |

The deployed design point (~4,096 connections at `PRISM_STRATUM_VARDIFF_TARGET_SECONDS=15`,
~273 shares/s) went from **crossing one full core inside the consensus range** — 66.9% of a core
at 1 MB, 133.5% at the 2 MB ceiling — to **0.30-0.53% of a core at every template size**. The
"~230-300× headroom" of the #143 findings comment was true only at 0 transactions; it is now
true everywhere.

Anchor check on this run set: the 0-tx composite measured **8.96 µs** against the findings
comment's published 9.0 µs, a ratio of **0.995** — an independent reproduction of the
anchor in §1, taken in a separate session from the sweep above.

Load average was 4.7-5.9 across all six runs. The alternating order is what makes
the delta trustworthy under that load, not the absolute values.

### What this does not change

The `serialize_block` implementation is untouched, so the §4 decomposition still describes what
a block landing pays: `validate_hex` is still 55% of the term, still a discarded `bytes.fromhex`
probe plus a `value.lower()` copy. That cost now falls once per found block instead of once per
share, which is why it was left alone here. The GIL gap named in §6 is likewise still open, and
still matters to #143's sharding argument — it is just no longer on the share path.
