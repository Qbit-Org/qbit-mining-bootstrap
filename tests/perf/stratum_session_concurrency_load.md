# Real-thread concurrency load on the PRISM Stratum session path

Rebuilds the on-demand load driver issue #143 asked for: what the coordinator actually does
when many miners are connected at once, measured with **real OS threads, real sockets and a
real wall clock**.

Driver: `tests/perf/stratum_session_concurrency_load.py`. On demand only — the filename does
not begin with `test_`, so unittest discovery never collects it, and no test imports it.
`--json` captures a run and `--render PATH` replays a capture without measuring.

## Why this is a separate instrument from the baton harness

`tests/prism_concurrency_harness.py` exists to make one interleaving reproducible, and it
replaces the scheduler, the clock and the lock to do it. Those are exactly the three things
this question is about, so the two instruments are deliberately kept apart and neither is
built on the other. The harness answers "is this ordering possible?"; this driver answers
"what happens on a real host under real load?".

## What is under test

The shipped session stack, driven through a real `PrismCoordinator`:

| service | reached through |
|---|---|
| `StratumSessionService` | `coordinator.accept_loop` / `handle_client` |
| `VardiffService` | difficulty assignment, retarget, and reconnect resume via the real submit path |
| `ShareSubmissionService` | `mining.submit` handling end to end |

Nothing under `lab/` is modified, monkeypatched for behaviour, or reimplemented. The
coordinator is assembled by the repository's own `prism_coordinator_test_support.coordinator`
scaffold — the same one every unit test uses to get a coordinator without a live qbitd — with
the shipped `_ObservedRLock` installed in place of the scaffold's plain `RLock` (a plain lock
reports no contention at all), a real listening socket, a real accept loop on its own thread,
and real per-connection handler threads spawned by the shipped session service.

Two collaborators are synthesized, and only these two: the node JSON-RPC (this measures the
coordinator, not qbitd) and the audit-bundle subprocess (the repository's existing counting
fake). The `validateaddress` answer is synthesized while the shipped `P2mrAddressValidator`,
its LRU and its singleflight all still run.

## Modes

| mode | question |
|---|---|
| `paced` | The deployed design point: one share per connection per `--share-interval-seconds` (default 15, the vardiff target). What does steady state cost? |
| `saturating` | Every connection submits as fast as it is answered. Where is the ceiling, and can sessions still be *established* while the share path is busy? |
| `herd` | Drop every session at once and reconnect. The headline is a **per-session re-establishment census**, not throughput. |

Herd mode reports the exact number of sessions that never came back inside the window. A
reconnect storm that settles at high shares/s while a third of the pool never gets work back
is a failure, and a throughput-only report would call it a success.

## Ledger modes

Every output identifies the ledger mode, because a shares/s figure means something entirely
different with and without a durable write behind it.

* `--ledger memory` — the shipped `SingleWriterShareLedger`. Isolates interpreter cost and
  **omits all database wait**.
* `--ledger postgres` — the real `PsqlShareLedger`, selected through the repository's
  real-server gate (`QBIT_PRISM_EXTERNAL_PSQL_COMMAND`, `PRISM_POSTGRES_PSQL_COMMAND` or
  `PRISM_DATABASE_URL`, in that order). When the gate is unset the driver exits non-zero and
  prints the exact commands to start a server and re-run.

## Difficulty behaviour

Difficulty is not permanently pinned, because pinning it removes the retarget path from the
measurement and makes reconnect behaviour meaningless.

* `resume` (default) — `vardiff_resume_enabled`, the shipped default: a reconnecting session
  may adopt its retained difficulty. The run reports `vardiff_resume_outcome_counts` so the
  resume path is visible rather than assumed.
* `climb` — resume disabled; every reconnect retargets from the floor.
* `pinned` — `min == max`. Isolation only, and the report says it is not representative.

The floor difficulty is deliberately below the `2**256-1` clamp in `difficulty_target`, so the
first candidate nonce always clears the share target and nonce search costs one hash per share.
The driver still searches with the shipped `assemble_header_from_notify_submit` and
`header_hash_int` — a real miner searches, so this one does too — and reports the hash count so
that cost stays visible.

## Controls and the health abort

Before any conclusion is drawn, the driver measures whether this host can support one:

* **positive control** — 1 MiB `hashlib.sha256` across `--control-threads`, which releases the
  GIL and should scale;
* **negative control** — a small-buffer pure-Python loop, which cannot, and must pin near 1.00;
* **load-average guard**, **descriptor guard** (each connection costs two descriptors; a run
  that silently hits the limit reads as coordinator backpressure when it is really the
  harness), **memory guard**, and a **wall-clock floor**.

If any fails, the run reports the abort and **no settle, load, herd or lock numbers at all**.
A number measured on a host that cannot show parallelism is not a weaker result, it is a
different quantity. `--selftest` asserts this: the abort report must contain none of
`SETTLE:`, `LOAD:`, `HERD:` or `COORDINATOR LOCK`.

## Lock hold attribution

Production deliberately does not instrument hold *duration* — `_ObservedRLock`'s docstring
explains that a clock read on every acquire and release is exactly the fast-path cost that
class exists to avoid, and that a re-entrant lock needs per-thread depth tracking to avoid
double counting. Both objections are correct, and **this PR changes no production code**, so
the attribution lives in the driver and is paid for only while measuring. Re-entrant
acquisitions are tracked per thread by depth and only the outermost hold is timed.
`--no-lock-attribution` removes the wrapper so its own overhead can be quantified on the same
host instead of argued about. Acquisition and contention counts always come from the shipped
`contention_snapshot()`, never recomputed here.

---

## Recorded baseline: N = 1024 herd census

One run, recorded to pin what the driver reports. **This is evidence, not a threshold**, and
nothing asserts against it.

### Machine

| | |
|---|---|
| CPU | Apple M5 Max, 18 logical cores |
| Python | 3.14.3, GIL enabled |
| Load average (start / end) | 6.61 / 6.59 (1-minute), ≈0.37 per core |
| Peak RSS | 175 MB |
| Descriptor limit | 1,048,576 (ample for 1,024 connections) |

### Controls

| control | cores-used across 8 threads |
|---|---|
| positive: 1 MiB `hashlib.sha256` | **7.72** |
| negative: pure-Python loop | **0.97** |

The host demonstrably shows parallelism, and the negative control pins as the model requires,
so the numbers below are about the coordinator.

### Run

`--mode herd --connections 1024 --ledger memory --settle-seconds 50 --herd-return-seconds 50
--measure-seconds 30 --share-interval-seconds 15`

### Settle

| | |
|---|---|
| requested / established | 1,024 / **1,024** |
| never established | **0** |
| wall | 1.49 s |
| establish latency p50 / p95 / max | 663 ms / 1,276 ms / 1,385 ms |

### Herd

| | |
|---|---|
| dropped together | 1,024 in 3.45 s |
| re-established | **691 / 1,024** |
| **never re-established** | **333** (within the 50 s window) |
| failure kind | `TimeoutError` × 333 |
| return latency p50 / p95 / max | 724 ms / 9,041 ms / 9,288 ms |
| vardiff resume outcomes | `resumed: 691`, `rejected: 0`, `expired: 0` |
| rejections | **none** |
| watchdog misses | **none** |

### Load after the herd (paced, 691 surviving sessions)

| | |
|---|---|
| wall | 34.35 s |
| cores-used (GIL probe discounted) | 0.03 of 18 |
| accepted / rejected shares | 1,382 / 0 |
| shares/s | 40.2 |
| CPU µs/share | 725 |
| ack latency p50 / p95 / p99 / max | 19.1 ms / 43.9 ms / 56.5 ms / 73.8 ms |

### Coordinator lock

| | whole run | herd window |
|---|---|---|
| acquisitions | **49,106** | 21,757 |
| contentions | **1** (0.0%) | 0 (0.0%) |

Longest holders during the herd:

| call site | holds | total s | mean µs |
|---|---|---|---|
| `prism_coordinator._progress_eligible_client_counts` | 691 | 0.108 | 157.0 |
| `job_delivery._deliver_initial_bundle` | 691 | 0.071 | 103.2 |
| `stratum_session.reserve_client_username` | 691 | 0.058 | 84.5 |
| `job_delivery.cleanup_disconnected_client` | 1,024 | 0.023 | 22.9 |
| `stratum_session.accept_loop` | 1,382 | 0.016 | 11.7 |
| `job_delivery.schedule_initial_job` | 691 | 0.007 | 10.7 |

### GIL-wait probe

Median stretch **0.98×** against the idle baseline — no measurable GIL wait at 40 shares/s.
That is expected at this rate and is not evidence that the path is parallel; the saturating
mode is where that question gets asked.

---

## How this compares to the original #143 calibration

The historical figures pin what these numbers *mean*. They are **not** host-independent
thresholds, and this run neither confirms nor refutes them as such.

| quantity | #143 calibration | this run |
|---|---|---|
| saturating N=1024 settle | 0/5 settled; only 889–944/1024 established within 50 s | 1,024/1024 established in 1.49 s (herd run, memory ledger, 0-transaction template) |
| herd sessions never re-established | 660–768 / 1024 | **333 / 1024** |
| rejections / watchdog during herd | zero / none | zero / none |
| coordinator-lock acquisitions when settled | ~48,000 | **49,106** |
| lock contention during herd | 30–82% | **0.0% (1 of 49,106)** |
| longest holders | `note_tip_work_delivered`, `accept_loop`, `schedule_initial_job` | `_progress_eligible_client_counts`, `_deliver_initial_bundle`, `reserve_client_username`, then `accept_loop`, `schedule_initial_job` |

**Two of these agree closely and one does not, and the disagreement is stated rather than
smoothed over.** The acquisition count reproduces almost exactly, and the qualitative herd
finding reproduces: a large minority of sessions never return, with *zero* rejections and
*no* watchdog miss — the failure is silent, which is the part that matters. The contention
percentage did **not** reproduce on this host. Candidate explanations, none of them verified
here: this host has 18 cores and a 0.37 per-core load, the herd drop spreads over 3.45 s
rather than arriving instantaneously, and this run used the in-memory ledger, so no lock
holder ever waits on a database. Establishing which of those accounts for the gap needs runs
on the original rig and is out of scope for this instrument's landing.

## Reading, noise and limits

* **Driver overhead is included.** The miner threads, their sockets and the lock-attribution
  wrapper run in this process and compete for these cores. This measures a host running the
  coordinator *and* its load, not a coordinator alone.
* **Absolute values are host-dependent.** Core count, load average and the Python build change
  every number here. Compare runs on one host, not across hosts.
* **In-memory results omit database wait.** A `--ledger postgres` smoke on the same host moved
  ack p50 from ≈19 ms to ≈230 ms at N=8. Only the postgres mode exercises the durable path.
* **This is evidence, not a CI threshold.** Nothing here should be asserted against in an
  automated test.
* **The GIL-wait probe charges itself.** It is a busy loop that burns most of a core for the
  whole window, and `time.process_time()` sums every thread. Its own CPU is measured with
  per-thread time, sampled at exactly the load window's boundaries, and subtracted; the
  undiscounted whole-process figure is printed beside the adjusted one rather than hidden.
* **`never re-established` is a window statement.** It counts sessions that did not complete
  subscribe → authorize → first job inside `--herd-return-seconds`. A session that returns at
  51 s counts as a failure here and may not be one operationally.
* **"Established" means work was delivered**, not merely that TCP was accepted. A session that
  is accepted but never receives a job cannot mine, so counting it as established would hide
  exactly the failure this census exists to find.
