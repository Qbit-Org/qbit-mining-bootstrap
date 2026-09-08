# Builder pipe transport latency: fixed polling sleeps vs readiness waits (#236)

Issue #236's cold window materializations spent 47.5-48.8 s of 54-56 s inside
`daemon_prepare`, a timer that covers lock admission, request encoding, both
pipe directions, the Rust fold and response handling. The shipped transport
helpers in `lab/prism/bundle_compiler.py` slept a fixed 20 ms after every
EAGAIN -- once per drained pipe capacity -- so a multi-megabyte exchange could
pay far more in idle sleeps than in copying. This report measures that on Linux
against the real daemon binary, before and after replacing the sleeps with
readiness waits bounded by the deadline and the existing 20 ms
cancellation-check cadence.

Driver: `tests/perf/window_builder_pipe_latency.py`. Standard library only; no
database, network or coordinator. It loads the baseline transport module
straight from a git revision and runs it beside the working tree in one
process, against the same peer subprocesses and the same daemon binary.

This is an **on-demand instrument, not a test**: it asserts no thresholds and
is not named `test_*`, so discovery never runs it (#160).

**Measured at** working tree on `e59bda34016f6dc0a6bc39f9b879b8df64f51f60`
(baseline = that commit's `lab/prism/bundle_compiler.py`, patched = the same
file with the readiness-wait change of this PR). Everything else, including
the Rust daemon, is identical between the two variants.

---

## Headline

**The fixed sleeps were the transport cost, and they are gone.** On this host,
a cold 210k-share `prepare_window` through the shipped
`BundleCompiler.prepare_payout_window` drops from **39.8-43.8 s to 12.2-13.8 s**.
The 90.4 MiB request write alone drops from **26.9-27.6 s to 0.13-0.15 s**
(1,327-1,362 twenty-millisecond sleeps replaced by 5,813-6,705 readiness
waits that each return as soon as the daemon drains the pipe). The bytes,
digests and canonical item bytes are identical across variants.

**What remains after the change is mostly not the coordinator's pipe loops.**
At 210k the patched round trip is 10.7-12.0 s of `response_wait` -- elapsed
wall time from the moment the pipe accepted the last request byte until the
envelope line was received, which contains the daemon's scheduling and
processing (parsing what it had not yet consumed, the fold, canonical
serialization) together with response readiness and receipt; it is **not** a
measured Rust CPU figure -- plus 1.0-1.2 s of unattributed coordinator time
before the write (request encoding and everything else outside the wrapped
calls) and 0.26-0.39 s reading the response section. Separating the residual
needs Rust-side profiling; this report does not attribute time inside the
daemon.

Raw pipe throughput of the helpers rises from **3-6 MiB/s to 30-215 MiB/s**
(1-16 MiB payloads), with the number of blocked waits essentially unchanged --
each is still one wait per 64 KiB pipe capacity -- because each wait now ends
on readiness rather than on a timer.

**Not established here:** the production improvement. This host is not the
production container (CPython 3.12.3, no Docker image, no PostgreSQL ledger
read in the loop), the measured path is the compiler's daemon exchange only
(no ledger read, record conversion, mirror validation or Stratum job build),
the host was shared with the other two #236 implementation workers during
the runs, and the incident's `daemon_prepare` was measured on different
hardware with production data. The recovery-time gate (first usable Stratum
work within 20 s at 210k) remains to be measured in the dedicated environment.

---

## 1. Machine and method

| | |
|---|---|
| Platform | Linux 6.8.0-106-generic x86_64, glibc 2.39 (KVM guest, 8 vCPU) |
| Python | CPython 3.12.3 |
| Pipe capacity | 65,536 bytes (`F_GETPIPE_SZ` on a fresh pipe) |
| `os.splice` | available |
| Load average at start | 0.38 / 0.49 / 0.52 |
| Daemon | `target/release/qbit-prism-build-audit-bundle` built from this tree (`cargo build --release -p qbit-prism`) |

Commands, in this order. The host was **not** idle: the PR 1 and PR 2
implementation workers for #236 were active on the same machine during both
runs, so absolute times carry that interference (the variants alternate per
repetition, so it lands on both).

```
python3 tests/perf/window_builder_pipe_latency.py \
    --baseline-rev e59bda34016f6dc0a6bc39f9b879b8df64f51f60 \
    --sizes-mib 1,4,16 --reps 5 --skip-daemon --json raw.json
python3 tests/perf/window_builder_pipe_latency.py \
    --baseline-rev e59bda34016f6dc0a6bc39f9b879b8df64f51f60 \
    --skip-raw --daemon-sizes 52000,210000,400000 --daemon-reps 2 --json daemon.json
```

The second command was stopped after the first 400k repetition of each variant
(see section 3); its JSON was therefore not written, and the daemon table
below is transcribed from the per-repetition log lines.

**Raw transport.** Each helper is called exactly as the daemon transport calls
it, on a `_ServeBuilderClient` wrapping a Python peer subprocess: the peer
writes N bytes to its stdout in 64 KiB blocks (reads: `read_exact`,
`read_line`), or reads its stdin to EOF in 64 KiB reads (writes: `write`,
`splice` from a temporary spool file). Timing starts after a go byte so peer
start-up is excluded. The peer reports a SHA-256 of what it sent or received
and every sample is checked byte-exact (`exact` column). Variants alternate per
repetition so host drift lands on both.

**Blocked waits.** For the patched module, the count of `_PipeReadinessWaiter.wait`
calls; for the baseline, the count of `time.sleep` calls (its loops resolve
`time.sleep` at call time, and every peer is a subprocess, so nothing else in
the process sleeps during a sample).

**Real daemon.** One cold `prepare_window` `full` round trip through the
shipped `BundleCompiler.prepare_payout_window` per repetition, on a freshly
spawned daemon, with records shaped like the ledger's rows (production
`username:block_hash_hex` share ids, 64 miners × 8 rigs, 16384 share
difficulty, window weight = total difficulty so every record is retained).
Phases come from wrapping the compiler's own transport methods:

| phase | contains |
|---|---|
| `spawn` | daemon spawn + handshake line read |
| `request_encode` | total minus every wrapped phase: unattributed coordinator time outside the transport calls, dominated by but not isolated to the whole-request `json.dumps` |
| `request_write` | `_serve_builder_write` of the request line: elapsed until the pipe accepted every byte (not proof the daemon consumed them; up to one pipe capacity may still be in flight) |
| `response_wait` | `_serve_builder_read_line` of the envelope: elapsed from write completion to envelope receipt -- daemon scheduling and processing (remaining parse, fold, canonical serialization) plus response readiness and receipt; wall time, not measured Rust CPU |
| `response_read` | `_serve_builder_read_exact` of the canonical items section and its newline: elapsed receipt of bytes the daemon produces after the envelope |

Request sizes (`json.dumps` of the same request, measured separately):
52k → 22.3 MiB, 210k → 90.4 MiB.

---

## 2. Raw transport (5 reps, peer chunk 64 KiB, peer pause 0)

| helper | bytes | variant | median ms | min ms | max ms | median waits | MiB/s | exact |
|---|---:|---|---:|---:|---:|---:|---:|---|
| read_exact | 1,048,576 | baseline | 204.81 | 184.20 | 206.04 | 10 | 4.9 | yes |
| read_exact | 1,048,576 | patched | 30.60 | 30.31 | 33.43 | 16 | 32.7 | yes |
| read_exact | 4,194,304 | baseline | 701.37 | 676.85 | 785.10 | 34 | 5.7 | yes |
| read_exact | 4,194,304 | patched | 54.50 | 51.13 | 58.89 | 63 | 73.4 | yes |
| read_exact | 16,777,216 | baseline | 4040.83 | 2982.84 | 4300.37 | 199 | 4.0 | yes |
| read_exact | 16,777,216 | patched | 127.70 | 123.48 | 140.50 | 256 | 125.3 | yes |
| read_line | 1,048,576 | baseline | 184.48 | 183.79 | 224.74 | 9 | 5.4 | yes |
| read_line | 1,048,576 | patched | 31.77 | 29.30 | 32.83 | 17 | 31.5 | yes |
| read_line | 4,194,304 | baseline | 647.92 | 628.02 | 707.43 | 31 | 6.2 | yes |
| read_line | 4,194,304 | patched | 55.20 | 52.84 | 61.62 | 65 | 72.5 | yes |
| read_line | 16,777,216 | baseline | 990.90 | 858.37 | 1096.40 | 36 | 16.1 | yes |
| read_line | 16,777,216 | patched | 175.04 | 163.06 | 206.02 | 256 | 91.4 | yes |
| write | 1,048,576 | baseline | 323.77 | 303.52 | 324.60 | 16 | 3.1 | yes |
| write | 1,048,576 | patched | 25.99 | 24.99 | 27.01 | 16 | 38.5 | yes |
| write | 4,194,304 | baseline | 1274.77 | 1235.51 | 1278.18 | 63 | 3.1 | yes |
| write | 4,194,304 | patched | 36.32 | 35.04 | 39.85 | 64 | 110.1 | yes |
| write | 16,777,216 | baseline | 5048.17 | 5035.91 | 5061.10 | 249 | 3.2 | yes |
| write | 16,777,216 | patched | 77.21 | 75.11 | 83.05 | 255 | 207.2 | yes |
| splice | 1,048,576 | baseline | 322.88 | 302.60 | 323.78 | 16 | 3.1 | yes |
| splice | 1,048,576 | patched | 27.43 | 26.28 | 37.18 | 16 | 36.5 | yes |
| splice | 4,194,304 | baseline | 1291.49 | 1212.55 | 1292.21 | 64 | 3.1 | yes |
| splice | 4,194,304 | patched | 37.11 | 35.95 | 43.09 | 64 | 107.8 | yes |
| splice | 16,777,216 | baseline | 5151.02 | 5128.69 | 5180.66 | 255 | 3.1 | yes |
| splice | 16,777,216 | patched | 74.04 | 72.86 | 82.28 | 256 | 216.1 | yes |

Reading the table:

- **Writes and splices are the cleanest case.** The baseline pays exactly one
  20 ms sleep per pipe capacity (16 per MiB → ~320 ms/MiB → 3.1 MiB/s at every
  size). The patched helper takes the same number of waits but each ends when
  the peer drains the pipe.
- **Reads show fewer baseline sleeps than pipe fills** (10 per MiB rather than
  16) because the peer keeps writing while the coordinator sleeps, so some
  wakeups find more than one block. The patched helper wakes once per block.
  The baseline's 16 MiB `read_line` (36 sleeps, 16 MiB/s) is the peer
  outrunning a sleeping reader, not the reader being fast; the same reader
  sleeps 199 times on `read_exact`.
- The patched numbers still show one wait per 64 KiB. That ping-pong per pipe
  capacity is the remaining transport cost (roughly 0.5 ms per block on this
  guest, mostly wakeup latency of two processes handing a full pipe back and
  forth). Raising the pipe capacity (`F_SETPIPE_SZ`) is the obvious follow-up
  lever; it is **not** part of this change and was not measured.

---

## 3. Real daemon: cold `prepare_window` (per repetition)

Time in seconds; the number in parentheses is blocked waits in that phase
(20 ms sleeps for the baseline, readiness waits for the patched module).

| records | variant | rep | status | total | spawn | encode | write (waits) | wait (waits) | read (waits) |
|---:|---|---:|---|---:|---:|---:|---:|---:|---:|
| 52,000 | baseline | 1 | prepared | 8.204 | 0.021 | 0.263 | 6.639 (328) | 1.191 (59) | 0.091 (2) |
| 52,000 | patched | 1 | prepared | 1.341 | 0.002 | 0.253 | 0.029 (1695) | 0.998 (50) | 0.059 (2) |
| 52,000 | baseline | 2 | prepared | 8.478 | 0.021 | 0.273 | 7.197 (356) | 0.910 (45) | 0.077 (1) |
| 52,000 | patched | 2 | prepared | 1.336 | 0.002 | 0.253 | 0.038 (1628) | 0.997 (50) | 0.047 (1) |
| 210,000 | baseline | 1 | prepared | 43.823 | 0.021 | 1.010 | 26.894 (1327) | 15.167 (744) | 0.731 (19) |
| 210,000 | patched | 1 | prepared | 13.760 | 0.002 | 1.241 | 0.133 (6705) | 11.997 (590) | 0.388 (7) |
| 210,000 | baseline | 2 | prepared | 39.777 | 0.021 | 1.182 | 27.581 (1362) | 10.737 (530) | 0.255 (1) |
| 210,000 | patched | 2 | prepared | 12.150 | 0.002 | 1.028 | 0.151 (5813) | 10.715 (529) | 0.255 (5) |
| 400,000 | baseline | 1 | fell back | 80.808 | 0.021 | 1.907 | 54.911 (2712) | 23.970 (1174) | — |
| 400,000 | patched | 1 | fell back | 22.856 | 0.006 | 2.298 | 0.363 (12079) | 20.188 (985) | — |

Parity: at 52k and 210k both variants returned `prepared` with the same
`share_snapshot_sha256` and the same canonical item bytes in every repetition
(the driver checks both).

Reading the table:

- **`request_write` is where the baseline's time went**: 6.6-7.2 s of 8.2-8.5 s
  at 52k, 26.9-27.6 s of 39.8-43.8 s at 210k, 54.9 s of 80.8 s at 400k. The
  patched write takes 29-38 ms, 133-151 ms and 363 ms respectively. The phase
  ends when the pipe has accepted the last byte, so up to one pipe capacity
  of the request may still be unread by the daemon at that point and lands in
  `response_wait` instead. The readiness waits are numerous (the daemon reads
  its stdin in small chunks, so the pipe drains and refills often) but each
  returns as soon as it can progress.
- **`response_wait` is the largest residual and is not addressed by this
  PR**: 0.9-1.2 s at 52k, 10.7-15.2 s at 210k, 20-24 s at 400k, in both
  variants. It is elapsed time between the pipe accepting the last request
  byte and the envelope's arrival, so it contains whatever request bytes the
  daemon had not yet consumed, the daemon's own processing, and the response
  becoming readable; it is not a Rust CPU measurement and its run-to-run
  spread (10.7 vs 15.2 s at 210k) is not attributed here. The baseline's
  sleeps inside it are the coordinator polling an idle pipe every 20 ms while
  the daemon works. Rust-side profiling is needed to split it.
- **`request_encode` (1.0-1.2 s at 210k)** is coordinator time outside the
  wrapped transport calls, derived by subtraction; it is dominated by the
  whole-request `json.dumps` that PR 2 targets but is not an isolated
  measurement of it.
- **Cold spawn + handshake** drops from 21 ms to 2 ms: the baseline's handshake
  read slept one full 20 ms slice before the daemon's first line arrived.

**The 400k point is incomplete and its cause is unresolved.** In both
variants `prepare_payout_window` returned `None` after `response_wait` (a
daemon anomaly: the daemon was retired and a caller would fold in-process);
`response_read` never ran. The compiler discards the anomaly text and the
daemon printed nothing to stderr, so the cause was not diagnosed, and the
observation that both variants fell back does not establish that they failed
for the same reason or that the transport is uninvolved. It should be
captured (the envelope's `error`, or the exception the transport raised)
before 400k is used as a release-gate size. The run was stopped after this
first repetition per variant to bound the experiment; the second repetition
and the JSON capture were not produced.

---

## 4. Relation to the incident numbers

The incident's cold `daemon_prepare` was 47.5-48.8 s at roughly 210k shares
on the production host. On this host the baseline transport spends 26.9-27.6 s
of a 39.8-43.8 s cold 210k prepare sleeping in `request_write`; that fraction
is consistent with the incident's `daemon_prepare` being dominated by transport
sleeps rather than Rust work, but this report does not claim the production
split -- different hardware, different pipe behavior under load, and the
production path also carries the ledger read and record conversion the driver
does not include.

What this change does establish: after it, the coordinator's pipe loops
contribute well under a second at 210k on Linux; the residual is
`response_wait` (daemon-side processing plus response receipt, not split
here) and the unattributed pre-write coordinator time PR 2 targets; and every
byte, digest and canonical item stream is unchanged.

---

## 5. What is not validated here

- The production container image and CPython 3.14 (this host runs 3.12.3 and
  Docker was not used).
- The end-to-end 210k cold path (ledger read → conversion → daemon →
  mirror → first usable Stratum job) and the 20 s recovery target.
- Lease-monitor lateness under this workload; the transport change removes
  sleeps from the builder thread and does not touch the lease thread.
- The pipe-capacity follow-up (`F_SETPIPE_SZ`) and the Rust-side profiling
  needed to split the 10-12 s `response_wait` at 210k.
- The 400k fallback above.
- Measurements on a host without concurrent unrelated load.
