# `mining.notify` fan-out cost, rendered per session vs once per work (#861)

**Building and serialising a new tip's `mining.notify` for 2,000 sessions took
104 ms of one core at the 2,600-recipient coinbase and 9.8 ms at a 130-recipient
one. Rendering the shared body once per published work takes that to 3.3 ms and
0.2 ms, and the allocations from 94,000 to 2,025 per fan-out.** The bytes each
session writes are unchanged (`crates/qbit-prism-server/tests/notify_render.rs`
compares them byte for
byte with the old path).

## What is measured

Driver: `crates/qbit-prism-server/tests/notify_render.rs`,
`fan_out_cost_before_and_after` (`#[ignore]`d). Single-threaded, no socket, no
database, under a second when optimised:

```sh
cargo test -p qbit-prism-server --test notify_render \
  --config 'profile.dev.package.qbit-prism-server.opt-level=3' \
  --config 'profile.dev.package.serde_json.opt-level=3' \
  --config 'profile.dev.package.hex.opt-level=3' \
  -- --ignored --nocapture
```

Without the `--config` flags it runs the unoptimised test build (about 25 s).

- **Work.** One published work per shape, built as the coordinator builds its
  shared base wire (`Job::from_manifest` over a `qbit_pool_builder` manifest):
  130 or 2,600 recipients with 32-byte P2MR programs, a 12-level merkle branch
  (a block of 2,049 to 4,096 transactions), mainnet-like bits.
- **Sessions.** 2,000 jobs `reassign`ed from that work, as
  `Coordinator::build_job` stamps them: each its own job ID
  (`<instance>-<32 hex>`) and extranonce1; `clean_jobs` alternates.
- **Before.** Per session, what `announce_job` did at `55581716`:
  `serde_json::to_vec(&job.notify())` and the newline `write_json` appends.
- **After.** Per session, `NotifyBodies::line`, with a fresh `NotifyBodies` per
  fan-out, so the first session of each fan-out renders the body and the other
  1,999 reuse it.
- **Statistic.** Wall time of a whole 2,000-session fan-out, fastest and median
  of 9 (130 recipients) or 3 (2,600) repetitions. Allocations: calls to the
  global allocator (alloc, alloc_zeroed, realloc) and bytes requested, on the
  measuring thread, over one fan-out.

## Results

AMD Ryzen 9 9955HX (22 logical CPUs, shared development host, load average
1.9 to 2.5 during the run), Ubuntu 26.04, Rust 1.98.1, measured 2026-11-12.

Optimised (`opt-level=3` for the server crate, `serde_json` and `hex`):

| coinbase | message | before: fastest / median | after: fastest / median | before: allocations, bytes | after: allocations, bytes |
| --- | --- | --- | --- | --- | --- |
| 130 outputs | 12,405 bytes | 9.8 / 9.8 ms | 0.2 / 0.2 ms | 94,000, 100.5 MB | 2,025, 24.9 MB |
| 2,600 outputs | 224,829 bytes | 104.4 / 104.6 ms | 3.3 / 3.3 ms | 94,000, 1,799.9 MB | 2,025, 450.6 MB |

Unoptimised test build (what CI's debug test binaries run), from the driver's
first run, with 21 and 5 repetitions and before the review fixes to the cache
lookup:

| coinbase | before: fastest / median | after: fastest / median |
| --- | --- | --- |
| 130 outputs | 312.9 / 318.5 ms | 1.8 / 1.8 ms |
| 2,600 outputs | 5,157.0 / 5,167.0 ms | 7.6 / 7.7 ms |

Per session, before: 47 allocations (the `Value` tree, a copy of each coinbase
half and of each hex branch entry, and the growing output buffer) and about
900 KB requested at 2,600 recipients. After: one allocation, the session's own
line (225 KB at 2,600 recipients), plus one body render per fan-out.

## Reading it

- The saving is per-session CPU and allocation on the runtime's worker
  threads, the same threads that turn share appends around, during a new
  tip's fan-out. At the 2,600-recipient shape it was about 50 µs per session
  (100 ms of one core per fan-out on this machine, more on a slower runner);
  at 130 recipients about 5 µs per session.
- The remaining cost is one copy of the body into each session's line, so
  that the line is still written with one `write_all`, as before. A vectored
  write of the head, the shared body and the tail would save that copy too;
  it was left out to keep the socket writes exactly as they were.
- At the start of a fan-out, the sessions that miss the body at the same
  moment each render it (at most one per runtime worker thread) and the first
  kept is used; this measurement is single-threaded and renders it once.
- Not measured here: the rest of the fan-out (job build, persistence, the
  `mining.set_difficulty` write, socket writes). The tip-delivery load presets
  (`run-load` on the PR) measure the fan-out end to end.
