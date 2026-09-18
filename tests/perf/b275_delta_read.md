# Bounded delta acquisition qualification (#275)

The candidate passed its predeclared **15% median improvement screen**, with
**30.11%** lower end-to-end last-client latency across four balanced 400k pairs.
**Every 400k case still missed one second.** This is a bounded acquisition
candidate; #275 remains open. The [acquisition proof and fallback rules](../../docs/refresh-window-split.md#bounded-delta-acquisition-275)
explain its deliberately restricted applicability.

## Source and method

Exact base: `d78659de608ad30645ba630f15eb8be998b3e42a`. The candidate is the
production implementation accompanying this report. Production source hashes
were frozen before building and remained unchanged through qualification;
subsequent edits extended tests and documentation only. Both used Rust 1.98.1,
x86_64 Linux, release optimization 3, thin LTO and one codegen unit, with at most
two Cargo build jobs. Primary ELF SHA-256 values:

- Base: `7d052237f6343cbec2a6ccd9a966fbd73753772db423bd5b339ffa7e892c924d`
- Candidate: `036da06c4d93d53464913e803a0ef0a2e472184ec7218be01d0570572a575406`

Runs on 2026-09-18 used the existing `b275_persistence_measure` harness,
**2,000 sessions on one frontend and exactly two Tokio workers**. Each case had a
fresh matched PostgreSQL schema, the same five-recipient `WindowPlan`, 400,000
preloaded shares, checked first/middle/last rows and exact prepared-window size.
Initial refresh and client login were outside timing. The timed synthetic parent
change advances the payout revision, so it invalidates as-is reuse and exercises
fresh acquisition even with zero newly appended shares. Existing native hashes,
prepared storage, batch persistence and client delivery all remain in the clock.

Timing begins immediately before concurrent refresh polling and ends at the last
client's decoded fresh `mining.notify` with the expected parent and clean flag.
The unchanged 120-second receive and 240-second body deadlines are not the
one-second target. Source fixtures still use loopback sockets, a synthetic node,
a small empty-transaction template, deterministic old shares and no automatic
refresh polling. All runtime, frontend, admission and collector settings were
identical between base and candidate; no runtime-worker experiment is included.

The host was a KVM x86_64 guest exposing 22 CPUs on an AMD Ryzen 9 9955HX.
A dedicated disposable PostgreSQL 16.15 primary used `fsync=on`,
`full_page_writes=on`, `synchronous_commit=on`, `wal_sync_method=fdatasync`,
128 MB shared buffers, 100 connections, 4 MB work memory, 1 GB maximum WAL,
300-second checkpoint timeout and zero parallel workers per gather. Heavy work
was serialized by one host lock. Other IDE/OS activity remained; samples about
0.5 seconds apart cannot rule out brief interference. Minimum available host
memory across all cases was 23,788,679,168 bytes. RSS below is the whole test
process high-water mark, excluding PostgreSQL, not a phase allocation counter.

## Complete observed results

Small functional cases preceded 400k warm AB, then measured AB / BA / AB / BA.
A means base and B means candidate. Last-client seconds are rounded to nine
places. Only the four measured pairs enter the primary median.

| Case | Base seconds | Candidate seconds | Reduction | Base peak RSS bytes | Candidate peak RSS bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 shares, functional | 0.798950340 | 0.804595039 | — | 99,598,336 | 100,769,792 |
| 400k warm | 2.854928298 | 1.957876016 | — | 807,112,704 | 803,917,824 |
| 400k pair 1, AB | 2.879785824 | 2.076545193 | 27.89% | 807,530,496 | 802,754,560 |
| 400k pair 2, BA | 2.912904581 | 2.048605249 | 29.67% | 807,841,792 | 802,942,976 |
| 400k pair 3, AB | 2.914530941 | 2.001175847 | 31.34% | 807,755,776 | 803,192,832 |
| 400k pair 4, BA | 2.881689705 | 1.993829557 | 30.81% | 808,058,880 | 803,479,552 |
| **Measured median** | **2.897297143** | **2.024890548** | **30.11%** | — | — |
| 400k rollback-gap control, AB | 2.895233627 | 2.077325020 | 28.25% | 808,415,232 | 802,656,256 |
| 400k split-leaf fallback control, BA | 2.969597360 | 3.011332568 | -1.41% | 806,576,128 | 807,710,720 |

All 16 cases completed and reconciled **32,000 unique timed notifications** to
32,000 durable child identities, with no unmatched issued rows. The fixture
checks parent, payout revision, prepared key, frontend, authenticated username,
extranonce1/extranonce2 size and live expiry against stored payloads. An external
artifact checker independently rejoined every received ID and payload identity
and recomputed receipt quantiles. All fixture schemas were removed; no process
supervisor limit fired. The supervisor bounded each process to 600 seconds,
8 GiB sampled RSS, at least 6 GiB available memory and no more than 512 MiB
additional swap. Those limits supplement the unchanged harness deadlines.

The primary candidate's observed RSS was slightly lower in every measured pair.
The implementation retains one moved window plus one SQL page, trims before
append and releases excess vector capacity after a large retirement. This is a
structural bound plus short observations, not a 24-hour memory soak.

## History controls and applicability

The final count proof permits ordinary sequence holes. The gap control invokes
the real revision-gated append with a false commit gate, verifies the rollback,
then appends one valid share and verifies the two-slot sequence advance. It does
this before initial acquisition, retaining a 400k suffix. Thus a persistent
normal rollback gap remains eligible on the timed revision-only refresh;
focused differential tests separately cover appended-delta acquisition itself.

The split control seeds the same 400k history across two native attached leaves
instead of one. Its candidate falls back to the full read. The single pair
observed 1.41% extra latency and 0.14% extra RSS; fallback overhead exists, and
this control establishes neither general non-regression nor a speedup.

These controls were external test-only copies of the same harness. The base and
candidate received identical fixture patches; production source stayed unchanged.
Their ELF SHA-256 values were respectively
`8702381e403f4bee676921ffa35141e4e64927b8030963bcfbff3225847aa2c5` and
`ed6bdc748bbfbabe4e46198585b0616fb14aa1aeddadab9ef1e0e34e262a56c9`.
An initial candidate control build incorrectly reused the base artifact through
a shared Cargo cache; identical hashes exposed it before any control ran.
That artifact was rejected, the server package was explicitly cleaned, and the
candidate was rebuilt from its own recorded source path before measurement.

Single-leaf proof and the final eligible-row count prevent endpoint-only reuse
across an interior partition hole or newly eligible/retroactively inserted row.
The count is still O(window) metadata work. Cross-partition windows, detach or
restore, absent evidence, shared cancelled ownership, short history, retargets,
regressed anchors/cutoffs and more than 4,096 delta sequence slots use full reads.
A default leaf spans 16,777,216 slots, but actual production eligibility rates
were not measured. No broad deployment policy or new database schema is added.

## Reproduction and limits

Build the exact base and candidate into separate output directories, then use a
dedicated durable PostgreSQL 16 database and run each binary serially:

```sh
CARGO_BUILD_JOBS=2 cargo test -p qbit-prism-server --release \
  --test b275_persistence_measure --no-run
PRISM_TEST_DATABASE_URL="$TEST_DATABASE_URL" \
PRISM_TEST_REQUIRE_INTEGRATION=1 \
PRISM_B275_WINDOW_SHARES=400000 PRISM_B275_FRONTEND_ORDER=1 \
  "$MEASUREMENT_BINARY" measure_2000_sessions_one_and_two_frontends \
  --ignored --exact --test-threads=1 --nocapture
```

Keep fixed settings and fresh schemas; warm AB, then alternate AB/BA across at
least three pairs. Archive binaries, source hashes, raw received/durable payloads,
process resource samples and every failed attempt. Compare last-client medians,
not summed overlapping histograms. The two history controls require the fixture
modifications described above; they are not new runtime environment controls.

This screen does not establish the one-second goal, 500k performance, two-frontend
non-regression, multiple processes/hosts, AArch64 results, real node/network cost,
share submission throughput, arbitrary recipient cardinality, cold start or
production retention frequency. Authority/settlement lock policy is unchanged.
Independent Tier 3 review remains required before merge.
