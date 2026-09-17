# Post-batching large-window delivery (#275)

**All 20 cases missed the unchanged one-second target.** Every case delivered
and reconciled exactly 2,000 timed jobs: **40,000 durable matches**, zero failed
delivery attempts, zero refresh/receive errors and zero unmatched issued rows.
Four cases are warmups; the other 16 are four balanced pairs at each window size.
Two-frontend non-regression remains unestablished, and #275 remains open.

## Source, clock and fixture

Measured source: `e4b3c9c9bac8e6c86323c85d71dee423a529db4a`, a measurement-only
change over runtime `3bb2a2dec917a46ea6e90be8e2ee55078bbb4d5c` (#435), including
`58b004fc` (#436) and `30f42150` (#437). All 20 cases used the same source and
release binary, SHA-256
`3da16fe72e08f53c805acc0dabd417c16eb35937bd98afd65160f0c21dec8269`.
Rust/Cargo 1.98.1, aarch64-apple-darwin, optimization 3, no debug assertions or
overflow checks, thin LTO, one codegen unit; build concurrency was two.

The existing harness used only 16 shares. These runs use its existing
`WindowPlan` at 400,000 and 500,000 shares, first/middle/last row round trips,
`ANALYZE`, and exact prepared-window share-count checks before and after refresh.
Modeled native serialization totals are 234,288,895 and 292,888,895 bytes
(about 586 bytes/share), **not measured SQL storage bytes**. Five recipients,
one payout order key, scaled share difficulty, old deterministic accepted
shares, an empty transaction template and a synthetic local node remain.
Bulk fixture loading bypasses share submission: this is not D1 throughput.

Each case gets a fresh schema, initial full-window refresh, then all client
logins and initial work before timing. No new shares or payout-state changes
arrive before the timed parent change. The 600-second snapshot interval and
unchanged cutoff/network/revision/balances permit retained-window reuse; every
whole test process finished within 77 seconds. Reuse is a source-supported
inference, not an exported cache-hit measurement. It avoids a new SQL snapshot
scan, but still permits full-window payout construction and audit hashing.
This qualifies the **window size of a warm-window delivery workload**, not
cold refresh, changing-share windows, recipient cardinality, large templates,
real-node/network latency, separate frontend hosts, full HA or #291.

The original common Tokio monotonic `Instant` starts immediately before
concurrent `refresh_once` polling after the synthetic parent switch and ends
per client at decoded fresh new-parent `mining.notify` with `clean_jobs=true`.
It includes construction, persistence, scheduling and client decoding; login
and automatic polling delay are excluded. The shared 120-second delivery
bound and 240-second body deadline are unchanged; neither replaces the
**one-second target**. Setup remains outside that body deadline.

Every row below has 2,000 total sessions: 2,000 on one frontend (A), or 1,000
each on two (B). Both topologies share one process and two Tokio workers.
Per frontend: two build workers, four PG connections, 128 job admission
permits, connection capacity 2,000, build/persist phase timeout 30s, socket
write timeout 20s, job retention 300s and vardiff disabled. Thus B doubles
aggregate connections/admission. Template/submit freshness, snapshot interval
and health timeout are 600s; no automatic refresh loop runs. The landed
collector settings are unchanged: 128 admitted entries, at most 64 children
per batch, one active batch through cleanup and 1ms collection dwell.

## Every observation

Cases ran serially on 2026-09-17, 15:25:41–15:50:15 UTC. Order is shown below:
400k warm AB then AB/BA/BA/AB; 500k warm BA then BA/AB/AB/BA.
All rows completed and passed durability checks; **every row failed 1s**.
Seconds are rounded to six decimals. p50 preserves the historical lower
median; p95 is nearest rank (receipt index 1,899 of 2,000).

| Window | Run | FEs | p50 s | p95 s | Maximum s | Refresh returns s |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 400k | warm-A | 1 | 3.230278 | 3.322110 | 3.326659 | 2.991360 |
| 400k | warm-B | 2 | 3.365928 | 3.426246 | 3.430160 | 3.163825 / 3.143013 |
| 400k | p1-A | 1 | 3.278070 | 3.362290 | 3.366513 | 3.041561 |
| 400k | p1-B | 2 | 3.440843 | 3.501393 | 3.505879 | 3.191590 / 3.192437 |
| 400k | p2-B | 2 | 3.514601 | 3.593961 | 3.597561 | 3.236020 / 3.239192 |
| 400k | p2-A | 1 | 3.356623 | 3.444127 | 3.448638 | 3.130142 |
| 400k | p3-B | 2 | 3.711224 | 3.769376 | 3.774574 | 3.453061 / 3.445235 |
| 400k | p3-A | 1 | 3.570553 | 3.673386 | 3.680419 | 3.315210 |
| 400k | p4-A | 1 | 3.395292 | 3.489196 | 3.492650 | 3.150861 |
| 400k | p4-B | 2 | 3.900819 | 3.962439 | 3.966664 | 3.618343 / 3.617821 |
| 500k | warm-B | 2 | 4.611609 | 4.699999 | 4.706349 | 4.375152 / 4.350348 |
| 500k | warm-A | 1 | 4.235811 | 4.364833 | 4.371359 | 3.968625 |
| 500k | p1-B | 2 | 4.940862 | 5.015358 | 5.018577 | 4.610626 / 4.611636 |
| 500k | p1-A † | 1 | 4.003933 | 4.105645 | 4.109549 | 3.758087 |
| 500k | p2-A | 1 | 3.974786 | 4.066470 | 4.070260 | 3.716437 |
| 500k | p2-B | 2 | 5.303104 | 5.421429 | 5.422711 | 4.990267 / 4.989880 |
| 500k | p3-A | 1 | 4.636992 | 4.738320 | 4.744466 | 4.344059 |
| 500k | p3-B | 2 | 4.551504 | 4.622688 | 4.627072 | 4.305201 / 4.306776 |
| 500k | p4-B | 2 | 4.613439 | 4.672259 | 4.675732 | 4.373455 / 4.374039 |
| 500k | p4-A | 1 | 4.214351 | 4.335089 | 4.340323 | 3.947809 |

Two/one maximum ratios for pairs 1–4 were **1.041, 1.043, 1.026, 1.136** at
400k and **1.221, 1.332, 0.975, 1.077** at 500k. These are observations from
colocated frontends on a shared host, with no predeclared non-regression
tolerance. They establish neither a general speedup nor non-regression.
The earlier 340/297ms small-fixture pair and historical debug/release reports
are different workloads/revisions and are not controlled comparison baselines.

After timing, each unique delivered ID matched a durable child with the
expected parent, payout revision, prepared key, frontend instance, authenticated
username, subscribed extranonce1/extranonce2 size and equal live SQL/payload
expiry. Issued-row counts equaled timed deliveries, and the decoded prepared
record remained unchanged. This checks identity/storage fidelity and liveness,
not TTL-policy correctness or cross-process search-space uniqueness. Raw
receipts retain client identities and durable payloads. A separate artifact
check recomputed every quantile and matched all 40,000 client/payload identities;
the SQL predicates were checked inside the test before schema cleanup.

## Host, storage and contention

Apple M4 Max (14 logical CPUs), 36 GiB RAM, macOS 26.5.1 (25F80), descriptor
limit 16,384. Dedicated disposable PG 16.14 at `127.0.0.1:55475/postgres`:
`fsync=on`, `full_page_writes=on`, `synchronous_commit=on`,
`wal_sync_method=open_datasync`, `shared_buffers=128MB`, `max_connections=100`,
`work_mem=4MB`, `maintenance_work_mem=64MB`, `wal_compression=off`,
`max_wal_size=1GB`, checkpoint timeout 300s. PG durability settings were verified
by every fixture. RPC and Stratum used loopback ephemeral ports; raw fixture
records contain RPC URLs, while exact Stratum ports were not retained.

The coordinator held other heavy validation. This was still a **shared host**:
GUI applications, static review/artifact readers and idle unrelated Rust
services remained. Before-case one-minute load ranged 4.57–12.66 and the second
one-second CPU sample was 52.85–78.77% idle. During-case memory-free observations
were 32–51%; reported swap ranged 15,856–17,752 MiB, mostly decreasing.
OS maximum test RSS was 794–1,746 MiB at 400k and 975–2,026 MiB at 500k,
excluding PostgreSQL. Full process/memory/swap snapshots were taken about every
0.5s plus command overhead; subsecond interference is not excluded.

**† 500k-p1-A has observed formatter activity.** One process sample at
15:43:10.065 UTC saw Cargo with parent `cargo-fmt`, at 1.9%/0.9% sampled CPU.
It occurred 35.773s into a case whose fixture load alone took 53.093s, so it
preceded delivery timing. The case is retained and labeled; there is no claim
of guaranteed isolation or a quiet-host speedup. No rustc or other test binary
was found in these snapshots. Six unrelated `ord`/`qord` services were present
with sampled CPU 0.0%; that does not prove zero unsampled activity.

Repeated full fixture loading caused frequent WAL-triggered checkpoints,
including PostgreSQL warnings about 21–24s checkpoint spacing. This is real
own-database storage load under the recorded default WAL budget. No checkpoint
was hidden or configuration changed mid-series. Exact checkpoint overlap with
the monotonic delivery bracket was not observed.

Successful-call histogram ranges below cover the four measured repetitions
per topology and sum across frontends. These overlapping concurrent call-seconds
are **not additive wall time, commit duration, lock hold time or per-job latency**.

| Window / FEs | Pool wait sum s (calls) | Settlement wait sum s (calls) |
| --- | --- | --- |
| 400k / 1 | 63.940–72.316 (8,046–8,047) | 0.001331–0.001506 (39–40) |
| 400k / 2 | 116.481–128.786 (8,062) | 0.009387–0.017946 (48) |
| 500k / 1 | 70.877–80.756 (8,046–8,047) | 0.001441–0.001796 (39–40) |
| 500k / 2 | 110.648–151.449 (8,062–8,063) | 0.009555–0.032117 (48–49) |

At 400k the latest refresh return accounts for 90.0–91.5% of final-client
elapsed time; the residual is 0.313–0.365s, not an exclusive fanout phase.
The ordinary observed settlement acquisition waits do not explain the seconds
before refresh returns. Full-window build/hash work is a source-supported
candidate, not a measured phase attribution. Build, hashing, executor scheduling,
prepared SQL and commit remain unsplit; `commit_seconds` is explicitly null.
No lock-removal optimization follows from these observations alone.

A separate supervisor stopped on 8 GiB sampled test RSS, less than 15% host
memory free, more than 512 MiB additional swap, observation failure or 600s
whole-process time. No limit fired and no scale was reduced. This cap does
not replace the original deadlines; standalone harness setup remains unbounded.
A supervisor stop requires checking retained schemas and stopping the owned
disposable primary, since killed processes cannot guarantee Rust teardown.
Every timed case confirmed zero remaining measurement schemas.

## Acceptance and review

| #275 requirement | Status from this task |
| --- | --- |
| 2,000 sessions within 1s; second frontend does not increase time | Not met / non-regression unestablished; every sample retained above |
| Persistence avoids a 3s settlement-lock stub | Not met: unchanged release control waited 3.003873s with an observed waiter |
| Original payout-revision fence survives lock wait | Existing direct compact-issued regression passed; named cross-frontend readiness test not rerun here |
| Concurrent new row-lock policy interleaving | No row-lock policy change authorized or implemented; not newly qualified |
| Lock topology documented | Existing topology retained; no lock independence claimed |
| Durable timed-notification reconciliation | 40,000 exact child matches, zero failed attempts/unmatched rows |

Frozen-source smoke passed 7 tests and 4/4 DB gates. Its intentional retry
control recovered 8 clients after 8 failed attempts and correctly retained
`complete=false`; their durable identities also matched. The two ordinary
8-session smoke maxima were 0.019162s and 0.024091s, not scale evidence.

Independent medium and thermo Opus, Fable adversarial and Sol adversarial
reviews were static during timing. Required reporting fixes landed separately
at `8fb465f4`: incomplete delivery remains the primary error even when validation
fails, other frontends are still checked, received identities survive failed SQL
validation, and delivered-subset verification cannot masquerade as complete
success. Run/progress envelopes explicitly name requested/completed topologies,
position and control sources without renaming gates; non-Unicode controls fail.
Received-subset quantiles remain labeled and include population counts.

These reporting fixes do not reattribute the 20 frozen measurements to a later
SHA. The follow-up release smoke passed 9 tests (4/4 DB gates), including the
failure-precedence controls; formatting, diff check and target release Clippy
with warnings denied passed. Seven real-entry invalid-control cases rejected
empty, zero, unsupported, NaN, bad-order and non-Unicode inputs before fixture
setup. Post-fix retry/ordinary 8-session maxima were 1.009587 / 0.017658 /
0.021085s; the retry remains incomplete despite durable recovered children.
Final review dispositions and CI are recorded in the PR description.

## Reproduction

Use the measured SHA above to reproduce these observations, or explicitly name
any later source. Build before timing, with a dedicated disposable durable PG16
primary and a coordinated interval. Select one topology per process to retain
an independent result even if its paired case fails; `1,2` and `2,1` also work.
The allowed window sizes are `16` (historical default), `400000`, `500000`;
frontend-order values are `1`, `2`, `1,2`, `2,1` (default). Session count is fixed.

```sh
CARGO_BUILD_JOBS=2 cargo test --release --locked -p qbit-prism-server \
  --test b275_persistence_measure --no-run
ulimit -n 16384
PRISM_TEST_DATABASE_URL="$disposable_pg16_url" \
PRISM_TEST_REQUIRE_INTEGRATION=1 PRISM_TEST_GATE_MANIFEST="$unique_manifest" \
PRISM_B275_WINDOW_SHARES=400000 PRISM_B275_FRONTEND_ORDER=1 \
CARGO_BUILD_JOBS=2 cargo test --release --locked -p qbit-prism-server \
  --test b275_persistence_measure measure_2000_sessions_one_and_two_frontends \
  -- --ignored --exact --test-threads=1 --nocapture
```

The observed supervisor invoked the freshly built binary directly, with those
arguments/environment, through `/usr/bin/time -l`; no compilation ran inside a
sample. Repeat the exact table order, use a fresh manifest for every invocation,
retain every exit/failure, and enforce/report the separate resource watchdog.
Test `executed` gates mean a test entered, not that both topologies ran or 1s
passed. On current source, read `B275_RUN`/`B275_RUN_PROGRESS` scope,
`B275_FIXTURE`, v4 `B275_MEASUREMENT`, receipts, issued-row counts and structured
`B275_DURABILITY` together; missing validation is unknown, never zero/success.
Raw JSON, child receipts, host logs, gate manifests, scripts and build metadata
remain outside Git in the coordinator's issue275-post-batch-20260917 artifact
bundle. No production/testnet endpoint was accessed and PR441 proxy code was
neither changed nor exercised by this harness.
