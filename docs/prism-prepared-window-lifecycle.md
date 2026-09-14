# Prepared-window loss and ready snapshots (#339)

This fixes a reproduced cache-eviction source of unnecessary ledger reads.
It does **not** establish the cause or frequency of the historical production
losses or the 17 ready snapshots in #254.

Investigation base: freshly fetched origin/2.x.x at
ede86019aa20b8f2dce0fd110ba6e8a26d5c582b. The isolated worktree branch is
daemon-window-snapshot-reuse. No main-branch code or PR #335 implementation
was substituted for that base.

Read: #339 and its acceptance criteria, #254 and its September 9 comment,
#332, #207/#228 and their closure comments, and PR #235. Existing issues and
PRs were checked before implementation. PR #335 was initially open at
f6a9fbcd31d56721fd5d2d663f660d308cd843c7 and advanced during this work to
02f4b1ee0d371c3c0e7bbb13fd2eabb26726b487, still open. Both heads were checked;
the delivered integration replay uses the latter. #340 owns generic shared-build
exception retention; #341 owns the remaining asynchronous ownership audit.
The 3.x.x refresh design in #274 is separate.

## Decision paths and established findings

There are three distinct identities:

1. The immutable canonical share digest identifies bytes. It does not alone
   identify a fold's anchor, window weight, or a daemon process.
2. A daemon cache entry is either an Uploaded build vector or a Prepared fold.
   Only the latter can advance. Its epoch tag is diagnostic; coordinator
   append fences decide validity.
3. The coordinator's armed payout artifact also binds payout generation,
   difficulty, balances, append epoch and a frozen anchor. It can serve a
   ready build even when the daemon needs its bytes uploaded again.
   A needs_window build response uploads the selected artifact without
   acquiring a ledger snapshot.

Before this change both daemon entry kinds shared a two-entry LRU. Full
preparation, byte-changing advances and successful bundle builds inserted
or promoted entries. Anchor-only advances replaced their entry in place.
Two distinct historical build uploads could evict the newest prepared base.
The next advance returned needs_full and the coordinator read the entire
ledger, despite an unchanged daemon process and verified coordinator bytes.

The failing-before test is
PreparedWindowLifecycleTests.test_build_upload_pressure_does_not_force_another_snapshot.
At the exact base, four successful small uploads followed by a coordinator
advance produced **two** full snapshots instead of the asserted **one**.
The same-process and digest-equality assertions passed. After this patch,
all assertions pass and the advance is incremental.

The correction reserves **one of the existing two slots** for the most recent
successful preparation/advance. Build recency cannot evict it. Another
preparation can supersede it; older prepared entries still expire under the
same capacity bound. The Python upload predictor mirrors the reservation
and remains a hint repaired by needs_window.

This changes retention preference, not the two-entry bound. A large prepared
window can remain resident while small historical uploads rotate through
the other slot. With only uploaded entries the old two-entry LRU remains.
Existing transient fold/build working sets are not reduced. No worker,
queue, authority, heap traversal or dependency is added.

| Event | Established decision and read cost |
|---|---|
| Template-only change with valid artifact | Reuse its bytes and frozen anchor; zero snapshots. Post-anchor shares do not invalidate it. |
| Ordinary post-anchor append | Delta read and advance; zero full snapshots. Old snapshots remain immutable. |
| Payout publication / tip change | Reconcile balances and generation, preserving the verified incremental window where valid. The replay exercises reserve/prepare/block/publish and real tip polling. |
| Build-cache pressure | Previously evicted the newest prepared base; now zero full snapshots in this case. |
| Competing preparations | Can evict an older base; one recovery snapshot when that base is next requested. |
| Same digest replaced at a newer anchor | Bytes still exist, but advancing backwards violates the fold's anchor invariant. The recovery read is distinct from absence. |
| Uploaded entry under required digest | Build bytes exist without advance state: uploaded_only, not a crash. |
| Process exit / cancellation / transport retirement | A replacement process lacks the fold; one recovery snapshot, with process generation and observed retirement category recorded separately. |
| Daemon busy | The process/base survive. An advance-required materialization can take one existing oracle fallback because the mirror cannot advance itself. Contention does not inherently require a second read later. |
| Late-visible predating append | The append epoch invalidates affected anchors. One exact read reacquires the window; old-epoch work cannot reinstall. |
| Periodic self-check / deliberate recenter | One independent oracle read. Oversize recenter still prepares the adopted digest; the next advance reads no second snapshot. |
| Ready artifact refused | Admitted immutable inputs determine the fallback. Attribution records the actual admission refusal, rather than a later re-probe after a race. |

window_daemon_state_lost remains the compatibility **outcome**, not crash
attribution. Historical reason strings and process-memory ratios do not
identify the dominant production cause. These reproductions prove mechanisms,
not their production occurrence rate.

## Bounded attribution

The daemon retains eight eviction tombstones containing only digest and
cause. Missing prepared lookup reports evicted_by_build, evicted_by_prepare,
uploaded_only, or not_held. The last includes expired diagnostic history and
older protocol-v2 daemons; it makes no causal claim. Invariant refusals include
the actual base anchor, weight and epoch. No discarded window is retained
for diagnostics.

The compiler assigns a monotonically increasing process generation at spawn.
Retirement categories distinguish shutdown, observed exit, cancellation,
timeout, EOF, I/O, malformed/protocol responses, rejected requests, handshake
failure and a partially written request. Observed return codes are included
where available. Commands and signing seeds are not logged. Cancellation
retirement is not labelled an unexplained crash.

The metric qbit_prism_window_lifecycle_total uses closed event/reason sets
from lab/prism/window_lifecycle.py. Ready snapshot reasons are disabled,
absent, payout_generation, difficulty, append_epoch, anchor_missing,
audit_ceiling, published_unavailable, publication_race, artifact_replaced,
balances, not_probed and unknown. Direct callers explicitly bypassing the
probe report not_probed. Normal shared admission captures the refusal in
request-local context and carries only its string through scheduling.

Counters include every preparation decision and ready snapshot attempt,
including failed reads. Existing full-rescan and ledger timing families
still account for background/self-check reads. No digest, PID, anchor,
generation, exception text or arbitrary daemon category becomes a metric
label. The diagnostic ring holds 64 scalar events. Each event/reason logs
at most once per 30 seconds; a separate counter reports suppressed logs.
Logs are samples; counters are complete. Neither retains payloads, exceptions
or callbacks.

Response metadata is additive protocol v2. Older clients ignore it; new
clients can talk to older daemons but cannot obtain the cache fix or complete
attribution from them. An approved rollout must include the rebuilt Rust
binary as well as Python source.

## Replay and validation

Run from an isolated checkout with a locally built daemon:

    cargo build --locked --release -p qbit-prism --bins
    PRISM_TOOL_BIN_DIR="$PWD/target/release" \
      python3 -m unittest -v tests.test_prism_prepared_window_lifecycle
    PRISM_TOOL_BIN_DIR="$PWD/target/release" \
      python3 tests/perf/prepared_window_lifecycle.py \
      --records 375000 --cycles 2 --json /tmp/window-375000.json
    PRISM_TOOL_BIN_DIR="$PWD/target/release" \
      python3 tests/perf/prepared_window_lifecycle.py \
      --records 400000 --cycles 2 --json /tmp/window-400000.json

For the unpatched base, copy only the replay, its test-support module and
window_lifecycle.py into a disposable checkout. Build that checkout's
unchanged Rust source and use --pressure-reads 1. This asserts the reproduced
unnecessary read, without altering its implementation. The added telemetry
module is not called by the baseline runtime.

The replay uses the real coordinator, shared-build scheduler, delivery/
disconnect registry, in-memory ledger and Rust daemon. It drives 2-second
logical template refreshes, 75-second tips, client disconnect/reconnect,
ordinary appends, payout-generation publication, cache pressure, prepared
eviction, process replacement, busy fallback, periodic checks, predating
append invalidation, cancellation and drain.

Only fixture RPC and socket sends are substituted. Tiny competing windows
are intentional: eviction uses an **entry** limit, so even small historical
uploads can cause a full production-sized read. Cancellation stops only the
task's disposable daemon, cancels a real request, retires the indeterminate
stream and joins its worker before recovery. No production process is
inspected or signalled.

Synthetic identities are padded to 620-byte canonical records. The initial
375,000-row window occupies 232,875,001 array bytes; 400,000 occupies
248,400,001. Ordinary and late-visible appends then change the bytes.
GC stays enabled; there is no forced collection in ordinary replay.
Logical cadence is accelerated, not a real-time soak.

The report records source/binary hashes, every stage's reads, reasons, wall
time, diagnostic wake samples, and declared-root ownership after drain.
Ownership inspection covers current artifacts/mirror, bundle/serialization
caches, ready work, jobs and graveyard contexts without parsing lazy sequences.
Canonical buffers are deduplicated by identity. Parsed-sequence row totals
are **not** distinct parsed objects: list/tuple aliases can share rows.
The fixture's durable ledger, transient frames, daemon allocator and other
#332/#341 roots are outside this inventory. It is not a production byte bound.

Measured locally on September 14, 2026, with Python 3.14.6 on macOS ARM64.
[Raw results](../tests/perf/prepared_window_lifecycle-results.json) retain every
stage, fixed-category counter, bounded diagnostic trace and exact source/
binary hash. The final replay corrected an export-only variable shadow that
had replaced the source identity; earlier exports are not the delivered data.
The patch cases use the recorded base plus the exact changed-file hashes;
the integration case uses PR #335 at
02f4b1ee0d371c3c0e7bbb13fd2eabb26726b487 plus this patch.

| Case (two cycles) | Initial canonical bytes | Full reads | Summed stage seconds | Max sampled wake lateness (ms) | Wakes >=400 / >=550 ms |
|---|---:|---:|---:|---:|---:|
| baseline-375000 | 232,875,001 | 17 | 73.169 | 244.884 | 0 / 0 |
| final-375000 | 232,875,001 | 15 | 67.047 | 283.881 | 0 / 0 |
| final-400000 | 248,400,001 | 15 | 71.238 | 263.376 | 0 / 0 |
| integration-400000 | 248,400,001 | 15 | 123.163 | 243.081 | 0 / 0 |

These are individual local observations on an uncontrolled host, not a
throughput comparison or a real-time lease result. Initial canonical digests
match between baseline/patched 375k and patched/integration 400k.

Each patched case reads once cold, then seven times per cycle. The 14 cycle
reads are: competing prepared eviction (2), observed process exit (2), busy
fallback (2), periodic self-check (2), late-visible append (2), cancellation
recovery (2), and deliberately absent ready artifact (2). Existing
window_daemon_state_lost groups the six eviction/exit/cancellation recoveries;
the new generation/retirement/base-state trace distinguishes them. The
baseline has two additional state-lost reads, both immediately after
unrelated build uploads. Valid template/client refreshes, ordinary appends,
payout publication and the post-check advance read zero full snapshots.

Cycle drain inventories and exported counters precede final teardown; the
finally block shuts down the task-owned executors and daemon. The drain
inventory deliberately exposes unresolved ownership limits:

- final-400000: canonical buffers 2 -> 2 (496,801,240 -> 496,802,482 bytes); parsed sequences 2 -> 5, aliased row totals 800,004 -> 2,000,014; graveyard entries 2 -> 3.

- integration-400000: canonical buffers 3 -> 4 (745,202,481 -> 993,606,206 bytes); parsed sequences 1 -> 2, aliased row totals 400,002 -> 800,006; graveyard entries 2 -> 3.

Current/history roots remain legitimate owners. Neither result proves a
numerical production ownership bound or a plateau; that remains #332/#341
work. The separately tracked returned artifact wrappers were all released
without forced GC, but that narrow observation excludes installed replacements.

Exact validation commands (PRISM_TOOL_BIN_DIR pointed at the local release
binaries for Python commands):

| Command | Result |
|---|---|
| cargo build --locked --release -p qbit-prism --bins | Passed; final build 2.79 s. |
| cargo test --locked -p qbit-prism --bin qbit-prism-build-audit-bundle --test audit_cli | 18 integration tests passed; binary has zero unit tests. |
| python3 -m unittest discover -s tests -p 'test_*.py' | 3,690 tests, 293.014 s, OK, 44 skipped. |
| python3 -m unittest tests.test_window_pipeline_parity tests.test_prism_payout_window_daemon_recenter | 33 passed, 0.245 s. |
| QBIT_WINDOW_PIPELINE_PARITY_ADAPTER=rust-daemon python3 -m tests.window_pipeline_parity_gate | 7 passed, 0.323 s. |
| python3 -m unittest tests.test_prism_prepared_window_lifecycle tests.test_prism_first_job_latency tests.test_prism_job_builder | Final metadata changes: 146 passed, 5.308 s. |
| PR #335 integration command below | Latest head: 346 passed, 12.856 s. |
| ruff check lab/prism/window_lifecycle.py tests/test_prism_prepared_window_lifecycle.py tests/perf/prepared_window_lifecycle.py | Passed. |
| git diff --check | Passed. |

The full run includes accepted-parent publication, stale-grace, append/
generation fencing, bundles, payout state and lease regressions. Its first
run found one introduced fake-process PID assumption; the corrected full run
passed. Earlier focused runs caught metric ordering assertions, also corrected.
The final process-generation metadata additions were checked by the 146-test
suite and the latest integration suite. The intentional failing-before cache
regression remains the evidence for the behavioral correction.

In the disposable PR #335 checkout, executed:

    python3 -m unittest \
      tests.test_prism_prepared_window_lifecycle \
      tests.test_prism_payout_window_daemon_recenter \
      tests.test_prism_window_pipeline_rust tests.test_prism_payout_state \
      tests.test_prism_job_builder tests.test_prism_window_oracle \
      tests.test_prism_async_failure_ownership tests.test_prism_metrics \
      tests.test_prism_first_job_latency

The earlier PR head f6a9fbcd31d56721fd5d2d663f660d308cd843c7 also passed
344 focused tests; its helper replay was superseded by the latest-head
400k measurement. Incorrect initial integration path/module invocations were
corrected before these passing runs.

| Acceptance requirement | Local coverage / remaining gate |
|---|---|
| Decision chain and bounded attribution | Process generation/retirement, cache states, invariant metadata and admission refusal are recorded separately. Historical production attribution remains unknown. |
| >=375k and 400k real coordinator/daemon | Both sizes pass; refreshed templates, client churn, shared cache, payout/tip generation, append invalidation, cancellation and drained queues exercised. |
| Count and explain every read | Exact per-stage assertions and raw reason counts; two cache-pressure reads removed; valid template-only reuse costs zero. |
| Replacement vs recenter vs busy/unavailable | Real-daemon regressions separate competing preparation, same-digest anchor replacement, uploaded-only state, process replacement, busy survival and oversize recenter. |
| Focused correction and correctness | Failing-before/passing-after cache regression, frozen real-daemon parity and relevant full-suite controls pass. |
| No second scan after verified recenter | Existing four recenter controls plus real-daemon oversize regression pass. Incorporation into #254 production qualification/approved soak remains outstanding. |


## Work budget and operational gates

The demonstrated budget is zero full reads for valid template-only reuse,
ordinary append, payout-only publication and unrelated build uploads.
The adversarial replay deliberately demands seven reads per cycle: one each
for competing-preparation eviction, process replacement, busy fallback,
periodic check, late-visible append, cancellation recovery and explicitly
retired ready artifact. Cold start adds one. The baseline adds an unnecessary
build-pressure read per cycle. No timing threshold or lease setting changes.

This is an event-conditioned count budget, not a production scans/hour claim.
Periodic checks retain their existing 3600-second configuration and bounded
critical-path deferral. Audit-age, append, generation and publication fences
still govern other reads. Production must measure event frequency and explain
ready-refusal reasons; moving recovery work to a helper does not justify it.

PR #335 integration uses a disposable checkout at its exact latest checked SHA.
Recheck compatibility if #335 or the target branch advances before merge. Textual
conflicts are confined to adjacent imports in payout_state.py and the ready
instrumentation insertion beside isolated_shares initialization in
job_bundle.py; retain both sides. No exception-retention, self-check
representation or isolated-oracle implementation is copied onto this branch.
The integration replay uses --spool to exercise the real #335 helper with
a fixture spool API. It does not qualify PostgreSQL MVCC or production I/O.

The correction is independently reviewable and does not block merging #335's
separate fixes. The demonstrated unnecessary read is a deployment
qualification issue for the cache-pressure workload; helper isolation alone
does not remove it. #340 and #341 keep their own dispositions.

Production source/runtime verification, historical exit attribution,
PostgreSQL qualification, numerical ownership limits under actual job-history/
stale-grace policy, and the separately approved **at least two-hour soak**
remain outstanding in #332/#254. Neither incident is closed. Local wake
samples are not the real lease monitor and do not prove its guarantee.
Push, merge, deployment, service restarts and incident closure require
separate authorized workflows; none is performed here.
