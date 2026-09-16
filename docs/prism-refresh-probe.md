# Coherent refresh economics probes

Issue [#434](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/434)
combines the refresh loop's paired payout-state and accepted-cutoff reads.
It saves one SQL execution and one checkout per pair. It does **not** reduce
the number of full balance reads or semantic balance hashes.

## Read and publication contract

The private `RefreshProbe` contains a `PayoutState` and an accepted share
sequence. A successful observation uses one connection and one repeatable-read
transaction:

1. Begin and set repeatable-read isolation without overriding read-only settings.
2. Read revision and accepted cutoff in one SELECT, retaining the existing
   primary, writable-session and fatal-state guards. The cutoff subquery uses
   the same `ACCEPTED_CUTOFF_SQL` constant as anchored snapshot selection.
3. Read current balances, decode them, and compute the existing semantic
   `prior_balances_digest` on a blocking thread. Their vectors are also
   destroyed there.
4. Commit before returning the observation.

The first data SELECT establishes the snapshot. A concurrent atomic update of
revision, cutoff and balances therefore produces a complete old or new triple.
The previous independent cutoff read could observe shares committed after the
payout-state snapshot; coalescing deliberately makes that selection coherent.
Shares committed after selection are observed on a later refresh.

Public `Ledger::payout_state()` remains share-free. It does not read accepted
metadata, share payloads or stored balance blobs. Both methods use the existing
current-balance SQL and the same digest decoder. No schema, digest recipe,
stored balance order or as-issued identity changes are required.

Refresh takes another probe after waiting for build admission. The admission
owner accompanies blocking decode/hash work, so cancelling the async caller
cannot release that capacity before the actual blocking cleanup finishes.
Database, decode, task and commit-response errors remain errors, including a
lost response to a commit that the server executed. No error becomes valid
zero state or a successful mismatch.

The final idle payout-state equality check remains in place, as do all compact
reservation/publication proofs. The probe is selection metadata, not an
anchored window or publication authority. New shares after window capture do
not veto publication of that immutable selection. Original readiness proofs,
anchors, expiry and deadlines retain their existing boundaries.

## Counted work

Counts cover payout probes and separate accepted-cutoff probes only. They
exclude chain observation/reconciliation, fee RPC, database clocks, anchored
snapshots/share pages, persistence, connection validation and other hashes.
One successful payout or combined probe executes five statements: BEGIN, SET,
metadata SELECT, balance SELECT and COMMIT. These are SQL executions, not a
claim about network packet counts.

| Path without publication-lock retry | Prior SQL / checkouts / balance hashes | Combined probe SQL / checkouts / balance hashes |
| --- | --- | --- |
| Unchanged empty or nonempty work | 11 / 3 / 2 | 10 / 2 / 2 |
| First build, no cached window | 21 / 5 / 4 | 20 / 4 / 4 |
| Build with a cached window | 27 / 7 / 5 | 25 / 5 / 5 |
| Each extra publication-lock proof | +5 / +1 / +1 | +5 / +1 / +1 |

The public refresh regression observes actual PostgreSQL Execute/Query and
DataRow frames. For both unchanged empty and nonempty work with one balance
recipient it measured 11 component executions and two returned balance rows
before integration, and 10 executions and two returned balance rows after it.
It also asserts that the same prepared object remains published and no share
payload is returned. Parse frames do not count as executions.

The private probe regression independently measures one checkout through the
existing acquisition metric and one actual digest callback per invocation,
with five executions and N+1 DataRows for N=0 and N=1. Full-path checkout/hash
totals in the table are derived from these observations and the call graph;
the build rows are call-graph counts, not an instrumented full-build benchmark.

The proxy logs full-refresh elapsed wall time separately. The helper logs its
own elapsed wall time, which includes SQL, transfer and blocking work. Neither
small correctness run supplies meaningful latency percentiles or a production
speedup claim, and nested or separate-process timings must not be added.

## Prior bounded cost investigation

The earlier investigation used PostgreSQL 16.14 with fsync on, 32 MB shared
buffers, no parallel query workers, C collation, a private Unix socket and a
five-second statement timeout. It used a reduced share fixture with 500,000
rows and the existing balance table/function. Balance fixtures had one active
partition per nonzero program group (M=N). This was not production data or the
full migrated schema.

A libpq driver executed the baseline 11-statement idle sequence and the
10-statement coalesced sequence. Ten samples followed warm-up, or five at
100,000 recipients, with alternating order. The Rust digest microtest used
the exact shared digest function and encoding helpers, compiled in release
mode on arm64 macOS; its timer excluded SQLx decoding, row transfer and input
string destruction.

| Returned balance groups N | Baseline DB-client p50 ms | Coalesced DB-client p50 ms | One sorted digest p50 ms |
| ---: | ---: | ---: | ---: |
| 0 | 0.361 | 0.254 | 0.0002 |
| 1 | 0.384 | 0.273 | 0.0007 |
| 100 | 0.398 | 0.371 | 0.0453 |
| 1,000 | 1.835 | 1.778 | 0.3048 |
| 10,000 | 14.342 | 14.463 | 2.3996 |
| 100,000 | 173.741 | 173.618 | 23.7845 |

This supports an overhead reduction at small N, not a material large-N CPU
reduction. Both sequences transfer and hash balances twice. SQL grouping cost
also depends on active/inactive partition rows M, which can exceed N, and
sorts may spill. The semantic digest still sorts bytewise independently of
database collation. No revision-only cache is valid: supported balance changes
can occur without a payout revision bump.

## Regression evidence and limits

| Scenario | Evidence |
| --- | --- |
| Same-revision amount, recipient, order-key and program changes | Real PostgreSQL private-probe test checks the native digest after each change; Coordinator tests rebuild while preserving old reservation balances |
| Concurrent revision, cutoff and balance update | First-SELECT advisory gate returns the old coherent triple; the same SQL under READ COMMITTED is an explicit failing control |
| Balance or revision drift during build admission | Gated Coordinator tests require a fresh snapshot after the wait |
| Checkout, metadata and balance cancellation or SQL timeout | Real PostgreSQL tests recover the single connection and preserve error categories |
| Running hash cancelled | A blocking gate retains build admission until cleanup, while transaction rollback releases its connection |
| Lost or cancelled commit acknowledgement | Wire faults and a commit-response gate prevent successful probe return and verify recovery |
| Original anchor, selected shares, immutable storage order and foreign resume | Existing `refresh_window_split`, `window_reference` and `window_balance_order` suites retained |
| Out-of-order refresh and publication waits | Existing readiness/tip and compact authority proofs retained; older-refresh regression remains green |

The deterministic fee-RPC regression records the unchanged two-observation
behavior: if same-revision balances change between the early and late probe,
the refresh returns `payout state changed during work reuse`, creates no new
reservation, and the next refresh builds fresh economics.

A single late economic probe could halve matching-idle balance scans, but is
not enabled here. A successful late mismatch would instead attempt the normal
fresh build in that refresh. It would no longer observe errors or transient
drift entirely before that final read. Those are explicit behavior changes;
coalescing alone is not evidence that removing the early read is equivalent.

Run the focused checks through `test/prism-native-tests.sh cargo-args` with
PostgreSQL 16, `CARGO_BUILD_JOBS=2` and a private `CARGO_TARGET_DIR`: the library
`ledger::window::payout_state::tests` and `coordinator::miner_tests::refresh_window`
filters, and the `refresh_window_split`, `window_reference`, and
`window_balance_order` integration targets. Added database cases are registered
in `test/prism-gated-tests.txt`. Explicit scale tests and large load benchmarks
are not required for this bounded correctness evidence.
