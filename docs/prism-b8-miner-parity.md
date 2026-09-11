# Native miner decision parity (B8)

Reference: `2.x.x` commit `95ffe063846d51f83999a66cc654da5f7476fdef`,
`tests/test_prism_retained_jobs.py`, `tests/test_prism_hot_path.py`,
`lab/prism/tip_refresh.py` and `lab/prism/job_delivery.py`.

The native tests execute `Coordinator::submit`, `refresh_once`, job construction,
persistence and resume. Narrow `SubmitLedger` and `WorkLedger` interfaces replace
only storage I/O in ungated tests. Production implementations delegate to the
existing ledger operations, including transactional `append_at_revision`.
Loopback RPC fixtures, controlled completion gates and a paused monotonic clock
exercise the real decision code. Socket tests use real Coordinator persistence
and resume; there is no successful in-memory resume substitute.

## Restored behavior

Detected chain state fences candidates immediately. Share credit selects one
coherent view containing the prepared payout and published tip's hash,
predecessor, observation age and publication provenance. Older asynchronous
observations and parent lookups cannot overwrite a newer view. Submit fallback RPC does not invent a published
transition or turn an unavailable chain lookup into a stale-share rejection.
After fallback RPC completes, admission captures the current publication pair
again; only a publication matching the returned hash can supply grace provenance.
The live CTV fee check still rejects an already obsolete fee before fallback RPC.

Long-poll replies wake refresh. A changed valid hash triggers fresh chain-info
verification without waiting for work construction or persistence. Verification
is ordered when its request starts, so a later observation can supersede it.
It detects the chain tip without publishing work or refreshing full-readiness
timestamps. Failed or malformed verification revokes admission; a notification
channel failure instead leaves last-good readiness to age and retries with
shutdown-aware backoff. Same-hash notifications require no verification RPC.

The last published work retains ordinary authority for
`PRISM_SUBMIT_TIP_MAX_AGE_SECONDS` (default 10). A detected departure also grants
the pinned legacy replacement-build lease, bounded by
`PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS` (default 120) from the first
departure. Either budget can preserve authority. Failed refreshes and newer
detected tips do not renew the lease; returning to the published tip clears it.
Zero submit-cache age forces RPC and disables this lease. A lease cannot revive
an older same-parent payout snapshot. This restores the setting's miner-credit
budget; native process supervision and template refresh retries are unchanged.

After successful publication, exactly-one-parent share grace starts at each
connection's first successful replacement delivery. Failed or pending delivery
keeps grace open. Same-tip deliveries preserve the first anchor; a real tip flip
reanchors it. Startup baseline and submit-only observation cannot open grace.
A coherent published retention hint survives cache expiry or cache age zero;
credit classification still performs its own authoritative checks.

Active eviction retains original context, worker, target and version mask in a
session-local graveyard. Its same-tip TTL starts at burial and its same-tip cap
is the configured count N. Previous-parent entries use delivery grace and are
removed when a known predecessor proves them ineligible. The graveyard's hard
3N cap accommodates the old active set, old graveyard and new same-tip graveyard;
including the unchanged active N bound, each session holds at most 4N entries.
Original username admission permits survive while retained work can credit and
are reused on same-connection reauthorization. Expiry, actual capacity eviction,
payout replacement and disconnect release them. Reconnect still uses the exact
worker ownership, same-parent, payout and absolute-expiry checks in Coordinator.
Absolute expiry is checked again after an awaited resume returns.
The connection timer checks both active and retained work before applying its
initial-work timeout. Expiry of an older resumed job cannot disconnect a mature
connection while newer retained work can still earn credit. Once both sets are
empty, the existing timeout and username-permit cleanup still apply.

Accepted records preserve issued economics and original-worker deduplication.
Eligible prior-parent shares use the stale-grace policy and the current durable
revision. Published-build lease shares retain ordinary policy. Neither exception
allows an old-parent or old-revision block candidate. Readiness revocation during
an awaited lookup rejects before persistence, and the ledger's transaction
remains the final cross-frontend revision fence.

## Prepared dependency lifetime

Issued work now retains its original shared prepared record atomically through
its absolute reconnect deadline. Expired metadata can be extended; a physically
pruned dependency is repaired from the original typed record under bounded
serialization capacity. Original worker, target, policy, coinbase suffix and
payout revision remain unchanged. See [the dependency lifetime contract and
focused regression evidence](prism-b8-prepared-dependency.md).

## Ungated regression coverage

| Boundary | Executable coverage |
| --- | --- |
| Frozen issued economics; grace delivery and exact deadline; startup; skipped/reorg tips; old-parent block-only rejection; same-parent payout replacement | `coordinator/miner_tests/credit.rs` |
| Cache reuse, no observation, aged/zero cache, fallback failure, parent RPC failure | `coordinator/miner_tests/observations.rs` |
| Slow observation and parent completions; coherent selection; readiness revocation; revision change before append | `coordinator/miner_tests/interleavings.rs` |
| Coupled payout/lease selection; real publication during fallback; unrelated publication rejection; CTV fee rejection before failed fallback | `coordinator/miner_tests/admission_races.rs` |
| Actual long-poll loop during blocked refresh; fresh verification ordering; failed/malformed verification; same-hash RPC count; shutdown during wait, verification and backoff | `coordinator/miner_tests/blockwait.rs` |
| Zero-grace published authority; nonrenewing divergence lease; expiry; return/redeparture; candidate-only observation | `coordinator/miner_tests/published_lease.rs` |
| Real refresh orchestration at 1/32/128 clients with identical RPC counts; build/persist/publish gates; repeated refresh failure; stale-vs-unavailable resume; obsolete payout issuance | `coordinator/miner_tests/refresh.rs` |
| Durable dependency expiry; delayed publication; return/redeparture; original bootstrap repair; concurrent repair and cancellation | `coordinator/miner_tests/prepared_expiry.rs` |
| Actual socket delivery, original-worker retained dedup/weight, unknown-before-node-RPC, absolute expiry crossing resume | `stratum/stale_grace_tests.rs` |
| Mature-session timer after resumed-job expiry, retained-job credit and final cleanup; unauthenticated/never-usable timeout controls | `stratum/session_timer_tests.rs` |
| Zero/aged cache undelivered grace, real reconnect ownership and parent controls, TTL/capacity/permit lifecycle, unrelated-parent pruning, hard retention bound | `stratum/retained_tests.rs` |

All paths are under `crates/qbit-prism-server/src/`. The prepared-dependency
change adds ten ungated regressions to the earlier miner-parity coverage. The config CLI test also covers both restored settings' defaults,
fractional values, zero behavior and invalid inputs. Existing protocol tests
exercise permit reuse across reauthorization, timer expiry, payout replacement,
and disconnect with active capacity eviction.

The grace negative control replaces the production grace predicate with false:
`observed_tip_pending_prepared_and_revision_still_credits_exact_prior_parent`
fails with stale-job. Restoring the predicate makes the same test pass.
Readiness revocation, stale resume classification and obsolete payout issuance
also reproduced as failing real-Coordinator tests before their fixes.

The review regressions were run against unchanged pre-fix production code on
the rebased branch. They reproduced an old payout borrowing a newer payout's
lease, publication-during-fallback returning stale-job, and a discarded long-poll
notification admitting an old-tip candidate while real refresh awaited persistence.
The payout race replays the handoff between two real refresh-built publications
at the existing locks; it executes the actual submit and final revision fence.
Restoring the fixes passes all 42 Coordinator miner tests, including the ten
new admission and long-poll cases.

## Qualification

Run the native suite with Rust 1.89 and the lockfile. The database and node
fixtures must be disposable; each database test uses an isolated schema.

```sh
CARGO_TARGET_DIR=/tmp/prism-b8-target \
PRISM_TEST_DATABASE_URL="$DISPOSABLE_POSTGRES_URL" \
QBITD_BIN="$LOCAL_REGTEST_QBITD" \
PRISM_TEST_PG_BIN_DIR="$LOCAL_POSTGRES_BIN" \
  cargo +1.89.0 test --locked -j2 -p qbit-prism-server --all-targets \
    -- --test-threads=2 --nocapture
```

With the same disposable environment, run the two explicit PostgreSQL acceptance
targets as CI does. They fail when the database configuration is missing:

```sh
cargo +1.89.0 test --locked -j2 -p qbit-prism-server \
  --test issued_job_dependency -- --ignored --test-threads=2
cargo +1.89.0 test --locked -j2 -p qbit-prism-server \
  --test stratum_admission_postgres -- --ignored --exact \
  ten_thousand_unsubscribed_connections_do_not_advance_postgres_sequence
```

Final shipping qualification also runs `--workspace --all-targets`, exercising
the separately landed payout-vector, window-corpus and SQL-window-oracle tests.
The commit-specific result below is historical; final-head results belong in the
PR's qualification record rather than being inferred from an older run.

A full qualification must report the gated database, live node and physical
failover tests as exercised, rather than counting their environment-skipped
early returns as proof. In the high-difficulty live test, a newly connected miner
can receive still-published work during replacement construction. The invalid
future-timestamp case therefore waits for delivery of the confirmed block's
parent before submitting; the existing no-ACK/no-credit and terminal outbox
assertions remain unchanged.

Qualification on 2026-09-10 after rebasing onto `3.x.x`
`f39442755448b1483bd5e183d8d972c7e461b997`, at runtime commit
`551b741cb4a1b7b8cb9729936b5b9f6929d60942`, with all three disposable integration
gates enabled:
208 tests passed, zero failed, zero ignored, across 20 target summaries. This
includes all six candidate-lease database tests, 29 ledger database tests,
eight real qbitd regtest cases, two physical PostgreSQL failover tests, five
readiness cases and 18 Stratum protocol tests. No environment-skip message was
emitted. The live high-difficulty case credited exactly 1,000,000 network work
and rejected the invalid future-timestamp block without ACK or share credit.
