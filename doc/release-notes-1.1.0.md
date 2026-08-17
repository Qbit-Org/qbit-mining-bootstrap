# qbit-mining-bootstrap 1.1.0 Release Notes

Release date: 2026-08-17

## Highlights

- Hardens the found-block landing path end to end: found blocks are submitted
  to the node before pool accounting, a node-accepted block is never
  terminally abandoned over a stale append epoch or a lost `submitblock`
  acknowledgement, carry-forward balances are maintained in O(recipients)
  instead of O(ledger history), and landing work runs under landing-class
  deadlines behind a degradable barrier.
- Makes the PRISM ledger writer lease fail closed under contention. Heartbeats
  no longer self-fence during accepted-block persistence, heartbeat staleness
  is measured from verification activity, and a lock-blocked renewal that
  races an expiry claim now fails closed instead of proceeding.
- Scales the PRISM tip-refresh and share-ingest pipeline for production hash
  rates: pay-once-per-generation ledger and serialization costs, epoch-scoped
  refresh fanout, converged latest-tip publication, bounded reconcile
  prefetch, and single-flight job builds that resolve or evict abandoned
  promises.
- Makes payout-artifact builds incremental and debounced, and re-lands
  event-driven artifact reuse behind the `PRISM_PAYOUT_ARTIFACT_REUSE`
  kill switch after the earlier anchor-scoped attempt was reverted.
- Adds the reward-window leaderboard to the public dashboard API. `window=reward`
  ranks the canonical live PRISM work window using counted share difficulty
  from the latest 8x permissionless-difficulty window and supports exact
  `recipient_id` lookup, while the legacy `window=3h` ranking is preserved.
- Adds an opt-in pool-fee-first coinbase output policy
  (`PRISM_COINBASE_OUTPUT_POLICY=pool-fee-first`), which requires an enabled
  pool fee, keeps that fee out of CTV fanout, and emits a positive fee output
  at coinbase vout 0.
- Adds per-worker rejects observability to the ckpool lane, native-crash
  diagnosability for the PRISM coordinator, progress-aware health checks, and
  watchdog restart recovery in roughly one second instead of 60s or more.
- Expands CI with Postgres production-scale fixture tests, a native Postgres
  client ledger suite, and a live ckpool rejects observability suite.

## Mainnet Compatibility

This release continues to pair with qbit `v1.0.0` at
`7ebcddb622d6e639041f005a189b048ec2a221fe`. The `QBIT_GIT_REF` and
`QBIT_GIT_COMMIT` defaults are unchanged, so operators on 1.0.0 keep the same
qbit source pin across this upgrade.

The documented AuxPoW chain IDs are unchanged:

- Mainnet: `47`
- Public testnet4: `31430`

The qbit mainnet genesis hash for this release line is unchanged:

`0000000000004d60aa5d46013991d0a0e2995d89ee98e53068ae196d763e79f2`

## Upgrade Notes

- Configuration is additive. No `.env.example` key was removed or renamed
  since 1.0.0. The two new behaviour switches are
  `PRISM_COINBASE_OUTPUT_POLICY`, which defaults to `canonical` and preserves
  the 1.0.0 coinbase ordering, and `PRISM_PAYOUT_ARTIFACT_REUSE`, which
  defaults to `1` and can be set to `0` to fall back to full artifact builds.
- The remaining new keys are PRISM tuning and capacity knobs covering block
  submit and landing timeouts, writer-lease TTL, tip-refresh fanout and worker
  pools, Stratum connection and initial-job limits, coordinator file-descriptor
  limits, payout-artifact cadence, CTV broadcaster batching, public read-model
  caching, and health staleness thresholds. They ship with production-facing
  defaults; review `.env.example` before overriding them.
- The share-ledger schema bundle in `crates/qbit-prism/sql/001_share_ledger.sql`
  is applied at coordinator startup. This release adds a
  `qbit_payout_carry_forward_current` summary table with maintenance triggers.
  A database that already holds active carry history is seeded exactly once
  during that first apply, so expect a one-time backfill proportional to the
  existing carry rows before the coordinator finishes starting.
- Review `.env.example` and `docs/mainnet-deployment.md` before upgrading a
  production or mainnet stack.
- Keep production images digest-qualified. `scripts/check-env.sh` is expected
  to reject mutable image references, missing qbit source provenance, and
  missing final chain-state pins in production mode.
- Run `make doctor` before starting the stack. For PRISM deployments, run
  `make prism-self-check` after startup and keep private RPC, Postgres,
  metrics, audit, health, volume, and signing-key surfaces off the public
  internet unless intentionally protected by access controls.

## Changes Since v1.0.0

This release rolls up every change merged to `main` after v1.0.0 (#28 through
#35) together with the `1.x.x` hardening line (#36 through #120).

### Block landing, submission, and accounting

- Fix the found-block landing livelock with O(recipients) carry-forward
  balances, landing-class deadlines, a degradable barrier, and replay
  hardening (#120).
- Submit found blocks to the node before accounting and keep the submitter
  alive under database saturation (#113).
- Never terminally abandon a node-accepted block over a stale append epoch
  (#119), and fix the earlier stale abandon of accepted blocks (#89).
- Close the accepted-block blind spot behind lost `submitblock`
  acknowledgements (#93) and carry the post-merge review hardening for that
  fix (#94).
- Skip replay window revalidation once the node offer is out.
- Reconcile accepted-share stats in the background and heartbeat block
  dispositions (#110).
- Avoid reconcile single-flight under the accepted-block payout lock (#96) and
  reduce accepted-block payout gate locking (#50).
- Judge ledger confirmation on settlement balances rather than identity labels
  (#87).

### Writer lease, share ledger, and audit

- Stop lease heartbeat self-fencing during accepted-block persistence (#114).
- Fail closed when a lock-blocked lease renewal races an expiry claim (#116).
- Measure lease heartbeat staleness from verification activity (#118).
- Release the writer lease before slow shutdown drains (#47).
- Make share-window reads scale with the window rather than ledger history
  (#115) and isolate share hot-path locking (#83).
- Recover gaps in audit share segments (#88) and fix audit-bundle memory
  amplification (#48).

### Tip refresh, job delivery, and Stratum

- Make tip-refresh ledger and serialization costs pay-once-per-generation
  (#99), converge refreshes on latest epochs (#101), and cut hashrate-weighted
  staleness in refresh fanout (#98).
- Scale tip refresh, block finalization, and share ingest for the whale ramp
  (#100) and for same-tip job retention (#31).
- Bound the tip-refresh reconcile prefetch join (#109), resolve or evict
  abandoned single-flight job-build promises (#108), and remove the redundant
  payout-ledger-artifact build from the reconcile path (#95).
- Prevent tip publication starvation (#66) and latest-tip publication livelock
  between concurrent refresh paths (#67); prioritize latest-tip bundle builds
  (#69) and reduce shared bundle latency (#58).
- Delay tip authority until replacement work is ready (#55), supersede
  obsolete refreshes promptly (#42), bound refresh build supersession (#57),
  and preempt obsolete fanout waiters (#43).
- Throttle tip-refresh retries and candidate finalize replays (#71), retry
  failed blockwait refreshes (#41), start the refresh failure budget on first
  failure (#51), and exclude coordination-blocked refreshes from the template
  failure budget (#56).
- Remove coordinator hot-path bottlenecks behind mainnet stale-job rejects
  (#40) and reduce tip-refresh latency under concurrent maintenance (#37).
- Flatten the first-job latency tail for new connections (#92), decouple ready
  bundles from representative clients (#46), reclaim cancelled initial-job
  queue capacity (#68), and bound reconnect storms during job startup (#45).
- Fix build cancellation and refresh races (#64) and the client cleanup lock
  leak (#52); keep Stratum ports open across fast restarts (#63); harden
  Stratum against descriptor exhaustion (#30); bound vardiff idle sweeps
  (#53).

### Payouts, artifacts, and CTV fanout

- Make payout-artifact builds incremental and debounced (#112).
- Re-land payout-artifact reuse with event-driven validity and a kill switch
  (#107), after the anchor-scoped reuse line (#102, #103) was reverted (#104).
- Shorten payout delivery gate critical sections (#49) and skip payout
  republish on covered block replays (#59).
- Add an explicit pool-fee-first coinbase output policy (#106).
- Skip no-op CTV fanout status writes and immature-row probes (#60) and
  prevent false CTV broadcaster watchdog exits (#35).

### Public dashboard API

- Expose the PRISM reward-window leaderboard (#36) and the pending maturity
  total (#28).
- Label CTV fanout payouts correctly in the public payout read model (#29).
- Smooth public hashrate-series points with a trailing window (#65) and reduce
  public dashboard load on the shared Postgres (#72).

### Observability, health, and diagnostics

- Add per-worker rejects observability to the ckpool lane (#97).
- Add native-crash diagnosability to the PRISM coordinator (#91).
- Recover watchdog restarts in roughly one second instead of 60s or more
  (#111) and bound the coordination-blocked template refresh watchdog (#90).
- Add progress-aware health checks (#54) and fix delivery health grace (#82).

## Operator Notes

- The historical 0.1.x and 1.0.0 release notes remain in `doc/` so public
  release history stays inspectable from the repository.
- `docs/prism-payout-artifact-measurement.md` documents how to measure
  payout-artifact build cost before tuning the reuse and cadence knobs.
- New Make targets in this release: `make test-prism-postgres-scale`,
  `make test-prism-postgres-native-ledger`, and
  `make test-ckpool-rejects-observability`.
