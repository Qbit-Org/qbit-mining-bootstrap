# PRISM Pool Claim to Evidence Inventory

This public readiness index maps the PRISM pool release surface to implementation evidence
to concrete artifacts in this repository. It is intentionally evidence-first:
incomplete rows list remaining hardening work rather than treating partial work
as acceptance.

## Base Decision

Claim: use the repo-owned direct qbit Stratum/PRISM path for the production
pool integration, with ckpool retained as a single-address comparison harness.
PRISM stands for Payouts, Rewards, and Integrity Settlement Manifest.

Evidence:
- `docs/pool-base-decision.md` records the decision and rationale.
- `make test-permissionless-p2mr` has proven the existing ckpool comparison
  path can mine a qbit `-p2mronly=1` regtest block with qbit's GBT-advertised
  version rolling mask.
- `lab/prism/direct_stratum.py` now contains the direct qbit job/submission
  primitives for the selected path.
- `lab/prism/prism_coordinator.py` proves the selected direct path with live
  Stratum clients on regtest.

Remaining:
- Production hardening spans the ledger, audit, and live Stratum surfaces.

## Standalone Deterministic Coinbase Builder

Claim: a standalone Rust builder emits byte-reproducible multi-output P2MR
coinbases, exact-value outputs, witness commitment, and signed payout
manifests.

Evidence:
- `crates/qbit-pool-builder` implements the library and CLI.
- `cargo test --workspace` covers deterministic manifests, stable largest
  remainder allocation, exact sum accounting, P2MR output scripts, witness
  commitment encoding, manifest signatures, and Stratum suffix support.
- `make test-builder-regtest` submits builder coinbases with 1, 50, and 500
  outputs to qbit regtest and compares on-chain bytes/outputs to the manifest.

Remaining:
- Production key-management policy is still outside this standalone builder
  slice.

## Ordered Single-Writer Share Ledger

Claim: the share ledger contract uses one canonical ordered share log.

Evidence:
- `crates/qbit-prism/sql/001_share_ledger.sql` defines `qbit_share_ledger`
  with `BIGSERIAL` ordering, exact integer difficulties, a single-writer lease
  table, reverse-window query, audit-window query, pool block rows, and payout
  entry rows, plus persisted audit bundle rows.
- `docs/prism-ledger-ops.md` documents the accepted operations contract:
  Stratum frontends feed a durable boundary, one logical writer owns the
  Postgres lease, `share_id` replay is idempotent, failover waits for lease
  expiry, and archive/compaction is archive-first.
- `lab/prism/share_ledger.py` provides the in-process single-writer sequence
  and snapshot API used by the live direct Stratum proof, and a
  `psql`-backed implementation selected by `PRISM_POSTGRES_PSQL_COMMAND` or
  `PRISM_DATABASE_URL`.
- `tests/test_prism_share_ledger.py` covers contiguous sequence assignment,
  snapshot freeze semantics, positive difficulty validation, and concurrent
  append ordering.
- `make test-prism-stratum-regtest-live` proves accepted live Stratum shares
  flow through the ordered helper before block construction.
- `make test-prism-stratum-postgres-regtest-live` proves live Stratum shares
  persist through the Postgres-backed ledger, keep contiguous `share_seq`
  values, carry the expected writer identity, and reproduce the submitted
  bundle's PRISM reward window from `qbit_audit_share_window`.
- `make test-prism-postgres-ledger` proves duplicate share replay is rejected
  without consuming a sequence value, competing writers cannot steal an
  unexpired lease, same-id/same-epoch processes are fenced by session token, a
  stale writer cannot append or persist after replacement, the replacement
  writer resumes at the next sequence,
  `qbit_shares_since_template_height` returns only accepted at-or-above-height
  rows in sequence order, and
  `qbit_audit_share_window` handles both exact 8x boundaries and partial oldest
  shares.
- `make test-prism-combined-regtest` proves the same Postgres ledger and
  audit API in a combined regtest flow: live Stratum miners append skewed
  shares, the first block is accepted through the coordinator, and subsequent
  ASERT/reorg blocks are persisted from the same canonical DB ledger state.
- `make test-prism-postgres-throughput` is an opt-in capacity harness that
  records synthetic-share insert throughput and `EXPLAIN (ANALYZE, BUFFERS,
  FORMAT JSON)` output for the audit-window query.
- The Postgres harnesses provision a Docker container by default but are
  Docker-optional: setting `QBIT_PRISM_EXTERNAL_PSQL_COMMAND` to a `psql`
  invocation runs them against any already-running Postgres, so the same proofs
  are reproducible without a container runtime.

Remaining:
- A target shares/sec benchmark on a VPS-class box, queue-fed multi-frontend
  failover/HA implementation, and a destructive compaction/archive harness are
  still production hardening gaps. The accepted proof uses one canonical
  Postgres share log and documents the non-active-active contract.

## PRISM TIDES-Style Reward Accounting

Claim: PRISM implements TIDES-style reward accounting: an 8x found-block
network-difficulty window, anchored at job issue, with per-proof weight equal to
target difficulty and exact pro-rata subsidy plus fees.

Evidence:
- `crates/qbit-prism` implements `compute_prism_window`,
  `build_prism_reward_manifest`, and builder handoff.
- Fixtures in `crates/qbit-prism/fixtures/` cover bootstrap-small-log,
  difficulty-change, and power-law accrual cases.
- `cargo test --workspace` covers job-issue anchoring, partial oldest-share
  counting, exact reward manifests, and difficulty-change window width.
- `make test-prism-regtest` now derives the TIDES-style reward-window
  network-difficulty input from actual qbit template compact bits and proves an
  ASERT bits change.
- `make test-prism-combined-regtest` feeds live Stratum/Postgres share rows
  into the same PRISM reward window and uses the current qbit compact bits for
  the ASERT-changed follow-up block.

Remaining:
- Production service scheduling/refresh policy remains outside the proof
  harness.

## Day-1 Payout Policy

Claim: sub-floor entitlements accrue transparently and on-chain outputs respect
the PQ spend-cost-derived floor.

Evidence:
- `PayoutPolicy::day_one_default()` encodes the 3,680-byte input estimate,
  1 bit/byte feerate, and 4x safety multiplier, producing a 14,720 bit floor.
- Direct PRISM Stratum can override the absolute floor with
  `PRISM_PAYOUT_MIN_OUTPUT_BITS`, or tune the formula inputs with the
  `PRISM_PAYOUT_*` spend-byte, feerate, and safety-multiplier environment
  variables. `PRISM_PAYOUT_MIN_OUTPUT_SATS` is still accepted as a legacy alias.
- `apply_payout_policy` records gross, prior, candidate, on-chain, and
  carry-forward balances per account.
- `power-law-accrual.prism-fixture.json` and Rust tests exercise a skewed
  distribution with below-floor accrued accounts.
- `make test-prism-regtest` verifies at least one accrued account while the
  on-chain coinbase pays the manifest outputs exactly.
- `make test-prism-postgres-ledger` proves
  `qbit_current_carry_forward_balances()` exposes signed payout priors,
  `qbit_current_owed_balances()` exposes clamped display balances, and reversed
  immature block balances are excluded.
- `make test-prism-combined-regtest` drives a skewed, power-law live Stratum
  share distribution and verifies persisted accrued balances through the
  DB-backed `/owed-balances` API.

Remaining:
- Production fee policy can tune constants later; the day-1 transparent
  accrual behavior is covered.

## Maturity and Reorg Handling

Claim: qbit coinbase maturity is 1000 blocks and immature pool payouts reverse
on disconnect without silently reversing matured payouts.

Evidence:
- `QBIT_COINBASE_MATURITY_BLOCKS` is 1000.
- Rust tests cover maturity transition at `block_height + 1000`, immature
  disconnect reversal, idempotent repeated disconnects, active-chain
  reconvergence, and mature-disconnect rejection.
- `qbit-prism-reorg-verify` independently checks disconnected/replacement
  payout manifests.
- `make test-prism-regtest` invalidates an immature PRISM regtest block, mines
  a distinct replacement at the same height, verifies the replacement coinbase,
  and runs `qbit-prism-reorg-verify`.
- `make test-prism-postgres-ledger` proves persisted pool block, payout entry,
  and carry-forward rows reverse for immature disconnects, that reversed owed
  balances disappear, and that mature block reversal raises.
- `make test-prism-combined-regtest` invalidates an immature persisted PRISM
  block on qbit regtest, reverses its persisted payout entries through
  `qbit_reverse_immature_pool_block`, verifies the API reports the block rows
  as `reversed`, and submits a distinct replacement block.

Remaining:
- Automatic chain-monitor orchestration remains production hardening; the
  accounting transition itself is proven against concrete qbit regtest blocks.

## Audit API and Independent Verifier

Claim: an independent verifier can recompute a block split from exported data
and match the on-chain coinbase exactly.

Evidence:
- `AuditBundle` exports accepted shares, found-block anchor, prior balances,
  payout policy, reward manifest, payout-policy manifest, and signed coinbase
  manifest.
- `qbit-prism-audit-verify` recomputes the reward, payout policy, signed
  coinbase manifest, optional on-chain coinbase match, and optional independently
  expected coinbase value.
- `qbit-prism-build-audit-bundle` emits signed bundles from exported share and
  found-block data, including optional Stratum scriptSig suffixes.
- Rust tamper tests reject modified share windows, payout manifests, signed
  coinbase manifests, mismatched on-chain coinbase hex, replayed ledger
  attestations across block/value scope, and unexpected coinbase value.
- `make test-prism-regtest` runs the verifier against qbit-accepted on-chain
  coinbases.
- `make test-prism-stratum-regtest-live` runs live Stratum clients through the
  coordinator and verifies the accepted on-chain coinbase against the signed
  audit bundle generated for the submitted extranonce suffix.
- `lab/prism/prism_coordinator.py` exposes `/healthz`, `/audit/latest`,
  `/owed-balances`, `/metrics`, and DB-backed audit endpoints for share windows,
  block payouts, and persisted bundles.
- `tests/test_prism_audit_api.py` covers endpoint JSON/text behavior and
  malformed block-hash rejection.
- `/metrics` includes accepted/submitted/stale/duplicate/low-difficulty shares,
  shares/sec, stale percentage, accepted and persisted blocks, owed-account
  count, latest coinbase weight headroom, PRISM vardiff enabled state, qbitd
  initial-block-download state, and qbitd peer count.
- `tests/test_prism_coordinator_vardiff.py` covers the operational
  metrics, fail-closed production defaults, trusted ledger-key source, PRISM
  vardiff retarget behavior, stale-share rejection, and pre-submit block
  persistence/rejected-candidate reversal.
- `QBIT_PRISM_LIVE_AUDIT_API=1 make test-prism-stratum-regtest-live` keeps the
  live coordinator up after an accepted regtest block and verifies
  `/audit/latest`, `/owed-balances`, and `/metrics` against the emitted
  evidence.
- `make prism-self-check` validates a running PRISM operator stack and reports
  qbit RPC, coordinator health, Stratum reachability, Postgres readiness,
  audit-dir writability, production key material, test-bypass flags, and basic
  mining configuration as PASS/WARN/FAIL rows.
- `docs/prism-rejections.md` defines canonical PRISM rejection reason IDs. The
  coordinator exports them through Stratum error data and
  `qbit_prism_rejections_total{reason_id="..."}` while retaining the older broad
  stale/duplicate/low-difficulty counters.
- `make test-prism-stratum-postgres-regtest-live` verifies DB-backed
  `/audit/share-window`, `/audit/blocks/{hash}/payouts`, and
  `/audit/blocks/{hash}/bundle` responses against the submitted audit bundle,
  then replays the API-returned bundle through `qbit-prism-audit-verify`.
- `make test-prism-combined-regtest` verifies the API bundle for the
  ASERT/reorg replacement block and checks the published payout rows after
  immature reversal.

Remaining:
- Production alert rules, metric retention, and dashboard packaging remain
  hardening beyond the verifier/API proof.
- Production verifiers must obtain `LEDGER_WRITER_PUBLIC_KEY_HEX` from trusted
  operator configuration and must not copy it from the bundle under review.

## Stratum Integration and Regtest E2E

Claim: the builder and reward engine are integrated into the chosen transport
and proven on regtest.

Evidence:
- `lab/prism/direct_stratum.py` covers direct qbit Stratum job construction,
  no-witness txid merkle hashing, full witness coinbase block serialization,
  submitblock candidate assembly, and GBT-driven qbit version rolling.
- `lab/prism/prism_coordinator.py` serves live Stratum clients, validates P2MR
  usernames or routes invalid username payouts to the configured fallback,
  records accepted shares, freezes a PRISM share snapshot, rebuilds the final
  signed audit bundle with the submitted extranonce suffix, and calls qbit
  `submitblock`.
- `tests/test_prism_direct_stratum.py` covers suffix splitting, transaction
  merkle branches, full coinbase reassembly, and version-bit rejection outside
  the negotiated GBT mask.
- `lab/prism/prism_coordinator.py` uses PRISM-path vardiff by default for small
  miners while keeping fixed share difficulty available with
  `PRISM_STRATUM_VARDIFF=0`.
- `tests/test_prism_coordinator_vardiff.py` proves default vardiff
  configuration, per-client retargeting, `mining.set_difficulty`, and clean job
  refresh.
- `make test-prism-regtest` proves deterministic end-to-end:
  six simulated miners, skewed power-law shares, PRISM reward window, payout floor
  accrual, signed multi-output P2MR coinbase, qbit `submitblock` acceptance,
  on-chain coinbase equals signed manifest, independent audit verification,
  ASERT bits change, and immature reorg replacement verification.
- `make test-prism-stratum-regtest-live` proves live end-to-end:
  3 explicit P2MR miners, Stratum subscribe/authorize/notify/submit, accepted
  shares, ordered in-process ledger, frozen PRISM snapshot, signed multi-output
  P2MR coinbase, qbit `submitblock` acceptance, and on-chain coinbase equality.
- `QBIT_PRISM_LIVE_AUDIT_API=1 make test-prism-stratum-regtest-live` additionally
  proves the live service API can publish latest evidence, owed balances, and
  metrics without mutating the ledger after block acceptance.
- `make test-prism-stratum-postgres-regtest-live` proves live Stratum through
  the Postgres-backed ledger, pre-submit persisted block/payout/bundle rows,
  DB-backed audit endpoints, and verifier replay of the API-exported bundle.
- `make test-prism-combined-regtest` combines the high-risk paths in one
  regtest scenario:
  live Stratum/Postgres share ingress from 6 P2MR miners, skewed power-law
  share weights, a coordinator-submitted first PRISM block, persisted owed
  balances, an ASERT compact-bits change from `207fffff` to `207f183f`, an
  immature block invalidation/reversal, and a distinct replacement block whose
  DB-backed audit bundle is published through the API.

Remaining:
- The combined proof keeps the second/replacement block construction
  transport-independent after live share ingress rather than turning the lab
  coordinator into a full production multi-block chain monitor. Production job
  refresh/reorg watching remains the next hardening layer.
- qbit-tools production defaults should reject `PRISM_ALLOW_MEMORY_LEDGER=1`,
  `PRISM_ALLOW_TEST_SIGNING_SEEDS=1`, and
  `PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=1`, and
  `PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN=1`, require Postgres, require the
  manifest and ledger signing seeds, publish the trusted ledger public key out
  of band, and enforce one active PRISM writer per ledger.
