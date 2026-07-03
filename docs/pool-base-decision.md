# Pool Base Decision For PRISM

Status: accepted and implemented by the direct PRISM Stratum proof.

Decision: build the PRISM pool on the repo-owned Stratum path that calls the
standalone Rust builder and PRISM verifier directly. PRISM stands for Payouts,
Rewards, and Integrity Settlement Manifest. Keep ckpool as the
single-address permissionless reference and comparison harness, but do not make
ckpool the Phase 1 non-custodial PRISM base.

## Why

The hard payout logic now lives outside the transport:

- `qbit-pool-builder` builds deterministic multi-output P2MR coinbases and
  signed payout manifests.
- `qbit-prism` owns the ordered-share TIDES-style reward accounting, day-1
  payout floor, carry-forward accounting, maturity/reorg accounting, and
  independent audit verifier.

The remaining base question is therefore only which transport can accept an
externally built per-job coinbase with the least invasive work while preserving
robust Stratum behavior.

## Evidence

### ckpool Path

What works:

- `make test-permissionless-p2mr` mined a qbit regtest block with qbitd running
  `-p2mronly=1`.
- The run showed ckpool preflight passing qbit assumptions
  (`weightlimit=2000000`, `witness_scale=1`, `coinbase_maturity=1000`) and
  qbit accepting block
  `00000003a7213328612c5e76850a9a135b5c37f62b115ddc4a8b43686d2c6b30` at
  height 1.
- The public ckpool overlay already has fractional-regtest difficulty,
  qbit-specific GBT/signature rule handling, dynamic version-mask probing, and
  BIP310 probe coverage.

What blocks it as the PRISM base:

- The public startup path emits a single ckpool `btcaddress` payout field, not a
  per-job externally supplied output set.
- The qbit ckpool patch footprint is already sizeable before adding PRISM:
  `docker/ckpool/qbit-regtest.patch` is 543 lines and
  `docker/ckpool/qbit-signet-gbt.patch` is 47 lines.
- The current public ckpool coverage proves startup, BIP310 negotiation, and
  single-address block acceptance. It does not prove external multi-output
  coinbase injection, per-share ledger writes, PRISM manifests, accrual rows, or
  independent manifest verification.
- Adding builder-owned multi-output coinbases inside ckpool would require C-side
  changes to job/coinbase assembly, per-job payout state, output serialization,
  and qbit witness/P2MR invariants. That work couples payout correctness to a
  transport fork.

### Repo-Owned Stratum Path

What works:

- `lab/auxpow/auxpow_coordinator.py` already implements the Stratum pieces that
  matter for robustness: client state, job refresh, BIP310 version rolling,
  vardiff, share accounting, duplicate/stale rejection, worker stats, and block
  submission.
- `lab/auxpow/stratum_codec.py` centralizes Stratum header assembly and enforces
  negotiated version masks.
- `lab/auxpow/vardiff.py` and its tests already cover retarget behavior.
- The Rust builder/test path has accepted deterministic P2MR coinbases with
  1/50/500 outputs on qbit regtest, and the audit verifier can recompute the
  complete split from exported data and match full coinbase transaction hex.

What was implemented:

- `lab/prism/direct_stratum.py` uses `getblocktemplate`, assembles direct qbit
  Stratum jobs, splits the final coinbase around the extranonce marker, and
  submits full blocks through `submitblock`.
- `lab/prism/prism_coordinator.py` persists accepted shares through the PRISM
  ledger before computing the found block's reward window.
- The PRISM regtest and live-Stratum harnesses export audit bundles and run
  `qbit-prism-audit-verify` against the on-chain coinbase.

## Recommendation

Use the repo-owned Stratum path for the live PRISM integration work.

The least risky implementation is a direct qbit Stratum coordinator that reuses
the existing Python Stratum codec/vardiff behavior where useful and treats the
Rust crates as the canonical accounting and coinbase authority. This keeps
reward-state correctness in one place, avoids deep ckpool coinbase surgery, and
lets the final E2E test assert the exact manifest-to-on-chain relationship.

ckpool should stay in the repo as:

- a working single-address permissionless qbit mining reference,
- a BIP310/version-mask comparison target,
- a regression smoke for qbit P2MR-only block acceptance.

It should not become the PRISM integration base unless the direct PRISM Stratum
path later fails operator requirements that ckpool can satisfy with less risk.

## Acceptance Mapping

- Written recommendation: this document.
- Evidence of one base mining a valid regtest block:
  `make test-permissionless-p2mr`, block
  `00000003a7213328612c5e76850a9a135b5c37f62b115ddc4a8b43686d2c6b30`.
- Comparative decision:
  ckpool wins existing single-address transport proof; repo-owned Stratum wins
  Phase 1 PRISM suitability because it can call the standalone builder and
  verifier without modifying ckpool's coinbase internals.
