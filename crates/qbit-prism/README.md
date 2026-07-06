# qbit-prism

`qbit-prism` owns PRISM (Payouts, Rewards, and Integrity Settlement Manifest)
accounting for the non-custodial qbit pool path. It is deliberately separate
from Stratum transport: frontends may scale horizontally, but every accepted
share must pass through one canonical ordered ledger before PRISM computes a
block split.

PRISM uses TIDES-style reward accounting. TIDES, Transparent Index of Distinct
Extended Shares, was originally documented by OCEAN for Bitcoin mining pools;
this crate applies that ordered-share reward-window model to qbit-specific P2MR
settlement, carry-forward, maturity, reorg, and audit-bundle policy.

## Ledger Contract

The Postgres schema in [`sql/001_share_ledger.sql`](sql/001_share_ledger.sql)
defines:

- `qbit_share_ledger`: append-only canonical share log ordered by `share_seq`
- `qbit_ledger_writer_lease`: one-row coordination table for a single logical
  writer/failover epoch
- `qbit_prism_window(...)`: deterministic newest-backward window query with
  partial oldest-share weighting

Stratum frontends should enqueue share submissions outside this table. Only the
active ledger writer inserts rows into `qbit_share_ledger`; that is what keeps
all miners in the same reward universe.

## Reward Rule

For a found block, PRISM's TIDES-style reward window uses shares whose job was
issued no later than the found block's job issue time. Starting with the newest
eligible share by `share_seq`, it includes share difficulty until the window
reaches `8 * network_difficulty`. If the log is smaller, the full eligible log
is used. If the oldest included proof crosses the boundary, only the remaining
weight is counted.

The resulting per-miner weights convert directly into
`qbit_pool_builder::WeightedEntitlement` and then into the signed coinbase
manifest.

## Day-1 Payout Floor

qbit P2MR spends carry large post-quantum signatures and qbit has
`WITNESS_SCALE_FACTOR=1`, so the pool must not create tiny outputs that are born
uneconomic to spend. The day-1 policy adopts the Phase 0 floor:

```text
3,680 bytes/input * 1 bit/byte * 4x safety = 14,720 bits
```

Live PRISM deployments can also set a fixed absolute floor with
`PRISM_PAYOUT_MIN_OUTPUT_BITS` or tune the formula inputs with
`PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES`,
`PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE`, and
`PRISM_PAYOUT_SAFETY_MULTIPLIER`. The old `_SATS` payout env names are still
accepted as legacy aliases.

## Non-DATUM Coinbase Output Caps

Until PRISM supports DATUM-style per-miner hardware profiles, it must serve one
conservative Stratum template that stock miner firmware can handle. The
settlement selector therefore keeps separate hard ceilings and launch defaults:

```text
hard policy ceiling:                  500 settlement coinbase outputs
non-DATUM default settlement cap:      16 settlement coinbase outputs
non-DATUM default direct payout cap:   12 direct recipient outputs
default CTV fanout chunk size:      1,000 recipients
```

The low defaults are hardware/operator policy, not qbit consensus limits. They
are based on the same byte-budget problem OCEAN DATUM handles with multiple
coinbase profiles for miner firmware. Direct payout overflow should route to one
or more CTV fanout covenant outputs, where each covenant output is only a normal
P2MR coinbase output while the large payout list lives in the later fanout
transaction and public manifest.

PRISM first computes gross per-miner entitlements through the TIDES-style
reward window. The payout policy then combines each gross amount with that
miner's prior carry-forward balance, and it also carries prior-only accounts
with zero current gross. That lets an above-floor accrued balance be paid even
when the miner has no current shares. Any account that cannot receive an
on-chain output at or above the floor is marked `accrued`; its positive
carry-forward balance remains auditable.

The selected on-chain accounts must have enough candidate balance to cover the
coinbase value. If they do not, policy construction returns an explicit error
instead of overpaying selected recipients and creating negative carry-forward.
For valid manifests, block value is assigned exactly to deterministic P2MR
outputs at or above the floor and the carry-forward ledger records every
per-miner delta. That means no sub-floor entitlement is silently dropped, and
every sat in the coinbase is accounted for in either an on-chain output or a
transparent carry-forward delta.

## Maturity And Reorgs

qbit coinbase maturity is `1000` blocks. Pool-found payout entries start
`immature`, become `mature` only when the active tip is at least
`block_height + 1000`, and can be reversed while immature if their block is
disconnected. Reversing a disconnected immature block marks both direct-output
entries and carry-forward rows as `reversed`; current owed balances are computed
by replaying active, confirmed carry-forward deltas per recipient. Exact zero
net balances produce no current row, while negative balances remain audit-visible
debt that offsets future gross rewards. A mature disconnect is treated as an
exceptional chain event and must not be silently rewritten.

Carry-forward integrity is exposed through the coordinator audit API at
`/audit/carry-forward-integrity`. The report recomputes active prior balances
from append-only deltas, flags any row whose stored prior/candidate/carry value
does not match replay, and publishes `audit_head_sha256`, a deterministic
hash-chain head over active carry-forward rows. Operators should mirror or pin
that head after each payout-affecting block.

Launch checkpoint policy is deliberately conservative: do not compact or delete
carry-forward rows while any balance they affect can still be owed. A future
compaction may replace old rows only after publishing a signed checkpoint that
contains the previous `audit_head_sha256`, the replacement opening balances,
the covered row range, and the ledger-writer key epoch. Until that exists,
append-only rows plus the public audit head are the recovery source of truth.

## Audit Exports

`AuditBundle` is the public per-block artifact for verification. It carries the
accepted share slice, found-block anchor, prior carry-forward balances, payout
floor parameters, PRISM reward manifest, payout-policy/accrual manifest, and the
signed deterministic coinbase manifest.

The verifier and canonicalizer also accept storage-oriented compact artifacts:
legacy `qbit.prism.audit-body-ref.v1` files and
`qbit.prism.audit-bundle.v2` proof bodies. Those formats keep share payloads in
referenced segment files, verify each segment or range hash, and reconstruct the
same canonical v1 bundle before checking payout math. Public `/public/v1`
responses remain logical v1 bundles.

`verify_audit_bundle` recomputes the full path from exported data and a
verifier-supplied trusted ledger writer public key. The ledger attestation binds
the share slice, prior-balance digest, anchor metadata, `block_height`, and
`coinbase_value_sats`, so an attestation cannot be replayed onto a different
block height or declared coinbase value:

```text
shares + found block
  -> PRISM reward manifest
  -> payout policy/accrual manifest
  -> deterministic P2MR coinbase manifest
  -> signed manifest verification
```

`verify_audit_bundle_against_coinbase_tx_hex` additionally compares the full
serialized on-chain coinbase transaction hex. The standalone verifier CLI is:

```sh
cargo run -p qbit-prism --bin qbit-prism-audit-verify -- audit-bundle.json \
  --coinbase-tx-hex "$COINBASE_TX_HEX" \
  --ledger-writer-public-key-hex "$LEDGER_WRITER_PUBLIC_KEY_HEX" \
  --expected-coinbase-value-sats "$EXPECTED_COINBASE_VALUE_SATS"
```

`LEDGER_WRITER_PUBLIC_KEY_HEX` must come from the operator's trusted key
distribution, not from the bundle being verified. `--expected-coinbase-value-sats`
is optional for compatibility, but production verification should pass the
independently computed subsidy-plus-fees value.

The SQL schema exposes endpoint-equivalent audit reads:

- `qbit_audit_share_window(anchor_job_issued_at, network_difficulty)`
- `qbit_audit_block_payouts(block_hash)`
- `qbit_current_carry_forward_balances()`
- `qbit_current_owed_balances()`

Service adapters can wrap those rows as HTTP/JSON endpoints without changing the
canonical reward, payout, or verifier logic.
