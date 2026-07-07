# PRISM

PRISM means **Payouts, Rewards, and Integrity Settlement Manifest**. It is the
direct qbit pool path in this repository: a Stratum-facing coordinator, a
single ordered share ledger, deterministic reward accounting, reproducible
coinbase construction, non-custodial settlement, and public audit artifacts that
let miners verify their payout independently.

PRISM is not qbit consensus code. qbit core owns block validation, RPC, chain
parameters, P2MR, CTV, maturity, and reorg semantics. This repository owns the
operator stack around those rules: Compose profiles, Stratum glue, share
accounting, payout manifests, audit bundles, public dashboard read models, and
end-to-end mining tests.

## What PRISM Optimizes For

PRISM is built around four constraints:

- **One canonical share ledger.** Every accepted share enters one ordered log.
  Reward windows are derived from that order, not from per-frontend summaries.
- **Transparent reward math.** A miner can recompute the share window, payout
  split, carry-forward state, and final coinbase from published artifacts.
- **Non-custodial settlement.** Whenever possible, miners are paid directly in
  the generation transaction. When that is not practical, CTV fanout outputs
  precommit the later payout transaction instead of sending funds to a pool
  custody address.
- **qbit-specific policy.** PRISM accounts for qbit P2MR outputs, 1000-block
  coinbase maturity, no witness discount, fast block cadence, carry-forward
  balances, and immature reorg reversal.

Operator-facing docs use **bits** for the smallest qbit unit. Some Rust and
Python internals still use Bitcoin-derived `*_sats` names. Treat those as the
same integer unit in this codebase.

## OCEAN And TIDES Inspiration

PRISM's reward model is inspired by OCEAN's TIDES payout documentation, not by
OCEAN's implementation. OCEAN defines TIDES as "Transparent Index of Distinct
Extended Shares" and describes a pool reward system where proofs are tracked
individually, kept in order, paid over an extended window, and auditable by
miners. See OCEAN's TIDES writeup:

- <https://ocean.xyz/docs/tides>
- <https://ocean.xyz/>

The design ideas PRISM carries over are:

- **Transparent:** miners should be able to calculate their expected split.
- **Index:** accepted shares retain their order.
- **Distinct:** shares are not collapsed into shifts or coarse buckets before
  payout calculation.
- **Extended:** the active reward window is large enough to reduce variance.
- **Shares:** each valid proof contributes work weight to the window.

Like OCEAN's public description of TIDES, PRISM uses the latest
`8 * network_difficulty` units of accepted share work when a pool block is
found. The block reward, including fees, is split pro-rata by counted work.

PRISM differs because it is qbit pool software. It uses qbit P2MR payout
programs, qbit's 1000-block coinbase maturity, qbit transaction policy, and the
qbit-specific settlement/audit artifacts in this repo. It also explicitly
models carry-forward balances and CTV fanout settlement.

## CTV Inspiration

CTV is shorthand for `OP_CHECKTEMPLATEVERIFY`. In Bitcoin, BIP-119 proposes CTV
as a covenant opcode that checks a hash commitment to fields of the spending
transaction. Bitcoin Optech summarizes CTV as a proposed opcode that commits an
output to a specific future spending template:

- <https://bitcoinops.org/en/topics/op_checktemplateverify/>
- <https://github.com/bitcoin/bips/blob/master/bip-0119.mediawiki>

PRISM uses that covenant shape for qbit settlement fanouts. A coinbase output
can commit to a later transaction that pays many miners. Once the coinbase is
mature, anyone can broadcast the committed fanout transaction. The operator does
not hold a spend key that can redirect miner funds.

Do not read this file as a claim about Bitcoin mainnet CTV activation. This
repo's CTV path is about qbit PRISM settlement.

## System Architecture

The PRISM profile is the `prism` Docker Compose profile. In operator mode it
starts:

- `qbitd`: qbit node and mining RPC provider.
- `prism-postgres`: canonical share ledger, payout state, audit bundle index,
  and CTV fanout state.
- `prism-coordinator`: direct qbit Stratum server, ledger writer, reward
  engine caller, block submitter, audit HTTP server, public dashboard API, and
  optional CTV broadcaster loop.

The main implementation files are:

- [lab/prism/prism_coordinator.py](lab/prism/prism_coordinator.py): live
  Stratum coordinator and audit/public HTTP server.
- [lab/prism/direct_stratum.py](lab/prism/direct_stratum.py): qbit
  `getblocktemplate` to Stratum job assembly.
- [lab/prism/share_ledger.py](lab/prism/share_ledger.py): in-memory and
  Postgres-backed ledger adapters plus audit artifact persistence.
- [crates/qbit-prism](crates/qbit-prism): reward windows, payout policy,
  maturity/reorg state, CTV manifests, audit bundles, and verifier CLIs.
- [crates/qbit-pool-builder](crates/qbit-pool-builder): deterministic qbit P2MR
  coinbase and signed payout manifest builder.
- [crates/qbit-prism/sql/001_share_ledger.sql](crates/qbit-prism/sql/001_share_ledger.sql):
  canonical Postgres schema and reward-window queries.

Current PRISM is single-log and single-writer. Stratum ingress can be split and
scaled later, but all accepted shares must still converge through one logical
ledger writer before rewards are computed. Active-active independent ledgers are
not compatible with the audit model.

## How Reward Accounting Works

1. A miner connects to direct PRISM Stratum and authorizes with
   `<qbit-payout-address>[.<worker>]`.
2. The coordinator validates the payout identity, builds qbit work from
   `getblocktemplate`, negotiates version rolling, and applies fixed difficulty
   or vardiff.
3. Valid submitted shares are appended to `qbit_share_ledger` with a monotonic
   `share_seq`, unique `share_id`, miner identity, P2MR payout program, share
   difficulty, template height, job issue time, and acceptance time.
4. When a submitted share solves a block, PRISM freezes the block view at that
   job's issue time.
5. PRISM walks backward through eligible shares by `share_seq` until it counts
   `8 * network_difficulty` units of share work. The oldest included share is
   partially counted if it crosses the boundary.
6. Counted share weights are aggregated by payout program and converted into a
   `qbit.prism.reward-manifest.v1`.
7. The payout policy combines current gross reward with each recipient's prior
   carry-forward balance, applies payout floors and optional pool fees, and
   emits `qbit.prism.payout-policy.v1`.
8. The builder emits a deterministic P2MR coinbase and signed payout manifest.
9. The coordinator submits the block to qbit and persists the block, payout
   rows, audit bundle, and settlement artifacts.

Eligibility is intentionally strict. A share can enter the found block's window
only when both `job_issued_at <= anchor_job_issued_at` and
`accepted_at <= anchor_job_issued_at`. That prevents a delayed old-job share
from appearing after the found-block anchor and changing the published split.

## Payout Policy

PRISM separates three concepts that are easy to conflate:

- **Reward entitlement:** the pro-rata gross amount produced by the share
  window.
- **Spendability floor:** the minimum output size considered economic for qbit
  P2MR spends.
- **Settlement shape:** whether an owed recipient is paid directly in the
  coinbase, through a CTV fanout, or carried forward.

The default spendability floor is:

```text
3,680 bytes/input * 1 bit/byte * 4x safety = 14,720 bits
```

Override it with `PRISM_PAYOUT_MIN_OUTPUT_BITS`, or tune the formula with
`PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES`,
`PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE`, and
`PRISM_PAYOUT_SAFETY_MULTIPLIER`. Legacy `_SATS` aliases are still accepted by
some code paths.

Sub-floor balances are not discarded. The payout manifest records gross amount,
prior balance, candidate balance, on-chain amount, settlement fee, and
carry-forward balance per account. Current owed balances are recomputed by
replaying active carry-forward deltas.

Pool fees are optional. When enabled, configure `PRISM_POOL_FEE_ENABLED`,
`PRISM_POOL_FEE_BPS`, and either `PRISM_POOL_FEE_ADDRESS` or
`PRISM_POOL_FEE_P2MR_PROGRAM_HEX`.

## Direct Coinbase Settlement

Without CTV settlement, PRISM pays selected accounts directly from the coinbase
and carries the rest forward. This is simple and non-custodial, but it is bound
by practical coinbase-output limits. qbit can accept large P2MR coinbases in
tests, but one public Stratum template must remain compatible with miner
firmware and operator policy.

The current non-DATUM-style launch defaults are:

```text
max settlement coinbase outputs: 16
max direct recipient outputs:    12
```

Those are policy defaults, not qbit consensus limits.

## CTV Fanout Settlement

CTV settlement lets PRISM keep the coinbase small while still assigning every
recipient to a non-custodial settlement path.

When `PRISM_CTV_SETTLEMENT_ENABLED=1`, PRISM partitions recipients into:

- **direct coinbase recipients:** usually the largest eligible balances, paid
  directly in the generation transaction;
- **CTV fanout chunks:** bounded groups of recipients paid by later
  precommitted fanout transactions.

Each fanout chunk becomes one covenant output in the coinbase. That output is a
qbit P2MR script-path output committing to `<ctv_hash> OP_CHECKTEMPLATEVERIFY`.
The `ctv_hash` is computed from the exact fanout transaction template. The
fanout transaction pays the miners' P2MR outputs and cannot be changed without
breaking the covenant.

Important properties:

- There is no pool custody address.
- There is no pool spend key for the fanout funds.
- After coinbase maturity, anyone with the artifact can broadcast the fanout.
- The manifest is an audit artifact, not by itself proof that a real block was
  mined. Verifiers must check the chain, block height, maturity, parent
  coinbase output, and audit commitment.

Current CTV policy defaults:

```text
PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS=10485760
PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS=16
PRISM_MAX_DIRECT_COINBASE_OUTPUTS=12
PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION=1000
PRISM_CTV_FANOUT_FEE_PREMIUM_BPS=12000
```

The direct coinbase floor uses OCEAN's public on-chain payout threshold as a
reference point and raises it for qbit launch policy. Balances below the direct
floor can route through CTV fanout instead of consuming scarce coinbase output
slots.

Fanout fees are fixed by the committed transaction. PRISM supports two fee
shapes:

- **Built-in-fee fanout:** the fanout reserves its parent fee up front and has
  no CPFP anchor.
- **CPFP-anchor fanout:** the parent pays zero fee and includes a keyless P2A
  anchor so any broadcaster can attach a fee-paying child package later.

The broadcaster is not a custodian. It cannot change fanout outputs; it can
only pay or bump package fees from its own funding input when configured.

## Maturity And Reorgs

qbit coinbase maturity is 1000 blocks. PRISM payout entries begin as
`immature`, become `mature` only when the active tip reaches
`block_height + 1000`, and can be reversed while immature if the block is
disconnected.

The coordinator keeps block and payout state in Postgres:

- accepted blocks and candidates in `qbit_pool_blocks`;
- direct and carry-forward payout entries in `qbit_pool_payout_entries`;
- current carry-forward deltas in `qbit_payout_carry_forward`;
- audit bundle rows in `qbit_pool_audit_bundles`;
- CTV fanout artifacts and broadcast attempts in CTV-specific tables.

Immature reorg handling does not mutate historical shares. It marks affected
block/payout/carry rows inactive or reversed, then owed balances are recomputed
from active rows. Mature disconnects are treated as exceptional and must not be
silently rewritten.

## Audit Bundles And Verification

The main per-block public artifact is `qbit.prism.audit-bundle.v1`. It contains:

- accepted shares in the reward window;
- found-block anchor data;
- prior carry-forward balances;
- payout policy inputs;
- ledger-window attestation;
- reward manifest;
- payout-policy manifest;
- optional settlement-mode and CTV fanout manifests;
- signed deterministic coinbase manifest.

The verifier recomputes:

```text
shares + found block
  -> reward manifest
  -> payout policy manifest
  -> coinbase manifest
  -> full coinbase transaction match
```

Use:

```bash
cargo run -p qbit-prism --bin qbit-prism-audit-verify -- audit-bundle.json \
  --coinbase-tx-hex "$COINBASE_TX_HEX" \
  --ledger-writer-public-key-hex "$LEDGER_WRITER_PUBLIC_KEY_HEX" \
  --expected-coinbase-value-sats "$EXPECTED_COINBASE_VALUE_SATS"
```

`LEDGER_WRITER_PUBLIC_KEY_HEX` must come from trusted operator distribution, not
from the bundle being verified. The bundle can prove consistency with that key;
it cannot prove the key itself is the right one.

For storage efficiency, the coordinator can store compact audit bodies and share
segments while preserving the same logical v1 bundle for verifiers and public
API readers.

## HTTP Surfaces

The coordinator exposes a private audit/ops listener and a dashboard-safe public
API from the same process.

Private/internal endpoints include:

- `/healthz`
- `/metrics`
- `/audit/latest`
- `/owed-balances`
- `/audit/share-window`
- `/audit/blocks/{block_hash}/payouts`
- `/audit/blocks/{block_hash}/bundle`
- `/audit/blocks/{block_hash}/ctv-fanouts`
- `/audit/fanouts/pending`
- `/audit/fanouts/{fanout_txid}/status`
- `/audit/carry-forward-integrity`

Do not expose `/audit/*`, `/metrics`, `/healthz`, Postgres, qbit RPC, or Docker
volumes directly to the internet.

Dashboard-safe endpoints live under `/public/v1`. The public API contract is:

- [docs/public-dashboard-api/README.md](docs/public-dashboard-api/README.md)
- [docs/public-dashboard-api-v1.openapi.yaml](docs/public-dashboard-api-v1.openapi.yaml)

Operators can expose only `/public/v1` through a reverse proxy or dashboard
frontend.

## Run PRISM Locally

Prerequisites:

- Docker with the Compose plugin and a running Docker daemon.
- `make`, `bash`, `git`, `rsync`, and Python 3.
- Rust/Cargo for key derivation and verifier tooling.

Create and review environment:

```bash
cp .env.example .env
$EDITOR .env
```

For the default `QBIT_PROVIDER=git` flow, the stack clones qbit from
`QBIT_GIT_URL`/`QBIT_GIT_REF`. To use a local qbit checkout, set
`QBIT_PROVIDER=source` and `QBIT_SRC_DIR=/absolute/path/to/qbit`.

Generate PRISM signing material:

```bash
PRISM_MANIFEST_SIGNING_SEED_HEX="$(openssl rand -hex 32)"
PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="$(openssl rand -hex 32)"
PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="$(
  cargo run -q -p qbit-pool-builder -- \
    --signing-key-seed-hex "$PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX" \
    --print-public-key-hex
)"
```

Store those three values in `.env`. Keep these disabled outside local test
harnesses:

```text
PRISM_ALLOW_MEMORY_LEDGER=0
PRISM_ALLOW_TEST_SIGNING_SEEDS=0
PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=0
PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN=0
```

For non-regtest deployments, also set `QBIT_PRODUCTION=1`, non-default qbit RPC
credentials, non-default Postgres credentials, and chain-specific qbit settings.

Start the pool:

```bash
make up-prism-pool
```

The target prints the Stratum URL. Miners should use:

```text
URL:      stratum+tcp://<host>:3340
username: <qbit-payout-address>[.<worker>]
password: x
```

Run readiness checks:

```bash
make prism-self-check
```

Static-only checks, before the stack is live:

```bash
python3 scripts/prism-self-check.py --skip-live
```

Stop services with normal Docker Compose controls or `make down`. The Makefile
intentionally keeps PRISM Postgres volume cleanup out of broad all-profile
cleanup because the ledger is operator state.

## Run On Signet

The stack defaults to regtest. To point PRISM at a qbit signet, set the normal
qbit signet overrides in `.env`:

```text
QBIT_CHAIN=signet
QBIT_CHAIN_FLAG=-signet
QBIT_NODE_EXTRA_ARG=-signetchallenge=<your_signet_challenge_hex>
QBIT_LISTEN=0
QBIT_RPC_PORT=38352
QBIT_P2P_PORT=38355
QBIT_RPC_PORT_HOST=127.0.0.1:38352
QBIT_P2P_PORT_HOST=127.0.0.1:38355
QBIT_MINER_ADDRESS=auto
```

Keep RPC loopback-only unless the deployment has explicit firewalling and
deployment-specific authentication.

## Enable CTV Settlement

CTV settlement is off by default. Enable it only on qbit networks/nodes where
the relevant CTV, P2MR, TRUC, and P2A policy paths are supported by the node
you are mining against.

Minimal environment:

```text
PRISM_CTV_SETTLEMENT_ENABLED=1
PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS=10485760
PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS=16
PRISM_MAX_DIRECT_COINBASE_OUTPUTS=12
PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION=1000
PRISM_CTV_FANOUT_FEE_PREMIUM_BPS=12000
```

Fee-rate selection:

- Leave `PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT` empty to use
  the node fee estimate path.
- Set it explicitly when you want a fixed market-fee input for deterministic
  policy.

Broadcaster:

```text
PRISM_CTV_BROADCASTER_ENABLED=1
PRISM_CTV_BROADCASTER_LIMIT=100
PRISM_CTV_BROADCASTER_INTERVAL_SECONDS=30
PRISM_CTV_BROADCASTER_FEE_BITS=0
```

If `PRISM_CTV_BROADCASTER_FEE_BITS` is positive, configure
`PRISM_CTV_BROADCASTER_WALLET` so the broadcaster can fund and sign the CPFP
child. Built-in-fee fanouts do not require a wallet for normal parent
broadcast.

## Useful Tests

Fast sanity:

```bash
make test-compose-prism-config
python3 scripts/prism-self-check.py --skip-live
```

Rust accounting, settlement, verifier, and CTV tests:

```bash
cargo test --locked --workspace --all-targets
```

PRISM end-to-end and ledger tests:

```bash
make test-prism-regtest
make test-prism-postgres-ledger
make test-prism-stratum-regtest-live
make test-prism-stratum-postgres-regtest-live
make test-prism-combined-regtest
```

Capacity harness:

```bash
make test-prism-postgres-throughput
```

The throughput harness measures schema/query capacity. It does not replace a
live miner-swarm load test.

## Operational Notes

- Use Postgres in production. The memory ledger is for local/regtest proof runs
  only.
- Distribute the trusted ledger writer public key out of band.
- Keep manifest signing and ledger attestation signing seeds distinct.
- Back up Postgres and audit artifacts together. Hashes prove artifact
  integrity; backups prove availability.
- Do not compact `qbit_share_ledger` until an archive proof exists.
- Mirror or pin `/audit/carry-forward-integrity` after payout-affecting blocks.
- Keep qbit RPC private and authenticated.
- Expose `/public/v1` through a dashboard or reverse proxy; keep audit and
  metrics private.

For storage and VM sizing, see
[docs/prism-storage-sizing.md](docs/prism-storage-sizing.md). For the formal
ledger operations contract, see
[docs/prism-ledger-ops.md](docs/prism-ledger-ops.md).

## Public Documentation Map

Start here:

- [README.md](README.md): repository overview and quick starts.
- [PRISM.md](PRISM.md): PRISM concept, runbook, and settlement model.
- [doc/mining.md](doc/mining.md): qbit mining operator guide.

Then use the focused docs:

- [docs/public-dashboard-api/README.md](docs/public-dashboard-api/README.md):
  public dashboard API boundary.
- [docs/prism-storage-sizing.md](docs/prism-storage-sizing.md): storage and VM
  sizing.
- [docs/prism-rejections.md](docs/prism-rejections.md): stable Stratum/API
  rejection reason IDs.
- [docs/router-integration-notes.md](docs/router-integration-notes.md): router
  guidance for the ckpool comparison path.

For the broader docs cleanup map, see [docs/README.md](docs/README.md).
