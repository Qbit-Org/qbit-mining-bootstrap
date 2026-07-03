# qbit-mining-bootstrap 0.1.2 Release Notes

Release date: 2026-07-03

## Highlights

- Added PRISM, the direct non-custodial qbit pool path with Stratum mining, Postgres share-ledger storage, audit artifacts, and operator self-checks.
- Added the `qbit-pool-builder` and `qbit-prism` Rust crates for deterministic P2MR coinbase construction, PRISM accounting, settlement, CTV fanout planning, and audit verification.
- Added a public dashboard API contract under `/public/v1` covering pool summary, hashrate series, leaderboard, blocks, miner views, payouts, fanouts, and settlement artifacts.
- Added CTV fanout settlement support, fee accounting, broadcaster tooling, recovery artifacts, and audit-bundle construction.
- Hardened pool operations with production env checks, PRISM writer leases, reorg reconciliation, job-cache fixes, public API caching, and storage sizing guidance.
- Updated the public qbit source default so Docker builds can fetch qbit from `https://github.com/Qbit-Org/qbit.git` without requiring a local qbit checkout.

## How PRISM Works

PRISM is the direct qbit pool path in this repository. It lets miners connect to
a qbit Stratum endpoint while the pool keeps non-custodial, auditable accounting
for accepted shares and block rewards.

At runtime, `prism-coordinator` connects to qbit RPC, builds miner jobs from the
current qbit template, and serves Stratum work directly to miners. Miners
authorize with `<qbit-payout-address>[.<worker>]` usernames. Accepted shares are
validated against the active job and written to the Postgres-backed share ledger
with deterministic ordering.

When a PRISM miner finds a block, the ledger defines the reward window used for
that block. PRISM computes each payout from the ordered share history, carries
immature or uneconomic amounts forward, and produces deterministic settlement
artifacts that miners and operators can audit independently.

Settlement uses qbit-specific P2MR and CTV fanout tooling. The Rust
`qbit-pool-builder` and `qbit-prism` crates build deterministic coinbase,
payout, fanout, and audit structures. The broadcaster path can publish eligible
fanout transactions, while audit CLIs and `/public/v1` read models expose enough
state for external verification without giving public access to private operator
endpoints.

The important trust boundary is the ledger writer key. Operators generate unique
PRISM signing seeds, publish the trusted ledger writer public key out of band,
and keep qbit RPC, Postgres, audit internals, metrics, health endpoints, Docker
volumes, and key material private. Miners and dashboards should consume only the
public read models and published audit artifacts.

## Operator Notes

- Review `.env.example` before upgrading. PRISM deployments require unique signing seeds, a trusted ledger writer public key, and production guard flags before `make up-prism-pool` starts.
- Run `make doctor` before starting the stack and `make prism-self-check` after starting PRISM.
- Keep qbit RPC, Postgres, ckpool sockets, `/audit/*`, `/metrics`, `/healthz`, Docker volumes, and PRISM key material private unless intentionally proxied with access controls.
- The qbit provider default only changes how the Docker image obtains qbit source. Operators can use the public git default, pin `QBIT_GIT_REF` and `QBIT_GIT_COMMIT`, or set `QBIT_PROVIDER=source` plus `QBIT_SRC_DIR=/absolute/path/to/qbit` for a local checkout.
- The historical 0.1.x release notes remain in `doc/` so public release history stays inspectable from the repository.
