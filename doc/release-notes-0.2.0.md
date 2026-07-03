# qbit-mining-bootstrap 0.2.0 Release Notes

Release date: 2026-07-03

## Highlights

- Added PRISM, the direct non-custodial qbit pool path with Stratum mining, Postgres share-ledger storage, audit artifacts, and operator self-checks.
- Added the `qbit-pool-builder` and `qbit-prism` Rust crates for deterministic P2MR coinbase construction, PRISM accounting, settlement, CTV fanout planning, and audit verification.
- Added a public dashboard API contract under `/public/v1` covering pool summary, hashrate series, leaderboard, blocks, miner views, payouts, fanouts, and settlement artifacts.
- Added CTV fanout settlement support, fee accounting, broadcaster tooling, recovery artifacts, and audit-bundle construction.
- Hardened pool operations with production env checks, PRISM writer leases, reorg reconciliation, job-cache fixes, public API caching, and storage sizing guidance.
- Updated qbit provider defaults so public examples clone `https://github.com/Qbit-Org/qbit.git`, while local source checkouts remain available through `QBIT_PROVIDER=source`.

## Operator Notes

- This is a minor release rather than a patch release because it introduces new runtime services, public APIs, release artifacts, and configuration surfaces.
- Review `.env.example` before upgrading. PRISM deployments require unique signing seeds, a trusted ledger writer public key, and production guard flags before `make up-prism-pool` starts.
- Run `make doctor` before starting the stack and `make prism-self-check` after starting PRISM.
- Keep qbit RPC, Postgres, ckpool sockets, `/audit/*`, `/metrics`, `/healthz`, Docker volumes, and PRISM key material private unless intentionally proxied with access controls.
- The historical 0.1.x release notes remain in `doc/` so public release history stays inspectable from the repository.
