# qbit-mining-bootstrap 0.1.0 Release Notes

Release date: 2026-06-17

## Highlights

- Initial public release of the qbit mining bootstrap lab.
- Added Docker Compose workflows for regtest mining operations, with signet-oriented configuration hooks.
- Added permissionless qbit mining support through ckpool, including startup preflight checks and version-rolling mask handling.
- Added AuxPoW lab support with qbitd, bitcoind, a Python coordinator, Stratum helpers, and example RPC payload scripts.
- Added operator documentation for mining flows, merge-mining protocol expectations, chain parameters, and router integration.
- Added shell smokes and Python unit tests covering ckpool startup, qbit RPC fixtures, AuxPoW coordinator behavior, vardiff logic, and Stratum codec behavior.

## Operator Notes

- Compose defaults to regtest for deterministic local validation.
- qbit source builds can be provided from a local checkout or a configured git ref.
- qbit core remains the canonical source for consensus, block validation, and mining RPC semantics; this repo validates operator workflows around those interfaces.
