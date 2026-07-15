# qbit-mining-bootstrap 1.0.0 Release Notes

Release date: 2026-07-15

## Highlights

- Promotes the bootstrap stack to the qbit mainnet release line by defaulting
  qbit source builds to `Qbit-Org/qbit` tag `v1.0.0` at commit
  `7ebcddb622d6e639041f005a189b048ec2a221fe`.
- Documents the qbit AuxPoW chain IDs used by this release line: mainnet
  chain ID `47` and public testnet4 chain ID `31430`.
- Bumps the repository `VERSION` and Rust package metadata to `1.0.0` so the
  bootstrap release line matches the qbit mainnet release line.
- Adds fail-closed mainnet prelaunch and go-live checks for source provenance,
  qbit image identity, qbit RPC reachability, ckpool configuration, PRISM
  readiness, production compose contracts, and final genesis-state pinning.
- Adds a real qbitd image build and runtime smoke test in CI so the bootstrap
  image is exercised from the pinned qbit source before release.
- Hardens ckpool mainnet prelaunch supervision, including explicit empty
  coinbase-signature handling and stricter qbit preflight checks.
- Makes PRISM capacity qualification optional while preserving the standalone
  evidence artifact path for operators who want Stratum-to-Postgres capacity
  proof before go-live.
- Expands PRISM readiness, share-ledger, vardiff, reward-window, and
  self-check coverage for mainnet launch operations.

## Mainnet Compatibility

This release is intended to pair with qbit `v1.0.0` at
`7ebcddb622d6e639041f005a189b048ec2a221fe`. The default source provider now
fetches that tag and verifies the pinned commit when `QBIT_GIT_COMMIT` is set.
Operators can still use `QBIT_PROVIDER=source` with a local checkout, but
production deployments should keep either the commit pin or equivalent
provenance evidence.

The documented AuxPoW chain IDs match the qbit release line:

- Mainnet: `47`
- Public testnet4: `31430`

The qbit mainnet genesis hash for this release line is:

`0000000000004d60aa5d46013991d0a0e2995d89ee98e53068ae196d763e79f2`

## Operator Notes

- Review `.env.example` and `docs/mainnet-deployment.md` before upgrading a
  production or mainnet-prelaunch stack.
- Keep production images digest-qualified. `scripts/check-env.sh` is expected
  to reject mutable image references, missing qbit source provenance, and
  missing final chain-state pins in production mode.
- Run `make doctor` before starting the stack. For PRISM deployments, run
  `make prism-self-check` after startup and keep private RPC, Postgres,
  metrics, audit, health, volume, and signing-key surfaces off the public
  internet unless intentionally protected by access controls.
- For final mainnet launch, set the production genesis and chain-state values
  from the live qbit node rather than carrying prelaunch placeholders forward.

## Changes Since v0.1.2.3

- Mainnet readiness and production environment gates.
- Mainnet prelaunch bootstrap readiness and ckpool supervision fixes.
- Real qbit image build and runtime CI smoke coverage.
- PRISM capacity evidence tooling and optional qualification gates.
- qbit `v1.0.0` source pin and chain-ID documentation updates.
