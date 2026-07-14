# qbit-mining-bootstrap

`qbit-mining-bootstrap` is the runnable mining lab for qbit. It stays outside qbit core on purpose: qbit owns node, RPC, and validation behavior; this repo owns operator workflows, Docker Compose, pool configs, helper services, runbooks, and end-to-end mining validation.

PRISM (Payouts, Rewards, and Integrity Settlement Manifest) is the open-source
qbit pool path in this repo. PRISM uses TIDES-style reward accounting: accepted
shares are recorded in one canonical ordered ledger, each found block is split
over the active share window, and published audit data lets miners recompute the
split independently. TIDES, Transparent Index of Distinct Extended Shares, was
originally documented by OCEAN for Bitcoin mining pools; PRISM is independent
qbit pool software with qbit-specific P2MR settlement, carry-forward, maturity,
and reorg policy. For the full PRISM guide, see [`PRISM.md`](PRISM.md).

## Scope

What stays in qbit core:

- consensus and block validation
- mining RPCs such as `getblocktemplate`, `submitblock`, `createauxblock`, and `submitauxblock`
- functional tests that prove node behavior

What this repo owns:

- the regtest operator lab and signet bootstrap path
- the permissionless pool path
- the AuxPoW coordinator path
- docs and CI that prove both flows still work

This repo is not the canonical source of qbit mining semantics. It assumes those semantics already exist in qbit and validates that operators can actually use them.

## Operator Mode

The repo has two jobs:

- `lab` mode proves the protocol paths in regtest with minimal harnesses
- `operator` mode is the human-facing flow: `qbitd`, a real pool or bridge, and an actual miner or parent-chain node

Today the lab stages qbit source from either a configured public git ref or a local checkout. Once release artifacts are available, the provider contract can switch to a qbit image or tarball without changing the operator commands.

## Current Defaults

- Compose defaults to regtest, with signet available through env overrides
- Compose supports `source` and `git` qbit providers
- the public sample env uses the `git` provider with `https://github.com/Qbit-Org/qbit.git`
- `source` remains available for local qbit checkouts via an absolute `QBIT_SRC_DIR`
- `QBIT_BIN_DIR` is optional and only used by the legacy local shell smokes
- permissionless mining uses `ckpool`
- AuxPoW uses an external Python coordinator plus `bitcoind`
- PRISM provides the direct non-custodial qbit pool path
- the near-term git provider model is a configured qbit repo/ref, with optional exact commit pinning

## Quick Start

Prerequisites:

- Docker with the Compose plugin and a running Docker daemon
- `make`, `bash`, `git`, and `rsync`
- Python 3 for the unit-test helpers

1. Copy `.env.example` to `.env`.
2. Review the qbit provider in `.env`:
   - keep the default `QBIT_PROVIDER=git` to clone `https://github.com/Qbit-Org/qbit.git`
   - or set `QBIT_PROVIDER=source` and `QBIT_SRC_DIR=/absolute/path/to/qbit` for a local checkout
   - optionally set `QBIT_GIT_COMMIT` to pin an exact qbit commit for reproducible runs
3. Start the operator stack:

```bash
cp .env.example .env
$EDITOR .env
make up
```

To validate the environment without starting containers, run:

```bash
make doctor
```

To validate a running PRISM operator stack, run:

```bash
make prism-self-check
```

`prism-self-check` prints PASS/WARN/FAIL rows for qbit RPC, coordinator
health, Stratum reachability, Postgres readiness, audit-dir writability,
production key material, forbidden test flags, and basic mining
configuration. It exits non-zero on hard failures.

### Run PRISM Pool

Direct PRISM Stratum requires Postgres and three key values before
`make up-prism-pool` starts. Generate unique deployment seeds and derive the
trusted ledger public key from the ledger attestation seed:

```bash
PRISM_MANIFEST_SIGNING_SEED_HEX="$(openssl rand -hex 32)"
PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="$(openssl rand -hex 32)"
PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="$(
  cargo run -q -p qbit-pool-builder -- \
    --signing-key-seed-hex "$PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX" \
    --print-public-key-hex
)"
```

Store those values in `.env`, keep all `PRISM_ALLOW_*` flags at `0`, and set
`QBIT_PRODUCTION=1` for non-regtest deployments. The public ledger key must be
distributed to verifiers out of band; do not ask verifiers to trust a key copied
from the audit bundle they are checking.

Then start and validate the pool:

```bash
make up-prism-pool
make prism-self-check
```

`make up-prism-pool` prints the Stratum URL. Miners authorize with
`<qbit-payout-address>[.<worker>]` usernames. The coordinator serves `/public/v1`
from its audit HTTP listener for dashboard-safe read models. Keep `/audit/*`,
`/metrics`, `/healthz`, Postgres, qbit RPC, ckpool command sockets, and Docker
volumes private unless you intentionally proxy them with access controls. See
[`docs/prism-storage-sizing.md`](docs/prism-storage-sizing.md) for PRISM
Postgres, audit artifact, and VM sizing guidance.

Useful entrypoints:

- `make up`
- `make up-permissionless`
- `make up-permissionless-pool`
- `make test-permissionless`
- `make test-permissionless-p2mr`
- `make up-real-miner`
- `make test-real-miner`
- `make up-auxpow`
- `make up-auxpow-bridge`
- `make up-auxpow-pool`
- `make up-prism-pool`
- `make prism-self-check`
- `make test-prism-regtest`
- `make test-prism-stratum-regtest-live`
- `make up-dual-pools`
- `make test-auxpow`
- `make test-auxpow-stratum`
- `make smoke-all`
- `make down`

`make down` stops every Compose profile and preserves all named volumes. Local
chain data, the share ledger, and audit artifacts therefore survive an ordinary
stop/restart cycle. The separate `make purge-local-volumes` target is only for
disposable development stacks: it refuses production and main-chain
configurations and requires the exact confirmation token printed by the
command before it will delete volumes.

When upgrading an existing project from the former two-pool smoke topology,
run `make down` from the new checkout before starting the selected services.
Its `--remove-orphans` cleanup removes containers that no longer exist in
Compose without deleting their named volumes. Retired volumes stay detached and
can be archived or removed separately.

For a fresh public-network deployment, use the fail-closed sequence and explicit
operator service sets in [`docs/mainnet-deployment.md`](docs/mainnet-deployment.md).

## Signet Mode

The compose stack stays regtest by default so the built-in smokes remain deterministic. To point qbit at a signet instead, set:

```bash
QBIT_CHAIN=signet
QBIT_CHAIN_FLAG=-signet
QBIT_NODE_EXTRA_ARG=-signetchallenge=<your_signet_challenge_hex>
QBIT_LISTEN=0
QBIT_RPC_PORT=38352
QBIT_P2P_PORT=38355
QBIT_RPC_PORT_HOST=127.0.0.1:38352
QBIT_P2P_PORT_HOST=127.0.0.1:38355
QBIT_MINER_ADDRESS=auto
CKPOOL_MINDIFF=<reviewed-share-floor>
CKPOOL_STARTDIFF=<reviewed-starting-share-difficulty>
CKPOOL_MAXDIFF=<optional-share-difficulty-cap>
```

Then use the normal operator entrypoints such as `make up-permissionless-pool` or `make up-auxpow-bridge`.

The qbit container stays private by default: `QBIT_LISTEN=0` prevents inbound
qbit P2P service, and the Docker P2P publish address is loopback-only. That lets
the pool use outbound peers without presenting itself as a reachable qbit node to
peer crawlers or DNS seeds. To intentionally run a public qbit peer, set
`QBIT_LISTEN=1` and publish `QBIT_P2P_PORT_HOST=0.0.0.0:<p2p-port>`.
The published qbit RPC and ZMQ ports are also loopback-only by default. Do not
publish RPC outside the host unless you also replace the example credentials
with deployment-specific authentication and firewall rules.

For non-regtest chains, ckpool startup fails closed unless qbit is out of IBD,
has at least `CKPOOL_MIN_PEERS` peer connection, exposes the expected qbit GBT
shape, and receives explicit `CKPOOL_MINDIFF` and `CKPOOL_STARTDIFF` values.
Regtest keeps the lab-only `1/256` difficulty floor.

The one launch-only exception is explicitly authorized mainnet prelaunch. It
requires all five values below; missing, invalid, or mismatched values fail
closed:

```bash
QBIT_CHAIN=mainnet
QBIT_PRODUCTION=1
QBIT_TOOLS_PRODUCTION=1
CKPOOL_NON_TEST_READINESS_GATE=0
QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0
```

Preflight still checks the RPC chain, the mandatory mainnet genesis hash, static qbit
assumptions, difficulty policy, and P2MR payout address, but defers launch-only
IBD, peer, live-template, freshness, and active-tip checks. CKPool remains
running with its Stratum listener bound and retries GBT until qbitd can serve
work. At launch, set both readiness flags to `1` and restart or redeploy CKPool;
the new supervisor then checks IBD, peer count, live-template freshness, and
active-tip agreement continuously. Runtime environment changes are not
hot-reloaded by an already-running supervisor.
`CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS` controls the interval and persistent
failures terminate CKPool after `CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS`.

`qbit-ckpool-preflight` supports three interfaces: no arguments runs the full
one-shot preflight; `--production-gate-only` runs only stateless production,
CKPool-knob, and explicit public-difficulty checks without RPC; and
`--supervise <command> [args...]` runs full initial checks, launches the command
without a shell, forwards termination signals, and applies the continuous
watchdog. Mainnet requires `QBIT_EXPECTED_GENESIS_HASH`; other chains may set it
to pin the connected node's genesis. Template time bounds are
`CKPOOL_TEMPLATE_MAX_AGE_SECONDS` and `CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS`.

If you want both public mining paths on one host, use:

```bash
make up-dual-pools
```

That starts:

- `qbitd`
- `ckpool` on `stratum+tcp://127.0.0.1:3333` for permissionless qbit mining
- `bitcoind`
- `auxpow-stratum` on `stratum+tcp://127.0.0.1:3335` for parent-chain merged mining

The `auxpow-stratum` service (also reachable via `make up-auxpow-pool`) refreshes
miner jobs whenever either chain tip changes, when the Bitcoin parent template
exceeds `AUXPOW_TEMPLATE_MAX_AGE_SECONDS` (default 120), or after
`AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS` (default 2700, i.e. 45 minutes). The latter
sits below qbit's default 60 minute `-auxpowtemplateexpiry`, so both parent work
and the cached qbit candidate receive bounded replacement.

The AuxPoW Stratum bridge advertises parent-chain work, enables per-miner vardiff by default, and uses `AUXPOW_STRATUM_VERSION_MASK=1fffe000` for BIP310 version rolling. qbit child candidates still come from `createauxblock`; permissionless qbit, ckpool, and direct PRISM Stratum prefer the connected qbit node's `getblocktemplate.versionrollingmask` when present. Older or unavailable GBT probes fall back to each service's configured mask: `CKPOOL_VERSION_MASK` for ckpool and `PRISM_VERSION_ROLLING_MASK` for direct PRISM. For byte-order investigations, set `AUXPOW_STRATUM_DIAG_VARIANTS=1` and optionally `AUXPOW_STRATUM_DIAG_JSONL=1`; normal operation uses `AUXPOW_STRATUM_HEADER_VARIANT=canonical`.

Vardiff defaults target one accepted share every 5 seconds, start miners at difficulty 8192, clamp the minimum advertised difficulty to 1024, and retarget every 120 seconds using share-weighted work with EWMA smoothing. By default retargets send `mining.set_difficulty` and defer the new share target until the next natural job refresh (`AUXPOW_STRATUM_VARDIFF_APPLY_MODE=next_job`), which avoids sending clean replacement jobs with identical header space. Set `AUXPOW_STRATUM_VARDIFF_APPLY_MODE=clean_job` only for miners that require immediate clean-job difficulty enforcement.

On a VM, expose or relay both Stratum ports if you want both URLs reachable from outside the host.

For a live AuxPoW endpoint, also set the `BITCOIN_RPC_*` and `BITCOIN_MINER_ADDRESS` envs to the parent-chain node you want to mine against. If you leave the defaults alone, the local `bitcoind` container stays in regtest mode and the AuxPoW URL is only a lab smoke.

The bundled Bitcoin Core image verifies its downloaded archive against the
architecture-specific SHA256 pinned in `config/upstream.env` before extracting
it. See [`docs/bitcoin-release-integrity.md`](docs/bitcoin-release-integrity.md)
before changing the Bitcoin Core version, download location, or release
digests.

### Bitcoin Testnet4 Parent Node

The checked-in `config/bitcoin/bitcoin.conf` keeps `dnsseed=0` and `discover=0` so regtest lab runs stay deterministic. For a local parent `bitcoind` container on public Bitcoin testnet4, enable discovery through env instead of editing that file:

```bash
BITCOIN_CHAIN_FLAG=-testnet4
BITCOIN_RPC_PORT=48332
BITCOIN_P2P_PORT=48333
BITCOIN_RPC_PORT_HOST=127.0.0.1:48332
BITCOIN_P2P_PORT_HOST=127.0.0.1:48333
BITCOIN_DNSSEED=1
BITCOIN_MINER_ADDRESS=<your_bitcoin_testnet4_payout_address>
```

`BITCOIN_DNSSEED` renders as a command-line flag, so it overrides the regtest-safe config default and lets `bitcoind` bootstrap outbound peers through DNS seeds. `BITCOIN_DISCOVER=1` is also available when you want `bitcoind` to discover and advertise its own reachable address. For less common parent-node flags, set `BITCOIN_NODE_EXTRA_ARGS` to whitespace-separated `bitcoind` arguments; they are appended after the compose defaults:

```bash
BITCOIN_NODE_EXTRA_ARGS="-dnsseed=1 -addnode=testnet4-peer.example:48333"
```

Use repeated `-addnode=host:port` entries in `BITCOIN_NODE_EXTRA_ARGS` when you want explicit peers instead of DNS seeding. No post-checkout patch to `config/bitcoin/bitcoin.conf` is required.

The `test/` shell smokes and the deterministic end-to-end harnesses remain regtest-only. Signet mode is for the bootstrap glue around `qbitd`, payout-address handling, pool wiring, and bridge wiring against a shared qbit signet.

## What The Lab Proves

`make test-permissionless` brings up:

- `qbitd`
- `ckpool`
- a minimal Stratum miner simulator

It validates an end-to-end permissionless flow:

- `ckpool -> Stratum miner -> qbit getblocktemplate/submitblock`

`make test-permissionless-p2mr` runs the same stack with `-p2mronly=1` so the permissionless lab also proves restricted-output compatibility end to end. The test forces
`QBIT_MINER_ADDRESS=auto` for that run so a local non-P2MR `.env` payout address cannot short-circuit the restricted-output smoke.

The operator-facing version of this flow is the same stack, except the built-in simulator is replaced by a real Stratum miner pointed at the ckpool port.

To bring your own miner instead of using the simulator:

```bash
make up-permissionless-pool
```

That starts `qbitd + ckpool` and prints the local Stratum endpoint. Point any compatible SHA256d Stratum miner at that port and use a qbit payout address as the username. If `QBIT_MINER_ADDRESS=auto`, the compose helpers derive the chain-default payout address from the node first. On public chains that resolves to P2MR.

By default, ckpool starts with `CKPOOL_VERSION_MASK_MODE=dynamic`: it asks qbitd
for `getblocktemplate` and uses the advertised `versionrollingmask` when the
connected node exposes it. Older qbitd builds that do not expose the field fall back to
the configured `CKPOOL_VERSION_MASK`; the sample env uses `1fffe000` because
current qbitd permissionless templates advertise that mask. The selected mask is
logged and rendered into `/etc/ckpool/ckpool.conf`. If qbitd advertises
`versionrollingmask=00000000`, ckpool disables BIP310 version rolling for that
node.

CKPool also exposes its private command socket below `CKPOOL_SOCK_DIR`
(`/tmp/qbitlab` by default). Override that path only to share the Unix socket
with a local private stats exporter or same-host operator tooling. Do not
publish the socket directory or `/var/log/ckpool` outside the mining-pool host.

To prove the repo against a real external miner client instead of the built-in simulator:

```bash
make test-real-miner
```

That starts the `real-miner-smoke` profile and points the bundled
`cpuminer-opt` client at the ordinary `ckpool` service on port 3333. The
`make up-permissionless-real` target remains as a deprecated compatibility
alias for `make up-real-miner` for one release.

`make test-auxpow` brings up:

- `qbitd`
- `bitcoind`
- the `auxpow-coordinator`

It validates an end-to-end AuxPoW flow:

- `createauxblock -> Bitcoin parent block with merged-mining commitment -> submitauxblock`

The operator-facing version of this flow is `qbitd + bitcoind + a real merge-mining pool or bridge`, with the coordinator acting as the current reference implementation.

To run the one-shot coordinator path without the negative-path assertions wrapper:

```bash
make up-auxpow
```

That starts `qbitd + bitcoind + auxpow-coordinator` and exits after submitting a valid AuxPoW block.

To run the long-lived bridge shape instead of the one-shot regression harness:

```bash
make up-auxpow-bridge
```

That starts `qbitd + bitcoind + auxpow-bridge`, which continuously requests fresh AuxPoW templates, mines local parent-chain work, and submits AuxPoW blocks back to qbit.

On regtest that flow is deterministic. In signet mode the bridge can talk to a signet qbit node and request `createauxblock`, but accepted `submitauxblock` results still depend on a parent-side worker that can satisfy the live qbit target.

The AuxPoW lab also checks the required negative paths:

- stale template rejection via `stale-prevblk`
- malformed commitment rejection via `bad-auxpow-commitment`
- invalid parent proof-of-work rejection via `bad-auxpow-parent-hash`

## Local Shell Smokes

The `test/` scripts are kept as lightweight local smokes. They launch temporary local `qbitd -regtest` instances and support either:

- `QBIT_BIN_DIR=/path/to/qbit/build/bin`
- explicit `QBITD_BIN` and `QBIT_CLI_BIN`
- globally installed `qbitd` and `qbit-cli`

They still require `QBIT_SRC_DIR` because they reuse qbit's functional-test helpers.

For development against a moving qbit repo, either:

- keep `QBIT_PROVIDER=source` and point `QBIT_SRC_DIR` at a local checkout
- switch to `QBIT_PROVIDER=git` and let the lab clone the ref configured in `.env`

Every `make` target stages a clean qbit source tree under `generated/` before calling Docker, so host build directories and repo layout changes do not leak into the container builds.

If you bypass `make` and invoke Compose directly, stage a qbit source tree first
and export the resolved `QBIT_SRC_DIR`. The `git` provider can do that from the
configured values in `.env`:

```bash
export QBIT_PROVIDER=git
export QBIT_SRC_DIR="$(bash scripts/prepare-qbit-source.sh)"
docker compose --env-file config/upstream.env --env-file .env -f compose.yaml ...
```

For a local qbit checkout, export `QBIT_SRC_DIR=/absolute/path/to/qbit` instead.
Add `--env-file .env` too if you are using local overrides.

Run them with:

```bash
export QBIT_SRC_DIR=/absolute/path/to/qbit
bash test/test-all.sh
```

## Important Port Note

qbit has its own JSON-RPC defaults. The qbit-specific `8355` value is the mainnet P2P port, not the mainnet RPC port.

| Network | Default RPC port | Default P2P port |
| --- | ---: | ---: |
| Mainnet | 8352 | 8355 |
| Testnet3 | 18352 | 18355 |
| Testnet4 | 48352 | 48355 |
| Signet | 38352 | 38355 |
| Regtest | 18452 | 18460 |

## Operator Notes

- Public qbit networks use P2MR-only wallet output types. Use default wallet-generated payout addresses, and keep payout transactions inside qbit's allowed output policy.
- Direct PRISM Stratum accepts `<qbit-payout-address>[.<worker>]` usernames. On qbit test chains, an invalid username payout falls back to `tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3` unless `PRISM_USERNAME_FALLBACK_ADDRESS` is set.
- Direct PRISM Stratum tags its coinbase scriptSig with `PRISM_COINBASE_TAG` before the Stratum extranonce. The default is `/PRISM/`; set `PRISM_COINBASE_TAG=` to disable it or set another short printable ASCII tag.
- Direct PRISM Stratum accrues sub-floor miner balances instead of emitting dust outputs. The default floor is `14,720` bits; set `PRISM_PAYOUT_MIN_OUTPUT_BITS` for a fixed floor such as `10000`, or tune the `PRISM_PAYOUT_*` formula inputs. `PRISM_PAYOUT_MIN_OUTPUT_SATS` is still accepted as a legacy alias.
- PRISM's launch settlement policy uses `10,485,760` bits as the default
  qbit-specific direct coinbase payout threshold. This is not the raw dust
  floor: balances below the direct threshold should route through CTV fanout
  when economically spendable, and only below-floor dust falls into
  carry-forward/fee-liability policy. Set `PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS`
  to override it; the old `_SATS` name is still accepted as a legacy alias.
- PRISM audit bundles automatically add a `qbit.prism.audit.commitment.v1` witness-merkle leaf to the coinbase witness commitment. The leaf commits to the canonical reward and payout-policy manifests without adding another coinbase output.
- PRISM exposes carry-forward ledger integrity at `/audit/carry-forward-integrity`. The report flags replay mismatches and publishes an `audit_head_sha256` over active carry-forward rows. Do not compact carry-forward rows until a signed checkpoint format is published and mirrored.
- Pool software that assumes Bitcoin defaults still needs explicit qbit overrides for `COINBASE_MATURITY=1000` and `WITNESS_SCALE_FACTOR=1`.
- `ckpool` is the first validated permissionless path, not the final word on post-launch pool support.
