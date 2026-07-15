# Mainnet Deployment

Mainnet should be deployed as a fresh stack with fresh data volumes and a
separate environment file. Do not reuse a test-chain data directory or convert a
running test deployment in place. The application code is shared across
networks; chain identity, immutable source inputs, credentials, payout policy,
and service selection are deployment inputs.

This guide deliberately contains no network-specific genesis hash, payout
address, credential, or fee rate. Supply the values approved for the release.

## Release Inputs

Freeze these inputs before building:

- the exact qbit source commit and its final genesis hash
- the Bitcoin Core release version and both verified architecture digests
- the container image digests produced from those inputs
- explicit qbit and Bitcoin payout addresses for every enabled mining lane
- the PRISM Postgres, signing, audit, difficulty, and CTV policies

Every deployed container must be an immutable, digest-qualified release
artifact. Set all of these to references of the form
`registry.example/image@sha256:<64 lowercase hex characters>`:

```dotenv
QBITD_IMAGE=<digest-qualified image>
CKPOOL_IMAGE=<digest-qualified image>
BITCOIND_IMAGE=<digest-qualified image>
AUXPOW_COORDINATOR_IMAGE=<digest-qualified image>
PRISM_COORDINATOR_IMAGE=<digest-qualified image>
PRISM_POSTGRES_IMAGE=<digest-qualified image>
```

Production validation requires a full 40-character `QBIT_GIT_COMMIT` for every
source provider and checks that the staged source is exactly that commit. Follow
[`bitcoin-release-integrity.md`](bitcoin-release-integrity.md) when changing the
parent-node release pins.

Validation runs in two tiers. The behavioral production checks (non-default
credentials, explicit payout addresses, strict difficulty and readiness
policies, commit-pinned sources) apply whenever `QBIT_PRODUCTION=1`,
`QBIT_TOOLS_PRODUCTION=1`, or `QBIT_CHAIN=mainnet`; zero stale grace
(`PRISM_STRATUM_STALE_GRACE_SECONDS=0`) is pinned only on mainnet, so a public
test-chain pool may credit shares that raced a block within a bounded grace
window. The
release-provenance checks (digest-qualified `*_IMAGE` references and absolute
`*_DATA_SOURCE` host paths) are enforced unconditionally on mainnet — no
environment variable can disable them there — and on other chains only with
`QBIT_REQUIRE_RELEASE_PROVENANCE=1`. A
production pool that builds images from the pinned source in place (for
example a public testnet4 deployment) runs with that flag unset and keeps
every behavioral check active.

## Host And Storage

Size qbit chain data, Bitcoin chain data, Postgres, Postgres WAL, audit
artifacts, and container storage separately. An unpruned Bitcoin mainnet parent
requires materially more space and synchronization time than the local lab
parent. Complete its initial synchronization before exposing AuxPoW Stratum.

For PRISM, retain encrypted off-host base backups and continuous WAL archives.
If the operating contract promises no loss after an acknowledged share, use a
synchronous standby on independent storage and exercise failover before launch.
See [`prism-storage-sizing.md`](prism-storage-sizing.md) for capacity and restore
criteria.

The production Compose override requires pre-created absolute host paths. Keep
these paths outside the source checkout and on storage sized for their distinct
write and retention profiles:

```dotenv
QBIT_DATA_SOURCE=/srv/qbit-mining-bootstrap/mainnet/qbit
BITCOIN_DATA_SOURCE=/srv/qbit-mining-bootstrap/mainnet/bitcoin
PRISM_POSTGRES_DATA_SOURCE=/srv/qbit-mining-bootstrap/mainnet/postgres/data
PRISM_POSTGRES_WAL_SOURCE=/srv/qbit-mining-bootstrap/mainnet/postgres/wal
PRISM_AUDIT_DATA_SOURCE=/srv/qbit-mining-bootstrap/mainnet/prism/audit
```

Create each directory before rendering Compose, then grant only the corresponding
runtime the required access. Compose refuses to create missing production bind
paths.

`PRISM_POSTGRES_WAL_SOURCE` is the primary's live WAL directory. Separating it
from `PGDATA` permits an independent capacity and I/O boundary only when the two
paths are backed by different mounts or devices. Two directories on one
filesystem remain one capacity and failure domain. In either case, the live WAL
path is not a backup and neither path is independently disposable; the database
requires both. Continuous WAL archives must still be copied to independent,
off-host storage alongside compatible base backups.

`POSTGRES_INITDB_WALDIR` takes effect only while initializing a new database
cluster. This runbook assumes a fresh mainnet cluster. Do not add the WAL mount
to an existing cluster and expect Postgres to relocate `pg_wal`; use a reviewed
Postgres migration procedure instead.

## Production Compose Contract

Always render and start mainnet with both Compose files. The override requires
immutable images and host-backed state, prevents Docker from silently creating a
missing state directory, and moves integration helpers out of the operator
profiles. It uses Compose's `!override` merge tag for that profile isolation;
the installed Docker Compose release must support that tag.

Capacity qualification is optional and external to startup. The standalone
validator and artifact format are documented in
[`prism-capacity-readiness.md`](prism-capacity-readiness.md); neither the
coordinator nor this Compose contract requires an artifact.

Export the deployment env path once. Make, source staging, environment
validation, and the PRISM self-check then use it as the sole local deployment
layer after upstream defaults; repository `.env` values are not mixed into the
deployment:

```sh
export DEPLOY_ENV_FILE=/etc/qbit-mining-bootstrap/mainnet.env
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-qbit-mining-bootstrap}"
```

`make doctor` stages the pinned source at `generated/qbit-src`, which is also the
default base Compose build context. Production commands pull immutable images
and use `--no-build`; they do not build from that context.

Run the static gate for the enabled lanes, then render an explicit service
graph. The example enables all three lanes; remove profiles and services that
are not part of the deployment.

```sh
MINING_LANES=ckpool,auxpow,prism make doctor

docker compose \
  --project-name "$COMPOSE_PROJECT_NAME" \
  --env-file config/upstream.env \
  --env-file "$DEPLOY_ENV_FILE" \
  -f compose.yaml \
  -f compose.production.yaml \
  --profile permissionless \
  --profile auxpow \
  --profile prism \
  config --quiet qbitd ckpool bitcoind auxpow-stratum prism-postgres prism-coordinator
```

Pull reviewed artifacts before stopping the prior release. Start operator
services with `--no-build` so the host cannot replace a reviewed artifact with
a locally built tag. On a fresh host, start only the nodes first:

```sh
docker compose \
  --project-name "$COMPOSE_PROJECT_NAME" \
  --env-file config/upstream.env \
  --env-file "$DEPLOY_ENV_FILE" \
  -f compose.yaml \
  -f compose.production.yaml \
  pull qbitd ckpool bitcoind auxpow-stratum prism-postgres prism-coordinator

docker compose \
  --project-name "$COMPOSE_PROJECT_NAME" \
  --env-file config/upstream.env \
  --env-file "$DEPLOY_ENV_FILE" \
  -f compose.yaml \
  -f compose.production.yaml \
  up -d --no-build --pull never qbitd bitcoind
```

Omit `bitcoind` when AuxPoW is disabled. Wait for each enabled public node to
report the exact chain and genesis, `initialblockdownload=false`, equal block
and header heights, peers, and a fresh usable template. Initial synchronization
can take a long time; do not start pool runtimes and let restart policies churn
through it.

After node readiness, initialize Postgres when PRISM is enabled, then start the
selected pool runtimes:

```sh
docker compose \
  --project-name "$COMPOSE_PROJECT_NAME" \
  --env-file config/upstream.env \
  --env-file "$DEPLOY_ENV_FILE" \
  -f compose.yaml \
  -f compose.production.yaml \
  up -d --no-build --pull never prism-postgres

docker compose \
  --project-name "$COMPOSE_PROJECT_NAME" \
  --env-file config/upstream.env \
  --env-file "$DEPLOY_ENV_FILE" \
  -f compose.yaml \
  -f compose.production.yaml \
  up -d --no-build --pull never ckpool auxpow-stratum prism-coordinator
```

Remove services for lanes that are not enabled. The `make
up-permissionless-pool`, `make up-auxpow-pool`, `make up-prism-pool`, and `make
up-dual-pools` targets use this production override and never build when the
deployment env selects production or mainnet. They start only already-cached
digest images with `--pull never`, so a restart does not depend on registry
availability. Run the explicit release `pull` step above before a first start or
upgrade. Lab/helper `up` targets refuse a production or main-chain
configuration.

Preserve the same Compose files, project, and `DEPLOY_ENV_FILE` for every later
`config`, `pull`, `up`, `stop`, and `down` command.

## Required Network Settings

Use exact selectors rather than relying on an empty main-chain default:

```dotenv
QBIT_CHAIN=mainnet
QBIT_CHAIN_FLAG=-chain=main
QBIT_EXPECTED_GENESIS_HASH=<64 lowercase hex characters>

BITCOIN_CHAIN=mainnet
BITCOIN_CHAIN_FLAG=-chain=main
BITCOIN_EXPECTED_GENESIS_HASH=<64 lowercase hex characters>
BITCOIN_DNSSEED=1
```

An explicit parent peer supplied through `BITCOIN_NODE_EXTRA_ARGS` may replace
DNS discovery. qbit and Bitcoin RPC ports should remain bound to a private or
loopback interface. Publish only the selected P2P and Stratum endpoints.

Mainnet automatically enables the production safety gates. Default passwords,
test signing keys, the in-memory ledger, diagnostic header variants, a fixed
ledger session token, an unpinned genesis, and a noncanonical chain selector all
cause startup validation to fail.

## Mining Service Selection

Compose profiles contain both operator services and deterministic integration
helpers. Start production operator services by name through the production
Compose contract:

| Lane | Operator services |
| --- | --- |
| CKPool solo | `qbitd ckpool` |
| PRISM | `qbitd prism-postgres prism-coordinator` |
| AuxPoW Stratum | `qbitd bitcoind auxpow-stratum` |

`permissionless-miner`, `real-miner`, `auxpow-real-miner`, and the one-shot
AuxPoW coordinator are smoke-test clients. `real-miner` belongs to the
`real-miner-smoke` integration profile; it reads the ordinary `ckpool`
service's resolved payout address and connects on port 3333. These helpers
should not be included in a mainnet service set.

Use separate service units when independent mining lanes must be deployable or
restartable without interrupting the others.

## CKPool Gate

CKPool fetches and publishes its own qbit jobs on its configured update cycle.
The container wrapper independently validates qbit before launching CKPool and
then supervises the child process. In strict mode, qbit must remain out of IBD,
connected to the minimum peer count, on the pinned genesis, and able to return a
fresh template whose `previousblockhash` matches its active tip. A persistent
failure terminates and reaps CKPool, then exits nonzero so Docker can restart the
complete runtime. A recovered validation resets the failure timer.

Both qbitd and CKPool use `restart: unless-stopped`, covering unexpected clean
exits and Docker daemon restarts. `docker compose stop` and `make down` remain
explicit operator shutdowns and do not delete named volumes.

`CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS` sets the validation cadence, and
`CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS` sets how long a failed validation may
persist before shutdown. Each RPC may take up to
`CKPOOL_PREFLIGHT_RPC_TIMEOUT_SECONDS`. The default watchdog poll interval is 5
seconds, and the default maximum future template time is 30 seconds.
`CKPOOL_UPDATE_INTERVAL` must be lower than `CKPOOL_TEMPLATE_MAX_AGE_SECONDS`
so CKPool cannot publish one job beyond that freshness bound.

The only relaxed mode is explicitly authorized mainnet prelaunch. It requires
all five values below; any missing, invalid, or mismatched value fails closed:

```bash
QBIT_CHAIN=mainnet
QBIT_PRODUCTION=1
QBIT_TOOLS_PRODUCTION=1
CKPOOL_NON_TEST_READINESS_GATE=0
QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0
```

Prelaunch still checks static policy, chain and mandatory genesis identity, and
the explicit payout address. It defers IBD, peer, GBT, freshness, and active-tip
checks so CKPool can bind its listener and retry GBT while qbitd starts. At
launch, set both readiness flags to `1` and restart or redeploy CKPool. A running
supervisor does not hot-reload environment changes.

## AuxPoW Gate

Mainnet AuxPoW requires explicit `QBIT_MINER_ADDRESS` and
`BITCOIN_MINER_ADDRESS` values. The coordinator must not create a payout wallet
implicitly on an economically valuable chain.

Before binding AuxPoW Stratum, the runtime verifies:

- both RPC-reported chains match their configured chains
- qbit and Bitcoin match their configured expected genesis hashes
- both public nodes are out of initial block download
- block and header heights match and at least one peer is connected
- qbit returns usable AuxPoW work
- Bitcoin returns a usable, fresh block template

`AUXPOW_TEMPLATE_MAX_AGE_SECONDS` bounds acceptable parent-template age, while
`AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS` rejects timestamps beyond Bitcoin's
two-hour consensus future-time window.
`AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS` bounds how long Stratum may fail
to publish fresh work before exiting for supervised restart. Exercise this path
before exposing the listener.

## PRISM Gate

Production PRISM uses Postgres and sends a successful share response only after
the share transaction commits. A block-worthy share commits its complete block
intent in the same transaction. The in-memory candidate queue is only a wakeup;
pending work is replayed from Postgres after a restart.

Before startup, set `PRISM_STRATUM_SHARE_DIFF`,
`PRISM_STRATUM_VARDIFF_MIN_DIFF`, `PRISM_STRATUM_VARDIFF_START_DIFF`, and
`PRISM_STRATUM_VARDIFF_MAX_DIFF` to explicit, reviewed, positive values.
Production rejects missing values, the local-lab `1e-9` profile, and bounds that
do not satisfy `minimum <= start <= maximum`. This direct safety check does not
require a capacity artifact.

Set the group-commit policy explicitly if the defaults are not appropriate:

```dotenv
PRISM_SHARE_COMMIT_BATCH_SIZE=64
PRISM_SHARE_COMMIT_LINGER_MILLISECONDS=5
PRISM_SHARE_COMMIT_TIMEOUT_SECONDS=15
```

Keep Postgres `fsync`, `full_page_writes`, and `synchronous_commit` enabled.
Measure acknowledgment latency under representative accepted-share load rather
than weakening database durability.

Production requires `PRISM_STRATUM_STALE_GRACE_SECONDS=0` until every published
audit consumer has demonstrated compatibility with stale-grace receipts.
`PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS` similarly bounds a persistent
PRISM template-refresh outage; it must be shorter than the operator alert and
response window.

### CTV At Genesis

A chain with no confirmed transaction history cannot produce a useful
`estimatesmartfee` result, even if it mines empty blocks. Configure a positive,
reviewed `PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT` for launch and
retain the configured fee premium.

For a 1 bit/vbyte launch floor, set the market-rate input to `1000`; the default
`12000` premium basis points then reserves a 20 percent margin. The live
preflight also rejects a configured rate below the node's relay or mempool fee
floor.

When the committed parent transaction already pays its fee, broadcasting is
walletless:

```dotenv
PRISM_CTV_SETTLEMENT_ENABLED=1
PRISM_CTV_BROADCASTER_ENABLED=1
PRISM_CTV_BROADCASTER_WALLET=
PRISM_CTV_BROADCASTER_FEE_BITS=0
```

A wallet is only required for optional positive CPFP sponsorship.

## Deployment Sequence

1. Provision the host, mount the separately sized storage, and pre-create every
   required production bind path with reviewed ownership and permissions.
2. Install the frozen source inputs, write digest-qualified image references,
   and create the new mainnet env file.
3. Export `DEPLOY_ENV_FILE` and `COMPOSE_PROJECT_NAME` as shown above, run the
   static environment validation for exactly the enabled lanes, and render
   their explicit production service graph:

   ```sh
   MINING_LANES=ckpool,auxpow,prism make doctor
   MINING_LANES=prism python3 scripts/prism-self-check.py --skip-live
   ```

4. Pull immutable images. Start qbit and, when AuxPoW is enabled, Bitcoin without
   the pool runtimes. Wait for exact chain, genesis, peer, header, IBD, and
   template checks to pass.
5. Initialize the fresh Postgres cluster with its separate live WAL path, then
   configure base backups, off-host WAL archiving, and any synchronous standby.
6. Start only the intended pool runtimes. Run `make prism-self-check` after
   PRISM is live.
7. Connect controlled miners to each selected lane. Confirm subscribe,
   authorize, initial difficulty, fresh jobs, accepted work, reconnect behavior,
   and accounting visibility.
8. Exercise one child-process restart at a time and verify readiness and miner
   recovery before admitting public traffic.

## Go-Live Checks

Do not expose a lane until all applicable checks pass:

- rendered qbit and Bitcoin commands contain exactly one `-chain=main`
- every service reports the frozen qbit genesis hash and every AuxPoW service
  reports the frozen Bitcoin genesis hash
- rendered operator images are digest-qualified and state mounts resolve to the
  reviewed absolute host paths
- node IBD, header, peer, and template readiness is healthy
- production credentials and signing material are nondefault
- CKPool and PRISM difficulty settings have passed a controlled-miner smoke/load
  check; the optional v2 capacity artifact is not required
- every successful PRISM share survives a coordinator restart exactly once
- a pending winning candidate resumes safely after a forced process exit
- walletless fee-bearing CTV fanout construction and broadcast have passed
- AuxPoW uses explicit payout addresses and no helper payout wallet
- backup restore, Postgres failover, disk alerts, and container restart policies
  have been exercised

## Stop And Rollback

Use the same environment and both Compose files with `stop` or `down` for a
normal rollback. Neither command deletes the production bind-mounted state.
Preserve the env and immutable image/source references, and restart the prior
reviewed artifact against the same state only when its schema compatibility is
known.

`make purge-local-volumes` is for disposable development stacks. It requires an
exact confirmation token and refuses production, qbit main-chain, and Bitcoin
main-chain configurations. It is not a mainnet reset mechanism.
