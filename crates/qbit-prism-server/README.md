# qbit-prism-server

Native Rust Prism runtime: multithreaded Stratum, PostgreSQL accounting,
in-process payout construction, durable block submission, CTV broadcasting, and
the existing audit/dashboard HTTP contracts. Several instances can serve the
same pool using one HA PostgreSQL writer endpoint.

See [PRISM.md](../../PRISM.md), the [ledger contract](../../docs/prism-ledger-ops.md),
and the [migration guide](../../docs/prism-rust-migration.md).

## Build and run

```sh
cargo build --locked --release -p qbit-prism-server -p qbit-prism --bins
target/release/qbit-prism-server check-config
target/release/qbit-prism-server migrate
target/release/qbit-prism-server run
```

Export configuration in the invoking process. The executable does not load a
`.env` file itself; Compose supplies that environment. Every runtime requires
`PRISM_DATABASE_URL`, distinct `PRISM_MANIFEST_SIGNING_SEED_HEX` and
`PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX`, and the matching trusted
`PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX`. Production requires reviewed non-lab share
and vardiff bounds and rejects the test-key allowances.

`PRISM_POSTGRES_INIT_SCHEMA=1` enables additive migration at startup; its native
default is false. Prefer an explicit `migrate` step for production cutover.
Migration refuses a live legacy Python writer lease and prevents reacquisition.

## Commands

| Command | Behavior |
| --- | --- |
| `run` (also no subcommand) | Serve Stratum, HTTP, and background settlement workers |
| `check-config` | Validate local configuration and key pairing; no listeners |
| `healthcheck [--url URL]` | Probe HTTP readiness; needs no keys or database connection |
| `self-check` | Check a configured live deployment's node, database integrity/durability, and HTTP health |
| `migrate` | Apply the additive PostgreSQL schema after stopping Python writers |
| `import-audits [--root PATH]` | Verify and import database-referenced legacy filesystem bundles |
| `backfill-ctv` | Reconstruct missing fanout sets from verified database audits |
| `broadcast-ctv` | Process one batch of mature fanout claims |
| `capacity-evidence FILE [options]` | Validate the retained strict v2 load-evidence format |
| `benchmark --shares N --miners N --iterations N [--output-json PATH]` | Measure synthetic native audit build and verification |

Use `--help` or `<command> --help` for the complete CLI. Database operator
commands need the configured keys even when no listener is started. Legacy
Python script-specific filtering flags are replaced by the commands above.

`self-check` also probes the optional high-difficulty listener's first advertised
difficulty. On an empty pool without a configured fallback/fee address, set
`PRISM_SELF_CHECK_ADDRESS` to a valid P2MR payout address for that probe.

## Resource and topology settings

| Setting | Native default | Purpose |
| --- | --- | --- |
| `PRISM_INSTANCE_ID` | Generated UUID | Unique process identity for durable claims/heartbeats |
| `PRISM_RUNTIME_WORKERS` | Available CPU parallelism | Tokio worker threads |
| `PRISM_JOB_BUILD_EXECUTOR_WORKERS` | Up to 4, bounded by runtime workers by default | Concurrent blocking CPU builds |
| `PRISM_DATABASE_MAX_CONNECTIONS` | 16 | Per-instance pool size, minimum 4 |
| `PRISM_DATABASE_STATEMENT_TIMEOUT_MS` | 15000 | PostgreSQL statement timeout |
| `PRISM_DATABASE_LOCK_TIMEOUT_MS` | 5000 | PostgreSQL lock wait timeout |
| `PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS` | 60 | Periodic reward snapshot renewal |
| `PRISM_BLOCKPOLL_SECONDS` | 2 | Template polling interval |
| `PRISM_BLOCKWAIT_ENABLED` | true | Additional node tip-change wakeup |
| `PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS` | 15 | Maximum healthy tip-poll age |
| `PRISM_CTV_SPEND_SCAN_BLOCKS` | 32 | Maximum historical blocks per no-txindex CTV scan pass |
| `PRISM_STRATUM_PORT` | 3340 | Primary Stratum listener |
| `PRISM_AUDIT_PORT` | 3341 | HTTP listener |

Binds default to loopback outside Compose. `QBIT_RPC_URL` overrides the host/port
URL assembled from `QBIT_RPC_HOST` and `QBIT_RPC_PORT`. Each physical server may
use a local qbitd, provided all nodes follow the same chain. All instances use
the same ledger and payout/signing configuration; ports and resource limits are
local. Allocate database pool capacity for the sum of all instances.

Existing Stratum/authorization, vardiff, high-difficulty port, payout, fee, CTV,
and public API settings remain available in [.env.example](../../.env.example).
The old `_SATS` monetary aliases remain supported. Python writer leases,
batch/linger queues, subprocess builders, and staged refresh schedulers were
removed; their tuning variables do not configure the native runtime.

## Tests

```sh
cargo test --locked -p qbit-prism-server --lib --test api_contract --test capacity
bash test/prism-native-tests.sh
QBITD_BIN=/path/to/qbitd bash test/prism-native-tests.sh live
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin \
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
```

Database tests use isolated schemas. The shell wrapper uses
`PRISM_TEST_DATABASE_URL` or starts a private PostgreSQL cluster with installed
server tools. Live regtest requires `QBITD_BIN` and exercises two native
instances, real RPC/block validation, and constrained CPU miners. The CI database
job runs the native package against PostgreSQL.

The separate physical failover test starts disposable primary and synchronous
standby PostgreSQL processes behind a stable TCP endpoint. It reconciles commits
from two ledger clients after immediate primary loss and standby promotion, then
checks deduplication and continued writes. It requires PostgreSQL server tools
through `PRISM_TEST_PG_BIN_DIR`; without that variable the test skips. Run it as
an unprivileged user. It validates this test topology; separately exercise the
production HA manager/proxy, storage, and replication policy.

The crate separates transport (`stratum`), job/chain coordination (`coordinator`),
durable accounting (`ledger`), settlement broadcasting (`broadcaster`), HTTP
(`api`), and operator tools (`tools`, `capacity`). Core reward math and canonical
verification remain in `qbit-prism`; no Python compatibility process is needed.
