# Native PRISM configuration

Run `qbit-prism-server check-config` before starting a frontend. The command
validates configuration without connecting to PostgreSQL or the node. It
checks the mining, public API, and background rollup settings together.
Malformed API numbers and booleans fail validation instead of selecting a
fallback value.

Every environment name beginning with `PRISM_` that the native process does
not support is listed in one diagnostic, including names whose values are
empty. Values are never included in this diagnostic. It is a warning in lab
mode and an error when `QBIT_PRODUCTION=1`, `QBIT_TOOLS_PRODUCTION=1`, or
`QBIT_CHAIN=mainnet` (also `main`) selects production mode. Settings belonging
to Compose or a replica bootstrap script should be passed to those tools,
not exported into the native server process.

Starting the coordinator runs the same check: `qbit-prism-server` with no
subcommand (the `run` serve path) refuses to start in production, naming each
unsupported variable, and warns in lab mode. The check runs before the
process parses its configuration or contacts the node, so a 2.x.x environment
is reported instead of a downstream startup failure. Unset the reported names
in the unit or Compose environment, or move them to the tool that reads them;
a name that belongs to no tool should be removed. `check-config` remains the
preflight, because it reports every invalid setting it covers without
starting a frontend; the public reader's own database settings are checked
separately by `check-public-database-config`.

The supported names live in
[`native-settings.txt`](../crates/qbit-prism-server/src/config/native-settings.txt).
The retired 2.x.x names live in
[`retired-settings.txt`](../crates/qbit-prism-server/src/config/retired-settings.txt).
The diagnostic also catches unknown names absent from either inventory.
Conditional settings remain recognized when their feature is disabled.

## Mounted signing seeds

Production frontends read signing seeds from files:

```sh
PRISM_MANIFEST_SIGNING_SEED_HEX_FILE=/run/secrets/qbit-prism/manifest-signing-seed-hex
PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX_FILE=/run/secrets/qbit-prism/ledger-attestation-signing-seed-hex
```

Each file contains one 32-byte hexadecimal seed. A trailing newline is
accepted. The process must be able to read the mounted files as its non-root
UID; provision ownership and permissions before starting it. Keep the
trusted public key in `PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX`; it must match the
ledger seed, and the two signing keys must differ.

Nonempty direct `PRISM_MANIFEST_SIGNING_SEED_HEX` and
`PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX` values are rejected in production.
Outside production, either the direct value or its `_FILE` form is accepted;
setting both fails rather than choosing one silently. Empty, unreadable,
malformed, or oversized secret files also fail. Development test seeds still
require `PRISM_ALLOW_TEST_SIGNING_SEEDS=1`, which production rejects.

This implements decision D4 in #260: signing keys remain on every mining
frontend, with mounted delivery, a non-root image, and disabled core dumps.
On Linux, the server disables process dumpability before loading credentials;
this also prevents dumps piped to a host collector, for which
[Linux ignores `RLIMIT_CORE`](https://man7.org/linux/man-pages/man5/core.5.html).
The key rotation rehearsal required before cutover is tracked by #291.

## Database-only commands and audit import

`migrate`, `import-audits`, and `backfill-ctv` load database configuration
without reading either signing seed. They still require
`PRISM_DATABASE_URL`; connection and instance budgets use the same validation
as the server. `backfill-share-hashes`, which finishes a deferred share-hash
backfill after go-live, reads `PRISM_DATABASE_URL` alone. Audit import and CTV backfill additionally require the public
trust pin `PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX` to verify stored artifacts.
Commands that build or sign work continue to require signing configuration.

For audit import, `--root` overrides `PRISM_AUDIT_DIR`. Without `--root`,
`PRISM_AUDIT_DIR` selects the directory containing legacy audit bodies.
An empty value is treated as unset. When neither is set, the ledger importer
resolves stored body URIs as before.

## Operator listener and health refresh

`PRISM_OPERATOR_BEARER_TOKEN` optionally protects the operator listener on
port 3341. Its `_FILE` form reads a mounted token instead. Tokens must contain at
least 16 visible ASCII characters with no whitespace. Use exactly one form,
and configure probes and monitoring clients with the matching bearer
token. The built-in `healthcheck` command reads that token for operator
probes; `healthcheck --public-api` probes the independent public role without
sending it. Probes refuse redirects.

`PRISM_HEALTH_REFRESH_SECONDS` controls the health publisher cadence as well
as the snapshot and `self-check` heartbeat staleness budgets, both
`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds. It must be a whole number
from 1 through 86400 seconds (default 2).
The public API remains a separate process and does not need signing seeds.

## Public audit artifact admission

`/public/v1/artifacts/<sha256>` serves a block's audit by digesting and parsing
its sealed canonical bytes or, for an unsealed native block, by rebuilding the
share window; both hold the whole window in memory and outlive the read
connection. `PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY` bounds how many run at
once in one process, a whole number from 1 through 64 (default 1). It is
independent of `PRISM_POSTGRES_READ_CONCURRENCY`, so a larger read pool admits
no more rebuilds. A request waits for a rebuild slot within its own
`PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS` deadline and gets 503
`read_timeout` when that runs out.

`PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT` caps the audit artifact requests
running or waiting for a slot, a whole number from 1 through 4096 (default 32).
With the response cache enabled (the default), identical concurrent requests
share one computation and one place under the cap. A request past the cap is refused at once with 503
`audit_artifact_busy` and `Retry-After: 5`, after two indexed point lookups and
before any audit read. CTV manifests served from the same route are never
counted or refused. Both settings apply to the public service and to the
operator listener's copy of the public routes; a value outside its range stops
startup with `<NAME> must be <min>..<max>`. See
[prism-ledger-ops.md](prism-ledger-ops.md#public-audit-artifact-admission) and
[prism-storage-sizing.md](prism-storage-sizing.md#native-audit-body-size).

## Share ledger partition maintenance

`qbit_share_ledger` is partitioned by `share_seq` and has no DEFAULT
partition, so an append whose sequence value has run past the last attached
bound is refused by PostgreSQL. Every frontend keeps the lead attached by
calling `qbit_prism_share_partition_ensure()` once at startup and then every
`PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS`, a whole number from 1
through 86400 seconds (default 60). The call is serialized per schema, so
running several frontends creates each partition once, and it creates nothing
while the lead is intact; a run that creates partitions logs at info, and one
that fails logs at warning and is retried on the next tick without ending the
task.

A server whose startup call fails does not start: an instance that cannot
maintain its partitions must not accept shares until its lead runs out. The
width of a partition and how many are kept ahead of the sequence are database
settings, not environment settings; they live in the one row of
`qbit_prism_share_partitioning` (default 2^24 rows and 4 partitions, about
67 million rows of headroom). See
[prism-share-ledger-partitioning.md](prism-share-ledger-partitioning.md) for
the design record and
[prism-ledger-ops.md](prism-ledger-ops.md#share-ledger-partitions-and-retention)
for the operator procedure. `qbit_prism_share_ledger_partition_lead_rows`
in `/metrics` reports the remaining headroom; see
[prism-native-metrics.md](prism-native-metrics.md).

## Stratum per-source cap and per-session budgets

Five settings bound what one source address or one connection can cost. Every
one defaults to the value that keeps today's behavior, and `check-config`
rejects an out-of-range value rather than falling back.

| Setting | Default | Accepted range | Failure when rejected |
| --- | --- | --- | --- |
| `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP` | 0 (disabled) | 0 through the semaphore capacity | `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP exceeds semaphore capacity` |
| `PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS` | 60 | finite, above 0, at most 3600 | `PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS must be finite, positive and at most 3600` |
| `PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL` | 0 (disabled) | 0 through 1000000 | `PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL must be at most 1000000` |
| `PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL` | 0 (disabled) | 0 through 1000000 | `PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL must be at most 1000000` |
| `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL` | 0 (disabled) | 0 through 1000000 | `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL must be at most 1000000` |

A value that is not a number fails as `invalid <name>`; an empty value selects
the default. The three budgets are rates measured over the shared interval, not
session lifetime totals: a full window's allowance is admitted as a burst and
refills whole at the next window. Valid `mining.submit` traffic is never
charged against any of them.

The per-source cap keys on the address this listener observed, which is the
last hop. Leave it at 0 unless the deployment preserves miner source addresses.
See [prism-b4-stratum-admission.md](prism-b4-stratum-admission.md) for the
runbook, the recommended starting values and the topologies that make a cap
unsafe.

## Stratum listen backlog

`PRISM_STRATUM_LISTEN_BACKLOG` (default 4096) is how many connections the kernel
queues on each Stratum listener before the accept loop takes them. It accepts 1
through 2147483647, the range `listen(2)` takes. `check-config` rejects an
out-of-range value with `PRISM_STRATUM_LISTEN_BACKLOG must be between 1 and
2147483647`, and a value that is not a number with `invalid
PRISM_STRATUM_LISTEN_BACKLOG`; an empty value selects the default. It applies to the primary and
high-difficulty listeners alike. A burst of miners reconnecting together, as
when a cutover re-points every miner, a frontend restarts or a load balancer
fails over, is held in that queue. Past it, the kernel drops their SYNs
(`TcpExtListenOverflows`), the miners retry a second later, and a load balancer
that observes layer-4 errors can mark the frontend down. The kernel caps the
value at `net.core.somaxconn` in the frontend's network namespace without an
error, so raise that too if it is lower. The frontend's `PRISM listening` line
logs both `listen_backlog` and `somaxconn`, and a warning names the cap when
somaxconn is the lower.

Before this setting the native listeners used mio's fixed backlog of 128; the
2.x.x runtime read the same name with a default of 1024. The operator and
public HTTP listeners use a fixed backlog of 1024.

## Block submission kill switch

`PRISM_BLOCK_SUBMIT_ENABLED` (boolean, default `1`) exists for rehearsals
against a real node and a restored ledger (#291). Set to `0`, a frontend sends
its node no found block and no transaction:

- the submit loop claims no candidate. A found block is still validated,
  credited and enqueued, but its outbox row stays `pending` and `submitblock`
  is never called;
- the CTV fanout broadcaster does not start, whatever
  `PRISM_CTV_BROADCASTER_ENABLED` says, and `broadcast-ctv` refuses before it
  connects. That broadcaster is PRISM's only sender of `sendrawtransaction`
  and `submitpackage`, and its only user of the CPFP wallet.

The frontend's node client also refuses, before sending, every call that would
relay a block or a transaction. Templates, readiness, share acceptance and the
database writes continue as usual. The setting is read at startup and applies
per frontend; it is not part of the cluster fingerprint. `0`, `false`, `no`
and `off` all turn it off. While it is `0`, `check-config` and `self-check`
lead with a warning. See
[the rehearsal procedure](prism-ledger-ops.md#block-submission-kill-switch-for-rehearsals)
for checking it on running frontends and for what to do with held blocks
afterwards. Never leave it at `0` on a production frontend: a block that
frontend finds is never offered, and `PrismBlockSubmissionHeld` pages a minute
after the frontend starts.

A ledger can also hold block submission for every frontend that connects to
it, whatever this setting says: `qbit-prism-server submission-hold set
--reason ...` (migration 023, #664). `check-config` never reads the database,
so it only says so; `submission-hold show` and `self-check` report the hold.
See [holding the whole cluster](prism-ledger-ops.md#holding-the-whole-cluster-023-664).

## Dual writer (3.1)

PRISM 3.1 can run two nodes, A and B, each with its own writable PostgreSQL.
Each node's frontend writes only to its local database and pulls the rows the
other node originated. `PRISM_DUAL_WRITER` (boolean, default `0`) turns this
on. While it is off, no setting below is read and a frontend behaves exactly as
3.0 does, whatever the others hold. With it on:

| Setting | Default | Meaning |
|---|---|---|
| `PRISM_NODE_INDEX` | required | `0` on node A, `1` on node B |
| `PRISM_CARRY_OWNER` | required | `1` on the one node that pays down carried balances (normally A), `0` on the other |
| `PRISM_PEER_DATABASE_URL` | required | the peer's PostgreSQL, as its read-only sync role |
| `PRISM_PEER_DATABASE_URL_FALLBACK` | unset | a second network path to the same database, tried when the first fails |
| `PRISM_PEER_SYNC_INTERVAL_MS` | `250` | wait between pulls that found nothing new, 10 to 60000 |
| `PRISM_PEER_SYNC_BATCH_ROWS` | `5000` | most rows of one stream a pull reads and inserts at once, 1 to 20000: each batch is one statement on the peer, under its 8 s timeout |
| `PRISM_PEER_INGEST_WAIT_MS` | `250` | before a found block's `submitblock`, the most it waits for the peer to hold what adopting the block needs, 0 (off) to 10000 |

Each error names its setting and never prints a value: the peer DSNs carry the
sync role's credentials. A peer DSN must use `postgres` or `postgresql`, name
a host, and not name this node's own database (`PRISM_DATABASE_URL`'s host,
port and database). The fallback must differ from the first DSN. In production
neither may carry the shipped `change-this` credential. These settings are per
node and not part of the cluster fingerprint: each node's database is its own
cluster, and the two nodes' identities differ by design. Migration 027 adds
the columns and tables the dual writer needs, and
`qbit-prism-server node-identity set --index N` personalises each database
before its first dual-writer start; see
[dual-writer node identity](prism-ledger-ops.md#dual-writer-node-identity-027).
A node rebuilt from a physical copy of its peer's database is re-personalised
instead, with `node-identity repersonalise --index N`
([rebuilding a node](prism-ledger-ops.md#rebuilding-a-node-from-its-peer)).

A dual-writer node has no failover standby, so `PRISM_OFFER_STANDBY_APPLICATION_NAME`
and `PRISM_OFFER_STANDBY_FLUSH_WAIT_MS` are left unset there. Its peer adopts a
block it found if it dies, and `PRISM_PEER_INGEST_WAIT_MS` takes the standby
wait's place: before the block leaves, the node reads the peer's cursors over
its own rows until they cover its shares through the block's window and the
prepared record the block was built on. The block is offered when the bound
passes or the peer cannot be read, as before; each outcome is counted in
`qbit_prism_peer_sync_offer_waits_total{outcome}`.

`PRISM_DUAL_WRITER_DOWNGRADE` (boolean, default `0`) belongs to the rollback
to one writer. A single-writer frontend refuses a database that has run as a
dual-writer node (its carry-owner journal `qbit_prism_node_roles` holds rows),
because it would pay carried balances beside the carry owner. Set to `1`, the
frontend starts anyway, with a warning. Only the deliberate rollback sets it.

## Readiness endpoint and admission (3.1)

A readiness-only HTTP listener for a balancer's checks, meant for a node's
public address; off unless `PRISM_READINESS_PORT` is set.

| Setting | Default | Meaning |
|---|---|---|
| `PRISM_READINESS_PORT` | `0` (off) | its TCP port; 9084 on the pair |
| `PRISM_READINESS_BIND` | `127.0.0.1` | its bind address; `0.0.0.0` inside a container |
| `PRISM_READINESS_TOKEN` or `PRISM_READINESS_TOKEN_FILE` | required with the port | the token checks must send in `X-Qbit-Healthcheck-Token`: at least 16 visible ASCII characters with no whitespace. Use exactly one form; a group-readable file is fine |
| `PRISM_READINESS_GRACE_SECONDS` | `10` | how long readiness may stay false, as through a tip or payout-revision rebuild, before the frontend stops admitting miners; 0 to 120 |

`GET /readyz` answers `200` while the frontend admits miners, `503` while it
does not, `401` to any request without exactly the right token, whatever its
path, and `404` to any other path or method that carries it. In dual-writer
mode the Stratum listeners also accept connections only while it admits. With
the port unset and `PRISM_DUAL_WRITER` off, `/healthz` and the Stratum
listeners behave as in 3.0 and no readiness statement runs; the new metric
families show only their HELP and TYPE lines, and
`PRISM_READINESS_GRACE_SECONDS` is still validated at startup. In dual-writer
mode `qbit-prism-server healthcheck` reports liveness, not readiness, and needs
the operator listener; the frontend's health path also keeps its own pool of
at most two database connections beside `PRISM_DATABASE_MAX_CONNECTIONS`. See
[dual-writer readiness](prism-dual-writer-readiness.md).

## Preventing stale guidance

CI runs `python3 scripts/check_prism_settings.py`. It checks the native name
inventory against source references and rejects retired settings in
`scripts/check-env.sh`, `.env.example`, `docs/`, `doc/`, and operator READMEs.
An explicitly historical Markdown mention
can use a same-line `retired-setting` HTML annotation naming that one setting.
This exception cannot suppress a shell check or other lines of documentation.
