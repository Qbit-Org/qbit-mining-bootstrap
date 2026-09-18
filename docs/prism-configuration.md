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
as the server. Audit import and CTV backfill additionally require the public
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

## Preventing stale guidance

CI runs `python3 scripts/check_prism_settings.py`. It checks the native name
inventory against source references and rejects retired settings in
`scripts/check-env.sh`, `.env.example`, `docs/`, `doc/`, and operator READMEs.
An explicitly historical Markdown mention
can use a same-line `retired-setting` HTML annotation naming that one setting.
This exception cannot suppress a shell check or other lines of documentation.
