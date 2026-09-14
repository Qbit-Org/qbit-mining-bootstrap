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
as the snapshot staleness budget. It must be a whole number from 1 through
86400 seconds (default 2).
The public API remains a separate process and does not need signing seeds.

## Preventing stale guidance

CI runs `python3 scripts/check_prism_settings.py`. It checks the native name
inventory against source references and rejects retired settings in
`scripts/check-env.sh`, `.env.example`, `docs/`, `doc/`, and operator READMEs.
An explicitly historical Markdown mention
can use a same-line `retired-setting` HTML annotation naming that one setting.
This exception cannot suppress a shell check or other lines of documentation.
