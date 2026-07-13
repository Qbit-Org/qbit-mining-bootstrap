# PRISM Storage and VM Sizing

This document sizes the PRISM pool storage footprint for operator planning. It
separates hot Postgres, audit artifacts, qbit/Bitcoin chain data, Docker
overhead, WAL, and backups because those are different capacity problems.

## Scope

PRISM stores permanent accounting data in Postgres and writes public audit
artifacts under `PRISM_AUDIT_DIR`.

Base Compose uses named volumes for local development:

- `prism-postgres-data` at `/var/lib/postgresql/data`
- `prism-postgres-wal` at `/var/lib/postgresql/wal`
- `prism-audit-data` at `/var/lib/qbit-prism/audit`
- `qbit-data` at `/var/lib/qbit`

Production Compose replaces these with pre-created absolute bind mounts supplied
through `PRISM_POSTGRES_DATA_SOURCE`, `PRISM_POSTGRES_WAL_SOURCE`,
`PRISM_AUDIT_DATA_SOURCE`, and `QBIT_DATA_SOURCE`. The data and live WAL sources
must be distinct paths and should be monitored as separate capacity boundaries.

The current production storage target is:

- keep the canonical accepted-share ledger in Postgres, or in a verified
  immutable archive when a future archive flow exists
- keep block, payout, carry-forward, maturity, reorg, and settlement state
  durable
- keep canonical audit artifact hashes in Postgres
- keep large exact audit artifact bodies outside hot Postgres once the artifact
  externalization work lands

## Pilot Baseline

A small pilot deployment measured on 2026-06-30 showed active PRISM state at
about 1.2 GB before long-term growth. The useful planning signal is the
composition of that active state:

| Active PRISM item | Size / count |
| --- | ---: |
| Postgres volume | about 195 MB |
| Database size | about 140 MB |
| Audit file volume | about 989 MB |
| qbit data volume | about 61 MB |
| Active PRISM total | about 1.2 GB |

The active table breakdown was:

| Table | Size / rows |
| --- | ---: |
| `qbit_pool_audit_bundles` | 126 MB, 571 rows |
| `qbit_share_ledger` | 3.7 MB, 5,624 accepted shares |
| payout, carry-forward, and block tables | about 2.5 MB combined |

The accepted-share ledger was not the disk driver in that pilot. Inline audit
bundle bodies dominated the hot database footprint, which is why newer artifact
externalization and share-segment formats matter for production planning.

## Growth Model

qbit targets 60-second aggregate block spacing, so the network produces about
1,440 blocks per day. PRISM vardiff targets about one accepted share per active
worker every 15 seconds.

Use:

```text
accepted_shares_per_second = active_workers / 15
accepted_shares_per_year = accepted_shares_per_second * 31,536,000
hot_share_ledger_bytes = accepted_shares_per_year * 1.2 KB
pool_blocks_per_day = 1,440 * pool_share_of_blocks
```

The measured share-ledger cost is about 0.7 to 0.8 KB per accepted share across
pilot data and local schema probes. Use 1.2 KB per share for planning to cover
indexes, bloat, and operational headroom.

The estimates below assume the optimized Postgres pattern: hot Postgres keeps
the canonical ledger, block/payout/carry rows, and artifact metadata, not the
full audit JSONB body for every block. If exact artifact bodies remain local,
provision artifact storage separately.

Externalizing exact current artifacts reduces hot Postgres size. It does not
reduce total retained bytes unless artifacts are compressed or replaced by a
future reduced/window proof format.

## One-Year Hot Postgres Scenarios

| Scenario | Active workers | Shares/sec | Pool share of blocks | Blocks/day | Estimated hot Postgres live data/year | Practical VM disk if backups/artifacts are not local |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Testnet / pilot | 15 | 1 | 1% | 14 | about 38 GB | 250-500 GB |
| Early public | 150 | 10 | 5% | 72 | about 386 GB | 1.5-2 TB |
| Serious pool | 1,500 | 100 | 20% | 288 | about 4.1 TB | 8-10 TB |
| Very large | 7,500 | 500 | 50% | 720 | about 22 TB | 45-60 TB |

If local artifact bodies, local physical backups, or WAL archives are kept on
the same VM, increase the practical disk target:

| Scenario | Practical disk with local artifacts/backups |
| --- | ---: |
| Testnet / pilot | 500 GB-1 TB |
| Early public | 4-8 TB |
| Serious pool | 15-25 TB or dedicated database storage |
| Very large | dedicated storage architecture, not a single small VM |

## Permanent Data

Keep these forever in hot storage or a verified immutable archive:

- accepted shares with exact `share_seq` order
- `share_id`, miner/payout identity, payout order key, and P2MR program
- share difficulty/work amount and network difficulty context
- job/template metadata, `job_issued_at`, `accepted_at`, and proof identifiers
- block reward-window anchors
- block, payout, carry-forward, maturity, reorg, and reversal rows
- payout policy inputs, fee policy, minimum payout/floor settings, and
  settlement-mode decisions
- canonical audit artifact hashes and verifier/schema versions
- coinbase txid/hex and manifest hashes needed to match chain evidence
- CTV/fanout settlement artifacts needed for third-party verification or
  broadcast recovery

Do not prune `qbit_share_ledger` until the archive contract in
`docs/prism-ledger-ops.md` is implemented and verified.

## Archiveable Or Ephemeral Data

These can have bounded retention once the policy is explicit:

- rejected-share details after operational/debug retention
- non-canonical candidate audit artifacts that never became accepted blocks
- dashboard caches and rollups that can be regenerated or are intentionally
  lower resolution over time
- verbose broadcast attempts after terminal status, if latest status, error
  summary, attempt count, and final artifacts are retained. The live schema
  now stores this summary on `qbit_ctv_fanout_artifacts` and caps retained
  detail rows with `PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT`.
- Docker images, Docker build cache, and old non-active TIDES volumes after
  backup or decommission confirmation

Inactive PRISM blocks are not deletion candidates simply because they are
inactive. They can reactivate after a reorg.

## Artifact Storage Strategy

The live coordinator stores accepted-block audit artifacts with these rules:

1. Exact artifact externalization: Postgres stores hash/pointer metadata while
   every reader still resolves the body to the logical
   `qbit.prism.audit-bundle.v1` expected by public API and verifier callers.
   New rows also store `audit_body_byte_len` so the canonical row has the
   artifact hash, stored byte size, schema version, and body pointer.
2. Share-segment proof bodies: when `PRISM_AUDIT_SHARE_SEGMENT_SIZE` is
   positive, new external body files use `qbit.prism.audit-bundle.v2`. The v2
   body keeps non-share bundle sections inline, stores a
   `qbit.prism.window-completeness-proof.v1`, and points at stable
   `qbit.prism.audit-share-segment.v1` segment-slot files with per-range hashes.
   This removes the old repeated inline tail/partial-window share arrays while
   preserving the same reconstructed v1 bundle hash.
3. Legacy body refs: older compact files using `qbit.prism.audit-body-ref.v1`
   remain readable. The Rust `qbit-prism-audit-verify` and
   `qbit-prism-audit-canonicalize` tools accept full v1 bundles, legacy body
   refs, and v2 proof bodies, verifying referenced segment/range hashes before
   reconstructing the canonical v1 bundle.

`prism-live-audit-bundle-*.json` files are now small operator envelopes that
point at the canonical body URI. They are not the durable audit body. Retention
may prune live envelopes and old candidates; it must not prune
`prism-audit-bundle-body-*` or `prism-audit-share-segment-*` unless those
artifacts have first been archived and the resolver policy is explicit. Stable
`prism-audit-share-segment-slot-*` files may grow as adjacent ranges are first
referenced, so old body files verify their original range hash against the
selected slice rather than against the whole mutable slot file.

CTV broadcast retries are bounded separately from audit bodies:

- `PRISM_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT` keeps only the newest N detailed
  rows per fanout in `qbit_ctv_fanout_broadcast_attempts`.
- `qbit_ctv_fanout_artifacts` retains total attempt count, per-status counts,
  first/last timestamps, last package txids/hexes, last submit result, last
  error, and retry backoff state.
- `rejected` and `failed` attempts move a fanout to terminal `failed`, and
  terminal failed fanouts are not selected for broadcast work.

## VM Recommendations

For near-term production or a pilot after audit artifact externalization:

- Use at least 2 TB NVMe if audit bodies and backups are externalized.
- Use 4 TB if keeping a local artifact mirror.
- Use 4-8 TB if compact artifact bodies remain local and the pool is expected
  to grow beyond pilot traffic.

For a serious public pool:

- Treat Postgres as a dedicated service or dedicated volume.
- Plan 10 TB or more for hot Postgres at roughly 100 accepted shares/sec.
- Keep immutable audit artifacts and backups in separate object or cold
  storage.
- Treat artifact availability separately from artifact integrity. Hashes prove
  fetched bytes are correct; backups and restore tests prove bytes remain
  available.

## Postgres Durability And Recovery

Postgres always uses write-ahead logging (WAL). WAL is the database transaction
log, not a second application-level share queue: changes are recorded in WAL
before their modified table pages are written. Keep `fsync=on`,
`full_page_writes=on`, and `synchronous_commit=on`. Disabling any of those to
reduce share-accept latency weakens the meaning of a committed share and is not
a production tuning option.

The production Compose contract sets `POSTGRES_INITDB_WALDIR` to the separately
mounted live WAL path. That setting is honored only when the official Postgres
entrypoint creates a fresh cluster. It does not move `pg_wal` for an existing
cluster. Verify the initialized cluster before admitting shares:

```sql
SHOW data_directory;
SELECT pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn());
```

Also verify that `readlink -f /var/lib/postgresql/data/pg_wal` inside the
container resolves to `/var/lib/postgresql/wal` and that the container mount
table maps that target to `PRISM_POSTGRES_WAL_SOURCE`; SQL alone cannot prove
the storage separation. Losing either the live data path or live WAL path makes
the primary unavailable, so snapshotting only one is not a recoverable backup.

A commit on one Postgres primary protects against a coordinator restart, but it
does not by itself protect against loss of that primary and its storage. Choose
the recovery objective explicitly:

- For a non-zero recovery point objective, use encrypted off-host base backups
  plus continuous WAL archiving and document the maximum acceptable data loss.
- For zero loss of acknowledged commits after primary storage failure, add a
  synchronous standby on independent failure-domain storage. Configure
  `synchronous_standby_names` and keep `synchronous_commit=on` so commit waits
  for the selected standby to durably flush WAL. Do not silently fall back to
  asynchronous replication when that guarantee is required.
- Replicas do not replace backups. Operator error, corruption, and accidental
  deletion can replicate immediately, so retain independent point-in-time
  recovery material.

Point-in-time recovery requires a compatible physical base backup and every WAL
segment from that backup through the requested recovery time. Store both
encrypted outside the database host, apply a tested retention policy, and run a
scheduled restore drill into an isolated Postgres instance. A drill is complete
only after schema checks, ledger continuity checks, artifact-pointer checks, and
an application read test pass.

Live WAL and archived WAL are different storage classes. Postgres writes active
segments to `pg_wal`; an archive command copies completed segments to the
independent point-in-time-recovery archive. The separate Compose WAL mount holds
the former, not the latter.

WAL consumes disk according to write volume and checkpoint behavior, not just
accepted-share row size. Healthy archived segments can be recycled from live
WAL; a failed archive command or an inactive replication slot can retain live
WAL without bound and fill its filesystem. Measure WAL generated during a
production-like load test, reserve several times the measured peak between
operator response windows, and alert on:

- time and bytes since the last successful archived WAL segment
- `pg_wal` bytes and growth rate
- replication slot retained bytes and standby replay/flush lag
- base-backup age and the last successful restore drill
- synchronous standby count whenever zero acknowledged-share loss is required

Treat recovery as a release gate: perform at least one full backup, primary-loss
simulation, point-in-time restore, and application verification before accepting
shares with economic value.

## Monitoring

Track at least:

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));

SELECT
  relname,
  n_live_tup::bigint AS est_rows,
  pg_size_pretty(pg_total_relation_size(relid)) AS total
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

SELECT count(*) FROM qbit_share_ledger WHERE accepted;

SELECT
  count(*) AS bundles,
  pg_size_pretty(sum(pg_column_size(audit_bundle))::bigint) AS inline_jsonb
FROM qbit_pool_audit_bundles
WHERE audit_bundle IS NOT NULL;
```

Filesystem checks:

```sh
df -h /
df -h "$PRISM_POSTGRES_DATA_SOURCE" "$PRISM_POSTGRES_WAL_SOURCE" "$PRISM_AUDIT_DATA_SOURCE"
du -sh "$PRISM_POSTGRES_DATA_SOURCE" "$PRISM_POSTGRES_WAL_SOURCE" "$PRISM_AUDIT_DATA_SOURCE"
du -sh /var/lib/docker /var/lib/containerd
docker system df
docker volume ls
```

Add alerts for:

- hot Postgres disk over 70% and 85%
- WAL or backup archive growth exceeding expected daily ingest
- audit artifact storage growth exceeding the block/artifact model
- `qbit_pool_audit_bundles` rows whose inline body remains large after artifact
  externalization lands
- missing or hash-mismatched artifact objects
- `/metrics` gauges `qbit_prism_audit_artifact_bytes` and
  `qbit_prism_audit_artifact_files`, split by body, share segment, live bundle,
  candidate, and other artifact kinds
- CTV retry pressure via `qbit_prism_ctv_fanouts_failed` and the fanout-row
  broadcast summary fields
