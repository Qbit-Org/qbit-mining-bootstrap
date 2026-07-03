# PRISM Storage and VM Sizing

This document sizes the PRISM pool storage footprint for operator planning. It
separates hot Postgres, audit artifacts, qbit/Bitcoin chain data, Docker
overhead, WAL, and backups because those are different capacity problems.

## Scope

PRISM stores permanent accounting data in Postgres and writes public audit
artifacts under `PRISM_AUDIT_DIR`.

Compose mounts:

- `prism-postgres-data` at `/var/lib/postgresql/data`
- `prism-audit-data` at `/var/lib/qbit-prism/audit`
- `qbit-data` at `/var/lib/qbit`

The current production storage target is:

- keep the canonical accepted-share ledger in Postgres, or in a verified
  immutable archive when a future archive flow exists
- keep block, payout, carry-forward, maturity, reorg, and settlement state
  durable
- keep canonical audit artifact hashes in Postgres
- keep large exact audit artifact bodies outside hot Postgres once the artifact
  externalization work lands

## Live Testnet4 Baseline

Measured on `qbit-pool-testnet4` on 2026-06-30:

| Item | Size / count |
| --- | ---: |
| Root disk | 234 GB total, 116 GB used, 106 GB free |
| `/var/lib/docker` | 69 GB |
| `/var/lib/containerd` | 34 GB |
| Active PRISM Postgres volume | 195 MB |
| Active PRISM database size | 140 MB |
| Active PRISM audit file volume | 989 MB |
| Active qbit data volume | 61 MB |
| Active PRISM total | about 1.2 GB |

Largest non-PRISM or legacy consumers on the same host:

| Item | Size |
| --- | ---: |
| old `qbit-mining-testnet4_tides-audit-data` | 41 GB |
| old `qbit-mining-testnet4_tides-postgres-data` | 5.4 GB |
| `qbit-mining-testnet4_bitcoin-data` | 14 GB |
| Docker images | 34.4 GB |
| Docker build cache | 16.8 GB |

Active PRISM table breakdown:

| Table | Size / rows |
| --- | ---: |
| `qbit_pool_audit_bundles` | 126 MB, 571 rows |
| `qbit_share_ledger` | 3.7 MB, 5,624 accepted shares |
| payout, carry-forward, and block tables | about 2.5 MB combined |

The accepted-share ledger is not the current testnet4 disk driver. Inline audit
bundle bodies dominate the hot database footprint.

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

The measured share-ledger cost is about 0.7 to 0.8 KB per accepted share on the
live testnet4 data and local schema probes. Use 1.2 KB per share for planning
to cover indexes, bloat, and operational headroom.

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
  summary, attempt count, and final artifacts are retained
- Docker images, Docker build cache, and old non-active TIDES volumes after
  backup or decommission confirmation

Inactive PRISM blocks are not deletion candidates simply because they are
inactive. They can reactivate after a reorg.

## Artifact Storage Strategy

There are two distinct optimization stages:

1. Exact artifact externalization: store the current full
   `qbit.prism.audit-bundle.v1` body in immutable artifact storage by content
   hash, and keep hash, size, schema, and pointer metadata in Postgres. This
   preserves current verifier semantics.
2. Reduced/window proof artifacts: introduce a new proof schema that verifies a
   TIDES reward window against retained ledger checkpoints or archive roots.
   This is a verifier-contract change and must prove window completeness,
   including the oldest partial-share boundary.

Stage 1 is the safe first production step. Stage 2 should be designed and
landed separately.

## VM Recommendations

For the current testnet4 host:

- The 234 GB root disk is enough for active PRISM itself.
- It is not roomy once old TIDES volumes, Docker image/build cache, Bitcoin
  data, and operational headroom are included.
- Clean old TIDES volumes/build cache or move to a larger disk before prolonged
  public testing.

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
