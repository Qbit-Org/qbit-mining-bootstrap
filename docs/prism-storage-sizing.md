# PRISM Storage and Resource Planning

Native Prism keeps the canonical ledger, block accounting, audit metadata,
normalized audit snapshots, and CTV recovery state in PostgreSQL. Multiple
frontends use this shared database; new accepted-block audits do not require a
shared local filesystem.

## What is retained

| Data | Storage and lifetime |
| --- | --- |
| Accepted shares and proof identity | Permanent immutable PostgreSQL rows |
| Blocks, payouts, carry-forward, maturity/reorg state | Permanent PostgreSQL accounting history |
| Native audit share snapshots | Permanent range/count/anchor/digest metadata referencing immutable shares |
| Native audit bodies | Non-share JSON plus snapshot reference in PostgreSQL |
| Imported legacy audits | Digest-checked canonical bytes (bytea) plus non-share metadata in PostgreSQL; an inline body survives only on inline-only rows; original backup retained |
| CTV manifests, transactions, outcomes | Durable PostgreSQL recovery and audit state |
| CPFP funding reservations and signed child packages | Durable recovery records; retain until reconciled |
| Pending block candidates and deferred credit | Durable until resolved; retained identity/status afterward |
| Jobs and prepared snapshots | Expiring shared records for reconnect/job recovery |
| Per-process HTTP caches and socket state | Reconstructible memory |
| qbit/Bitcoin chain data | Separate node storage, sized independently |

Accepted ledger rows cannot be updated, deleted, or truncated. Native audits
reconstruct their exact share slice from those rows and verify the digest and
canonical bundle hash. Overlapping windows therefore share ledger storage
instead of embedding another copy of every share in every accepted-block audit.
There is no supported archive/prune command for this immutable history.

Legacy external body refs and v2 segment files remain readable by the offline
Rust tools. During [migration](prism-rust-migration.md), import these into the
shared database before relying on another frontend to serve old audits. Preserve
all referenced bodies and segments in the migration backup. A small live-evidence
envelope or a stored SHA cannot replace missing artifact bytes.

## Estimate from measured ingest

Estimate share growth from accepted share rate, not miner hashrate alone:

```text
accepted_shares_per_second ≈ active_workers / target_share_interval_seconds
accepted_shares_per_year = accepted_shares_per_second × 31,536,000
annual_ledger_bytes = accepted_shares_per_year × measured_bytes_per_share
```

At a 15-second target, 150 active workers produce about ten accepted shares per
second. Vardiff bounds, hashrate changes, reconnects, and listener profiles can
change the actual rate; measure it under the intended configuration.

The following is arithmetic using a **planning assumption** of 1.2 kB per
accepted share including indexes. It is not a measured native capacity claim:

| Accepted shares/second | Shares/year | Ledger bytes/year at 1,200 bytes/share |
| ---: | ---: | ---: |
| 1 | 31.5 million | 37.8 GB |
| 10 | 315 million | 378 GB |
| 100 | 3.15 billion | 3.78 TB |
| 500 | 15.8 billion | 18.9 TB |

Native proof deduplication adds indexed rows; measure its footprint alongside
the ledger. Non-share audit sections, per-recipient payouts/carry rows, CTV
transactions, and imported legacy inline bodies add independent growth. The
number of recipients and pool-found blocks can matter as much as the share rate.
Do not multiply just one table's row size and call it the database requirement.

Measure a representative interval after migration and under load. Record table
and index growth, WAL generation, pending outbox size, expiring job storage,
backup size, and process peak RSS. Include cold startup and simultaneous job
refresh across all frontends. Large historical reward windows still require
memory and CPU to construct or export a full logical audit, even though their
persistent shares are normalized.

Provision headroom for peak ingest, autovacuum, index growth, restore staging,
backups, and the time needed to respond to disk alerts. Estimate replicas and
backup retention separately; they are additional copies, not free capacity.

## Local and external database deployments

Local development Compose uses:

- `prism-postgres-data` for PostgreSQL data
- `prism-postgres-wal` for live WAL
- `prism-audit-data` for legacy artifact access/import
- `qbit-data` for the qbit node

Production Compose maps these to pre-created absolute paths through
`PRISM_POSTGRES_DATA_SOURCE`, `PRISM_POSTGRES_WAL_SOURCE`,
`PRISM_AUDIT_DATA_SOURCE`, and `QBIT_DATA_SOURCE`. The audit mount remains useful
for migration and archived operator artifacts; the native runtime does not
write repeated live audit bodies there for every block.

For several physical frontends, use the external-database Compose overlay and
one HA writer endpoint. Size the external database's data, WAL, replicas, and
backup archive according to its deployment profile. Frontend hosts primarily
need CPU, memory, logs, image storage, and any locally managed qbit chain data.
A separate qbit node per frontend has its own chain-storage requirement.
CTV recovery without transaction indexing needs retained historical blocks for
its durable bounded scan; account for that when choosing a node pruning policy.

The native runtime uses `PRISM_RUNTIME_WORKERS` and a bounded
`PRISM_JOB_BUILD_EXECUTOR_WORKERS` CPU builder limit. Increasing threads cannot
remove PostgreSQL commit or storage bottlenecks. Sum
`PRISM_DATABASE_MAX_CONNECTIONS` across frontends before setting the database
connection budget; reserve operational connections as well.

## Durability, WAL, and recovery

Keep `fsync=on`, `full_page_writes=on`, and `synchronous_commit=on`. WAL is the
PostgreSQL transaction log, not a second application share queue. Durable share
ACKs depend on successful commit; disabling these settings to improve benchmark
numbers changes that guarantee.

The production local database sets `POSTGRES_INITDB_WALDIR` to the separately
mounted WAL path. This setting applies only when creating a fresh cluster and
does not relocate an existing `pg_wal`. Inspect the actual filesystem link and
mounts after initialization as well as the SQL settings:

```sql
SHOW data_directory;
SHOW fsync;
SHOW full_page_writes;
SHOW synchronous_commit;
SHOW synchronous_standby_names;
SELECT pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn());
```

A primary-local durable commit survives a frontend crash. To preserve every
acknowledged share after loss of the database primary, configure synchronous
replication on independent storage and fail over only to a standby containing
those flushed commits. An asynchronous standby or a highly available DNS name
does not establish this recovery point objective.

Keep encrypted off-host base backups plus continuous WAL archives and conduct
isolated restore drills. Replicas do not replace recovery history: accidental
changes and corruption can replicate. A restore drill should verify schema,
share order, representative canonical audit hashes, carry-forward integrity,
CTV state, and application reads. Include unimported external audit artifacts
and signing-key recovery material in the recovery plan.

Live WAL and archived WAL are distinct. Failed archiving or an inactive
replication slot can retain live WAL without bound. Measure peak WAL generation
and reserve enough capacity for the incident response interval. Alert on live
WAL growth, retained slot bytes, archive failures, standby flush/replay lag,
synchronous standby availability, backup age, and the last successful restore.

## Read-only storage inspection

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));

SELECT relname, n_live_tup::bigint AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_with_indexes
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

SELECT count(*) AS audit_count,
       count(*) FILTER (WHERE share_snapshot_sha256 IS NOT NULL) AS native_references,
       count(*) FILTER (WHERE audit_bundle IS NULL AND body_uri IS NOT NULL) AS external_bodies,
       coalesce(sum(pg_column_size(audit_bundle)), 0) AS stored_audit_json_bytes
FROM qbit_pool_audit_bundles;

SELECT count(*) AS snapshots,
       coalesce(sum(share_count), 0) AS referenced_shares_with_reuse
FROM qbit_prism_audit_snapshots;

SELECT slot_name, active,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```

`referenced_shares_with_reuse` counts references across snapshots, not unique
stored share rows. `n_live_tup` is an estimate and can lag; exact counts can be
expensive on a large ledger. Full logical audit response size is also different
from stored non-share JSON size.

Inspect data/WAL/legacy artifact filesystems and container image/log usage:

```sh
df -h "$PRISM_POSTGRES_DATA_SOURCE" "$PRISM_POSTGRES_WAL_SOURCE" "$PRISM_AUDIT_DATA_SOURCE"
docker system df
```

Use explicit retention for logs, expired operational records, and image caches.
Do not delete inactive pool blocks, referenced shares, snapshots, or canonical
settlement evidence as routine cleanup. Track storage growth and restore time
against the measured ingest model rather than relying on the former Python
pilot's repeated-audit-file footprint.
