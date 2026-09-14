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
| Native audit bodies | Non-share JSON plus snapshot reference in PostgreSQL: the logical bundle minus its top-level `shares` and minus `reward_manifest.shares`, both rebuilt on read. Rows landed before 3.x.x #267 still hold `reward_manifest.shares` and stay readable as they are |
| Imported legacy audits | Digest-checked canonical bytes (bytea) plus non-share metadata in PostgreSQL; an inline body survives only on inline-only rows; original backup retained |
| CTV manifests, transactions, outcomes | Durable PostgreSQL recovery and audit state |
| CPFP funding reservations and signed child packages | Durable recovery records; retain until reconciled |
| Pending block candidates and deferred credit | Durable until resolved; retained identity/status afterward |
| Jobs and prepared snapshots | Expiring shared records for reconnect/job recovery |
| Per-process HTTP caches and socket state | Reconstructible memory |
| qbit/Bitcoin chain data | Separate node storage, sized independently |

Accepted ledger rows cannot be updated, deleted, or truncated. Native audits
reconstruct their exact share slice from those rows, rebuild the counted-share
window of `reward_manifest` from that slice (the stored header must match the
rebuilt one field for field), and verify the snapshot digest and the canonical
bundle hash of the whole body. Overlapping windows therefore share ledger
storage instead of embedding another copy of every share in every
accepted-block audit. There is no supported archive/prune command for this
immutable history.

### Native audit body size

Measured with `cargo test -p qbit-prism-server --test audit_body_normalization
-- --ignored measure_landing_body_and_settlement_lock_hold` (release build, one
landed block, the synthetic five-recipient window of
`tests/support/window_fixture.rs` at about 581 B per share):

| Window (counted shares) | Stored `audit_bundle` as JSON text | Stored on disk, `pg_column_size` (TOAST-compressed) | `audit_body_byte_len` (full canonical artifact) |
| ---: | ---: | ---: | ---: |
| 20,000, landed before #267 | 10,901,855 B | 353,091 B | 22,250,397 B |
| 20,000, landed since #267 | 12,949 B | 3,087 B | 22,250,397 B |
| 100,000, landed before #267 | 54,401,879 B | 1,707,134 B | 111,190,422 B |
| 100,000, landed since #267 | 12,972 B | 3,088 B | 111,190,422 B |

Before #267 the stored body kept `reward_manifest.shares`, one record per
counted share, so it grew by about 545 B of JSON text per share: the production
window of 372,257 shares measured about 235 MB as one JSONB value (issue #267),
88% of PostgreSQL's 268,435,455-byte limit, and the reduced-size JSONB ceiling
gate projected 233 MB at 400,000 synthetic shares. The body landed since #267
does not grow with the window. Its residual size is O(distinct miners and payout accounts),
not a fixed bound: `reward_manifest.entitlements`, the payout policy manifest's
accounts, `prior_balances`, and the CTV fanout manifests scale with the number
of distinct payout programs, and the synthetic window above has five. A pool
paying thousands of distinct programs stores proportionally more per block.
The synthetic window also compresses about 27:1 in TOAST, so the on-disk column
is optimistic for production data; the JSON text column is not.

Two lengths describe one native audit and neither is derivable from the other:
`audit_body_byte_len` is the length of the full canonical artifact, both share
copies included, exactly as `/public/v1/artifacts/<sha256>` serves it;
`sum(pg_column_size(audit_bundle))` (the storage query below) is the stored,
compressed length of the non-share body.

Serving a native audit rebuilds the window on every request: the range read,
the counted-window fold, and the canonical hash. The same harness measured that
read at 1.0 s for 20,000 and 5.4 s for 100,000 shares (release build, one
request, no contention); budget CPU and the public read deadline for it, and
for the concurrency limit those decodes share with imported audits.

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
expensive on a large ledger. `stored_audit_json_bytes` is the compressed
non-share body; the full logical audit each native row serves is
`audit_body_byte_len` bytes, orders of magnitude larger, and
`sum(audit_body_byte_len)` is the size of the artifacts the public route can be
asked for, not of anything stored.

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
