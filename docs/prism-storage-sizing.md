# PRISM Storage and Resource Planning

Native Prism keeps the canonical ledger, block accounting, audit metadata,
normalized audit snapshots, and CTV recovery state in PostgreSQL. Multiple
frontends use this shared database; new accepted-block audits do not require a
shared local filesystem.

## What is retained

| Data | Storage and lifetime |
| --- | --- |
| Accepted shares | Immutable PostgreSQL rows, never updated, deleted or truncated. Online while the share is inside the online horizon, then archived outside PostgreSQL one whole partition at a time and the partition dropped |
| Proof identity (`qbit_prism_share_hashes`) | Permanent immutable PostgreSQL rows, one per share, about 150 B each. Not partitioned and never removed: it is the global `share_id` authority the append path consults first |
| Blocks, payouts, carry-forward, maturity/reorg state | Permanent PostgreSQL accounting history |
| Native audit share snapshots | Permanent range/count/anchor/digest metadata referencing immutable shares |
| Native audit bodies | Non-share JSON plus snapshot reference in PostgreSQL: the logical bundle minus its top-level `shares` and minus `reward_manifest.shares`, both rebuilt on read. Rows landed before 3.x.x #267 still hold `reward_manifest.shares` and stay readable as they are. Before the shares a body depends on are archived, the body is sealed: its full canonical artifact is stored in `canonical_audit_bytes` and served from there instead of being rebuilt, which costs the artifact size back per archived block, about 22 MB at a 20,000-share window and about 111 MB at 100,000 (`audit_body_byte_len` in the table below), TOAST-compressed in the column |
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
accepted-block audit.

Retention of that history is detach-and-archive, never deletion. There is no
row-level prune or share-compaction command, and no `DELETE` runs on the
ledger. `qbit-prism-server share-archive` retires a whole partition once no
online reader can still need it: seal the audits that depend on it, write the
partition to an archive outside PostgreSQL, verify the archive against the live
rows, detach, and only then drop the detached relation, with the archive as the
copy of record from then on. Size and retain that archive as accounting data,
not as a cache. The procedure, the online horizon it checks and the archive
format are in
[ledger operations](prism-ledger-ops.md#share-ledger-partitions-and-retention).

### Archived share partitions

A partition at the default width holds 2^24 = 16,777,216 rows, about 7.5 GB of
heap plus about 20 GB of indexes at the production row shape. Archiving it
writes newline-delimited canonical JSON, one object per ledger row with every
column, gzipped. Uncompressed that JSON is larger than the heap it came from,
since numerics and timestamps become decimal strings and the program becomes
hex; gzip on rows of this shape should recover roughly 5:1. Treat that as an
estimate to be replaced by a measurement, not a planning number: the manifest
of the first real `archive` run records `rows_bytes` and `rows_gz_bytes` and
gives the exact ratio for this pool's row shape.

The archive is an additional copy, not a saving on the database volume until
the partition is dropped. Budget both for the interval between `archive` and
`drop`, plus whatever the archive retention policy keeps off-host.

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
request, no contention); budget CPU and the public read deadline for it.

### Memory bound on artifact reads

`/public/v1/artifacts/<sha256>` holds a whole window in memory per read,
whichever shape the row has: an unsealed native block is rebuilt from its share
range, and a sealed one has its stored canonical bytes digested and parsed.
Since #267's admission half, both run under
`PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY` (default 1), which is independent of
`PRISM_POSTGRES_READ_CONCURRENCY`: raising the read concurrency admits more
ordinary reads and **no** more window-sized reads *of this route*, and its
transient memory is

```text
artifact_read_bytes = PRISM_PUBLIC_AUDIT_REBUILD_CONCURRENCY x peak_bytes_per_read
```

That bound is the artifact route's alone. `/public/v1/blocks/<hash>/settlement-artifacts`
falls back to the audit-bundle reader when the block has no CTV fanout set, and
that reader still rebuilds or decodes the same window under the shared
imported-audit decode limit, which is sized from `PRISM_POSTGRES_READ_CONCURRENCY`.
Its bound is therefore `PRISM_POSTGRES_READ_CONCURRENCY x peak_bytes_per_read`,
and raising the read concurrency does raise it; size a public process for both.
Moving that route under the artifact limit is a behaviour change on another
route and is not part of #267's admission half.

What the bound covers is the work admitted under the permit: the range read and
fold, the canonical serialization, and the stored bytes' digest and parse. It
does not cover the response body, which stays alive after the permit is
released until the client has drained it, so a read's resident footprint
outlasts its permit by about one more copy of the artifact.

CPU outside the permit matters as much as memory. Serving an audit artifact used
to hash the whole body twice more on a runtime worker thread — once to check the
returned bytes against the requested address, once for the ETag. One SHA-256
pass over a 111 MB artifact measures 1.6-1.8 s in a debug build, so at 50
requests per second those passes saturated the public process's worker threads:
a task that only sleeps overshot by 23.5 s, the node RPC behind
`/public/v1/pool-summary` timed out, and that route failed its 20 s deadline
even though the read pool was idle (p99 pool-summary latency 0.107 s in the same
run). Both passes are gone. The bytes are digested against `audit_bundle_sha256`
inside the blocking job, under the permit; the row is found by that column, so
the route compares it to the requested address as a string; and the ETag is that
same address rather than a fresh hash of the body. The verification is the same
digest it always was, and responses are byte-identical. After the change, the
same load holds pool-summary at 0.118-0.364 s maximum with no failed sample and
the sleeping probe's overshoot at 0.32-0.44 s. A public process still needs CPU
headroom for the rebuild itself, which is what the permit bounds.

The legacy fallback body — an audit row with neither canonical bytes nor a share
snapshot, served through the audit-bundle reader — is still hashed on a runtime
thread for its ETag, because it has no proven content address to carry. It is a
pre-#267 import shape, and it is not admitted by the rebuild limit either.

`peak_bytes_per_read` is a multiple of the canonical artifact length, which is
`audit_body_byte_len` in the table above. Measured at a 5,000-share window by
`cargo test -p qbit-prism-server --test artifact_admission -- --nocapture
artifact_route_load_keeps_pool_summary_within_its_read_deadline` (debug build,
process peak resident memory across one uncontended read, `VmHWM` reset before
each): the canonical artifact was 5,570,372 B (**1,114 B per share**, measured),
one rebuild peaked about **34 MiB** above the resident baseline (about 6x the
artifact) and one sealed decode about **11 MiB** (about 2x). Those multiples are
what the bound below extrapolates; they are measured only at that window and in
a debug build, and the test process also holds the response body.

Derived, not measured, at the sizes this pool plans for: at 1,114 B per share a
400,000-share window is a **445 MB** canonical artifact, so one rebuild is on
the order of **2.5 GB** of transient memory and one sealed decode on the order
of **1 GB**, times the rebuild concurrency. No measurement at 400,000 shares
exists; run the `#[ignore]` variant
(`artifact_route_load_at_a_large_window`, `PRISM_ARTIFACT_ADMISSION_SHARES`) on
a machine with that headroom before raising the setting. The default of 1 keeps
one window in flight per public process; the in-flight cap
(`PRISM_PUBLIC_AUDIT_ARTIFACT_MAX_IN_FLIGHT`, default 32) bounds only how many
requests may queue for it, not memory, because a queued request holds neither a
read connection nor a window.

Legacy external body refs and v2 segment files remain readable by the offline
Rust tools. During [migration](prism-rust-migration.md), import these into the
shared database before relying on another frontend to serve old audits. Preserve
all referenced bodies and segments in the migration backup. A small live-evidence
envelope or a stored SHA cannot replace missing artifact bytes.

### Refresh build memory per frontend

Since #502 the refresh build serializes the share array and folds the counted
shares in chunks on the frontend's builder pool (`PRISM_REFRESH_BUILD_THREADS`,
default three quarters of the host's cores clamped to 1..4, `0` for the serial
build, at most 64), and it holds more memory per frontend process than the
serial build did. The trade is accepted. #502's own confirmation pair on its
merge base measured per-frontend RSS after the tips at 328-342 MiB before and
568-608 MiB after, and peak RSS at 557 MiB before and 669 MiB after; an
independent 400,000-share probe measured 251-254 MiB retained after the drop
with the serial build against 284-330 MiB with eight workers. Budget roughly
0.5 GiB more retained memory on a two-frontend host.

Two things hold that memory. The pool's threads keep the counted-share strings
and chunk buffers in their own allocator arenas, one per thread, so the retained
part grows with the thread count (at 400,000 shares, sixteen threads retained
about 130 MiB more per frontend than four for the same refresh time); the
thread count is the knob that bounds it, and on a memory-tight host
`MALLOC_ARENA_MAX` (for example `MALLOC_ARENA_MAX=2` in the frontend's
environment) caps glibc's arenas whatever the thread count. And the build slot
(`PRISM_JOB_BUILD_EXECUTOR_WORKERS`) is released before the previous refresh's counted-share
body is freed, so the slot count no longer bounds the number of live
counted-share bodies exactly: expect one extra body per slot, transiently, on
top of the bound the slot gives.

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
settlement evidence as routine cleanup. Aged shares leave through
`share-archive` and its checks, one partition at a time, and never through a
`DELETE`. Track storage growth and restore time against the measured ingest
model rather than relying on the former Python pilot's repeated-audit-file
footprint.
