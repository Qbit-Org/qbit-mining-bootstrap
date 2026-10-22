# Share ledger partitioning and retention (design record, #144)

This record resolves decision **D6** of #260 (retention and archival of
shares and audit bodies) and specifies what migrations 016 and 017, the
`share-archive` operator command and the reader changes of #144 do. The
first slice, the index trim (#153, migration 013), is recorded in
[prism-ledger-ops.md](prism-ledger-ops.md#share-ledger-indexes). The
migration mechanics are in
[prism-rust-migration.md](prism-rust-migration.md#migration-017-the-share-ledger-partition-conversion-applied-online),
the operator procedure in
[prism-ledger-ops.md](prism-ledger-ops.md#share-ledger-partitions-and-retention).

## Why

`qbit_share_ledger` is one row per accepted share and nothing ever left it.
Every insert maintained the primary key, a global `UNIQUE (share_id)` and
four secondary indexes at ever-growing depth; vacuum, base backups, restore
drills and cache hit rates all degraded with lifetime share count. What
reads the table is narrow: the payout window (newest slice, difficulty
bounded), the audit range of each landed block, dashboard aggregates over
the last 24 hours, and a handful of probes by `share_id` seconds after the
share was written. Nothing needs an aged share row online, except the
audit of a block whose window it belongs to, and since #267 that audit is
rebuilt from the ledger at read time.

## Decision D6

**Online horizon.** A share row stays online while any of these holds:

1. it can still be in a payout window: its `share_seq` is at or above the
   floor of the current window taken at four times the requested weight
   (`qbit_prism_window(clock_timestamp(), 8 * D * 4)`, D the network
   difficulty the operator supplies), so a difficulty rise of up to 4x
   between two retention runs cannot reach into archived history;
2. it is younger than the retention age (default 30 days), which covers
   every dashboard read of raw rows (the longest is 24 hours) with margin;
3. the hashrate rollup watermark (`qbit_hashrate_rollup_progress`) has not
   passed it, so the permanent rollup tables hold its contribution;
4. a landed block's audit still depends on it: an audit row whose share
   snapshot intersects the partition and that has no stored
   `canonical_audit_bytes` (see the next point);
5. an unfinished block candidate or deferred share references it.

Everything above is a property of a whole partition, checked by
`share-archive plan`, and a partition leaves only when all five clear.

**Archived blocks are served from stored canonical bytes (option 2 of the
D6 discussion).** Before a partition is detached, every audit row whose
snapshot intersects it is *sealed*: the canonical artifact is rebuilt from
the still-online shares, its digest checked against the advertised
`audit_bundle_sha256`, and the bytes stored in `canonical_audit_bytes`,
exactly as imported legacy audits are stored since #325. Readers prefer
stored bytes over reconstruction whatever the row's shape, so the artifact
route, the audit bundle route and the operator tools keep serving the
block with its advertised digest after its shares are gone. The price is
the artifact size back per archived block (about 111 MB uncompressed at
100,000 shares, TOAST-compressed in the column); the alternative, pinning
every landed block's window online forever, would have left cost scaling
with blocks found, against the point of this work. Sealing is idempotent
and can run ahead of any detach. The snapshot metadata row
(`qbit_prism_audit_snapshots`) stays: it records the range and digest the
bytes were proved against.

**Archive format.** A detached partition is written, before it is
detached, as newline-delimited canonical JSON rows (every column of the
table, exact timestamps in microseconds) compressed with gzip, plus a
manifest carrying the bounds, counts, digests and a hash-chain link to the
previous archived partition. The format is specified
[below](#archive-format-v1). A restore recreates the partition table from
the archive, verifies count and digest, and can attach it back for
inspection or service.

**Retention is detach-and-archive, never delete.** No `DELETE` runs on the
ledger; the immutability triggers stay on the parent and on every leaf.
Space is reclaimed by `DETACH PARTITION ... CONCURRENTLY` followed, after
the archive has been re-read and verified against the live rows, by
`DROP TABLE` of the detached table, which reads the archive back once more
first. The archive is the copy of record from
then on, and the catalog row (`qbit_prism_share_partitions`) keeps the
bounds, the archive location, the digests and the timestamps of each step.

## The shape

- **`RANGE (share_seq)`.** `share_seq` is the routing key every ordered
  read already carries, `bigserial` routing cannot fail, and `PRIMARY KEY
  (share_seq)` survives on the parent. No DEFAULT partition: a missing
  partition would otherwise silently collect rows that a later ATTACH
  would have to move.
- **A grid of cells.** Partition width is `partition_rows` in
  `qbit_prism_share_partitioning` (default 2^24 = 16,777,216 rows; about
  7.5 GB of heap and 20 GB of indexes at the production row shape, five
  days at 39 shares/s, nine hours at 500 shares/s). Each new partition is
  one width wide and starts where the highest attached one ends, so with
  the width unchanged cell k covers `[k*rows, (k+1)*rows)`. Names are
  `qbit_share_ledger_p<n>`, n from a counter over every name the catalog
  has recorded (`qbit_prism_share_partition_next_number()`), so a name is
  never reused and never depends on the width. Changing the width affects
  partitions created afterwards. The release table becomes
  `qbit_share_ledger_p0`, `[MINVALUE, bound)`, spanning as many cells as
  it needs.
- **Lead partitions.** `qbit_prism_share_partition_ensure()` keeps
  `lead_partitions` (default 4, about 67 M rows) attached above the next
  `share_seq`. The server calls it at startup and every
  `PRISM_SHARE_PARTITION_ENSURE_INTERVAL_SECONDS` (default 60) from every
  instance; the call is serialized per schema and creates nothing when the
  lead is intact. A new partition is a standalone table `LIKE` the parent
  with a validated bound `CHECK`, its own `UNIQUE (share_id)` and the
  immutability trigger, then attached (SHARE UPDATE EXCLUSIVE on the
  parent: appends and reads continue). Should an insert ever find no
  partition (SQLSTATE 23514), the append path runs `ensure` once and
  retries the share.
- **Per-leaf indexes** are the release set after 013: the primary key,
  `accepted_recent_idx`, `accepted_block_suffix_idx`,
  `accepted_seq_walk_idx`, `accepted_miner_history_idx`, plus the leaf's
  `UNIQUE (share_id)`. Each leaf's indexes are cache-resident while it is
  hot and are never touched again once it is not.
- **The catalog.** `qbit_prism_share_partitioning` (one row: width, lead,
  the conversion bound and timestamp) and `qbit_prism_share_partitions`
  (per partition: bounds, `attached` / `detached` / `dropped`, sealed,
  archived, verified, detached and dropped timestamps, archive URI and
  digests). PostgreSQL's `pg_inherits` is the authority on what is
  attached; the catalog records what happened to a partition afterwards
  and is cross-checked by every command.

## `share_id` uniqueness after partitioning

A unique index on a partitioned table must include the partition key, so
the global `UNIQUE (share_id)` cannot exist on the parent. Three tiers
replace it:

1. **`qbit_prism_share_hashes` is the global authority.** The native
   append already writes one row per share there, keyed by the header
   hash with `UNIQUE (share_id)`, in the same transaction as the ledger
   row. It is not partitioned, its rows are never removed, and it stays
   O(1) per share. The append now consults it *first*: a header hash it
   holds under another `share_id` is the cross-identity duplicate, refused
   as before. It then probes the ledger for the `share_id` with the bound
   of tier 3 (at most three leaves), which finds an exact replay or a
   payload mismatch of any recent row, including rows written around the
   append path, which the hash table does not know. Only when the hash
   table holds the header under this `share_id` and the bounded probe
   misses is the probe repeated without the bound, so a replay of any
   online row is still matched exactly; a replay whose row has left the
   online ledger is refused as the duplicate it is. The coordinator's
   replays are seconds old, so that last case is unreachable in practice
   and safe if reached.
   Rejected rows reserve their exact IDs separately in
   `qbit_prism_rejected_share_ids`, without reserving their header hashes.
   Verification records those IDs and sequences atomically with
   `archive_verified_at`, before detach is permitted; fresh imports register
   them atomically with attachment. These records survive detach and drop.
   Append uses the retained sequence to compare an online rejected row and
   refuses its ID when the row is archived. Unregistered legacy rejected rows
   remain covered by a lookup below the conversion bound.
2. **Per-leaf `UNIQUE (share_id)`** on every partition, created explicitly
   by the partition procedure (a `PARTITION OF` table would get none).
3. **Bounded probes.** Every `share_id` lookup on the ledger (the replay
   comparison, the block-only reconciliation probes, the vardiff evidence
   lookup) carries
   `share_seq >= qbit_prism_share_probe_floor()`: the lower bound of the
   attached partition two below the one the next `share_seq` lands in,
   read from the catalog, so PostgreSQL prunes to at most three leaves
   holding rows (plus the empty lead) at executor start (verified:
   "Subplans Removed" in the plan) instead of descending one `share_id`
   index per attached partition. The floor follows the bounds actually
   attached, not `partition_rows`: subtracting two current widths from
   the sequence would span every narrower partition still attached once
   the width was raised. A probe without the bound costs one index
   descent per partition per share, which the design spike measured at
   31 buffers against 8; the two lookups that fall back to an unbounded
   probe on a miss (the credited replay above, and the vardiff evidence
   lookup) do so only for a row older than the floor, at least two full
   partitions of rows, off the share path.

The invariant this leaves is stated plainly: a row inserted *around* the
append path (a direct `INSERT` by an operator or a harness) with a
`share_id` already present in another partition is not refused by
PostgreSQL. The archive verification and the `plan` command report such
duplicates across attached partitions; the native writers cannot create
one.

The two inbound foreign keys onto `share_id` (the 001 outbox key and the
002 `qbit_prism_share_hashes` key) are dropped by 016: a foreign key onto a
partitioned parent's non-key column cannot exist, and PostgreSQL
re-validates inbound keys at DETACH, so a single retained outbox row would
pin its block's partition forever. Rule from here on: **no object may
carry a foreign key to `qbit_share_ledger`**.

## The conversion (migration 017)

Shape 2 of the design spike, priced by prototype and measured again here:
the release table is attached as the first partition, nothing is copied,
no index is rebuilt.

1. **Prepare.** `CHECK (share_seq < bound) NOT VALID` on the release
   table, `bound` the first grid boundary at least two partition widths
   above the next `share_seq` (33.5 M rows: nineteen hours at 500
   shares/s, ten days at 39). The constraint is enforced on new rows at
   once. The sequence is never restarted: rows keep landing in the release
   table until it reaches the bound, then in the next cell, so `share_seq`
   stays gap-free. Every name the swap takes is checked before this step.
2. **Validate.** `ALTER TABLE ... VALIDATE CONSTRAINT`: one scan, SHARE
   UPDATE EXCLUSIVE, appends and reads continue; hours on the production
   ledger. If a run is interrupted and resumed after the sequence has come
   within one width of the bound, the bound is moved out and validated
   again.
3. **Swap.** One transaction: rename the table and its six indexes to
   their `_p0` names; create the parent `LIKE` the release table (same
   columns, defaults, `CHECK` constraints under the same names, which is
   what ATTACH matches on), give it the primary key and the four secondary
   indexes under the release names, move the sequence's ownership and the
   grants; `ATTACH PARTITION ... FROM (MINVALUE) TO (bound)`, which proves
   the bound from the validated constraint and adopts every existing index
   (the function refuses if any index was built instead); recreate
   `qbit_shares_since_template_height`, the one release function bound to
   the old row type; install the immutability trigger on the parent;
   record the catalog; create the lead partitions. Measured on the
   container: 16 ms at 5,000 rows and none of its terms is a function of
   the row count. The ACCESS EXCLUSIVE lock is requested with a two-second
   `lock_timeout` and retried for up to ten minutes, so a long read delays
   the swap without holding the appends queued behind the request.

Where the ledger is empty (a fresh deployment, an empty 2.x.x source) the
three steps run inside the migration transaction under the cutover locks;
a fresh deployment's first partition is one cell. Everywhere else they run
after the commit on a dedicated connection, resumable from whatever stage
the database is in, and 16 is recorded last. Until then every start
refuses the database.

**One-way, per D5.** The revert is a second cutover: it must rebuild a
global `UNIQUE (share_id)` over the whole table under ACCESS EXCLUSIVE
(43 s at 5.5 M rows in the spike, proportional to the table), and it fails
at the end on the first duplicate `share_id` accepted across two leaves.
No revert script ships; recovery is the isolated-restore reconciliation of
#287.

## Every native reader, and the partition key

| Reader | Predicate | After 017 |
| --- | --- | --- |
| `Ledger::snapshot` cutoff, `max(share_seq)` | ordered walk | ordered append, newest leaf first, stops at the first row |
| `Ledger::snapshot` page walk, `qbit_prism_window` pages, `qbit_audit_share_window` | `share_seq < cursor ORDER BY share_seq DESC LIMIT 4096` | pruned by the cursor; older leaves never executed |
| landing durable-range proof, `read_range`, `read_range_paged`, `probe_share_rows`, `durable_range_exists`, the landing count | `share_seq` range | pruned to the leaves the range covers |
| `rollups.sql` batch | `share_seq > watermark` | pruned |
| `dashboard_hashrate_rollups.sql` tail | `share_seq > watermark` | pruned; the boundary pass keeps its documented full index-only scan (a query change, not this one) |
| replay comparison in `append_in`, block-only reconciliation probes, `share_accepted_at_ms` | `share_id = $1` | consult `qbit_prism_share_hashes` first, or carry `share_seq >= qbit_prism_share_probe_floor()`: at most three leaves |
| `dashboard_leaderboard.sql`, `dashboard_pool_snapshot.sql`, `dashboard_miner_share_summary.sql`, `dashboard_miner_worker_rows.sql`, `dashboard_hashrate_series.sql` (raw fallback) | `accepted_at` range, 3 h to 24 h | one `accepted_recent_idx` / `miner_history_idx` descent per attached leaf, empty for every leaf outside the range: O(attached partitions) buffer reads per request, a few hundred at most with retention in place; shown not to need the key |
| block solver lookups in blocks, leaderboard, reward leaderboard, pool snapshot | `lower(right(share_id, 64)) = block_hash` | no longer read the ledger: `qbit_pool_blocks.solver_*`, written at landing and backfilled by 016 |
| `latest_evidence` lifetime counts | none | served from the permanent rollup tables plus the raw tail above the watermark |
| `tools.rs` latest miner | ordered walk | newest leaf |
| `read_schema_ready.sql` | `to_regclass` | a partitioned parent resolves |

## Lifetime-scoped consumers and what a detach changes

The design spike's nine Python consumers reconcile to these native ones:

- **Block solver attribution** (four dashboard queries): moved onto
  `qbit_pool_blocks` by 016 and written at landing. Unaffected by detach.
- **`accepted_share_count` and `distinct_miner_count`** in
  `/audit/latest-evidence`: read from `qbit_hashrate_rollup_pool` and
  `qbit_hashrate_rollup_miner` at the daily grain plus the raw tail, which
  the rollup sweep folds before any partition can leave (rule 3 above).
  Unaffected by detach.
- **`GET /public/v1/hashrate-series?range=all`**: served from the rollups
  whenever the rollup tables exist (always, on 3.x.x). Unaffected.
- **A miner's `last_share_at`** in the miner share summary: the newest
  share within the online horizon. A miner whose last share is older than
  the retention age reads `null`. Documented change.
- **`/audit/share-window?anchor=`** for an anchor inside an archived
  range: `rows: []`. The shares are in the archive and in every sealed
  artifact whose window covers them. Documented change; an anchor-to-
  archive index is deferred.
- **Pool readiness** never depended on a lifetime miner count on 3.x.x
  (`PRISM_MIN_READY_MINERS` is retired), <!-- retired-setting: PRISM_MIN_READY_MINERS -->
  so the spike's collection-mode regression has no native counterpart.
- **Audit reconstruction** of a landed block: sealed before detach (D6).

## Retention procedure

`qbit-prism-server share-archive <command>` runs as the operator against
the primary, with the frontends running:

| Command | Effect |
| --- | --- |
| `plan --network-difficulty D [--retention-days N] [--window-multiple M] [--check-duplicates]` | every partition with its bounds, row count, age, and each of the five conditions above with its blocker named; nothing is changed |
| `seal <partition>` | stores canonical bytes for every audit row whose snapshot intersects the partition and has none, verifying each against its advertised digest; records `sealed_at` when none is left |
| `archive <partition> --dir <root> [--force]` | writes `<root>/qbit_share_ledger/<partition>/<manifest-sha256>/rows.ndjson.gz` and `manifest.json`, records the URI, digests and row count; refused while the share sequence has not passed the partition, since appends could still land in it, and refused out of order, so the chain of manifests stays contiguous, and refused over a predecessor whose archive is not verified, so every link is to a certified manifest; `--force` writes an archive again, clearing its verification and that of every later archive, which must then be written and verified again in order, each over its verified predecessor, and is refused once a later archived partition has left the ledger |
| `verify <partition> --dir <root>` | re-reads the archive, checks both digests and that the manifest chains, without a gap, to the nearest archived partition, whose own archive has to be verified, and, while the partition is attached, streams the live rows again and compares; records `archive_verified_at` only for that full comparison, and only once the share sequence has passed the partition, so a verify after the detach reports but never counts as the proof the detach required |
| `detach <partition> --network-difficulty D [--retention-days N] [--window-multiple M] [--check-duplicates]` | requires every plan condition, sealed, archived and verified, and counts the live rows against the archive again; `DETACH PARTITION ... CONCURRENTLY`, or FINALIZE for an attempt interrupted after PostgreSQL marked the partition detach-pending, held to every condition again because the mark does not tell a `share-archive detach` from a statement run by hand; the table stays as a standalone relation |
| `drop <partition> --dir <root>` | requires `detached` and verified; reads the recorded archive back from disk, checking both digests against the catalog, and counts the rows against it again; `DROP TABLE`; the archive is the copy of record |
| `restore <manifest> --dir <root> [--attach]` | recreates the partition table from the archive, verifies count and digests, and optionally attaches it under its recorded bounds |

`plan` also reports the attached partition count, the lead ahead of the
sequence, and, with `--check-duplicates` (one pass over every attached
leaf, so not the default), any `share_id` present in more than one attached
leaf; `detach` with that flag refuses while such a duplicate exists. A
partition that is not attached gets `not_applicable` for each condition
rather than `clear`, and a condition whose evidence is missing (no rollup
watermark row, a catalog row without its relation) is reported as unknown,
never as clear. Every command prints JSON. `restore` creates the table,
loads the rows, re-streams the digest and, with `--attach`, attaches and
records the catalog in one transaction, so a failed restore leaves nothing
behind; `detach`, `drop` and the restore run their DDL without statement or
lock timeouts, as the migration runner does.

While a partition carries the detach-pending mark, PostgreSQL hides its rows
from every new query of the parent. The payout window is read from the
parent, so `plan` counts the hidden partition's accepted rows into that
condition itself: a window that ran out of visible rows before reaching its
weight still reaches into the partition and blocks it. A pending partition
that fails a condition is brought back first; the refusal prints the
`FINALIZE` and `ATTACH PARTITION` statements that do it. The rollup sweep
reads the parent too and cannot fold rows it does not see, so a partition
whose rows are not yet folded must never be detached by hand: the watermark
passes them while they are hidden and nothing can show it afterwards.

## Archive format v1

```
<root>/qbit_share_ledger/<partition_name>/<manifest-sha256>/
    rows.ndjson.gz     gzip; one JSON object per line, share_seq ascending
    manifest.json      the record below, UTF-8, no trailing newline
```

Each write creates a new version directory. The catalog switches to it only
after both files and their directory entries are durable. Prior versions stay
on disk, including after `--force`, so a failed catalog update cannot destroy
the recorded copy. Verification also accepts existing v1 archives in the
original layout without the digest directory.

A row is the whole ledger row with a fixed key order:

```
{"share_seq":1,"share_id":"alice:…","miner_id":"alice","payout_order_key":"…",
 "p2mr_program_hex":"…","share_difficulty":"1","network_difficulty":"100",
 "template_height":100,"job_id":"job","job_issued_at_us":1000000,
 "accepted_at_us":1758000000000000,"ntime":1,"accepted":true,"reject_reason":null,
 "credit_policy":null,"writer_id":"…","writer_epoch":0}
```

Difficulties are decimal strings (`numeric(78,0)`), timestamps are
microseconds since the epoch (exact for `timestamptz`), the program is
hex. `rows_sha256` is the SHA-256 of the uncompressed byte stream, so a
verifier can stream the file without materializing it.

The manifest:

| Field | Meaning |
| --- | --- |
| `schema` | `qbit.prism.share-archive.v1` |
| `partition_name`, `lower_seq` (null for MINVALUE), `upper_seq` | the recorded bounds |
| `row_count`, `first_share_seq`, `last_share_seq`, `first_accepted_at_us`, `last_accepted_at_us` | what the rows hold; zero rows is a valid archive |
| `rows_sha256`, `rows_gz_sha256`, `rows_bytes`, `rows_gz_bytes` | digests and sizes of the stream and of the file |
| `previous_manifest_sha256`, `previous_upper_seq` | the chain: the archived partition with the next-lower `upper_seq`, or null for the first; a gap between `previous_upper_seq` and `lower_seq` means a partition is missing from the chain, which `archive` refuses to write and `verify` refuses to certify; both also refuse a link to a predecessor whose archive is not verified, so the chain is written and certified in order and a verification stands for every link below it |
| `schema_versions` | `qbit_prism_schema_migrations` at archive time |
| `created_at`, `created_by` | UTC timestamp and the operator tool's instance id |

The manifest's own SHA-256 is what the catalog and the next manifest
record, and a manifest is accepted only when it is the canonical encoding
of itself (re-serializing it reproduces the bytes on disk), so a hand
edit is detected before any digest is compared. `created_by` is the
operator process's `PRISM_INSTANCE_ID`. Signing manifests with the ledger
key is a possible extension; the public artifact digests already commit
to every share a block paid on.

## Measurements

Container evidence (PostgreSQL 16.15 in Docker on an Apple M-series
laptop, a synthetic ledger of 1,000,000 production-shaped rows, 1,000 MB
with its indexes, `VACUUM (ANALYZE)` before each read; 2,000 single-row
inserts timed from PL/pgSQL with `clock_timestamp()`):

| Measurement | Before 017 | After 017 | Notes |
| --- | ---: | ---: | --- |
| prepare (bound `NOT VALID`) | | 2.3 ms | catalog only |
| validate (one scan) | | 47 ms | SHARE UPDATE EXCLUSIVE; scales with heap size, about 300 MB here |
| swap | | 16.7 ms | rename, parent, five indexes adopted, attach, lead partitions; none of its terms is a function of the row count |
| insert latency p50 / p95 / p99 | 0.031 / 0.048 / 0.066 ms | 0.032 / 0.047 / 0.069 ms into the release partition; 0.025 / 0.038 / 0.061 ms into a fresh lead partition | a hot leaf's indexes are small and cache-resident |
| `VACUUM (ANALYZE)` of the ledger | 0.32 s | 0.44 s for all partitions; 0.02 s for one lead partition | retention keeps the set vacuum has to visit bounded |
| page walk, `qbit_prism_window` over 200k shares | 15,809 shared buffers | 17,353 shared buffers | same plan shape through the partitioned parent |
| bounded `share_id` probe | | 8 shared buffers over three leaves | `Subplans Removed` appears once the floor clears a partition (shown in the prototype at 100 M) |

They are lock-semantics and phase-shape evidence, not production
absolutes. The production run of the same measurements
(insert latency percentiles from `/metrics`, `VACUUM (VERBOSE)` duration,
`pg_stat_user_indexes` sizes, the page-walk `EXPLAIN`) before and after
017 on a production-sized copy is acceptance criterion 4 of #144 and is
listed as a runbook in the operations guide; it needs production access
and is left to the operator.

## Deferred

- Pruning `qbit_prism_share_hashes` to the replay horizon (it stays
  lifetime-scoped by design, about 150 B per share, and is the uniqueness
  authority).
- Dropping `accepted_block_suffix_idx` once production confirms every
  block carries its solver columns (its only readers were the four
  queries 016 rewrote).
- An anchor-to-archive index for `/audit/share-window`.
- Bounding the dashboard `accepted_at` scans by `share_seq` from the
  catalog (`accepted_at` is monotone in `share_seq` for native rows).
