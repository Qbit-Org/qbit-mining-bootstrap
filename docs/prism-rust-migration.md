# Migrate Prism to Rust and multiple instances

The Rust server replaces all Python Prism runtime and operator scripts from the
`2.x.x` branch. Stratum, public/audit HTTP routes, signed audit formats, payout rules, and existing
PostgreSQL accounting history remain compatible. The runtime uses native threads
and allows multiple frontends to write to one shared PostgreSQL database.

This is a coordinated cutover. **Do not run Python and Rust coordinators against
the same database at the same time.** The additive native migration checks the
legacy writer lease and prevents a Python process from acquiring it afterward.
Stopping the Python services and retaining a recoverable backup are required
before migration.

## Prepare the deployment

1. Pin the source revision and native image digest. Build and test the native
   binary and validate the reviewed environment with `qbit-prism-server
   check-config`. Keep the same manifest signing seed, ledger attestation seed,
   trusted ledger public key, chain, payout policy, pool fee, and CTV policy.
2. Choose the shared PostgreSQL writer endpoint. All frontends need access to
   that same database and a synchronized qbitd on the same chain. Configure TLS,
   credentials, connection limits, and replication through the database's
   existing operating procedures.
3. Give each frontend a unique `PRISM_INSTANCE_ID` or use generated UUIDs.
   Configure `PRISM_RUNTIME_WORKERS`, `PRISM_JOB_BUILD_EXECUTOR_WORKERS`, and
   `PRISM_DATABASE_MAX_CONNECTIONS` per host. Budget total database connections
   across all frontends and administrative clients.
4. Record a baseline: latest accepted `share_seq`, accepted share count, pool
   blocks and pending candidates, carry-forward integrity report and
   `audit_head_sha256`, CTV states, and representative audit SHA values. Save
   canonical audit downloads and independently verify them against the trusted
   ledger key and on-chain coinbase.
5. Back up external audit bodies **and every referenced segment**, including
   legacy compact body refs and v2 proof-body segments. A live evidence envelope
   alone is not an audit backup. Preserve recorded file paths or plan a mount at
   the original absolute path for import. Include the `2.x.x` canonical
   `prism-audit-bundle-canonical-*.json.gz` sidecars.
6. **Pre-migration canonical backfill rehearsal (required).** On an isolated
   restore of the source database and artifact backup, run the native `migrate`
   and `import-audits` commands in the recovery procedure below. Native import
   is also the canonical backfill: if a sidecar never existed, it reconstructs
   bytes from a recoverable inline bundle or external body/segments, verifies
   the trusted ledger signature, recorded coinbase, and existing published SHA,
   then stores the bytes. Both completeness counts must be zero before the
   production migration. A missing body or a digest mismatch stops cutover;
   recover the original evidence rather than changing the published SHA. This
   uses the native equivalent, not the `2.x.x` canonical-backfill command, so
   it does not depend on #174/#176 or mutate historical public content on the
   source. Record restore, migrate, import, verification times and history size
   on a production-sized copy; #291 supplies the release rehearsal evidence.

## Drain and migrate

1. Remove the Python frontends from new connection routing, stop the Python
coordinators, public read services, and standalone CTV broadcaster daemons, and wait for admitted
writes and candidate accounting to finish. Inspect pending candidate state and
resolve it while the old runtime is still available. Confirm every old process
has stopped and its writer lease has been released or expired. A paused old
process is not a completed shutdown.

2. Take the final consistent database backup after draining, along with the audit
filesystem backup and securely retained key/configuration recovery material.
Retain the old pinned image for recovery. Rehearse restoring this backup into an
isolated database before the production change.

3. Run the following with the reviewed operator environment exported, using the
new executable and the existing database:

```sh
qbit-prism-server check-config
qbit-prism-server migrate
qbit-prism-server import-audits --root /var/lib/qbit-prism/audit
```

4. Before admitting native traffic, compare the source and migrated recovery
   evidence using the numbered procedure below, before running
   `qbit-prism-server backfill-ctv`; record any intentional CTV repair delta
   separately afterward. `import-audits` must report
   `missing_stored_bodies: 0` and `missing_canonical_bytes: 0`. Run production
   `self-check` with the intended services available and retain its JSON and
   exit status. A missing body or canonical representation makes it fail in
   production; lab mode reports the counts without failing for incompleteness.
   Native normalized audits use their stored metadata and immutable snapshot
   for canonical reconstruction, so they do not require a duplicate full byte
   copy. These counts check availability; import and artifact reads authenticate
   the contents. Fetch representative historical artifacts through the actual
   public edge, including the oldest history, a sidecar-only import and a
   reconstructed body. Require the exact SHA and matching ETag, as below.

`migrate` applies the existing schema and additive native migration under a
transaction lock. It preserves share sequence/history, balances, audit metadata,
and settlement rows. New tables hold shared configuration/revision, instance
heartbeats, expiring jobs, immutable audit snapshots, and durable claim state.
Migration 3 retains `2.x.x` publication ordinals, retained worker difficulty,
hashrate rollups, and their watermark. Migration 6 pins the accepted `2.x.x`
source schema, records what was migrated, and declares the schema capability
every later start checks. The base schema and native migrations apply in one
transaction, including carry-forward summary repair, except migration 013,
the share ledger index trim, whose index builds run after the commit with
`CREATE INDEX CONCURRENTLY` ([below](#migration-013-the-share-ledger-index-trim-applied-online)). The migration is
idempotent; a refusal due to an old active writer, an unsupported source
schema, or unresolved legacy work must be resolved before admitting native
traffic.

### Supported 2.x.x source schemas

The minimum supported source release is **v2.0.1** (`95ffe06`). A v2.0.0
(`f6854a0`) database has the identical schema and migrates as the same source
state, but drain it with the v2.0.1 or later image, because the offline
recovery command below first shipped in v2.0.1. The newest supported source is
**v2.0.2** (`504846c`, #258). The SQL those releases applied is frozen
byte-for-byte under `crates/qbit-prism-server/tests/fixtures/schema_2x/`. The
migrator classifies the database before it runs any DDL, and after
`001_share_ledger.sql` has run it checks that what 001 left alone is the
release definition:

| Source state | Evidence | Verdict |
| --- | --- | --- |
| fresh | no `qbit_share_ledger`, no other object `001_share_ledger.sql` creates, no `002_candidate_bodies.sql` object: an empty database, with or without objects of the operator's own | accept |
| partial 001 | no `qbit_share_ledger`, but some table, sequence, index, trigger or function that 001 creates is present: a selective restore, or part of the schema installed by hand | refuse before any DDL, naming the objects present; restore the full pre-migration backup or migrate into an empty database |
| pre-#258 (v2.0.0, v2.0.1) | `001_share_ledger.sql` only: no `qbit_prism_schema_capabilities`, no `002_candidate_bodies.sql` object | accept after the drain check |
| #258 applied (v2.0.2) | `candidate_storage_version = 2` and every `002_candidate_bodies.sql` object present | accept after the drain check |
| partial 002 | some 002 objects or the capability row, but not all (v2.0.2 applies 001 and 002 as two script calls, and a restart between them leaves this) | refuse, naming the missing object; finish 002 with the v2.0.2 release (`PRISM_POSTGRES_INIT_SCHEMA=1`) or restore the backup |
| newer | `candidate_storage_version > 2`, or a capability this release does not know | refuse before any DDL; a newer PRISM release wrote the database. A native database also gets a capability check before later DDL; after migration 6, its candidate version must be 1, the format the native claim lane can process |
| native collision | a table, sequence, index, trigger, function or column that a native migration (`002_multi_instance.sql` to `013_share_ledger_index_trim.sql`) creates and the `2.x.x` release does not is already present, in an empty database or a `2.x.x` one, or a reserved relation name is held by a relation of another kind (a view, an index backing an operator's constraint): a leftover of an earlier native attempt, a selective restore, or something installed by hand | refuse before any DDL, naming the objects; nothing is dropped; restore the full pre-migration backup, or check what the objects hold and remove them, then migrate again |
| drifted 001 | a 001 (or 002) object whose definition, after 001 has run, differs from the frozen release: a table, column, index, sequence or named constraint that 001's `IF NOT EXISTS` skipped, or any 002 object, with a dropped constraint, a changed type, nullability or default, a different index definition, an altered sequence (a lowered maximum, a different increment), a table or sequence made `UNLOGGED` (or temporary), a release constraint left `NOT VALID` (other than the pinned `qbit_share_ledger_credit_policy_check`), a release foreign key whose enforcement triggers were disabled, row-level security enabled or forced on a release table or a policy on one, a child table created with `INHERITS` on a release table or a release table made a child or partition of another, a replaced function body, a disabled trigger or a trigger the release does not create on a release table | refuse transactionally, naming each object and what differs; the migration rolls back and the database is unchanged; restore the pre-migration backup or bring the database to the release schema with the `2.x.x` release, then migrate again |

Migration requires visible `qbit_` relations and functions to resolve in
`current_schema()`, where its unqualified DDL creates objects. An empty first
schema followed by a legacy ledger schema in `search_path` is refused before
any DDL, preventing a parallel ledger from hiding accounting history.
Set the intended ledger schema first and ensure no `qbit_` objects resolve
from other schemas. A later empty schema or unrelated operator objects are
allowed.

**The release definitions.** They are not a stored fingerprint: before any
DDL touches the source, inside the migration transaction, the migrator opens
a savepoint, creates a scratch schema, applies the frozen 001 there (plus
`002_candidate_bodies.sql` for a #258 source), reads every table, column,
constraint, index, trigger, function and sequence that produced, and rolls
the savepoint back, so the comparison is exact for the PostgreSQL version in
use and nothing from the scratch apply survives. Both checks below use that
one reading.

**The fresh check.** A database without `qbit_share_ledger` and without any
002 object is fresh only if it has none of the other objects 001 creates.
Anything else, a `qbit_pool_blocks` restored on its own, a leftover
`qbit_audit_publication_sequence_seq`, a hand-created function, is a partial
001: 001's `IF NOT EXISTS` would keep it exactly as it is, so the migration
is refused before any DDL, naming what is present:

```
refusing to migrate a partial 001 source before any DDL: the database has no qbit_share_ledger but
holds 4 object(s) that the 2.x.x release's 001_share_ledger.sql creates (table qbit_pool_blocks;
index qbit_pool_blocks_audit_publication_sequence_idx on qbit_pool_blocks; index
qbit_pool_blocks_maturity_idx on qbit_pool_blocks; index qbit_pool_blocks_public_recent_idx on
qbit_pool_blocks), so it is neither an empty database nor a 2.x.x ledger, and 001's IF NOT EXISTS
would keep those objects whatever they hold. Nothing was changed. Restore the full pre-migration
backup, or migrate into an empty database
```

Objects the release does not create, an operator's own table for instance,
do not disqualify a fresh database; they are kept and logged at warning
level.

**The native-collision check.** The same scratch apply continues with the
native migrations, in the order the migrator applies them, and is read
again; the second reading minus the first is every table, sequence, index,
trigger and function a native migration creates and the release does not
(`qbit_prism_schema_migrations`, which the migrator creates itself, is never
in it), and, per release table, every column a native migration adds to it
(the claim columns 002 adds to the outbox and the fanout artifacts, for
instance). A `2.x.x` database or an empty one that already has one of those
objects or columns is refused before any DDL, whatever it holds: `CREATE
TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS` and their kin would keep it
as it is, the migration would then record a schema it did not build, and a
writer would fail only afterwards. A reserved name held by a relation of
another kind is the same collision, because `IF NOT EXISTS` looks at the
name alone: a view under a native table's name, or an operator table whose
unique constraint is named `qbit_prism_candidate_claim_idx` and whose index
therefore owns that name, would make 002 skip the outbox index it meant to
create and leave candidate polling to scan the outbox; each is named by
what holds the name (`view qbit_prism_jobs`, `index
qbit_prism_candidate_claim_idx backing constraint
qbit_prism_candidate_claim_idx on operator_notes`). For example, a leftover
`qbit_prism_cluster(singleton boolean PRIMARY KEY)` on an otherwise empty
database is refused like this:

```
refusing to migrate a native collision source before any DDL: the empty database already holds
1 object(s) that the native migrations create and the 2.x.x release does not (table
qbit_prism_cluster), so a native migration's IF NOT EXISTS would keep each such table, sequence,
index, trigger, function or column whatever it holds and the migration would record a schema it
did not build. Nothing was changed. Restore the full pre-migration backup, or check what those
objects hold and remove them yourself, then migrate again
```

A column already on a release table under a reserved name, a `claim_token
integer` on the outbox say, is named as `column
qbit_block_candidate_outbox.claim_token`. Nothing is dropped for you. An
object or column in both readings, the capability table on a #258 source
for instance, or the outbox's `storage_version` there (the release 002
added it; on a pre-#258 source 006 adds it, so it is reserved), stays
governed by the release checks.

**The release-schema check.** 001 is the idempotent schema every `2.x.x`
start re-applied, so the migrator applies it next: that repairs everything
001 re-asserts (its functions, its triggers, the columns and constraints it
alters or re-adds by name). What `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF
NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` skip is checked after it, before
`002_multi_instance.sql` or any later native migration alters those tables.
It runs on a fresh database too, where 001 has just created everything and
it passes trivially. Each object needs an equivalent in the schema the
migrator runs in: for every table and sequence its persistence (the release
creates ordinary logged relations, and an `UNLOGGED` or temporary table is
one whose rows PostgreSQL truncates after a crash, so it is drift whatever
its columns say; a sequence follows its table's persistence on PostgreSQL 15
and later); for every table its row-level security, both flags (`ENABLE ROW
LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY`) and every policy on it, by
name, with its command, whether it is permissive or restrictive, its roles
and its `USING` and `WITH CHECK` expressions (the release creates none of
this; a policy that hides rows from the migrate role, or forced security
with no policy at all, would make the drain check see an empty outbox and
the native claim lane never see the legacy rows it left behind, so a policy
on a release table is drift, not an extra: `table qbit_block_candidate_outbox
differs: expected row-level security disabled, found enabled and forced`,
`policy hide_pending on qbit_block_candidate_outbox: FOR ALL USING ((state
<> 'pending'::text)); the release has no row-level security policy on this
table`); for every table its inheritance, both ways (the release creates
none: a child created with `INHERITS (qbit_share_ledger)` has its rows
included in every query of the ledger, the share reads included, without
the release constraints ever checking them, and a release table attached as
a partition of or inheriting from another table is no longer the relation
the release defined, so either is drift: `table qbit_share_ledger differs:
expected no child table, found child table(s) qbit_share_ledger_2025`;
inheritance among the operator's own tables is theirs to keep); column
type, NOT NULL, default, identity and generated status, collation;
constraints per table by definition, validation state and the enabled state
of the internal triggers that enforce them; index definitions and validity;
trigger definitions and enabled state; function arguments, result, language, body,
volatility and settings; and for every sequence, the ones behind `bigserial`
columns and the explicit `qbit_audit_publication_sequence_seq` alike, its
data type, start, increment, minimum, maximum, cache and cycle. The value a
sequence has reached is data and is not compared, so a ledger whose share
sequence has advanced migrates and keeps its position. Schema qualification,
column order, comments and the names of auto-generated constraints are
ignored. A constraint's validation state is compared: a release constraint
that is `NOT VALID` in the source, a foreign key or CHECK dropped and
re-added without checking the rows that were there, is drift (`constraint
qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox is
NOT VALID; the release validates it`), because those rows may be orphaned
or otherwise invalid. The one exemption is pinned:
`qbit_share_ledger_credit_policy_check`, which the release 001 itself adds
`NOT VALID` to a `qbit_share_ledger` upgraded from before the column
existed and never validates, is accepted in either state. A test derives
that list from the frozen release SQL, so it cannot drift from the release.
The internal triggers that enforce a foreign key are compared the same way:
a superuser's `ALTER TABLE ... DISABLE TRIGGER ALL` during a bulk load
leaves the constraint's definition and validation state as they were while
no new row is checked against it, and 001 never re-enables a trigger it did
not create, so it is drift (`constraint
qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox
differs: enforcement triggers expected 4 enabled, found 4 disabled`).
A trigger the release does not create on a release table is drift too, not
an extra: it can refuse or rewrite rows the migrator and the native writers
put there. A guard that rejects `writer_epoch =
0`, say, lets the `2.x.x` writer's leased epochs through and fails every
native share insert, which writes epoch 0, so it is named with what it runs
(`trigger operator_epoch_guard on qbit_share_ledger: CREATE TRIGGER
operator_epoch_guard BEFORE INSERT ON qbit_share_ledger FOR EACH ROW EXECUTE
FUNCTION operator_reject_epoch_zero(); the release has no trigger on this
table`); a trigger on a table the release does not create is the operator's
own and is kept. Rewrite rules on release tables are compared by definition
and enabled state too. An extra rule is refused even when disabled: an
`ON INSERT DO INSTEAD` rule can suppress or redirect native inserts before
triggers run. Rules on operator-only tables are kept.
An extra constraint on a release table is also drift:
`CHECK (writer_epoch > 0)` accepts legacy shares but rejects native shares,
so migration refuses it by name and definition, regardless of validation or
enforcement state. Foreign keys from operator tables into release tables are
also refused, including owners in other schemas: their internal triggers can
block native updates or deletes, such as pruning old fanout broadcast attempts.
Constraints involving only operator tables are kept.
An extra `NOT NULL` column on a release table without a default, identity
or generated expression is also drift: native inserts omit it and would
fail. Extra defaults, identities and generated expressions are also refused:
PostgreSQL can evaluate them during native writes, and migration cannot prove
that they will succeed (for example, a generated `10 / writer_epoch` fails
for native epoch-zero shares). Only plain nullable extra columns remain
accepted, and their types must not be domains. Domain defaults, constraints
and nullability can reject an omitted value even when the column itself
looks nullable. Columns on operator-only tables are unaffected. All extra
indexes on release tables are refused, including plain nonunique indexes: an access method or custom operator
class can execute code that rejects native writes. Unique, expression and
partial indexes can also constrain or evaluate writes. The migrator reports
the index without removing it; review and remove extra release-table indexes
before migrating. Indexes on operator-only tables remain accepted.
Accepted columns and extra tables, functions and sequences are kept and
logged at warning level. A missing or different object refuses the migration:

```
refusing to migrate a drifted 001 source: after 001_share_ledger.sql ran, the database does not
match the v2.0.x release schema (v2.0.1, 001_share_ledger.sql), 3 object(s) differ (column
qbit_pool_blocks.parent_hash differs: expected NOT NULL, found nullable; sequence
qbit_share_ledger_share_seq_seq differs: maximum expected 9223372036854775807, found 1000000;
missing constraint qbit_block_candidate_outbox_share_id_fkey on qbit_block_candidate_outbox:
FOREIGN KEY (share_id) REFERENCES qbit_share_ledger(share_id)). Nothing was changed: the migration
rolled back. Restore the pre-migration backup, or bring the database to the release schema with the
2.x.x release (v2.0.1 or later; v2.0.2 for a #258 database) and take a new backup, then migrate again
```

The check needs the migrate role to hold `CREATE` on the database, for the
scratch schema; without it `migrate` refuses and names the grant. The
migration record is unchanged by this check: `pre_258` still means the
database is equivalent to the v2.0.1 release schema.

`3.x.x` never applies `002_candidate_bodies.sql` and does not import #258's
chunked candidate bodies. On a #258 source it keeps the 002 tables and
triggers; native writers store version 1 JSONB candidates, which 002's
dual-format rule accepts. After validating and draining the source, 006
records its former capability value of 2 in the source metadata and changes
the live declaration to 1. Version 2 is accepted as legacy source evidence,
never as native runtime support.

**#258's rollback floor.** Once a `storage_version = 2` outbox row exists, a
writer that predates v2.0.2 must not run against the database (see the header
of `002_candidate_bodies.sql`). The same floor applies to this cutover from the
other side: `3.x.x` cannot replay a v2 row either, so every v2 row must be
drained by the v2.0.2 coordinator before migration. Never roll a #258 database
back below v2.0.2 to drain it.

**The drain requirement covers v1 and v2 rows.** A pending v1 candidate
(JSONB body without the native `payout_revision`, `bundle` and `block_hash`
fields) and a pending v2 candidate (`candidate` NULL, body in the chunk
tables) both cannot be replayed by Rust. `migrate` refuses them
transactionally, without changing the schema, and names the blocking rows:

```
legacy Python block outbox is not drained: 2 pending 2.x.x candidate row(s) cannot be
replayed natively (block_hash=... storage_version=1 ...; block_hash=... storage_version=2 ...).
Drain them with the pinned 2.x.x release before migrating: ...
```

The check reads outbox rows, not the capability row: 002 declares
`candidate_storage_version = 2` whatever the writer stored, so the row proves
002 ran, not that v2 work is pending. Drain with the pinned `2.x.x` image:

1. Start the `2.x.x` coordinator (v2.0.2 for a #258 database, v2.0.1 or later
   otherwise) and let its block submitter finish every pending candidate; it
   replays every durable pending row on start.
2. For a block that is already accepted on the active chain but cannot complete
   through normal replay, follow the
   [pinned `2.x.x` offline recovery procedure](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/504846cc0b72e8f86ed17f896d4ccbbe196a31dc/docs/prism-ledger-ops.md#L372-L399)
   from the `2.x.x` image, with the managed coordinator and its restart
   supervisor stopped. Select the exact hashes with `--block-hash` and inspect
   the read-only plan first; repeat the same allowlist with `--apply` to drain.

   That command was removed from `3.x.x` with the Python runtime; it only
   exists in the `2.x.x` image.
3. Confirm `qbit_block_candidate_outbox` has no `pending` rows, then repeat the
   backup and migration boundary. Do not delete pending rows merely to bypass
   this check.

A v2 row that reaches the native claim lane anyway (one written after the
drain, for instance) is parked, not retried: the lane records why in
`last_error`, releases the claim, and sets `next_attempt_at` to `infinity`, so
no lease expiry offers it again. Find parked rows with
`SELECT block_hash, storage_version, last_error FROM qbit_block_candidate_outbox
WHERE state = 'pending' AND next_attempt_at = 'infinity'` and drain them with
the `2.x.x` image as above. That replay advice applies to the legacy pending
rows being drained before migration. After 011, changing `next_attempt_at`
only reschedules reconciliation for a reserved or offered row; it never
grants another offer. Follow [the offer lifecycle recovery guidance](prism-ledger-ops.md)
for those rows and retain their evidence.

**A database an earlier 3.x.x build migrated before 006.** `3.x.x` is a
development line with no supported production upgrade, but a database an
earlier `3.x.x` build brought to native schema 3, 4 or 5 can still hold a
pending v2 row: that build's drain check used the v1-only predicate, which
never counted a row whose `candidate` is NULL. `migrate` therefore runs the
same column-aware drain check on a native schema 3, 4 or 5 before 004, 005
and 006, and refuses before any DDL when such a row is pending, naming the
rows as above:

```
refusing to apply migration 006 to a native database at schema migrations 2, 3, 4, 5, 8, 9: an earlier
3.x.x build migrated it before the drain rule covered these rows, and the legacy Python block outbox is
not drained: 1 pending 2.x.x candidate row(s) cannot be replayed natively (block_hash=...
storage_version=2 ...). Nothing was changed. Restore the pre-migration 2.x.x backup and drain them with
the pinned 2.x.x release ...
```

Before the drain check or native DDL, an existing outbox `storage_version`
column must match `integer NOT NULL DEFAULT 1`, with no identity or generated
expression. Migration 006 uses `ADD COLUMN IF NOT EXISTS`, so it would keep a
malformed column that breaks subsequent native candidate inserts or claims.
A mismatch refuses the upgrade, names the differing properties, and leaves
the database unchanged. An absent column is accepted and created by 006.
The `qbit_prism_migration_source` name must also be absent when migration 6
is not recorded. A partial, complete or populated metadata table is refused
before native DDL: 006 must create its own record, never retain unverified
provenance through `IF NOT EXISTS`. Restore the full backup, or review and
move the existing object aside before retrying; migration removes nothing.
The pre-006 native outbox must have neither row-level security (`ENABLE` or
`FORCE`) nor policies, including inactive policies. The drain check reads
these settings from the catalog before counting pending candidates, so a
policy cannot hide a legacy row and make the database appear drained.
Refusal preserves the rows and security settings for operator review.

Native pending rows the earlier build wrote carry the native fields and are
not counted; a v2 body, a `body_id`, a `storage_version` other than 1, or a
v1 body without the native fields is. The remedy is the one in the recovery
section below: the `2.x.x` release is not supported against a native schema
(its revert script refuses one, and this guide never points it at a migrated
database), so restore the pre-migration `2.x.x` backup, drain there with the
pinned image, take a new backup and migrate again. If native traffic was
admitted after the earlier migration, that restore discards it and needs the
reconciliation decision the recovery section describes.

The capability rows of that database are checked before the drain check, so
`migrate` never applies 004, 005 and 006 to a database a newer release wrote:
one that declares `candidate_storage_version > 2` or a capability this
release does not know is refused before any DDL, the "newer" verdict of the
table above, and nothing is recorded for it. A database already at 6 gets
the runtime check before 008 and 009: any candidate version above 1 is
unsupported, even 2. The remedy is the startup gate's: upgrade the server.

```
refusing to migrate a native database at schema migrations 2, 3, 4, 5, 8, 9 before any DDL: database declares
candidate_storage_version = 3, but this server understands candidate_storage_version 1 to 2 only: a newer
PRISM release wrote this database; upgrade the server before starting it here
```

A migration record with 3 and not 2 is refused before any DDL as well. Every
native build records both in the one transaction that applies them, so such
a record was edited or restored selectively; every start refuses the gap,
and `migrate` neither re-runs `002_multi_instance.sql` on a database at 3
nor records it unseen, which would vouch for objects it never checked, so
004 to 009 are not applied above it either. Restore the full pre-migration
backup, or, once every object 002 creates is verified present, record it by
hand (`INSERT INTO qbit_prism_schema_migrations(version) VALUES(2)`) and
migrate again.

Migration history itself is validated before its rows are trusted at startup
or migration. `qbit_prism_schema_migrations` must retain its native logged-table,
integer primary-key and timestamp definitions, without extra columns,
constraints, indexes, triggers, rules, inheritance or row security. A fresh
or 2.x.x source must not already hold this table without migration 3. Refusal
leaves the database unchanged; restore the full backup or review and repair
the history table (move a non-native object aside) before retrying.

**What was migrated.** After a successful migration
`qbit_prism_migration_source` holds one row: the accepted source state
(`pre_258`, `258_applied`, `fresh`, or `native` for a database that was
already on the Rust schema), the `2.x.x` release and commit that source
corresponds to, the capability value the source declared, the highest
schema migration recorded before this one, and which instance migrated it. `migrate` prints it,
every start logs it, and a repeated `migrate` never rewrites it.
Once migration 6 is recorded, both the table and its readable singleton row
are required at startup and before applying later migrations. Missing or
malformed metadata is refused without committing later DDL or migration
versions. Restore the full backup, including the original source record;
the migrator never invents provenance for an already-migrated database.

**Startup gate.** Every start reads `qbit_prism_schema_migrations` and
`qbit_prism_schema_capabilities`, with or without
`PRISM_POSTGRES_INIT_SCHEMA`. This release requires migrations 2 through 13,
each checked on its own rather than as a high-water mark: a later migration
being present never stands in for an earlier missing migration. Stop all
older frontends before applying 011; it refuses live pre-upgrade claims and
quarantines previously attempted candidates for reconciliation without
another offer. See [the offer lifecycle upgrade procedure](prism-ledger-ops.md)
for the quiesce and recovery steps. 013 is recorded only once its online
index builds have completed, so a start after an interrupted build is
refused until `migrate` finishes them. A database missing any of them is refused
at connect, naming the gap, before any accounting statement runs, and so is
one declaring a
capability or a runtime `candidate_storage_version` other than 1. A native
version-2 declaration is refused before the server can claim and park a
newer writer's candidate; a #258 migration records its source version of 2
separately and declares runtime version 1 within the migration transaction.
The capability relation must be an ordinary table; views, materialized
views, foreign tables and partitioned relations cannot substitute for it.
Its catalog kind is checked before any capability rows are read, at startup
and before migration DDL. Restore the original table from the full backup
if a selective restore replaced it.
The declaration must also belong to the current schema, where the migration
history was verified: `search_path` resolves an unqualified name past a
schema that lost its table to a later schema's, and that declaration
describes another ledger. Startup refuses it, naming both schemas, instead
of trusting a version-1 row from elsewhere; `migrate` already refuses any
`qbit_` object resolved from another schema before any DDL.
A database at 6 must declare its capabilities: 006 created
`qbit_prism_schema_capabilities` and its `candidate_storage_version` row and
nothing native removes them, so a start refuses a database missing either,
naming the remedy, rather than reading the absence as a legacy state, and
`migrate` refuses the same database before any DDL.
The capability table must also have row-level security disabled, including
its `FORCE` flag. Startup and migration check the catalog before trusting
its rows, because a policy could hide an unknown capability while exposing
the required candidate-version declaration. Nothing disables a policy
automatically; operators must review the table before retrying.
A migration this release does not know is accepted with a warning that names
it: native migrations are additive, and a release whose format an older
binary must not touch declares a capability. With the native default
`PRISM_POSTGRES_INIT_SCHEMA=0` that means a newer binary refuses to start
until `migrate` has run, instead of failing later in the claim path.

`import-audits` processes database rows whose audit body is external. It resolves
full v1/v1.1 bodies, legacy body refs, and v2 proof bodies, verifies segment
references, the trusted ledger signature, recorded coinbase, and canonical
bundle hash, then stores the logical body in PostgreSQL. Original files and
published hashes are retained. Canonical gzip sidecars are verified against
their uncompressed hash and stored byte-for-byte in the database. Content-addressed
HTTP responses preserve these exact bytes. This makes history available on every frontend
without a shared filesystem.

`--root` resolves relative body paths and restricts imports to that directory;
it does **not** rewrite an absolute URI to a different root. Mount old absolute
paths at their recorded locations. Keep referenced segment paths readable as
well. Import failure is a reason to repair the missing/corrupt artifact before
continuing; a matching hash without recoverable bytes is insufficient.

`backfill-ctv` scans verified stored bundles and restores missing fanout records.
Run import first. It is idempotent for matching existing records and replaces the
old Python repair command's individual-path/block selection interface.

### Migration 013: the share ledger index trim, applied online

Migration 013 (#153) trims the secondary indexes of `qbit_share_ledger` to
what the native readers use. It replaces
`qbit_share_ledger_accepted_seq_window_idx`, a `share_seq DESC` index carrying
seven INCLUDE columns that no native read ever fetched from it, with
`qbit_share_ledger_accepted_seq_walk_idx`, whose INCLUDE is exactly what the
newest-first page walk and the landing count read; replaces
`qbit_share_ledger_accepted_miner_recent_idx` with
`qbit_share_ledger_accepted_miner_history_idx`, without the unread
`payout_order_key`; and drops `qbit_share_ledger_accepted_window_idx` and
`qbit_share_ledger_template_height_idx`, which no native query uses. The
[index inventory](prism-ledger-ops.md#share-ledger-indexes) maps every index
to its readers and lists the measurements to take afterwards.

On a fresh deployment or an empty 2.x.x source it is applied inside the
migration transaction while the cutover locks exclude writers. Existing
native ledgers always apply it outside that transaction, even when no
shares are visible: the first share may still be uncommitted, and native
writers do not take the migration lock. Populated 2.x.x sources also use
this online path. `CREATE INDEX CONCURRENTLY` cannot run in a transaction
block, and a plain `CREATE INDEX` holds a SHARE lock on the ledger for the
whole build, so every append would queue behind it. Inside the migration
transaction the file is then applied only to the scratch schema, so the
migrator learns the index definitions as this PostgreSQL renders them. After
the commit, on a dedicated connection with no statement or lock timeout and
under a session-level advisory lock keyed by the ledger's schema, `migrate`
(or a start with `PRISM_POSTGRES_INIT_SCHEMA=1`) builds each new index with
`CREATE INDEX CONCURRENTLY`, drops each replaced one with `DROP INDEX
CONCURRENTLY`, and records 13 last. Appends continue throughout. A
concurrent build waits for the transactions that were already using the
table, so a long payout-window read delays it without blocking anything
else; two frontends starting together build once, the second waiting on the
lock and finding the version recorded.

Until 13 is recorded, every start refuses the database, naming it, like any
other required migration. That is the resumable state: an interrupted build
leaves an invalid index behind, still maintained by every insert, and the
next `migrate` drops it and builds again; an index built before the
interruption is kept when it is valid and matches the declared definition. A
valid index under one of the two new names with any other definition, or a
relation of another kind under one of the six names, is refused by name and
left alone, as with every native collision:

```
refusing to apply migration 13: index qbit_share_ledger_accepted_seq_walk_idx on qbit_share_ledger
already exists with a different definition (CREATE INDEX qbit_share_ledger_accepted_seq_walk_idx ON
qbit_share_ledger USING btree (share_seq)); the migration declares CREATE INDEX
qbit_share_ledger_accepted_seq_walk_idx ON qbit_share_ledger USING btree (share_seq DESC) INCLUDE
(job_issued_at, accepted_at, share_difficulty) WHERE accepted. The migration is not recorded and
nothing was changed by it. Check what that index serves, then rename or drop it and migrate again
```

Plan for the build on a large ledger. Both replacements are built before
anything is dropped, so the transient footprint is the two new indexes on
top of the existing ones; on the production ledger measured for #144 the two
replaced indexes held 44 GB, and the replacements are a fraction of that, but
keep that headroom free. A concurrent build reads the table twice. Run
`migrate` from a shell where a multi-hour command is acceptable, rather than
relying on a starting frontend, whose readiness stays down until the build
completes. Then run `ANALYZE qbit_share_ledger` and take the measurements the
inventory lists before scheduling #144.

## Bring up native instances

Start one instance first and verify readiness before admitting controlled miners:

```sh
qbit-prism-server run
# In a separate shell with the same environment:
qbit-prism-server healthcheck --url http://127.0.0.1:3341/healthz
qbit-prism-server self-check
```

For the supplied external database Compose overlay:

```sh
# Public reads may use the writer, or a separate standby endpoint with mode=require.
export PRISM_PUBLIC_DATABASE_URL="$PRISM_DATABASE_URL"
export PRISM_PUBLIC_REPLICA_MODE=off
docker compose -f compose.yaml -f compose.prism-external-db.yaml \
  --profile prism up -d qbitd prism-coordinator prism-public-api
```

Set `PRISM_DATABASE_URL` to the external writer endpoint on every host. The
overlay removes dependencies on the local primary and replica containers.
Set `PRISM_PUBLIC_STRATUM_URL` to the advertised mining endpoint. The public
service uses its own read pool and does not require signing keys or an audit
filesystem mount. To read a standby, set `PRISM_PUBLIC_DATABASE_URL` to it and
`PRISM_PUBLIC_REPLICA_MODE=require`; health then requires a recovering server
and a fresh replication stream. Replay lag is reported separately.
Use an up-to-date Docker Compose release supporting `!override`. Production
operators should combine their pinned-image/storage production configuration
with this overlay **last**, and render `docker compose ... config` before start.

`QBIT_RPC_HOST` is overridable. With an already managed external qbitd, set its
RPC connection and start only the coordinator with `up -d --no-deps
prism-coordinator`. The local qbitd dependency then does not launch another node.
The `make up-prism-pool` helper manages the local node/database/public-service stack; use the
explicit Compose overlay commands for the external database topology.

Add the remaining instances with identical signing and payout configuration.
Keep TCP connection routing stable while a miner is connected; reconnects may
land on another instance. Existing TCP sessions do not move between processes.
The shared extranonce allocator, durable job records, proof deduplication, and
canonical ledger provide cross-instance accounting continuity.

Check these before restoring ordinary traffic:

- Every frontend is healthy and follows the intended qbit chain/tip.
- Controlled miners subscribe, authorize, receive difficulty/jobs, submit shares,
  and recover after reconnect or a frontend restart.
- Database share order and counts reconcile with unique committed proofs;
  duplicate submission on another instance gives no second credit.
- Baseline audit downloads have the same canonical hashes; carry-forward
  integrity and CTV state agree with the pre-migration records.
- Public dashboard and audit routes return the expected data on every instance.
- Candidate and CTV claim recovery work after a process interruption.

A subsequent rollout between compatible **Rust** versions may drain and replace
one frontend at a time. Review each version's schema compatibility separately.
Run the new release's `migrate` first; frontends still on the previous release
keep starting on the newer schema while its migrations are additive and it
declares no capability they do not understand, so they can be drained and
replaced one at a time. A release that declares a new capability needs every
frontend stopped first.

## HA durability and failover

Multiple frontends remove dependence on one Prism process. The database still
has one authoritative primary/writer endpoint. All accounting writes and reads
must reach that endpoint, rather than independent primaries or lagging read
replicas. The separately served public dashboard may use a checked read replica;
it never makes accounting or settlement decisions.

Keep `fsync=on`, `full_page_writes=on`, and `synchronous_commit=on`. The last
setting makes a local commit durable but does not create replication. If the
requirement is no loss of acknowledged shares after primary storage loss,
configure synchronous replication and promote only a standby holding all those
durable commits. An asynchronous failover can lose already acknowledged work.
Avoid silently degrading the required synchronous policy during an outage.
The bundled public-read replica uses asynchronous replication by default; its
presence alone does not provide this lossless accounting failover guarantee.

Exercise the actual database failover endpoint under controlled mining load.
Reconcile ACKed proof IDs with the promoted database and inspect retry/dedupe,
payout, and candidate outcomes. Use independent backups and WAL archives even
when replication is synchronous; replicas can reproduce operator mistakes.

The repository also includes a disposable physical PostgreSQL failover test:

```sh
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin \
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
```

It checks two ledger clients' acknowledged IDs through a stable TCP proxy,
synchronous standby promotion after immediate primary loss, duplicate replay,
and continued writes. Run it with installed PostgreSQL server tools as an
unprivileged user. This test complements production-specific failover drills.

## Configuration and observable changes

Retained interfaces include miner username syntax, BIP310 version rolling,
vardiff and high-difficulty listeners, the payout/CTV policy variables, public
`/public/v1` routes, audit route aliases, canonical bundle hashes, and legacy
`*_SATS` monetary aliases. Keep explicit mainnet share/vardiff bounds. As on
`2.x.x`, mainnet permits bounded one-parent stale-share grace (default 3 seconds).
The grace interval starts when that client receives replacement work; stale
blocks are never submitted. Retained vardiff hints are shared by listener and
exact username, with accepted-work evidence controlling their expiry.

Native submit classification also preserves the published-tip authority used by
`2.x.x` at `95ffe063846d51f83999a66cc654da5f7476fdef`. A detected tip immediately
fences block candidates. Miner share credit continues against the last published
work while replacement is prepared, including when stale grace is zero. Its
ordinary freshness budget is `PRISM_SUBMIT_TIP_MAX_AGE_SECONDS` (default 10);
zero forces a live tip RPC for each share. A detected replacement may extend
published authority through `PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS`
(default 120), measured from the first departure. Repeated detections and failed
refresh attempts do not restart that deadline. Once both budgets have expired,
submit falls back to the node; RPC failures remain backend-unavailable.

Successful work publication opens one-parent stale grace after the startup
baseline. The deadline starts separately for each connection's first delivery
of that tip, and same-tip refreshes do not slide it. An undelivered replacement
keeps eligible retained work in a bounded per-connection graveyard;
absolute reconnect-job expiry is never extended. Share credit retains the
original issued worker, target, network difficulty and policy. Both ordinary
published-work credit and stale-grace credit still commit under the current
transactional payout revision. These are restorations of miner behavior, with
ungated real-coordinator decision and socket/session regression tests; they do
not relax current-chain candidate submission checks.
See the [miner decision parity reference](prism-b8-miner-parity.md) for retained
work bounds, the regression coverage map and disposable qualification commands.

`PRISM_USERNAME_FALLBACK_ADDRESS` applies when validation explicitly identifies
an invalid address or a recognized address type that Prism cannot pay. RPC
failures and malformed validation responses reject authorization without
substituting another payout recipient.

Mainnet `check-config` and `run` require `QBIT_EXPECTED_GENESIS_HASH` to contain
the trusted 64-hex genesis hash. Startup compares it to the connected node;
production flags reject regtest. Public-chain readiness also requires completed
initial block download, matching block/header heights, and at least
`PRISM_MIN_PEERS` connected peers (default 1). Templates must satisfy
`PRISM_TEMPLATE_MAX_AGE_SECONDS` (default 120). A failed readiness observation
closes mining readiness until a fresh valid poll succeeds.

General node and wallet RPCs use `PRISM_RPC_TIMEOUT_SECONDS` (15 seconds).
`PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` (1 second) bounds only `submitblock`;
ambiguous submission results retain the durable candidate for reconciliation.
Candidate claims renew throughout processing, including waits for build workers.
Losing the lease cancels the attempt; the durable row remains recoverable.
Fresh candidates take priority over retries, with periodic oldest-due selection
to keep older recovery work moving.
Wallet selection preserves any configured `QBIT_RPC_URL` proxy path prefix.
The independent public service retains its separate public read deadline.

Canonical payout changes retire prior jobs even when the parent tip is
unchanged. Miners receive replacement work with `clean_jobs=true`; ordinary
same-tip refreshes keep valid retained jobs, and previous-parent share grace
remains bounded by each connection's notification time.
Reauthorizing a connection preserves the original payout identity of retained
work. When `PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME` is enabled, that work
also keeps its original username's capacity slot until it expires or is discarded.

CTV fee policies, including explicit rates, are checked against the node's live
`minrelaytxfee` and `mempoolminfee` before building payout artifacts. A configured
rate or discounted premium below these floors fails the build; correct the fee
policy before admitting mining work.

Removed implementation settings include Python writer IDs/epochs/session leases,
share batch/linger queues, psql/native-client selection, subprocess builders,
incremental window schedulers, refresh rollout gates, and their watchdog knobs.
Remove them from the operator environment; they do not tune the Rust runtime.
The supported settings are documented in [.env.example](../.env.example) and the
[native server README](../crates/qbit-prism-server/README.md).

The combined audit/operator HTTP port remains 3341; the separate native
`public-api` role preserves public port 3342. `/healthz` retains its readiness role;
process metrics describe native work rather than the old Python scheduler.
Migrate deployed alerts using the [native inventory](prism-native-metrics.md)
and [complete alert migration and deployment diff](prism-alert-migration.md).
Queue-pressure intent moves to `qbit_prism_share_ack_seconds` and
`qbit_prism_rejections_total{reason_id}`; Python lease wake delay moves to
`qbit_prism_runtime_lag_seconds`, retained `qbit_prism_runtime_poll_lag_seconds`
and `qbit_prism_runtime_task_stalled`. Refresh impact uses
`qbit_prism_stratum_oldest_pending_initial_job_seconds` and
`qbit_prism_stratum_current_tip_coverage_gap_seconds`; pending-candidate age and
count retain their names. Guard body-based rules with #277's
`qbit_prism_metrics_snapshot_available` / `qbit_prism_metrics_snapshot_stale`
and candidate count/age and RSS rules with `qbit_prism_collector_available` so
unknown -1 is never healthy zero. The pool-wait histogram is read live at scrape
time; its rule requires `up == 1` and sufficient per-instance observations instead
of cached-body or collector availability. Deploy that producer on every target
before activating this rule. The first-offer histogram is declared without samples
(A/#266); advisory-lock waits are recorded since #328; rules for both remain
deferred. D3's dedicated standby alerts require the
primary's deployment-provided PostgreSQL exporter, not public read replica data.
Dashboard totals continue to derive from the shared database. The native
`self-check` emits structured JSON and fails nonzero on an error instead of
printing the Python checker's old PASS/WARN/FAIL table.

The newer block-marker, dual-rate chart, network-hashrate, and reorg-view
contracts are retained. Native block intents are persisted before acceptance,
so the default active block view, chart markers, and found-block totals include
confirmed blocks only. Use `chain_state=all` to inspect every candidate with its
explicit state. Previously landed blocks that disconnect appear as `reversed`
with their disconnection time, while remaining eligible for native reactivation.
Earnings and payout histories remain confirmed-only.

Hashrate history uses the existing three rollup grains and raw tail. Any
frontend may advance the shared watermark; an atomic comparison prevents
concurrent passes from double-counting. `PRISM_HASHRATE_ROLLUP_ENABLED`,
`PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS` (15), and
`PRISM_HASHRATE_ROLLUP_BATCH_SHARES` (50000, maximum 100000) retain their roles.

Bootstrap now pays the solver only when there are no historical shares; there
is no three-miner readiness gate. A network-valid proof below its assigned share
target is credited at network difficulty only after its block is confirmed on
the active chain. These deliberate accounting simplifications were approved
under decision D2 in #260, and belong in the operator release notes. Their
payout effect is recorded in
[Payout differences from 2.x.x (decision D2)](#payout-differences-from-2xx).

`2.x.x` also read `PRISM_STRATUM_SHARE_WEIGHT` (default 1) and <!-- retired-setting: PRISM_STRATUM_SHARE_WEIGHT -->
`PRISM_STRATUM_SHARE_WEIGHTS_JSON` (default empty), a JSON object keyed by miner <!-- retired-setting: PRISM_STRATUM_SHARE_WEIGHTS_JSON -->
username or payout address. A worker with an entry in that object was credited
`max(1, override)` instead of its share target's difficulty; the default weight
only filled the job's delivered `share_weight` field. `3.x.x` does not support
these per-worker credited-difficulty overrides. With the `2.x.x` defaults, which
set no overrides, both versions credit a share at its share target's difficulty,
so crediting is unchanged. A deployment that set either variable must remove it
before cutover, because `3.x.x` ignores it.

<a id="payout-differences-from-2xx"></a>

## Payout differences from 2.x.x (decision D2)

`crates/qbit-prism/fixtures/vectors/` holds money-path vectors exported from
`2.x.x` at `504846cc0b72e8f86ed17f896d4ccbbe196a31dc`. They were computed by the `2.x.x` engine
and rule code, not by hand. `crates/qbit-prism/tests/money_path_vectors.rs`
replays every case through this engine. Window clipping, carry-only recipients,
pool fees, dust recompute, remainder ties, and CTV chunking match `2.x.x`
exactly. Every other difference is one of the entries below, under decision D2 of issue #260. Each
differing case stores both the `2.x.x` and `3.x.x` payout and names its entry
by anchor. The test fails if the anchors the vectors use differ from the
entries below, and it pins every vector file's sha256, so a vector changes only
through a re-export in a reviewed commit that updates the pin. All amounts are
in sats.
All vectors use the day-one floor of 14720 sats.

<a id="d2a-bootstrap-pooling"></a>

### D2a: bootstrap pooling

- **Vector:** `bootstrap_transition.json`, case
  `below-gate-with-other-miners-shares`.
  - miner-a has a 30-difficulty share and miner-b a 20-difficulty share in the ledger.
  - miner-b solves a 500000000-sat block.
- **2.x.x:** miner-b is paid 500000000.
  - Two distinct miners is below the `PRISM_MIN_READY_MINERS` gate (default 3). <!-- retired-setting: PRISM_MIN_READY_MINERS -->
  - So the job is a collection job: one synthetic solver share, and the solver
    is paid the whole coinbase.
- **3.x.x:** miner-a is paid 300000000 and miner-b 200000000.
  - The window is proportional as soon as any share exists.
- **Reason:** `crates/qbit-prism-server/src/coordinator.rs` (bundle selection
  after the ledger snapshot) builds the solver-only bundle only when
  `snapshot.shares` is empty. There is no readiness gate.
- **Matching cases:** `at-readiness-gate` (three miners, prior balances kept on
  both) and `empty-ledger` (the solver is paid everything on both) match `2.x.x`.
- **D2 item:** bootstrap pooling.

<a id="d2b-below-target-credit"></a>

### D2b: below-target block credit

- **Vector:** `below_target_credit.json`, cases `block-only-proof-accepted`,
  `block-only-proof-confirmed-after-reconciliation`, and
  `block-only-proof-reorged-after-acceptance`.
  - The listener floor holds the share target at difficulty 4000000.
  - Network difficulty is 1000000.
  - miner-a submits a proof that meets the network target but not the share
    target.
- **Credited amount:**
  - **2.x.x:** the assigned share difficulty, 4000000.
  - **3.x.x:** network difficulty, 1000000.
- **Next block's payout** (8000000 window; miner-b has 2000000 + 2000000, miner-c 1000000):
  - **2.x.x:** miner-a 250000000, miner-b 187500000, miner-c 62500000.
    - The 9000000 of credited work overfills the window.
    - miner-b's oldest share counts only 1000000.
  - **3.x.x:** miner-a 83333334, miner-b 333333333, miner-c 83333333.
    - The window holds 6000000.
- **Timing:**
  - **2.x.x:** credits at node acceptance of the block.
  - **3.x.x:** credits inside the transaction that marks the block confirmed on the
    active chain.
    - That is either after acceptance or during reconciliation.
    - The Stratum acknowledgement waits for that credit row.
    - A block that is abandoned instead fails the submission with "block-only
      proof was not accepted on the active chain".
    - The acknowledgement waits at most `block_only_ack_timeout`
      (`max(60 s, share_commit_timeout)`), and a candidate still pending then
      is answered `ledger-outcome-unknown` while its credit can still land.
  - On both versions a credited share survives a later reorg.
  - **Modeling assumption:** the `3.x.x` next-block payout assumes the
    deferred credit lands before the next block's anchor, because the credit
    row is stamped at confirmation, not at submission.
- **Reason:**
  - `crates/qbit-prism-server/src/coordinator/miner_submit.rs` credits `network` difficulty
    when `share_pass` is false, and holds the share as the candidate's
    `deferred_share` until the credit row exists.
  - `crates/qbit-prism-server/src/ledger/blocks.rs` (`credit_deferred_share`)
    appends it only on confirmation.
- **Matching cases:** `block-only-proof-rejected` (no credit on either) and
  `share-and-block-proof-control` (a share-passing proof is credited at the
  assigned difficulty on both) match `2.x.x`.
- **D2 item:** below-target credit.

<a id="d2c-prior-balances-during-bootstrap"></a>

### D2c: prior balances during bootstrap

**Status:** approved on 2026-09-11 under decision D2 in #260. This entry is
recorded separately from D2a because it was approved on its own.

- **Vector:** `bootstrap_transition.json`, case
  `bootstrap-carry-only-account-at-or-above-floor`.
  - The share window is empty.
  - miner-old has a 20000-sat carry and no share.
  - miner-b solves a 500000000-sat block.
- **2.x.x:** miner-b is paid 500000000.
  - miner-old is not in the payout manifest and is not paid in this block.
  - Its 20000-sat balance waits for a later block.
- **3.x.x:** miner-old is paid 19999, and carries 1.
  - miner-b is paid 499980001, and carries 19999.
  - The engine allocates the coinbase over both accounts' candidate balances.
- **Reason:**
  - The `2.x.x` collection bundle passes `prior_balances=[]`
    (`lab/prism/job_bundle.py` `build_collection_bundle`, which is `2.x.x`
    code at `504846cc0b72e8f86ed17f896d4ccbbe196a31dc` and does not exist on
    `3.x.x`).
  - `crates/qbit-prism-server/src/coordinator.rs` `build_bundle` passes
    `snapshot.prior_balances` in bootstrap too.
  - So a carry-only account at or above the floor can be paid in a bootstrap
    block.
- **D2 item:** D2c, prior balances during bootstrap.

## Retained mining work

Same-connection active eviction preserves the original worker, target and
version mask, including after reauthorization. The configured per-connection
retention count N bounds both the existing active set and the same-tip graveyard.
A previous parent can temporarily own its former active set plus graveyard;
the graveyard has a conservative total cap of 3N (4N including active work).
Same-tip graveyard TTL starts at eviction. Previous-parent entries require a
published transition and delivery-based grace; unrelated parents and absolute
resume expiries are removed. Original username admission permits remain held
while this retained work can credit, and are released on actual expiry, capacity
eviction, payout replacement or disconnect. Reauthorization reuses the same
connection's permit. Cross-connection resume still requires the exact original
worker, the current published template's parent and payout revision, and an
unexpired absolute job deadline. The durable current-revision fence remains
separate from those original published payout inputs.

## Recovery and rollback

D5 declares a **one-way migration with forward repair and isolated-restore
reconciliation**. There are no down-migrations for native 002–010, including
006/007/008, or permission to remove migration guards. The legacy publication
ordinal revert refuses native schemas and points here. The following boundary
matches the [unreleased 3.0.0 release notes](../doc/release-notes-3.0.0.md):

> Before the first native share is acknowledged, restore the complete
> pre-migration database and artifact backup and restart the pinned old image
> in isolation from the migrated database. After the first native share is
> acknowledged, restoring an older database loses those accepted records.
> Any rollback that discards acknowledged history requires an explicit
> accounting reconciliation and recovery decision; it is not an ordinary
> image rollback. Keep every native frontend stopped while performing an
> isolated restore/recovery operation against its replacement database.

The ACK boundary does not prove that nothing else committed: a committed share
can lose its reply, and candidate/CTV work can progress before admission. Always
compare the complete evidence and retain the latest database. Prefer forward
repair or a compatible native image preserving all durable history.

1. **Establish isolation and retain evidence.** Stop every native frontend,
   public service, candidate worker and CTV broadcaster, and confirm no old
   Python writer is running. Block their network access to the restore. Keep
   the latest database and WAL, the drained pre-migration database backup, all
   external bodies/segments, pinned images, and key/configuration recovery
   material. Never restore over the current database. Record whether any native
   share was acknowledged; an unknown boundary is treated as post-ACK.
2. **Capture the drained source baseline before migration.** Configure named
   libpq services `prism-source`, `prism-current`, and `prism-restore` for the
   distinct databases, with the intended ledger schema first in `search_path`.
   Keep credentials in protected libpq files, not the evidence. The export is a
   single repeatable-read, read-only transaction. Reserve disk for ordered share
   history; the summary hashes it a row at a time. With writers still stopped:

   ```sh
   umask 077
   PGSERVICE=prism-source pg_dump --format=custom --file=pre-native.dump
   PGSERVICE=prism-source psql -XqAt -v ON_ERROR_STOP=1 \
     -f scripts/prism-recovery-evidence.sql > source.rows.jsonl
   python3 scripts/prism-recovery-evidence.py source.rows.jsonl > source.summary.json
   ```

   Keep the filesystem backup from the same drained boundary. Require
   `unfinished_candidates` zero and both integrity mismatch counts zero. The
   summary records ordered share counts/digests, accepted count and high-water
   sequence, block/publication order, audit SHA identities, payout/carry and CTV
   state. `audit_head_sha256` is the exact `2.x.x` active carry hash chain. The
   SQL integrity function alone does **not** calculate that head; the companion
   Python summarizer does. Compare it with the previously mirrored legacy head.
   The checked-in SQL contains the exact reconciliation queries.
3. **Restore and verify the pre-ACK recovery path.** Provision a new empty
   database behind `prism-restore` on an isolated host with the matching
   PostgreSQL version, roles and extensions. Restore the full backup (not a
   selected table dump) and its filesystem backup at the original paths:

   ```sh
   pg_restore --exit-on-error --single-transaction \
     --dbname='service=prism-restore' pre-native.dump
   PGSERVICE=prism-restore psql -XqAt -v ON_ERROR_STOP=1 \
     -f scripts/prism-recovery-evidence.sql > restored.rows.jsonl
   python3 scripts/prism-recovery-evidence.py restored.rows.jsonl > restored.summary.json
   cmp source.summary.json restored.summary.json
   ```

   Require exact equality. Only after it passes may the pinned old image be
   started against this isolated legacy database for recovery verification.
   Keep miner routing and broadcast access disabled. Stop it before rehearsing
   the native migration. Never point the old image at a native schema.
4. **Rehearse forward migration and canonical repair.** Set the native tool's
   `PRISM_DATABASE_URL` to the isolated restore, verify that endpoint, mount the
   recovered artifacts, and supply the original trusted
   `PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX`. The import does not need signing seeds:

   ```sh
   time qbit-prism-server migrate
   time qbit-prism-server import-audits --root /var/lib/qbit-prism/audit
   PGSERVICE=prism-restore psql -XqAt -v ON_ERROR_STOP=1 \
     -f scripts/prism-recovery-evidence.sql > migrated.rows.jsonl
   python3 scripts/prism-recovery-evidence.py migrated.rows.jsonl > migrated.summary.json
   cmp source.summary.json migrated.summary.json
   ```

   Import must exit zero with both completeness counts zero; it reconstructs
   canonical bytes when recoverable bodies have no sidecar. A present corrupt
   sidecar is an error, not permission to use a different body. It commits one
   verified audit at a time; after restoring missing evidence, rerun it safely.
   Check equality before running `backfill-ctv`, which can intentionally repair
   missing CTV rows; retain and explain that repair's later evidence delta.
5. **Verify canonical reads and the public edge.** On the isolated public API,
   fetch stored artifact SHA values from the audit export. Compare downloaded
   bytes with the source backup, their SHA-256, and the quoted SHA ETag. After
   actual cutover repeat through the configured public edge, not only origin:

   ```sh
   # PUBLIC_EDGE is the reviewed HTTPS public endpoint; SHA is a recorded audit SHA.
   curl --fail --silent --show-error -D artifact.headers \
     "$PUBLIC_EDGE/public/v1/artifacts/$SHA" -o artifact.json
   printf '%s  artifact.json\n' "$SHA" | shasum -a 256 -c -
   rg -i "^etag: \"$SHA\"" artifact.headers
   ```

   Require no `x-prism-artifact-canonical-state: missing` fallback header.
   Successful canonical responses omit that header. If the edge still serves a cached
   missing response, invalidate that affected cache entry and verify again
   before admission. Published canonical bytes and hashes must not change.
6. **Reconcile the post-ACK boundary.** Export `prism-current` with the same SQL
   and summarizer into `current.rows.jsonl` and `current.summary.json`. Compare
   with `restored.summary.json`, retaining the ordered JSONL when a digest
   differs. The following exact query on the current database identifies the
   accepted share tail beyond the source high-water mark; substitute the
   recorded integer as the psql variable `source_last_share_seq`:

   ```sql
   SELECT share_seq, share_id, miner_id, share_difficulty, credit_policy
   FROM qbit_share_ledger
   WHERE accepted AND share_seq > :source_last_share_seq
   ORDER BY share_seq;
   SELECT state, count(*) FROM qbit_block_candidate_outbox GROUP BY state ORDER BY state;
   SELECT qbit_carry_forward_integrity_report();
   ```

   Tail counts alone cannot prove equality: reconcile changed block states,
   carry balances/head, payouts, pending candidates and CTV broadcasts against
   chain evidence too. A nonempty native tail demonstrates records an older
   restore would discard. Preserve that database and repair forward. Do not
   copy rows into the legacy schema or resume miners until the accounting owner
   has approved a documented plan preserving or explicitly reconciling every
   difference, including any proposed loss of acknowledged history.
7. **Complete the rehearsal and release gate.** Record backup/restore/migration/
   import/verification durations, row counts, retained evidence locations,
   artifact samples, image digests and the measured outage window. With the
   intended native services available, run production `self-check`; require
   zero completeness and integrity counts and successful readiness. #291 must
   repeat this procedure on production-sized history and retain this exact data
   loss boundary when completing `doc/release-notes-3.0.0.md` before rollout is approved.
