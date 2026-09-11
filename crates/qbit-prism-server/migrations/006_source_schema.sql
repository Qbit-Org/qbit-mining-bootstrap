-- Pin the 2.x.x source schema the migrator accepts, and record what it
-- migrated. The native migrator applies 001_share_ledger.sql and its own
-- numbered migrations; it never applies 2.x.x's 002_candidate_bodies.sql and
-- it does not import #258's chunked candidate bodies. Before any DDL it
-- classifies the database into one of these source states. The same table is
-- data in ledger/migration.rs (SOURCE_STATES); this comment is its record.
--
--   source state   evidence                                             verdict
--   fresh          no 001 or 002 object at all                          accept
--   partial 001    no qbit_share_ledger, but some 001 object present    refuse before any DDL, naming the objects present
--   pre-#258       no qbit_prism_schema_capabilities, no 002 object     accept after the drain check
--   #258 applied   candidate_storage_version = 2, every 002 object     accept after the drain check
--   partial 002    some 002 objects or the capability row, not all      refuse, naming the missing object
--   newer          candidate_storage_version > 2, unknown capability    refuse before any DDL
--   drifted 001    a 001 (or 002) object whose definition, after 001     refuse transactionally, naming the object
--                  has run, differs from the frozen release
--
-- The drain check looks at outbox rows, never at the capability row: 002
-- upserts candidate_storage_version = 2 whatever the 2.x.x writer stored, so
-- the row proves 002 ran, not that v2 rows exist or do not. The same check,
-- with the predicate built from the columns the outbox has, also runs on a
-- database an earlier 3.x.x build migrated to native schema 3, 4 or 5, before
-- 004, 005 and this migration: that build's v1-only predicate never counted
-- a v2 row, so one can still be pending there. Its capability rows are
-- checked before that drain check, and before 004, 005 and this migration
-- (or 009 on a database already at 6) run: a row beyond this release is
-- refused before any DDL, the newer verdict above, so migrate never alters
-- a database a newer release wrote. Connect refuses the same rows again.
--
-- The release definitions come from the migrator applying the same release
-- SQL (001, plus 002 for a #258 source) to a scratch schema under a
-- savepoint in the migration transaction, reading every table, column,
-- constraint, index, trigger, function and sequence it created, and rolling
-- the savepoint back, all before any DDL touches the source.
--
-- A database without qbit_share_ledger (and without any 002 object) is
-- fresh only if it has no table, sequence, index, trigger or function that
-- 001 creates; otherwise it is a partial 001, a selective restore or a
-- hand-installed piece of the schema, refused before any DDL and naming the
-- objects present. Objects the release does not create do not disqualify a
-- fresh database; they are kept and logged.
--
-- The last row is decided after 001_share_ledger.sql has run on the source
-- and before any native DDL: 001 is idempotent and repairs what it
-- re-asserts, but its IF NOT EXISTS leaves an existing table, column, index,
-- sequence or named constraint as it is. The migrator requires an equivalent
-- of every release definition in the source schema, on a fresh database too,
-- where 001 has just created everything. A table or sequence must have the
-- release's persistence: an UNLOGGED or temporary one is drift whatever its
-- columns say, because PostgreSQL truncates it after a crash. A sequence is
-- compared by its structure (type, start, increment, bounds, cache, cycle),
-- never by the value it has reached. A constraint's validation state is
-- compared: a release constraint left NOT VALID in the source is drift,
-- except qbit_share_ledger_credit_policy_check, which 001 itself adds NOT
-- VALID on an upgraded table (NOT_VALID_EXEMPT in ledger/migration.rs, pinned
-- to the frozen release by a test). Column order, comments and
-- auto-generated constraint names are ignored; extra objects are kept and
-- logged; a missing or different one fails the migration, which rolls back
-- whole.

-- What the database came from, written once by the migration that accepted
-- it. Later starts and operators read it; a repeated migrate never rewrites it.
CREATE TABLE IF NOT EXISTS qbit_prism_migration_source (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    source_state text NOT NULL
        CHECK (source_state IN ('fresh', 'pre_258', '258_applied', 'native')),
    source_release text,
    source_commit text,
    candidate_storage_version integer,
    prior_schema_version integer NOT NULL CHECK (prior_schema_version >= 0),
    migrated_by text NOT NULL,
    migrated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Every native database declares its capabilities, so a process refuses a
-- database newer than itself at connect. A #258 source keeps its row at 2;
-- every other source declares the version 1 JSONB candidates native writers
-- produce. DO NOTHING keeps whatever the source declared.
CREATE TABLE IF NOT EXISTS qbit_prism_schema_capabilities (
    capability text PRIMARY KEY,
    capability_value integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('candidate_storage_version', 1)
ON CONFLICT (capability) DO NOTHING;

-- The claim lane decodes a candidate by its storage version. 002 added this
-- column on a #258 source; every other source gets the same column with the
-- same default so one claim statement serves them all. The chunk tables and
-- the body reference are not carried: a v2 pending row is drained before
-- migration, and the claim lane parks any that appears later.
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS storage_version integer NOT NULL DEFAULT 1;
