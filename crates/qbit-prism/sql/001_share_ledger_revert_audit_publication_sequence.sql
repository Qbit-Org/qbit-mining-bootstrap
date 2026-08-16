-- Schema revert for the audit publication ordinal objects installed by
-- 001_share_ledger.sql: the qbit_pool_blocks.audit_publication_sequence
-- column, its validated CHECK constraint and unique index, the
-- qbit_pool_blocks_assign_publication_ordinal trigger and function, and the
-- qbit_audit_publication_sequence_seq allocator.
--
-- This file is the second, belt-and-braces rollback path. The primary
-- rollback path needs no schema change at all: the assignment trigger keeps
-- a pre-ordinal writer's plain chain_state UPDATE satisfying the validated
-- CHECK constraint, so rolling back one release of coordinator code runs
-- correctly against the migrated schema. Apply this file only when the
-- schema itself must return to its pre-ordinal shape: rolling back more
-- than one release, or aligning a live ledger with a restored pre-migration
-- base backup.
--
-- DATA LOSS: dropping the column discards every assigned publication
-- ordinal. Read "Publication Ordinal Rollback And Schema Revert" in
-- docs/prism-ledger-ops.md first; it states what is lost and the required
-- procedure (stop the coordinator, take a backup).
--
-- The revert is one transaction serialized behind the same advisory lock as
-- the forward migration. Any failing step aborts the whole file, so the
-- schema is never left half-reverted. Each step is guarded in the style of
-- the forward migration: re-running this file after a resolved failure, on
-- a partially migrated schema, on a never-migrated schema, or on an
-- already-reverted schema is safe, and the last two are no-ops.

BEGIN;
SELECT pg_advisory_xact_lock(
    hashtext('qbit_audit_publication_sequence_migration')
);

-- Fail before touching anything if the pool-block table is missing, then
-- take the table lock every later step relies on. ACCESS EXCLUSIVE is what
-- DROP COLUMN acquires anyway; taking it first excludes every reader and
-- writer for the whole revert instead of escalating mid-way, and stalls
-- here -- loudly, under any configured lock_timeout -- behind a coordinator
-- that was not stopped, instead of breaking it mid-transaction.
DO $$
DECLARE
    table_namespace text := current_schema();
    table_oid oid;
    discarded_count bigint;
BEGIN
    SELECT pool_blocks.oid
    INTO table_oid
    FROM pg_class pool_blocks
    JOIN pg_namespace namespace
      ON namespace.oid = pool_blocks.relnamespace
    WHERE namespace.nspname = table_namespace
      AND pool_blocks.relname = 'qbit_pool_blocks'
      AND pool_blocks.relkind = 'r';
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'missing qbit_pool_blocks in current schema';
    END IF;
    EXECUTE format(
        'LOCK TABLE %I.qbit_pool_blocks IN ACCESS EXCLUSIVE MODE',
        table_namespace
    );
    IF EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = table_oid
          AND attname = 'audit_publication_sequence'
          AND attnum > 0
          AND NOT attisdropped
    ) THEN
        EXECUTE format(
            'SELECT count(*) FROM %I.qbit_pool_blocks '
            'WHERE audit_publication_sequence IS NOT NULL',
            table_namespace
        ) INTO discarded_count;
        RAISE NOTICE
            'reverting audit publication ordinal schema: discarding % assigned ordinal(s)',
            discarded_count;
    END IF;
END;
$$;

-- 1/5: the CHECK constraint. Dropped first so no step of this file -- even
-- replayed statement by statement outside the transaction -- ever leaves a
-- state that requires confirmed rows to carry an ordinal after anything
-- that assigns one is gone.
DO $$
BEGIN
    EXECUTE format(
        'ALTER TABLE %I.qbit_pool_blocks DROP CONSTRAINT IF EXISTS '
        'qbit_pool_blocks_audit_publication_sequence_check',
        current_schema()
    );
END;
$$;

-- 2/5: the assignment trigger, then its function. The trigger must go
-- before the function it executes; the function drop deliberately omits
-- CASCADE so an unexpected surviving dependency fails the revert instead of
-- being silently destroyed.
DO $$
DECLARE
    table_namespace text := current_schema();
BEGIN
    EXECUTE format(
        'DROP TRIGGER IF EXISTS '
        'qbit_pool_blocks_assign_publication_ordinal '
        'ON %I.qbit_pool_blocks',
        table_namespace
    );
    EXECUTE format(
        'DROP FUNCTION IF EXISTS '
        '%I.qbit_pool_blocks_assign_publication_ordinal()',
        table_namespace
    );
END;
$$;

-- 3/5: the unique index. Dropping the column would remove it implicitly;
-- dropping it explicitly first lets step 4 insist that nothing else still
-- hangs off the column. Refuse a same-named relation that is not an index
-- on qbit_pool_blocks rather than dropping an impostor.
DO $$
DECLARE
    table_namespace text := current_schema();
    table_oid oid;
    index_oid oid;
BEGIN
    SELECT pool_blocks.oid
    INTO table_oid
    FROM pg_class pool_blocks
    JOIN pg_namespace namespace
      ON namespace.oid = pool_blocks.relnamespace
    WHERE namespace.nspname = table_namespace
      AND pool_blocks.relname = 'qbit_pool_blocks'
      AND pool_blocks.relkind = 'r';
    SELECT index_relation.oid
    INTO index_oid
    FROM pg_class index_relation
    JOIN pg_namespace namespace
      ON namespace.oid = index_relation.relnamespace
    WHERE namespace.nspname = table_namespace
      AND index_relation.relname =
          'qbit_pool_blocks_audit_publication_sequence_idx';
    IF index_oid IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index index_definition
        WHERE index_definition.indexrelid = index_oid
          AND index_definition.indrelid = table_oid
    ) THEN
        RAISE EXCEPTION
            'refusing to drop %.qbit_pool_blocks_audit_publication_sequence_idx: not an index on qbit_pool_blocks',
            table_namespace;
    END IF;
    EXECUTE format(
        'DROP INDEX %I.qbit_pool_blocks_audit_publication_sequence_idx',
        table_namespace
    );
END;
$$;

-- 4/5: the column itself. This is the destructive step: every assigned
-- ordinal is discarded with it. The migration's own constraint and index
-- were dropped above and the forward migration refuses duplicates of
-- either, so anything still attached to the column here was created by
-- someone else; enumerate and refuse instead of letting DROP COLUMN
-- silently take auto-dependent indexes or constraints down with it.
-- Dependencies outside the table -- a view over the column, for example --
-- still fail the plain (non-CASCADE) DROP COLUMN itself.
DO $$
DECLARE
    table_namespace text := current_schema();
    table_oid oid;
    ordinal_attnum smallint;
    leftover_dependents text;
BEGIN
    SELECT pool_blocks.oid
    INTO table_oid
    FROM pg_class pool_blocks
    JOIN pg_namespace namespace
      ON namespace.oid = pool_blocks.relnamespace
    WHERE namespace.nspname = table_namespace
      AND pool_blocks.relname = 'qbit_pool_blocks'
      AND pool_blocks.relkind = 'r';
    SELECT attnum
    INTO ordinal_attnum
    FROM pg_attribute
    WHERE attrelid = table_oid
      AND attname = 'audit_publication_sequence'
      AND attnum > 0
      AND NOT attisdropped;
    IF ordinal_attnum IS NULL THEN
        RETURN;
    END IF;
    SELECT string_agg(dependent_name, ', ' ORDER BY dependent_name)
    INTO leftover_dependents
    FROM (
        SELECT index_relation.relname AS dependent_name
        FROM pg_index index_definition
        JOIN pg_class index_relation
          ON index_relation.oid = index_definition.indexrelid
        WHERE index_definition.indrelid = table_oid
          AND (
              ordinal_attnum = ANY (index_definition.indkey)
              OR EXISTS (
                  SELECT 1
                  FROM pg_depend dependency
                  WHERE dependency.classid = 'pg_class'::regclass
                    AND dependency.objid = index_definition.indexrelid
                    AND dependency.refclassid = 'pg_class'::regclass
                    AND dependency.refobjid = table_oid
                    AND dependency.refobjsubid = ordinal_attnum
              )
          )
        UNION
        SELECT table_constraint.conname
        FROM pg_constraint table_constraint
        WHERE table_constraint.conrelid = table_oid
          AND table_constraint.conkey @> ARRAY[ordinal_attnum]
    ) dependents;
    IF leftover_dependents IS NOT NULL THEN
        RAISE EXCEPTION
            'refusing to drop qbit_pool_blocks.audit_publication_sequence: still referenced by %',
            leftover_dependents;
    END IF;
    EXECUTE format(
        'ALTER TABLE %I.qbit_pool_blocks '
        'DROP COLUMN audit_publication_sequence',
        table_namespace
    );
END;
$$;

-- 5/5: the allocator sequence, once nothing assigns from it. Refuse a
-- same-named relation that is not a free-standing sequence: a column-owned
-- sequence here belongs to some other schema arrangement, not to this
-- migration. The plain (non-CASCADE) drop still fails loudly on any other
-- surviving dependency, such as a column DEFAULT.
DO $$
DECLARE
    table_namespace text := current_schema();
    sequence_oid oid;
BEGIN
    SELECT sequence.oid
    INTO sequence_oid
    FROM pg_class sequence
    JOIN pg_namespace namespace
      ON namespace.oid = sequence.relnamespace
    WHERE namespace.nspname = table_namespace
      AND sequence.relname = 'qbit_audit_publication_sequence_seq';
    IF sequence_oid IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE oid = sequence_oid
          AND relkind = 'S'
    ) THEN
        RAISE EXCEPTION
            'refusing to drop %.qbit_audit_publication_sequence_seq: not a sequence',
            table_namespace;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_depend dependency
        WHERE dependency.classid = 'pg_class'::regclass
          AND dependency.objid = sequence_oid
          AND dependency.refclassid = 'pg_class'::regclass
          AND dependency.refobjsubid > 0
          AND dependency.deptype IN ('a', 'i')
    ) THEN
        RAISE EXCEPTION
            'refusing to drop column-owned %.qbit_audit_publication_sequence_seq',
            table_namespace;
    END IF;
    EXECUTE format(
        'DROP SEQUENCE %I.qbit_audit_publication_sequence_seq',
        table_namespace
    );
END;
$$;

-- Restore the pre-ordinal confirmation function. The migrated body assigns
-- nextval() on the sequence dropped above and raises if that sequence is
-- missing, so leaving it installed would break every confirmation for a
-- deployment that applies this revert and then starts pre-ordinal
-- coordinators with PRISM_POSTGRES_INIT_SCHEMA=0. This is the exact
-- pre-migration definition; CREATE OR REPLACE also clears the migration's
-- pinned search_path, which the pre-migration function did not have.
CREATE OR REPLACE FUNCTION qbit_confirm_pool_block(
    confirmed_block_hash text,
    active_tip_height bigint,
    active_writer_id text,
    active_writer_epoch bigint,
    active_writer_session_token text,
    lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    lease_count integer;
    confirmed_count integer;
BEGIN
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + lease_duration,
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = active_writer_id
      AND writer_epoch = active_writer_epoch
      AND writer_session_token = active_writer_session_token;
    GET DIAGNOSTICS lease_count = ROW_COUNT;

    IF lease_count = 0 THEN
        RAISE EXCEPTION 'writer lease is not active';
    END IF;

    UPDATE qbit_pool_blocks
    SET chain_state = 'confirmed'
    WHERE block_hash = confirmed_block_hash
      AND block_height = active_tip_height
      AND chain_state = 'prepared'
      AND maturity_state = 'immature';
    GET DIAGNOSTICS confirmed_count = ROW_COUNT;

    IF confirmed_count = 0
       AND EXISTS (
           SELECT 1
           FROM qbit_pool_blocks
           WHERE block_hash = confirmed_block_hash
             AND block_height = active_tip_height
             AND chain_state = 'confirmed'
             AND maturity_state <> 'reversed'
       ) THEN
        RETURN 1;
    END IF;

    RETURN confirmed_count;
END;
$$;

-- The migration also pinned qbit_reactivate_pool_block's search_path; its
-- body never changed. Clear the pin, when the function exists, to finish
-- restoring the pre-migration shape.
DO $$
DECLARE
    installation_schema text := current_schema();
BEGIN
    IF to_regprocedure(
        format(
            '%I.qbit_reactivate_pool_block('
            'text, bigint, text, bigint, text, interval)',
            installation_schema
        )
    ) IS NOT NULL THEN
        EXECUTE format(
            'ALTER FUNCTION %I.qbit_reactivate_pool_block('
            'pg_catalog.text, pg_catalog.int8, pg_catalog.text, '
            'pg_catalog.int8, pg_catalog.text, pg_catalog.interval) '
            'RESET search_path',
            installation_schema
        );
    END IF;
END;
$$;
COMMIT;
