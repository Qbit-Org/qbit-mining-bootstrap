-- 3.1 dual writer (CONTRACT D-14): the share ledger by node, in share_seq
-- order. Two readers need it, each bounded on (origin_node, share_seq):
--
-- * the peer sync's share pull (D1): the peer's rows after the stream's
--   cursor, origin_node = <peer> AND share_seq > <cursor> ORDER BY share_seq
--   LIMIT <batch>;
-- * the window cut (D2): this node's newest share, max(share_seq) WHERE
--   origin_node = <own> AND accepted, read under ORDER_LOCK.
--
-- Without it each is a walk of the primary key from one end until it meets
-- a row of the node it wants. On a node that has written nothing lately the
-- cut walks the whole ledger under ORDER_LOCK, and every append waits.
--
-- Index only and additive: no capability and no shutdown proof, as 024. A
-- binary that does not know the index never reads it. Applied online, as 013
-- and 024 are (ONLINE_MIGRATIONS in ledger/migration.rs), but on the
-- partitioned ledger of 017, and PostgreSQL cannot build an index of a
-- partitioned table CONCURRENTLY. On an existing ledger the runner
-- (ledger/migration/online.rs) builds one leaf per partition with CREATE
-- INDEX CONCURRENTLY while appends continue, then creates this index ON ONLY
-- the parent and attaches each leaf, catalog work whose locks it takes with
-- a short lock timeout and retries. PostgreSQL marks the index valid when the
-- last partition's leaf is attached, and the runner records 31 then; until
-- then every start refuses the database. `migrate --offline-indexes` builds
-- the leaves with a plain CREATE INDEX instead, in the transaction that
-- records 31, once no instance is live. A fresh or empty source applies this
-- file inside the migration transaction.
--
-- Every leaf is named after its partition and this index,
-- <partition>_origin_seq_idx, as qbit_prism_share_partition_create names the
-- leaves of every partition it creates afterwards, and as the runner names
-- the ones it builds; the loop below renames the ones PostgreSQL names after
-- the columns.
CREATE INDEX qbit_share_ledger_origin_seq_idx
    ON qbit_share_ledger (origin_node, share_seq);

DO $$
DECLARE
    leaf record;
BEGIN
    FOR leaf IN
        SELECT child.relname AS child_name, part.relname AS partition_name
        FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_index x ON x.indexrelid = child.oid
        JOIN pg_class part ON part.oid = x.indrelid
        WHERE i.inhparent = 'qbit_share_ledger_origin_seq_idx'::regclass
    LOOP
        IF leaf.child_name <> leaf.partition_name || '_origin_seq_idx' THEN
            EXECUTE format('ALTER INDEX %I RENAME TO %I',
                leaf.child_name, leaf.partition_name || '_origin_seq_idx');
        END IF;
    END LOOP;
END $$;
