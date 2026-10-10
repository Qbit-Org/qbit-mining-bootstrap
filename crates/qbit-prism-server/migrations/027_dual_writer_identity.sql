-- 3.1 dual writer, node identity (D1): two nodes, A (0) and B (1), each with
-- its own writable PostgreSQL, each pulling the rows the other originated
-- by identity. Additive only: no capability and no shutdown proof. A binary
-- that does not know these columns and tables never names them, and every
-- new column has a constant default or none, so adding it rewrites no table
-- and validates nothing by a scan: each ALTER holds its table's lock only for
-- the catalog change, until the migration commits. The share ledger and its
-- header mappings are altered last, so a frontend still serving waits on them
-- only from that last step to the commit. With PRISM_DUAL_WRITER off nothing
-- reads or writes any of it except the defaults below, which leave every
-- existing statement's result unchanged.
--
-- 1. origin_node on every table the peer sync copies: the node that wrote
--    the row, 0 for A and 1 for B. Every row from before 3.1 is node 0.
--    `qbit-prism-server node-identity set` personalises a database as one
--    node (ledger/node_identity.rs): it sets these defaults to the node's
--    index, so no INSERT names the column, and the peer sync names it,
--    copying the peer's value. No CHECK: on the share ledger one would scan
--    every partition under the lock; personalisation and the sync only ever
--    write 0 or 1. The share ledger and the header mappings are at the end of
--    this file.
ALTER TABLE qbit_pool_blocks ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_prism_audit_snapshots ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_pool_audit_bundles ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_pool_payout_entries ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_payout_carry_forward ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_ctv_fanout_sets ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_ctv_fanout_artifacts ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_prism_templates ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_prism_balance_snapshots ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_prism_jobs ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;

-- 2. The pull order of the two streams that are not shares. A landed block
--    (its qbit_pool_blocks row and the audit, payout, carry-forward and
--    fanout rows its landing writes in the same transaction) and a prepared
--    job (its qbit_prism_jobs row and the template and balance blobs it
--    names) are each pulled as one unit, in sync_seq order of the root row.
--    The share stream needs no such column: share_seq is allocated and
--    committed under ORDER_LOCK, so its order is commit order.
--
--    These writers hold no lock that orders their commits, so a pull must
--    not pass a number whose transaction may still commit. The default takes
--    a shared transaction-scoped advisory lock (0x505249534d000008, the sync
--    barrier) before it takes the number, and keeps it until the transaction
--    ends. The puller calls qbit_prism_sync_barrier(), one statement that
--    tries the same lock exclusively, transaction-scoped, never waiting for
--    it, and reads the sequence while it holds it: every row at or below the
--    position read has committed or never will. The lock goes with the
--    statement's own transaction, which a cancelled statement or a vanished
--    client ends too, so the puller can never leave it held, and a writer
--    waits on it for that one statement at most. The barrier is per
--    database, so no transaction elsewhere on the server holds a pull back.
--    Rows from before 027 keep NULL and are never pulled: both databases of a
--    pair start from one ledger and hold them already.
CREATE SEQUENCE IF NOT EXISTS qbit_prism_sync_seq AS bigint;

CREATE OR REPLACE FUNCTION qbit_prism_next_sync_seq()
RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(5787769093247467528);
    RETURN nextval('qbit_prism_sync_seq');
END;
$$;

-- taken: whether no transaction that drew a sync_seq was open;
-- sync_position: the sequence's last value then, NULL before the first draw.
CREATE OR REPLACE FUNCTION qbit_prism_sync_barrier(OUT taken boolean, OUT sync_position bigint)
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    taken := pg_try_advisory_xact_lock(5787769093247467528);
    IF taken THEN
        SELECT CASE WHEN is_called THEN last_value END INTO sync_position FROM qbit_prism_sync_seq;
    END IF;
END;
$$;

ALTER TABLE qbit_pool_blocks ADD COLUMN IF NOT EXISTS sync_seq bigint;
ALTER TABLE qbit_pool_blocks ALTER COLUMN sync_seq SET DEFAULT qbit_prism_next_sync_seq();
CREATE INDEX IF NOT EXISTS qbit_pool_blocks_origin_sync_idx
    ON qbit_pool_blocks (origin_node, sync_seq) WHERE sync_seq IS NOT NULL;

ALTER TABLE qbit_prism_jobs ADD COLUMN IF NOT EXISTS sync_seq bigint;
ALTER TABLE qbit_prism_jobs ALTER COLUMN sync_seq SET DEFAULT qbit_prism_next_sync_seq();
CREATE INDEX IF NOT EXISTS qbit_prism_jobs_prepared_origin_sync_idx
    ON qbit_prism_jobs (origin_node, sync_seq)
    WHERE sync_seq IS NOT NULL AND job_id LIKE 'prepared:%';

--    Each node's expired jobs by its own range: a dual writer keeps the
--    peer's prepared records a day past their expiry (CONTRACT D-3), so the
--    expiry prune of one node's rows would otherwise walk the other's in the
--    expiry index. The expiry prune keeps a single writer's table to its
--    unexpired jobs, so the build is short, and the table is locked from the
--    first ALTER above to the commit either way.
CREATE INDEX IF NOT EXISTS qbit_prism_jobs_origin_expiry_idx
    ON qbit_prism_jobs (origin_node, expires_at);

-- 3. The carry-owner journal (CONTRACT D-4): which node pays down carried
--    balances, as each node last recorded it. A node's current role is its
--    highest-epoch row; epochs are pair-wide (a new row takes the highest
--    epoch either node holds, plus one). The carry-owner guard and its
--    transfer command write the rows (seed, release, acquire). Copied by the
--    peer sync like every table above, recovered with the own log after a
--    restore, and readable by the peer's sync role. Append-only.
CREATE TABLE IF NOT EXISTS qbit_prism_node_roles (
    origin_node smallint NOT NULL DEFAULT 0 CHECK (origin_node IN (0, 1)),
    epoch bigint NOT NULL CHECK (epoch >= 0),
    carry_owner boolean NOT NULL,
    action text NOT NULL CHECK (action IN ('seed', 'release', 'acquire')),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    recorded_by text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(detail) = 'object'),
    PRIMARY KEY (origin_node, epoch)
);

CREATE OR REPLACE FUNCTION qbit_prism_preserve_node_roles()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'qbit_prism_node_roles is an append-only journal: record a new epoch instead';
END;
$$;
DROP TRIGGER IF EXISTS qbit_prism_node_roles_append_only ON qbit_prism_node_roles;
CREATE TRIGGER qbit_prism_node_roles_append_only BEFORE UPDATE OR DELETE OR TRUNCATE
    ON qbit_prism_node_roles FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_node_roles();

-- 4. Which node this database is (CONTRACT D-9). Empty until
--    `qbit-prism-server node-identity set --index N` personalises the
--    database during the bootstrap or cutover; a frontend never writes it.
--    In dual mode a frontend whose PRISM_NODE_INDEX this row does not name,
--    or a database without it, fails closed. Local state: never copied, and
--    not part of own-log recovery.
CREATE TABLE IF NOT EXISTS qbit_prism_node_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    node_index smallint NOT NULL CHECK (node_index IN (0, 1)),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    recorded_by text NOT NULL DEFAULT session_user
);

-- 5. This node's own log (CONTRACT D-8, D-17). The floors are where its own
--    share_seq and sync_seq stood when it was personalised: every row it
--    originates afterwards is above them, so the peer's first pull of this
--    node's rows starts there at the latest. The verification records the
--    server this database ran on when the own-log check last proved every
--    row this node originated present here: its system identifier and WAL
--    timeline. A restore or promotion starts a new timeline and an initdb a
--    new identifier; either, or no record, means the check must pass against
--    the peer again before the node serves. A plain restart changes neither.
--    peer_tail_lost_at is an operator's declaration, during a long peer
--    outage, that the peer's rows this node has not pulled are lost and the
--    peer will be rebuilt from this node (docs/prism-ledger-ops.md). Until it
--    is set, the share archive stops at the safe peer mark (6), since a peer
--    row can still arrive above it; while it is set it does not. Local
--    state, never copied; the peer's sync role reads the floors.
CREATE TABLE IF NOT EXISTS qbit_prism_node_lineage (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    share_seq_floor bigint NOT NULL,
    sync_seq_floor bigint NOT NULL,
    personalised_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    verified_system_identifier bigint,
    verified_timeline integer,
    verified_at timestamptz,
    peer_tail_lost_at timestamptz,
    CHECK (num_nulls(verified_system_identifier, verified_timeline, verified_at) IN (0, 3))
);

-- 6. How far this node has pulled each stream of the peer's rows: the
--    position scanned through, in the stream's key on the peer (share_seq
--    for 'shares', sync_seq for 'blocks' and 'prepared'), and the highest
--    peer-originated key inserted. Written in the transaction that inserts
--    the rows, so a restore rewinds both together and the pull repeats what
--    the restore lost. Local state, never copied; the peer's sync role reads
--    it when the peer recovers its own log.
CREATE TABLE IF NOT EXISTS qbit_prism_peer_sync_cursors (
    stream text PRIMARY KEY CHECK (stream IN ('shares', 'blocks', 'prepared')),
    peer_node smallint NOT NULL CHECK (peer_node IN (0, 1)),
    scanned_through bigint NOT NULL,
    ingested_through bigint,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- The safe peer mark (CONTRACT D-14): every peer-originated share row with a
-- share_seq at or below it is committed in this database, and none below it
-- can arrive later, because the shares stream is pulled in share_seq order
-- and the mark is written in the transaction that inserts the rows it covers.
-- NULL before the first pull: no peer row is eligible.
CREATE OR REPLACE FUNCTION qbit_prism_peer_share_mark()
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT scanned_through FROM qbit_prism_peer_sync_cursors WHERE stream = 'shares';
$$;

-- 7. Rows the sync found under an identity this node already holds with
--    different content. The local row is never overwritten; each conflict
--    is recorded once, counted on every sighting, and alerted. Local state,
--    never copied.
CREATE TABLE IF NOT EXISTS qbit_prism_peer_sync_conflicts (
    source_table text NOT NULL,
    row_key text NOT NULL,
    origin_node smallint NOT NULL CHECK (origin_node IN (0, 1)),
    detail text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    seen_count bigint NOT NULL DEFAULT 1 CHECK (seen_count > 0),
    PRIMARY KEY (source_table, row_key)
);

-- 8. Last, the share ledger and its header mappings (see 1): appends wait on
--    these two from here to the commit only. The mappings go first, in the
--    order an append takes them: its first statement reads the mapping of
--    its header, and only later ones read and write the ledger. The other
--    order could deadlock with an append waiting between the two.
ALTER TABLE qbit_prism_share_hashes ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
ALTER TABLE qbit_share_ledger ADD COLUMN IF NOT EXISTS origin_node smallint NOT NULL DEFAULT 0;
