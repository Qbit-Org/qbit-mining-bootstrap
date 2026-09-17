-- Everything the share ledger partitioning (#144, migration 017) needs
-- before the table changes shape, applied inside the migration transaction.
--
-- 1. The two foreign keys onto qbit_share_ledger(share_id) go. A unique
--    index on a partitioned table must include the partition key, so the
--    global share_id UNIQUE cannot survive 017, and no foreign key can
--    reference share_id afterwards. Both keys also pin every partition
--    holding a referenced row: PostgreSQL re-validates inbound foreign keys
--    at DETACH, and the outbox is never pruned, so one retained row would
--    pin the partition that holds its block's solving share forever. The
--    outbox keeps its UNIQUE (share_id); qbit_prism_share_hashes keeps its
--    UNIQUE (share_id) and its header_hash primary key, and becomes the
--    global uniqueness authority the native append consults first.
--    Schema rule from here on: no object may carry a foreign key to
--    qbit_share_ledger.
--
-- 2. The partition catalog. qbit_prism_share_partitioning holds the one
--    row of settings the maintenance function reads (the partition width in
--    rows and how many partitions to keep ahead of the sequence).
--    qbit_prism_share_partitions records each partition's bounds and
--    lifecycle: attached, sealed (every audit row whose snapshot intersects
--    it stores its canonical bytes), archived (written and verified outside
--    PostgreSQL), detached, dropped. PostgreSQL knows the attached set and
--    bounds; the catalog adds what happened to a partition after it left.
--
-- 3. The maintenance functions. qbit_prism_share_partition_ensure() keeps
--    lead_partitions empty partitions attached ahead of the sequence, each
--    created as a standalone table (LIKE the parent, with a validated bound
--    CHECK, a per-leaf UNIQUE (share_id) and the immutability trigger) and
--    then attached, which takes only SHARE UPDATE EXCLUSIVE on the parent:
--    appends and reads continue. qbit_prism_share_ledger_convert_prepare()
--    and qbit_prism_share_ledger_convert_swap() are the two halves of 017's
--    conversion, defined here so the online runner and the transactional
--    path run the same code.
--
-- 4. Solver attribution moves onto qbit_pool_blocks. Four dashboard queries
--    found each block's solving share by suffix-matching every block's hash
--    against the ledger on every request; after a detach that lookup would
--    silently blank a historical block's solver. The columns are backfilled
--    here, written at landing from now on, and the queries read them.

ALTER TABLE qbit_block_candidate_outbox
    DROP CONSTRAINT IF EXISTS qbit_block_candidate_outbox_share_id_fkey;
ALTER TABLE qbit_prism_share_hashes
    DROP CONSTRAINT IF EXISTS qbit_prism_share_hashes_share_id_fkey;

-- Rejected rows do not reserve a header hash, but their exact IDs must not
-- become reusable when their partition leaves. Verification fills this
-- registry before certifying a partition for detach; fresh archive imports
-- fill it atomically with attachment. Keep these rows after detach/drop.
-- No ledger FK: retaining an ID must not pin its partition.
CREATE TABLE qbit_prism_rejected_share_ids (
    share_id text PRIMARY KEY,
    share_seq bigint NOT NULL
);

CREATE TABLE qbit_prism_share_partitioning (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    -- Rows per partition. 2^24 rows is about 7.5 GB of heap plus about
    -- 20 GB of indexes at the production row shape, five days at 39
    -- shares/s and nine hours at 500 shares/s. Changing it affects
    -- partitions created afterwards only: bounds already attached stay,
    -- the next partition starts where the last one ends, and names come
    -- from a counter in the catalog, never from the width.
    partition_rows bigint NOT NULL DEFAULT 16777216
        CHECK (partition_rows >= 1048576 AND partition_rows <= 1073741824),
    -- Empty partitions kept attached ahead of the sequence.
    lead_partitions integer NOT NULL DEFAULT 4
        CHECK (lead_partitions >= 1 AND lead_partitions <= 64),
    -- The exclusive upper bound chosen for the release table when the
    -- conversion was prepared, and when the swap completed.
    conversion_bound bigint CHECK (conversion_bound > 0),
    converted_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO qbit_prism_share_partitioning(singleton) VALUES (true)
ON CONFLICT DO NOTHING;

CREATE TABLE qbit_prism_share_partitions (
    partition_name text PRIMARY KEY,
    -- NULL is MINVALUE: the release table attached as the first partition.
    lower_seq bigint,
    upper_seq bigint NOT NULL,
    state text NOT NULL DEFAULT 'attached'
        CHECK (state IN ('attached', 'detached', 'dropped')),
    attached_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sealed_at timestamptz,
    archive_uri text,
    archive_manifest_sha256 text
        CHECK (archive_manifest_sha256 IS NULL OR archive_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    archive_rows bigint CHECK (archive_rows IS NULL OR archive_rows >= 0),
    archive_rows_sha256 text
        CHECK (archive_rows_sha256 IS NULL OR archive_rows_sha256 ~ '^[0-9a-f]{64}$'),
    archived_at timestamptz,
    archive_verified_at timestamptz,
    detached_at timestamptz,
    dropped_at timestamptz,
    CHECK (lower_seq IS NULL OR lower_seq < upper_seq),
    CHECK ((archive_uri IS NULL) = (archive_manifest_sha256 IS NULL)),
    CHECK ((archive_uri IS NULL) = (archived_at IS NULL)),
    CHECK (archive_verified_at IS NULL OR archived_at IS NOT NULL),
    CHECK (state <> 'detached' OR detached_at IS NOT NULL),
    CHECK (state <> 'dropped' OR (detached_at IS NOT NULL AND dropped_at IS NOT NULL)),
    -- A partition is only ever detached after it was archived and verified.
    CHECK (state = 'attached' OR archive_verified_at IS NOT NULL)
);

ALTER TABLE qbit_pool_blocks
    ADD COLUMN IF NOT EXISTS solver_miner_id text,
    ADD COLUMN IF NOT EXISTS solver_share_id text,
    ADD COLUMN IF NOT EXISTS solver_share_difficulty numeric(78, 0)
        CHECK (solver_share_difficulty IS NULL OR solver_share_difficulty > 0),
    ADD COLUMN IF NOT EXISTS solver_network_difficulty numeric(78, 0)
        CHECK (solver_network_difficulty IS NULL OR solver_network_difficulty > 0);
-- Old frontends can keep landing blocks during online conversion without
-- naming the new columns. Capture their solver at insertion as well as in
-- the initial backfill, before retention can remove the supporting shares.
CREATE FUNCTION qbit_prism_capture_block_solver()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.solver_share_id IS NULL THEN
        SELECT share.miner_id, share.share_id, share.share_difficulty, share.network_difficulty
        INTO NEW.solver_miner_id, NEW.solver_share_id, NEW.solver_share_difficulty, NEW.solver_network_difficulty
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = NEW.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER qbit_pool_blocks_capture_solver
BEFORE INSERT ON qbit_pool_blocks
FOR EACH ROW EXECUTE FUNCTION qbit_prism_capture_block_solver();

-- Backfill from the ledger, one suffix-index probe per block, exactly the
-- lookup the dashboard queries performed on every request.
UPDATE qbit_pool_blocks block
SET solver_miner_id = solver.miner_id,
    solver_share_id = solver.share_id,
    solver_share_difficulty = solver.share_difficulty,
    solver_network_difficulty = solver.network_difficulty
FROM (
    SELECT found.block_hash, share.miner_id, share.share_id, share.share_difficulty, share.network_difficulty
    FROM qbit_pool_blocks found
    CROSS JOIN LATERAL (
        SELECT share.miner_id, share.share_id, share.share_difficulty, share.network_difficulty
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = found.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) share
    WHERE found.solver_share_id IS NULL
) solver
WHERE solver.block_hash = block.block_hash;

-- The next value the share sequence will hand out.
CREATE FUNCTION qbit_prism_share_next_seq()
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END
    FROM qbit_share_ledger_share_seq_seq;
$$;

-- The share_seq floor a share_id probe may carry: the lower bound of the
-- attached partition two below the one the next share_seq lands in, read
-- from the catalog. A probe on the partitioned ledger then descends the
-- per-leaf indexes of at most three partitions that hold rows (plus the
-- empty lead) instead of one per attached partition, whatever
-- partition_rows was when each partition was created. Subtracting two
-- current widths from the sequence would not do: raise partition_rows
-- after narrower partitions were attached and that floor spans every one
-- of them still attached. The floor is always at least two full
-- partitions of rows below the sequence; a replay the native coordinator
-- retries is seconds old, and a share older than the floor is answered by
-- qbit_prism_share_hashes, the global authority. Before the conversion no
-- partition is cataloged and there is no floor.
CREATE FUNCTION qbit_prism_share_probe_floor()
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT COALESCE((
        SELECT COALESCE(lower_seq, 0)
        FROM qbit_prism_share_partitions
        WHERE state = 'attached'
          AND (lower_seq IS NULL OR lower_seq <= qbit_prism_share_next_seq())
        ORDER BY upper_seq DESC
        OFFSET 2 LIMIT 1
    ), 0);
$$;

-- Create one standalone table shaped like the ledger for [lower, upper),
-- with the per-leaf share_id uniqueness, the validated bound, and the
-- immutability trigger, then attach it. The bound is validated on the
-- empty table, so ATTACH proves it without a scan and holds only SHARE
-- UPDATE EXCLUSIVE on the parent. Names are reserved: a relation already
-- under the partition's name is refused, never adopted.
CREATE FUNCTION qbit_prism_share_partition_create(
    partition_name text,
    lower_seq bigint,
    upper_seq bigint
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    leaf_index record;
    desired text;
BEGIN
    IF to_regclass(partition_name) IS NOT NULL THEN
        RAISE EXCEPTION 'refusing to create share ledger partition %: a relation already holds that name', partition_name
            USING ERRCODE = 'duplicate_table';
    END IF;
    EXECUTE format(
        'CREATE TABLE %I (LIKE qbit_share_ledger INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)',
        partition_name
    );
    EXECUTE format(
        'ALTER TABLE %I ADD CONSTRAINT %I UNIQUE (share_id)',
        partition_name, partition_name || '_share_id_key'
    );
    EXECUTE format(
        'ALTER TABLE %I ADD CONSTRAINT %I CHECK (share_seq >= %s AND share_seq < %s)',
        partition_name, partition_name || '_bound', lower_seq, upper_seq
    );
    EXECUTE format(
        'CREATE TRIGGER qbit_prism_immutable_share_history BEFORE UPDATE OR DELETE OR TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_audit_history()',
        partition_name
    );
    EXECUTE format(
        'ALTER TABLE qbit_share_ledger ATTACH PARTITION %I FOR VALUES FROM (%s) TO (%s)',
        partition_name, lower_seq, upper_seq
    );
    -- LIKE names the copied indexes after their columns; name each leaf
    -- index after the parent index it now belongs to.
    FOR leaf_index IN
        SELECT child.relname AS child_name, parent.relname AS parent_name
        FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_index x ON x.indexrelid = child.oid
        WHERE child.relkind = 'i' AND parent.relkind = 'I'
          AND x.indrelid = to_regclass(partition_name)
    LOOP
        desired := partition_name || substr(leaf_index.parent_name, length('qbit_share_ledger') + 1);
        IF leaf_index.child_name <> desired THEN
            EXECUTE format('ALTER INDEX %I RENAME TO %I', leaf_index.child_name, desired);
        END IF;
    END LOOP;
    INSERT INTO qbit_prism_share_partitions(partition_name, lower_seq, upper_seq)
    VALUES (partition_name, lower_seq, upper_seq);
END;
$$;

-- The number the next partition is named with: one above the highest
-- qbit_share_ledger_p<n> the catalog has ever recorded, in any state. A
-- name is never reused, so a partition that was detached and dropped keeps
-- its name reserved for its archive, and the names stay in bound order
-- however partition_rows changes over time. Deriving the number from the
-- bound divided by the width would map a bound back onto an existing name
-- as soon as the width changed.
CREATE FUNCTION qbit_prism_share_partition_next_number()
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT COALESCE(max(substring(partition_name FROM '^qbit_share_ledger_p(\d+)$')::bigint), -1) + 1
    FROM qbit_prism_share_partitions;
$$;

-- Keep lead_partitions partitions attached above the next share_seq.
-- Returns how many were created. Serialized per schema with the online
-- migration runner's lock class, so two frontends never race on one name
-- and no partition is created while a conversion swaps the table. Before
-- the conversion it does nothing. Each new partition is partition_rows
-- wide and starts at the highest attached upper bound, so the bounds are
-- contiguous; with the width never changed they are the grid cells
-- [k*rows, (k+1)*rows). Names are qbit_share_ledger_p<n> with n from
-- qbit_prism_share_partition_next_number(); the release table converted by
-- 017 is qbit_share_ledger_p0 whatever its upper bound.
CREATE FUNCTION qbit_prism_share_partition_ensure()
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    settings qbit_prism_share_partitioning%ROWTYPE;
    covered bigint;
    target bigint;
    created integer := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(1347571027, hashtext(current_schema()));
    SELECT * INTO settings FROM qbit_prism_share_partitioning WHERE singleton;
    IF settings.converted_at IS NULL THEN
        RETURN 0;
    END IF;
    SELECT max(upper_seq) INTO covered
    FROM qbit_prism_share_partitions WHERE state = 'attached';
    IF covered IS NULL THEN
        RAISE EXCEPTION 'share ledger is converted but no partition is attached in qbit_prism_share_partitions';
    END IF;
    target := qbit_prism_share_next_seq() + settings.lead_partitions * settings.partition_rows;
    WHILE covered < target LOOP
        PERFORM qbit_prism_share_partition_create(
            'qbit_share_ledger_p' || qbit_prism_share_partition_next_number(),
            covered,
            covered + settings.partition_rows
        );
        covered := covered + settings.partition_rows;
        created := created + 1;
    END LOOP;
    RETURN created;
END;
$$;

-- First half of the conversion (017). On the still-plain release table,
-- add CHECK (share_seq < bound) NOT VALID, where bound is the smallest
-- partition grid boundary at least headroom rows above the next share_seq,
-- and record it. Returns the bound. The constraint is enforced on new rows
-- at once and validated separately (ALTER TABLE ... VALIDATE CONSTRAINT,
-- one scan under SHARE UPDATE EXCLUSIVE, appends continue). With it
-- validated, ATTACH PARTITION needs no scan. The headroom is what keeps
-- appends inside the bound until the swap: rows keep landing in the release
-- table until the sequence reaches the bound, then in the next cell; the
-- sequence is never restarted, so share_seq stays gap-free. Called again
-- while the constraint is pending, it returns the existing bound, unless
-- the headroom has shrunk below one partition, in which case the pending
-- constraint is replaced by one with a new bound and must be validated
-- again.
CREATE FUNCTION qbit_prism_share_ledger_convert_prepare(headroom_rows bigint DEFAULT NULL)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    settings qbit_prism_share_partitioning%ROWTYPE;
    existing record;
    next_seq bigint;
    bound bigint;
    headroom bigint;
BEGIN
    SELECT * INTO settings FROM qbit_prism_share_partitioning WHERE singleton;
    IF settings.converted_at IS NOT NULL THEN
        RAISE EXCEPTION 'share ledger is already converted';
    END IF;
    IF (SELECT relkind FROM pg_class WHERE oid = 'qbit_share_ledger'::regclass) <> 'r' THEN
        RAISE EXCEPTION 'qbit_share_ledger is not a plain table';
    END IF;
    headroom := COALESCE(headroom_rows, 2 * settings.partition_rows);
    next_seq := qbit_prism_share_next_seq();
    SELECT k.conname, k.convalidated,
           substring(pg_get_constraintdef(k.oid) FROM 'share_seq < (\d+)')::bigint AS bound
    INTO existing
    FROM pg_constraint k
    WHERE k.conrelid = 'qbit_share_ledger'::regclass
      AND k.conname = 'qbit_share_ledger_p0_bound';
    IF FOUND THEN
        IF existing.bound IS NULL THEN
            RAISE EXCEPTION 'qbit_share_ledger_p0_bound exists with an unexpected definition: %',
                (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'qbit_share_ledger'::regclass AND conname = 'qbit_share_ledger_p0_bound');
        END IF;
        IF existing.bound - next_seq >= settings.partition_rows THEN
            RETURN existing.bound;
        END IF;
        -- Too little headroom left for the swap: bound again, further out.
        ALTER TABLE qbit_share_ledger DROP CONSTRAINT qbit_share_ledger_p0_bound;
    END IF;
    bound := ((next_seq + headroom + settings.partition_rows - 1) / settings.partition_rows)
             * settings.partition_rows;
    EXECUTE format(
        'ALTER TABLE qbit_share_ledger ADD CONSTRAINT qbit_share_ledger_p0_bound CHECK (share_seq < %s) NOT VALID',
        bound
    );
    UPDATE qbit_prism_share_partitioning
    SET conversion_bound = bound, updated_at = clock_timestamp()
    WHERE singleton;
    RETURN bound;
END;
$$;

-- Validate every CHECK constraint the release table still carries as NOT
-- VALID: the bound above, and any the release left pending (001 adds
-- qbit_share_ledger_credit_policy_check NOT VALID where the column was
-- added later, the one exemption the migrator tolerates). ATTACH refuses
-- a partition whose constraint is NOT VALID while the parent's is
-- validated, so each is one scan under SHARE UPDATE EXCLUSIVE before the
-- swap; appends and reads continue. Returns the names validated.
CREATE FUNCTION qbit_prism_share_ledger_convert_validate()
RETURNS text[] LANGUAGE plpgsql AS $$
DECLARE
    pending text;
    validated text[] := ARRAY[]::text[];
BEGIN
    IF (SELECT relkind FROM pg_class WHERE oid = 'qbit_share_ledger'::regclass) <> 'r' THEN
        RAISE EXCEPTION 'qbit_share_ledger is not a plain table';
    END IF;
    FOR pending IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'qbit_share_ledger'::regclass AND contype = 'c' AND NOT convalidated
        ORDER BY conname
    LOOP
        EXECUTE format('ALTER TABLE qbit_share_ledger VALIDATE CONSTRAINT %I', pending);
        validated := validated || pending;
    END LOOP;
    RETURN validated;
END;
$$;

-- Second half of the conversion (017): one transaction of catalog work.
-- Requires the bound validated. Renames the release table and its indexes
-- to their _p0 names, creates the partitioned parent LIKE it with the same
-- constraint names, gives the parent the primary key and the four
-- secondary indexes under the release names, moves the sequence and the
-- grants over, attaches the release table as [MINVALUE, bound) (adopting
-- its indexes, no build), recreates the one release function that returned
-- the old row type, installs the immutability trigger on the parent,
-- records the catalog and creates the lead partitions. Every name it
-- creates is checked first, so a refusal changes nothing.
CREATE FUNCTION qbit_prism_share_ledger_convert_swap()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    settings qbit_prism_share_partitioning%ROWTYPE;
    bound bigint;
    validated boolean;
    reserved text;
    index_name text;
    old_indexes oid[];
    grant_row record;
    p0_indexes integer;
BEGIN
    PERFORM pg_advisory_xact_lock(1347571027, hashtext(current_schema()));
    SELECT * INTO settings FROM qbit_prism_share_partitioning WHERE singleton;
    IF settings.converted_at IS NOT NULL THEN
        RAISE EXCEPTION 'share ledger is already converted';
    END IF;
    IF (SELECT relkind FROM pg_class WHERE oid = 'qbit_share_ledger'::regclass) <> 'r' THEN
        RAISE EXCEPTION 'qbit_share_ledger is not a plain table';
    END IF;
    SELECT k.convalidated,
           substring(pg_get_constraintdef(k.oid) FROM 'share_seq < (\d+)')::bigint
    INTO validated, bound
    FROM pg_constraint k
    WHERE k.conrelid = 'qbit_share_ledger'::regclass
      AND k.conname = 'qbit_share_ledger_p0_bound';
    IF NOT FOUND OR bound IS NULL THEN
        RAISE EXCEPTION 'qbit_share_ledger_p0_bound is missing; run qbit_prism_share_ledger_convert_prepare() first';
    END IF;
    IF NOT validated THEN
        RAISE EXCEPTION 'qbit_share_ledger_p0_bound is not validated; run qbit_prism_share_ledger_convert_validate() first';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'qbit_share_ledger'::regclass AND contype = 'c' AND NOT convalidated) THEN
        RAISE EXCEPTION 'qbit_share_ledger carries a NOT VALID CHECK constraint (%); run qbit_prism_share_ledger_convert_validate() first',
            (SELECT string_agg(conname, ', ' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'qbit_share_ledger'::regclass AND contype = 'c' AND NOT convalidated);
    END IF;
    IF bound <> settings.conversion_bound THEN
        RAISE EXCEPTION 'qbit_share_ledger_p0_bound reads % but the catalog recorded %', bound, settings.conversion_bound;
    END IF;
    IF qbit_prism_share_next_seq() >= bound THEN
        RAISE EXCEPTION 'the share sequence has reached the conversion bound %; prepare again with more headroom', bound;
    END IF;
    FOREACH reserved IN ARRAY ARRAY[
        'qbit_share_ledger_p0', 'qbit_share_ledger_p0_pkey', 'qbit_share_ledger_p0_share_id_key',
        'qbit_share_ledger_p0_accepted_recent_idx', 'qbit_share_ledger_p0_accepted_block_suffix_idx',
        'qbit_share_ledger_p0_accepted_seq_walk_idx', 'qbit_share_ledger_p0_accepted_miner_history_idx'
    ] LOOP
        IF to_regclass(reserved) IS NOT NULL THEN
            RAISE EXCEPTION 'refusing to convert the share ledger: a relation already holds the reserved name %', reserved;
        END IF;
    END LOOP;
    FOREACH index_name IN ARRAY ARRAY[
        'qbit_share_ledger_pkey', 'qbit_share_ledger_share_id_key',
        'qbit_share_ledger_accepted_recent_idx', 'qbit_share_ledger_accepted_block_suffix_idx',
        'qbit_share_ledger_accepted_seq_walk_idx', 'qbit_share_ledger_accepted_miner_history_idx'
    ] LOOP
        IF to_regclass(index_name) IS NULL
           OR (SELECT x.indrelid FROM pg_index x WHERE x.indexrelid = to_regclass(index_name)) <> 'qbit_share_ledger'::regclass THEN
            RAISE EXCEPTION 'refusing to convert the share ledger: index % is not on qbit_share_ledger', index_name;
        END IF;
    END LOOP;
    SELECT array_agg(indexrelid) INTO old_indexes FROM pg_index WHERE indrelid = 'qbit_share_ledger'::regclass;

    ALTER TABLE qbit_share_ledger RENAME TO qbit_share_ledger_p0;
    ALTER INDEX qbit_share_ledger_pkey RENAME TO qbit_share_ledger_p0_pkey;
    ALTER INDEX qbit_share_ledger_share_id_key RENAME TO qbit_share_ledger_p0_share_id_key;
    ALTER INDEX qbit_share_ledger_accepted_recent_idx RENAME TO qbit_share_ledger_p0_accepted_recent_idx;
    ALTER INDEX qbit_share_ledger_accepted_block_suffix_idx RENAME TO qbit_share_ledger_p0_accepted_block_suffix_idx;
    ALTER INDEX qbit_share_ledger_accepted_seq_walk_idx RENAME TO qbit_share_ledger_p0_accepted_seq_walk_idx;
    ALTER INDEX qbit_share_ledger_accepted_miner_history_idx RENAME TO qbit_share_ledger_p0_accepted_miner_history_idx;

    -- The parent: the same columns, defaults (share_seq keeps drawing from
    -- qbit_share_ledger_share_seq_seq) and CHECK constraints under the same
    -- names, which is what ATTACH matches on; minus the bound.
    CREATE TABLE qbit_share_ledger (LIKE qbit_share_ledger_p0 INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
        PARTITION BY RANGE (share_seq);
    ALTER TABLE qbit_share_ledger DROP CONSTRAINT qbit_share_ledger_p0_bound;
    ALTER TABLE qbit_share_ledger ADD CONSTRAINT qbit_share_ledger_pkey PRIMARY KEY (share_seq);
    ALTER SEQUENCE qbit_share_ledger_share_seq_seq OWNED BY qbit_share_ledger.share_seq;
    CREATE INDEX qbit_share_ledger_accepted_recent_idx
        ON qbit_share_ledger (accepted_at DESC)
        INCLUDE (share_difficulty, miner_id, share_seq)
        WHERE accepted;
    CREATE INDEX qbit_share_ledger_accepted_block_suffix_idx
        ON qbit_share_ledger ((lower(right(share_id, 64))), accepted_at DESC, share_seq DESC)
        INCLUDE (miner_id, share_difficulty, network_difficulty)
        WHERE accepted AND length(share_id) >= 65;
    CREATE INDEX qbit_share_ledger_accepted_seq_walk_idx
        ON qbit_share_ledger (share_seq DESC)
        INCLUDE (job_issued_at, accepted_at, share_difficulty)
        WHERE accepted;
    CREATE INDEX qbit_share_ledger_accepted_miner_history_idx
        ON qbit_share_ledger (miner_id, accepted_at DESC)
        INCLUDE (share_difficulty, share_seq, share_id)
        WHERE accepted;
    -- Queries through the parent are authorized on the parent alone.
    FOR grant_row IN
        SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END AS grantee,
               a.privilege_type, a.is_grantable
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.oid = 'qbit_share_ledger_p0'::regclass
          AND a.grantee <> c.relowner
    LOOP
        EXECUTE format('GRANT %s ON qbit_share_ledger TO %s%s',
            grant_row.privilege_type, grant_row.grantee,
            CASE WHEN grant_row.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
    END LOOP;

    EXECUTE format(
        'ALTER TABLE qbit_share_ledger ATTACH PARTITION qbit_share_ledger_p0 FOR VALUES FROM (MINVALUE) TO (%s)',
        bound
    );
    -- ATTACH adopts a partition index whose definition matches the parent's
    -- and builds one otherwise. A build here would hold the swap's ACCESS
    -- EXCLUSIVE lock for hours on a large ledger, so every index on the
    -- release table must have been adopted, not replaced.
    SELECT count(*) INTO p0_indexes FROM pg_index x
    WHERE x.indrelid = 'qbit_share_ledger_p0'::regclass AND NOT (x.indexrelid = ANY (old_indexes));
    IF p0_indexes <> 0 THEN
        RAISE EXCEPTION 'attaching qbit_share_ledger_p0 built % new index(es) instead of adopting the existing ones', p0_indexes;
    END IF;

    -- The one release function bound to the old row type by OID.
    DROP FUNCTION qbit_shares_since_template_height(bigint);
    CREATE FUNCTION qbit_shares_since_template_height(min_template_height bigint)
    RETURNS SETOF qbit_share_ledger
    LANGUAGE sql
    STABLE
    AS $body$
        SELECT *
        FROM qbit_share_ledger
        WHERE accepted
          AND template_height >= min_template_height
        ORDER BY share_seq ASC;
    $body$;
    -- Statement triggers are not cloned to partitions: the parent refuses
    -- statements against the whole ledger, each leaf keeps its own.
    CREATE TRIGGER qbit_prism_immutable_share_history BEFORE UPDATE OR DELETE OR TRUNCATE
        ON qbit_share_ledger FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_audit_history();

    INSERT INTO qbit_prism_share_partitions(partition_name, lower_seq, upper_seq)
    VALUES ('qbit_share_ledger_p0', NULL, bound);
    UPDATE qbit_prism_share_partitioning
    SET converted_at = clock_timestamp(), updated_at = clock_timestamp()
    WHERE singleton;
    PERFORM qbit_prism_share_partition_ensure();
END;
$$;
