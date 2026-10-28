-- #711: a share append without a block does its work under ORDER_LOCK in
-- one server-side call. Every client round trip an append makes while it
-- holds ORDER_LOCK is time every other share of the cluster waits behind it.
-- Since #719 the hold was the lock statement, three statements and the
-- COMMIT; with qbit_prism_append_share it is this one call and the COMMIT.
-- The frontend still opens the transaction (`BEGIN; SET LOCAL
-- plan_cache_mode = force_generic_plan`, #482), decides its pre-commit gate
-- (#716) and sends COMMIT. An append that carries a block, and the
-- settlement's deferred-share credit, keep their statements
-- (ledger/window.rs) and stamp with qbit_prism_ledger_clock too, as does the
-- snapshot anchor.
--
-- Additive only: two functions and no table, no capability and no shutdown
-- proof. No table is altered, so none is locked up front for this file
-- (ledger/migration/table_locks.rs): CREATE FUNCTION takes no lock a serving
-- frontend's statement waits for. An earlier frontend never calls either
-- function and goes on appending with its own statements beside one that
-- calls them, under the same ORDER_LOCK. It stores each stamp it takes in
-- the cluster row's ledger_clock_ms, which qbit_prism_ledger_clock reads as
-- its floor, and qbit_prism_ledger_clock stamps that floor, the database's
-- clock or an own share's stamp the floor already covers, which the earlier
-- frontend reads in turn; only a backward step of the database's own clock
-- while both serve can stamp a share below an earlier one
-- (docs/prism-rust-migration.md, migration 032).

-- The ledger clock in UNIX ms (#738): the greatest of `stored` (the cluster
-- row's ledger_clock_ms), the database's clock and the newest stamp among
-- this node's own accepted shares. A share append stamps accepted_at with it
-- and writes nothing to the cluster row, so a snapshot held on the primary
-- no longer leaves a dead version of the singleton per share. The stored
-- clock is written only by the snapshot anchor (this clock plus one, once
-- per window) and raised by the peer sync's own-log recovery, both under
-- ORDER_LOCK, so it is a floor every append reads. Together:
-- * an own share's stamp is at least every earlier own share's: stamps rise
--   with share_seq, whatever the database's clock does;
-- * a share appended after an anchor is stamped above it;
-- * an anchor is at or above every own share already committed, so a
--   backward step of the database's clock never places one below a share
--   at or under the window's cutoff;
-- * the peer sync's floor holds for every later stamp.
-- These are the stamps and anchors the per-append UPDATE of the stored clock
-- gave before: that clock was the newest own stamp, or an anchor or a floor
-- above it.
--
-- `own` is the frontend's dual-writer node, or NULL for the node this
-- database was personalised as (0 when nobody personalised it): its appends
-- never name origin_node, so they write the default personalisation set
-- with the identity. Only own shares count: a peer's rows carry the peer's
-- clock, and a peer row above an own share can be stamped either side of
-- it. The newest own share is looked for between the share probe's bounds
-- (`probe_floor`, `probe_ceiling`) first, where a node that appended lately
-- finds it in the lead, then, only when that finds nothing (a node that
-- idled while its peer served, or that has no own share), in every
-- partition, origin_node bounded from both sides so the (origin_node,
-- share_seq) index of 031 serves the order (as window/cut.rs's
-- OWN_CUT_SQL); `accepted` skips legacy rejected rows. Its millisecond is the
-- least one whose conversion, as an append and a window's predicate convert
-- it (to_timestamp(ms/1000)), is at or above the row's stamp: the one the
-- row was stamped with, which its stored conversion misses by a microsecond
-- either way past about 2^43 ms, and for a row stamped with a fraction of
-- one, the next.
CREATE FUNCTION qbit_prism_ledger_clock(stored bigint, own smallint, probe_floor bigint, probe_ceiling bigint)
RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    node smallint := COALESCE(own, (SELECT node_index FROM qbit_prism_node_identity WHERE singleton), 0);
    newest bigint;
BEGIN
    SELECT CASE WHEN to_timestamp(n.ms::double precision / 1000) >= n.accepted_at THEN n.ms ELSE n.ms + 1 END
    INTO newest
    FROM (SELECT floor(extract(epoch FROM l.accepted_at) * 1000)::bigint AS ms, l.accepted_at
          FROM qbit_share_ledger l
          WHERE l.origin_node BETWEEN node AND node
            AND l.share_seq >= probe_floor AND l.share_seq < probe_ceiling AND l.accepted
          ORDER BY l.origin_node DESC, l.share_seq DESC LIMIT 1) n;
    IF newest IS NULL THEN
        SELECT CASE WHEN to_timestamp(n.ms::double precision / 1000) >= n.accepted_at THEN n.ms ELSE n.ms + 1 END
        INTO newest
        FROM (SELECT floor(extract(epoch FROM l.accepted_at) * 1000)::bigint AS ms, l.accepted_at
              FROM qbit_share_ledger l
              WHERE l.origin_node BETWEEN node AND node AND l.accepted
              ORDER BY l.origin_node DESC, l.share_seq DESC LIMIT 1) n;
    END IF;
    RETURN GREATEST(stored, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint, newest);
END;
$$;

-- One share without a block, appended under ORDER_LOCK (5787769093247467522,
-- 0x505249534d000002). It takes the lock, then, each in a statement of its
-- own, so each with a snapshot taken after the lock and after any wait of
-- the statement before it, reads the write guard and the payout revision
-- (FOR SHARE when the append is fenced, that is when p_expected_revision is
-- not NULL, as the frontend read them), the share_id probe's inputs, the
-- share_id probe, between its bounds and then its fallback, and, for a new
-- share, stamps it with the ledger clock and inserts the row and its header
-- mapping: the statements the frontend sent before, in their order.
--
-- It returns one row: `inserted` true with the new share_seq and
-- accepted_at_ms, or false with the existing row's columns (those of
-- ledger.rs's SELECT_SHARE) for the frontend to compare with the share; and
-- the server's measure of the lock wait and of the hold from the grant to
-- the return, in microseconds, which the frontend records as it recorded
-- its own measures of the lock statement and the hold.
--
-- A refusal is raised before any write, with a SQLSTATE the frontend maps
-- to the error it returned before, and DETAIL holding the two measures and,
-- for a moved revision, the revision found ("waited_us held_us [observed]"):
--   QP001 the cluster is halted (MESSAGE: fatal_error);
--   QP002 a live legacy Python writer lease;
--   QP003 the payout revision moved past p_expected_revision;
--   QP004 the share_id is a retained rejected share's, its row archived;
--   QP005 the header is credited to another share;
--   QP006 the header is credited to this share, its row archived;
--   QP007 the share's job was issued after the ledger clock.
-- Any other error (a lock or statement timeout, a missing partition's 23514
-- from the ledger INSERT) fails the call as it failed its statement before,
-- and the transaction rolls back with nothing written.
CREATE FUNCTION qbit_prism_append_share(
    p_share_id text, p_miner_id text, p_order_key text, p_program_hex text,
    p_share_difficulty text, p_network_difficulty text, p_template_height bigint,
    p_job_id text, p_job_issued_at_ms bigint, p_ntime bigint, p_credit_policy text,
    p_writer_id text, p_header_hash text, p_expected_revision bigint, p_own smallint)
RETURNS TABLE (
    inserted boolean, share_seq bigint, accepted_at_ms bigint,
    share_id text, miner_id text, payout_order_key text, program text,
    difficulty text, network_difficulty text, template_height bigint, job_id text,
    job_issued_at timestamptz, accepted_at timestamptz, ntime bigint, credit_policy text,
    lock_waited_us bigint, lock_held_us bigint)
LANGUAGE plpgsql VOLATILE AS $$
#variable_conflict use_column
DECLARE
    v_called timestamptz := clock_timestamp();
    v_granted timestamptz;
    v_waited bigint;
    v_fatal text;
    v_legacy boolean;
    v_revision bigint;
    v_credited text;
    v_rejected_seq bigint;
    v_legacy_bound bigint;
    v_floor bigint;
    v_ceiling bigint;
    v_existing record;
    v_stored bigint;
    v_clock bigint;
    v_seq bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(5787769093247467522);
    v_granted := clock_timestamp();
    v_waited := floor(extract(epoch FROM v_granted - v_called) * 1000000)::bigint;
    IF p_expected_revision IS NULL THEN
        SELECT c.fatal_error,
               EXISTS(SELECT 1 FROM qbit_ledger_writer_lease WHERE lease_expires_at > clock_timestamp()),
               c.payout_revision
        INTO STRICT v_fatal, v_legacy, v_revision
        FROM qbit_prism_cluster c WHERE c.singleton;
    ELSE
        SELECT c.fatal_error,
               EXISTS(SELECT 1 FROM qbit_ledger_writer_lease WHERE lease_expires_at > clock_timestamp()),
               c.payout_revision
        INTO STRICT v_fatal, v_legacy, v_revision
        FROM qbit_prism_cluster c WHERE c.singleton FOR SHARE OF c;
    END IF;
    IF v_fatal IS NOT NULL THEN
        RAISE EXCEPTION USING ERRCODE = 'QP001', MESSAGE = v_fatal,
            DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
    END IF;
    IF v_legacy THEN
        RAISE EXCEPTION USING ERRCODE = 'QP002', MESSAGE = 'live legacy Python writer lease',
            DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
    END IF;
    IF p_expected_revision IS NOT NULL AND v_revision <> p_expected_revision THEN
        RAISE EXCEPTION USING ERRCODE = 'QP003', MESSAGE = 'payout revision changed before share commit',
            DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint || ' ' || v_revision;
    END IF;
    -- The share_id probe's inputs, as ledger/window.rs's APPEND_PROBE_SQL
    -- reads them.
    SELECT (SELECT h.share_id FROM qbit_prism_share_hashes h WHERE h.header_hash = p_header_hash),
           (SELECT r.share_seq FROM qbit_prism_rejected_share_ids r WHERE r.share_id = p_share_id),
           p.conversion_bound,
           COALESCE((SELECT COALESCE(s.lower_seq, 0) FROM qbit_prism_share_partitions s
                     WHERE s.state = 'attached' AND (s.lower_seq IS NULL OR s.lower_seq <= n.value)
                     ORDER BY s.upper_seq DESC OFFSET 2 LIMIT 1), 0),
           n.value
    INTO STRICT v_credited, v_rejected_seq, v_legacy_bound, v_floor, v_ceiling
    FROM qbit_prism_share_partitioning p,
         (SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END AS value
          FROM qbit_share_ledger_share_seq_seq) n
    WHERE p.singleton;
    IF v_legacy_bound IS NULL THEN
        RAISE EXCEPTION 'qbit_prism_share_partitioning has no conversion_bound';
    END IF;
    -- The share_id probe between the bounds, then, on a miss, the retained
    -- rejected sequence, the legacy range, or, for a credited header, the
    -- whole ledger (ledger/window.rs, Ledger::append_probed).
    SELECT l.share_seq, l.share_id, l.miner_id, l.payout_order_key,
           encode(l.p2mr_program, 'hex') AS program, l.share_difficulty::text AS difficulty,
           l.network_difficulty::text AS network_difficulty, l.template_height, l.job_id,
           l.job_issued_at, l.accepted_at, l.ntime, l.credit_policy
    INTO v_existing
    FROM qbit_share_ledger l
    WHERE l.share_id = p_share_id AND l.share_seq >= v_floor AND l.share_seq < v_ceiling
    LIMIT 1;
    IF NOT FOUND THEN
        IF v_rejected_seq IS NOT NULL THEN
            SELECT l.share_seq, l.share_id, l.miner_id, l.payout_order_key,
                   encode(l.p2mr_program, 'hex') AS program, l.share_difficulty::text AS difficulty,
                   l.network_difficulty::text AS network_difficulty, l.template_height, l.job_id,
                   l.job_issued_at, l.accepted_at, l.ntime, l.credit_policy
            INTO v_existing
            FROM qbit_share_ledger l
            WHERE l.share_id = p_share_id AND l.share_seq = v_rejected_seq
            LIMIT 1;
        ELSIF v_credited IS NULL THEN
            SELECT l.share_seq, l.share_id, l.miner_id, l.payout_order_key,
                   encode(l.p2mr_program, 'hex') AS program, l.share_difficulty::text AS difficulty,
                   l.network_difficulty::text AS network_difficulty, l.template_height, l.job_id,
                   l.job_issued_at, l.accepted_at, l.ntime, l.credit_policy
            INTO v_existing
            FROM qbit_share_ledger l
            WHERE l.share_id = p_share_id AND l.share_seq < v_legacy_bound
            LIMIT 1;
        ELSE
            SELECT l.share_seq, l.share_id, l.miner_id, l.payout_order_key,
                   encode(l.p2mr_program, 'hex') AS program, l.share_difficulty::text AS difficulty,
                   l.network_difficulty::text AS network_difficulty, l.template_height, l.job_id,
                   l.job_issued_at, l.accepted_at, l.ntime, l.credit_policy
            INTO v_existing
            FROM qbit_share_ledger l
            WHERE l.share_id = p_share_id
            LIMIT 1;
        END IF;
    END IF;
    IF FOUND THEN
        inserted := false;
        share_seq := v_existing.share_seq;
        share_id := v_existing.share_id;
        miner_id := v_existing.miner_id;
        payout_order_key := v_existing.payout_order_key;
        program := v_existing.program;
        difficulty := v_existing.difficulty;
        network_difficulty := v_existing.network_difficulty;
        template_height := v_existing.template_height;
        job_id := v_existing.job_id;
        job_issued_at := v_existing.job_issued_at;
        accepted_at := v_existing.accepted_at;
        ntime := v_existing.ntime;
        credit_policy := v_existing.credit_policy;
        lock_waited_us := v_waited;
        lock_held_us := floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
        RETURN NEXT;
        RETURN;
    END IF;
    IF v_rejected_seq IS NOT NULL THEN
        RAISE EXCEPTION USING ERRCODE = 'QP004',
            MESSAGE = 'rejected share_id is retained globally, and its share is archived',
            DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
    END IF;
    IF v_credited IS NOT NULL THEN
        IF v_credited <> p_share_id THEN
            RAISE EXCEPTION USING ERRCODE = 'QP005', MESSAGE = 'header already credited globally',
                DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
        END IF;
        RAISE EXCEPTION USING ERRCODE = 'QP006',
            MESSAGE = 'header already credited globally, and its share is archived',
            DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
    END IF;
    SELECT c.ledger_clock_ms INTO STRICT v_stored FROM qbit_prism_cluster c WHERE c.singleton;
    v_clock := qbit_prism_ledger_clock(v_stored, p_own, v_floor, v_ceiling);
    IF p_job_issued_at_ms > v_clock THEN
        RAISE EXCEPTION USING ERRCODE = 'QP007', MESSAGE = 'share references a job from the future',
            DETAIL = v_waited || ' ' || floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
    END IF;
    INSERT INTO qbit_share_ledger(share_id, miner_id, payout_order_key, p2mr_program, share_difficulty,
                                  network_difficulty, template_height, job_id, job_issued_at, ntime,
                                  accepted_at, credit_policy, accepted, writer_id, writer_epoch)
    VALUES (p_share_id, p_miner_id, p_order_key, decode(p_program_hex, 'hex'), p_share_difficulty::numeric,
            p_network_difficulty::numeric, p_template_height, p_job_id,
            to_timestamp(p_job_issued_at_ms::double precision / 1000), p_ntime,
            to_timestamp(v_clock::double precision / 1000), p_credit_policy, true, p_writer_id, 0)
    RETURNING qbit_share_ledger.share_seq INTO v_seq;
    INSERT INTO qbit_prism_share_hashes(header_hash, share_id) VALUES (p_header_hash, p_share_id);
    inserted := true;
    share_seq := v_seq;
    accepted_at_ms := v_clock;
    lock_waited_us := v_waited;
    lock_held_us := floor(extract(epoch FROM clock_timestamp() - v_granted) * 1000000)::bigint;
    RETURN NEXT;
END;
$$;
