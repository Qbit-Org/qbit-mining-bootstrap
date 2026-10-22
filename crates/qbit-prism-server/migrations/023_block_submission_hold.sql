-- #664: a cluster-wide block submission hold, stored in the ledger, that
-- every frontend obeys whatever its PRISM_BLOCK_SUBMIT_ENABLED says (#661).
--
-- STOP EVERY pre-023 FRONTEND AND TOOL BEFORE APPLYING THIS MIGRATION, keep
-- them stopped until it has committed, and restart only upgraded binaries.
-- The runner refuses to apply it while any registered instance has not
-- reported `drained` or `stopped`, and the capability declared at the end is
-- read when a binary connects, never while it runs.
--
-- A restored copy of the mainnet ledger is held for a #291 rehearsal with
-- `qbit-prism-server submission-hold set`, before any frontend starts on it.
-- While the hold is set, no frontend claims a block candidate, reserves a
-- block's offer or claims or sends a CTV fanout, so a frontend started
-- without PRISM_BLOCK_SUBMIT_ENABLED=0 still sends its node nothing. A
-- pre-023 binary would ignore the hold, so it must not run on this schema:
-- the capability below makes it refuse to start.
--
-- The hold is a row of its own rather than columns of qbit_prism_cluster. An
-- offer reservation reads it FOR SHARE and only `submission-hold set` and
-- `clear` lock it FOR UPDATE, so a hold commits either before a reservation,
-- which is then refused, or after it. Reading it adds no lock on the cluster
-- row, so no reservation waits for a landing or blob GC because of the hold.
CREATE TABLE qbit_prism_submission_hold (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    reason text,
    set_at timestamptz,
    set_by text,
    CONSTRAINT qbit_prism_submission_hold_consistent CHECK (
        (reason IS NULL AND set_at IS NULL AND set_by IS NULL)
        OR (length(btrim(reason)) BETWEEN 1 AND 4096
            AND set_at IS NOT NULL AND set_by IS NOT NULL)
    )
);
INSERT INTO qbit_prism_submission_hold (singleton) VALUES (true);

-- Every claim and reservation reads the row; without it they would fail
-- rather than report the hold, so it is never deleted.
CREATE FUNCTION qbit_prism_keep_submission_hold()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the block submission hold row is permanent; use qbit-prism-server submission-hold clear';
END;
$$;
CREATE TRIGGER qbit_prism_keep_submission_hold
    BEFORE DELETE OR TRUNCATE ON qbit_prism_submission_hold
    FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_keep_submission_hold();

-- Each set that holds the cluster and each clear that releases it, with who
-- ran it and why: clearing releases the blocks the hold kept back, so the
-- record outlives the hold.
CREATE TABLE qbit_prism_submission_hold_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action text NOT NULL CHECK (action IN ('set', 'clear')),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    operator_identity text NOT NULL DEFAULT session_user,
    database_role text NOT NULL DEFAULT current_user,
    reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 4096),
    pending_candidates bigint NOT NULL CHECK (pending_candidates >= 0)
);

CREATE FUNCTION qbit_prism_preserve_submission_hold_events()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'block submission hold events are immutable';
END;
$$;
CREATE TRIGGER qbit_prism_preserve_submission_hold_events
    BEFORE UPDATE OR DELETE OR TRUNCATE ON qbit_prism_submission_hold_events
    FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_submission_hold_events();

-- Declared last. A pre-023 binary that checks after this commit refuses the
-- database; nothing here stops one that is already running, which is why
-- the runner requires every earlier instance to have reported shutdown.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('block_submission_hold', 1);
