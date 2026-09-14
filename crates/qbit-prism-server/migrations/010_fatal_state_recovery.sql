-- Historical halts have no reliable set time: leave it NULL rather than use
-- the cluster's updated_at, which also tracks unrelated accounting changes.
ALTER TABLE qbit_prism_cluster ADD COLUMN IF NOT EXISTS fatal_error_set_at timestamptz;

CREATE OR REPLACE FUNCTION qbit_prism_stamp_fatal_state()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.fatal_error IS DISTINCT FROM OLD.fatal_error THEN
        NEW.fatal_error_set_at := CASE WHEN NEW.fatal_error IS NOT NULL
            THEN clock_timestamp() ELSE NULL END;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER qbit_prism_stamp_fatal_state
    BEFORE UPDATE OF fatal_error ON qbit_prism_cluster FOR EACH ROW
    EXECUTE FUNCTION qbit_prism_stamp_fatal_state();

CREATE TABLE qbit_prism_fatal_state_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cleared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    operator_identity text NOT NULL DEFAULT session_user,
    database_role text NOT NULL DEFAULT current_user,
    reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 4096),
    fatal_error text NOT NULL,
    fatal_error_set_at timestamptz,
    instances jsonb NOT NULL,
    reconciliation jsonb NOT NULL
);

CREATE FUNCTION qbit_prism_preserve_fatal_state_events()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'fatal-state recovery events are immutable';
END;
$$;
CREATE TRIGGER qbit_prism_preserve_fatal_state_events
    BEFORE UPDATE OR DELETE OR TRUNCATE ON qbit_prism_fatal_state_events
    FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_fatal_state_events();
