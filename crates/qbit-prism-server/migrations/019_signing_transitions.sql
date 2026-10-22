-- Signing-key rotation resets the pinned cluster fingerprint. The journal
-- keeps the retired policy document, and with it both old public keys, as
-- the anchor for verifying bundles signed before the rotation. Additive:
-- a binary that does not know this table never reads or writes it.
CREATE TABLE qbit_prism_signing_transitions (
    transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    operator_identity text NOT NULL DEFAULT session_user,
    database_role text NOT NULL DEFAULT current_user,
    previous_fingerprint text NOT NULL,
    previous_policy jsonb NOT NULL,
    -- Every instance row at decision time with its measured heartbeat age,
    -- and the window a row other than `stopped` had to exceed.
    instances jsonb NOT NULL,
    heartbeat_stale_after_seconds double precision NOT NULL,
    -- Unchanged by a rotation: a key change is not a payout-policy change.
    payout_revision bigint NOT NULL,
    CHECK (heartbeat_stale_after_seconds >= 15),
    CHECK (jsonb_typeof(previous_policy->'ledger_key') = 'string'),
    CHECK (jsonb_typeof(previous_policy->'manifest_key') = 'string'),
    CHECK (jsonb_typeof(instances) = 'array')
);

CREATE FUNCTION qbit_prism_preserve_signing_transitions()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'signing transition events are immutable';
END;
$$;
CREATE TRIGGER qbit_prism_preserve_signing_transitions
    BEFORE UPDATE OR DELETE OR TRUNCATE ON qbit_prism_signing_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_signing_transitions();
