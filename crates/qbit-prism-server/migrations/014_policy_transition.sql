-- Offline fee-policy changes keep the signing keys and all historical bytes.
CREATE TABLE qbit_prism_policy_transitions (
    transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    operator_identity text NOT NULL DEFAULT session_user,
    database_role text NOT NULL DEFAULT current_user,
    previous_fingerprint text NOT NULL,
    config_fingerprint text NOT NULL,
    previous_policy jsonb NOT NULL,
    policy jsonb NOT NULL,
    previous_revision bigint NOT NULL,
    payout_revision bigint NOT NULL,
    instances jsonb NOT NULL,
    abandoned_candidates bigint NOT NULL,
    retained_candidates bigint NOT NULL,
    CHECK (previous_fingerprint <> config_fingerprint),
    CHECK (payout_revision = previous_revision + 1)
);

CREATE FUNCTION qbit_prism_preserve_policy_transitions()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'policy transition events are immutable';
END;
$$;
CREATE TRIGGER qbit_prism_preserve_policy_transitions
    BEFORE UPDATE OR DELETE OR TRUNCATE ON qbit_prism_policy_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_policy_transitions();
