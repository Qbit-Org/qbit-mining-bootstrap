-- Close startup that checked pre-011 capabilities, then waited to register
-- behind the migration lock. CHECK is enforced after that wait, before the
-- initial heartbeat can commit. Existing shutdown/health evidence is kept.
-- Only a frontend implementing reservation-before-offer writes this marker.
ALTER TABLE qbit_prism_instances
    ADD CONSTRAINT qbit_prism_instances_offer_startup CHECK (
        status->>'state' IS DISTINCT FROM 'starting'
        OR COALESCE(status @> '{"candidate_offer_lifecycle":1}'::jsonb, false)
    );

INSERT INTO qbit_prism_schema_capabilities(capability, capability_value)
    VALUES ('instance_offer_startup', 1);
