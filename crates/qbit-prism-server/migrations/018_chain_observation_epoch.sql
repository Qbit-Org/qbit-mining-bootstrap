-- All earlier writers, including one-shot tools and startups already past
-- their capability check, must be stopped before this transaction begins.
-- The runner holds the instance registration lock through commit; old
-- binaries must stay stopped. A connect-time capability cannot evict them.
-- 016/017 are reserved by the independent share-partitioning migration.
-- Plain ADD/INSERT deliberately refuse preexisting undeclared metadata.
ALTER TABLE qbit_prism_cluster
    ADD COLUMN chain_epoch bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT qbit_prism_cluster_chain_epoch_check CHECK (chain_epoch >= 0);

-- Historical observations are gone at this offline upgrade boundary.
-- Subsequent accepted chain changes increment epoch atomically with the tip
-- and payout revision; ordinary restart/accounting must never reset it.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('chain_observation_epoch', 1);
