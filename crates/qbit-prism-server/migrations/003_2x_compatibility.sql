-- Preserve byte-exact historical audit identities when importing 2.x gzip
-- sidecars. Native audits reconstruct canonical bytes from immutable share
-- ranges, avoiding a second full copy of each overlapping payout window.
ALTER TABLE qbit_pool_audit_bundles
    ADD COLUMN IF NOT EXISTS canonical_audit_bytes bytea;

-- Native immature disconnections remain recoverable. The legacy
-- disconnected_at column requires terminal maturity_state='reversed'.
-- Publication ordinals distinguish previously accepted inactive blocks from
-- candidates that were prepared and then rejected before acceptance.
ALTER TABLE qbit_pool_blocks
    ADD COLUMN IF NOT EXISTS inactive_since timestamptz;
