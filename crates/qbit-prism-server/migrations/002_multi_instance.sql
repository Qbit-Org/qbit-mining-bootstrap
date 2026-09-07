-- Rust instances coordinate through transaction locks, never a process-local
-- writer lease. Cutover is explicit: a live Python lease is rejected before
-- this migration and Python cannot reacquire it afterwards.
CREATE TABLE IF NOT EXISTS qbit_prism_cluster (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    config_fingerprint text,
    payout_revision bigint NOT NULL DEFAULT 0,
    best_chainwork numeric(78,0) NOT NULL DEFAULT 0 CHECK(best_chainwork>=0),
    best_tip_hash text,
    best_tip_height bigint CHECK(best_tip_height>=0),
    ledger_clock_ms bigint NOT NULL DEFAULT 0,
    fatal_error text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO qbit_prism_cluster(singleton) VALUES(true) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION qbit_prism_reject_legacy_writer()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'database has migrated to Rust multi-instance PRISM; stop the legacy Python writer';
END;
$$;
DROP TRIGGER IF EXISTS qbit_prism_no_legacy_writer ON qbit_ledger_writer_lease;
CREATE TRIGGER qbit_prism_no_legacy_writer BEFORE INSERT OR UPDATE
    ON qbit_ledger_writer_lease FOR EACH ROW
    EXECUTE FUNCTION qbit_prism_reject_legacy_writer();

CREATE TABLE IF NOT EXISTS qbit_prism_share_hashes (
    header_hash text PRIMARY KEY CHECK (header_hash ~ '^[0-9a-f]{64}$'),
    share_id text NOT NULL UNIQUE REFERENCES qbit_share_ledger(share_id)
);
-- Legacy worker-scoped identifiers could contain the same header twice.
-- Preserve that history while prohibiting any new replay across identities.
INSERT INTO qbit_prism_share_hashes(header_hash,share_id)
SELECT DISTINCT ON (lower(right(share_id,64))) lower(right(share_id,64)),share_id
FROM qbit_share_ledger
WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$'
ORDER BY lower(right(share_id,64)),share_seq
ON CONFLICT DO NOTHING;

CREATE SEQUENCE IF NOT EXISTS qbit_prism_session_sequence AS bigint MINVALUE 1 MAXVALUE 4294967295 NO CYCLE;
CREATE TABLE IF NOT EXISTS qbit_prism_instances (
    instance_id text PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    status jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS qbit_prism_jobs (
    job_id text PRIMARY KEY,
    instance_id text NOT NULL,
    parent_hash text NOT NULL,
    payout_revision bigint NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS qbit_prism_jobs_expiry_idx ON qbit_prism_jobs(expires_at);
CREATE INDEX IF NOT EXISTS qbit_prism_mature_checkpoint_idx
    ON qbit_pool_blocks(block_height DESC,block_hash DESC)
    WHERE chain_state='confirmed' AND maturity_state='mature';

ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS claim_instance_id text,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp();
ALTER TABLE qbit_ctv_fanout_artifacts
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS claim_instance_id text,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS confirmed_block_hash text,
    ADD COLUMN IF NOT EXISTS confirmed_block_height bigint,
    ADD COLUMN IF NOT EXISTS confirmed_depth bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS spend_scan_next_height bigint,
    ADD COLUMN IF NOT EXISTS spend_scan_anchor_height bigint,
    ADD COLUMN IF NOT EXISTS spend_scan_anchor_hash text;

CREATE INDEX IF NOT EXISTS qbit_prism_fanout_checkpoint_idx
    ON qbit_ctv_fanout_artifacts(confirmed_block_height DESC,fanout_txid DESC)
    WHERE settlement_status='confirmed' AND confirmed_depth>=1000;

-- Wallet locks are always backed by a durable reservation before lockunspent.
-- The signed package is committed before submitpackage and replayed verbatim
-- after a lost response, crash or claim takeover. Spent outpoints remain unique.
CREATE TABLE IF NOT EXISTS qbit_prism_cpfp_packages (
    fanout_txid text PRIMARY KEY REFERENCES qbit_ctv_fanout_artifacts(fanout_txid),
    funding_txid text NOT NULL,
    funding_vout integer NOT NULL CHECK (funding_vout>=0),
    funding_value_sats bigint NOT NULL CHECK (funding_value_sats>0),
    wallet_name text NOT NULL,
    signed_child_hex text,
    child_txid text,
    wallet_lock_released boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(funding_txid,funding_vout),
    CHECK ((signed_child_hex IS NULL)=(child_txid IS NULL))
);

CREATE INDEX IF NOT EXISTS qbit_prism_candidate_claim_idx
    ON qbit_block_candidate_outbox(next_attempt_at,created_at)
    WHERE state='pending';

-- Keep below-target share credit recoverable even after an abandoned outbox
-- attempt: a previous owner's in-flight RPC can land a block after failover.
CREATE TABLE IF NOT EXISTS qbit_prism_deferred_shares (
    block_hash text PRIMARY KEY REFERENCES qbit_block_candidate_outbox(block_hash),
    share jsonb NOT NULL,
    share_sha256 text NOT NULL CHECK (share_sha256 ~ '^[0-9a-f]{64}$')
);

-- Audit bodies refer to immutable share history. Overlapping payout windows
-- do not copy their full accepted-share arrays into every block row.
CREATE TABLE IF NOT EXISTS qbit_prism_audit_snapshots (
    snapshot_sha256 text PRIMARY KEY CHECK (snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    first_share_seq bigint NOT NULL,
    last_share_seq bigint NOT NULL,
    anchor_ms bigint NOT NULL,
    share_count bigint NOT NULL CHECK (share_count > 0),
    inline_shares jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (first_share_seq > 0 AND last_share_seq >= first_share_seq)
);
ALTER TABLE qbit_pool_audit_bundles
    ADD COLUMN IF NOT EXISTS share_snapshot_sha256 text REFERENCES qbit_prism_audit_snapshots(snapshot_sha256);

CREATE OR REPLACE FUNCTION qbit_prism_preserve_audit_history()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Rust PRISM accepted share history is immutable and must be retained for audit reconstruction';
END;
$$;
DROP TRIGGER IF EXISTS qbit_prism_immutable_share_history ON qbit_share_ledger;
CREATE TRIGGER qbit_prism_immutable_share_history BEFORE UPDATE OR DELETE OR TRUNCATE
    ON qbit_share_ledger FOR EACH STATEMENT EXECUTE FUNCTION qbit_prism_preserve_audit_history();
