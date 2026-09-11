-- B273's storage/reader foundation. Stop all old frontends before cutover.
-- Existing inline jobs remain unchanged, with all reference columns NULL.
-- The runtime prepared writer/resumer switches in the coordinated A265/B273
-- integration; applying this additive schema alone does not enable it.
ALTER TABLE qbit_prism_jobs
    ADD COLUMN IF NOT EXISTS window_anchor_ms bigint,
    ADD COLUMN IF NOT EXISTS window_prior_balances_sha256 text,
    ADD COLUMN IF NOT EXISTS window_first_share_seq bigint,
    ADD COLUMN IF NOT EXISTS window_last_share_seq bigint,
    ADD COLUMN IF NOT EXISTS window_share_count bigint,
    ADD COLUMN IF NOT EXISTS window_snapshot_sha256 text,
    ADD COLUMN IF NOT EXISTS template_sha256 text;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'qbit_prism_jobs'::regclass
                     AND conname = 'qbit_prism_jobs_window_check') THEN
        ALTER TABLE qbit_prism_jobs ADD CONSTRAINT qbit_prism_jobs_window_check CHECK (
            (num_nulls(window_anchor_ms, window_prior_balances_sha256) = 2
             AND num_nulls(window_first_share_seq, window_last_share_seq,
                           window_share_count, window_snapshot_sha256) = 4)
            OR (num_nonnulls(window_anchor_ms, window_prior_balances_sha256) = 2
                AND window_prior_balances_sha256 ~ '^[0-9a-f]{64}$'
                AND (
                    num_nulls(window_first_share_seq, window_last_share_seq,
                              window_share_count, window_snapshot_sha256) = 4
                    OR (num_nonnulls(window_first_share_seq, window_last_share_seq,
                                    window_share_count, window_snapshot_sha256) = 4
                        AND window_first_share_seq >= 1
                        AND window_last_share_seq >= window_first_share_seq
                        AND window_share_count BETWEEN 1 AND window_last_share_seq - window_first_share_seq + 1
                        AND window_snapshot_sha256 ~ '^[0-9a-f]{64}$'))));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'qbit_prism_jobs'::regclass
                     AND conname = 'qbit_prism_jobs_template_digest_check') THEN
        ALTER TABLE qbit_prism_jobs ADD CONSTRAINT qbit_prism_jobs_template_digest_check
            CHECK (template_sha256 IS NULL OR template_sha256 ~ '^[0-9a-f]{64}$');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS qbit_prism_templates (
    template_sha256 text PRIMARY KEY CHECK (template_sha256 ~ '^[0-9a-f]{64}$'),
    template_bytes bytea NOT NULL
);

-- Compact native CarryForwardBalance JSON bytes, sorted bytewise by
-- (order_key, recipient_id, p2mr_program_hex). The key is qbit-prism's
-- prior_balances_digest of that set, NOT a SHA-256 of the serialized bytes.
CREATE TABLE IF NOT EXISTS qbit_prism_balance_snapshots (
    prior_balances_digest text PRIMARY KEY CHECK (prior_balances_digest ~ '^[0-9a-f]{64}$'),
    balances bytea NOT NULL
);

CREATE OR REPLACE FUNCTION qbit_prism_immutable_prepared_blob() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'immutable prepared blob: %', TG_TABLE_NAME USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS qbit_prism_immutable_template ON qbit_prism_templates;
CREATE TRIGGER qbit_prism_immutable_template BEFORE UPDATE ON qbit_prism_templates
    FOR EACH ROW EXECUTE FUNCTION qbit_prism_immutable_prepared_blob();
DROP TRIGGER IF EXISTS qbit_prism_immutable_balances ON qbit_prism_balance_snapshots;
CREATE TRIGGER qbit_prism_immutable_balances BEFORE UPDATE ON qbit_prism_balance_snapshots
    FOR EACH ROW EXECUTE FUNCTION qbit_prism_immutable_prepared_blob();

-- Save/repair and bounded GC will share SETTLEMENT_LOCK. No FK to audit rows:
-- audit retention and bootstrap inline shares have different lifetimes.
CREATE INDEX IF NOT EXISTS qbit_prism_jobs_template_sha256_idx
    ON qbit_prism_jobs(template_sha256) WHERE template_sha256 IS NOT NULL;
CREATE INDEX IF NOT EXISTS qbit_prism_jobs_window_balances_idx
    ON qbit_prism_jobs(window_prior_balances_sha256)
    WHERE window_prior_balances_sha256 IS NOT NULL;
