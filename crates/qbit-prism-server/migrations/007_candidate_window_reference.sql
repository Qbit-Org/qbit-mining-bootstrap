-- A265's candidate-side window reference. The mirror of 008: the same six
-- columns and the same three-state CHECK, on qbit_block_candidate_outbox
-- instead of qbit_prism_jobs. Applying this additive schema alone changes no
-- runtime behaviour; nothing writes these columns until the enqueue/claim
-- switch lands. Migration order is membership-based, so this does not assume
-- that 006 or 008 has run.
--
-- The runner refuses to reach this file while any pre-007 pending row is left
-- in the outbox (an inline `bundle` candidate, #258's chunked version-2 row,
-- or a NULL candidate); see the refusal in ledger/connect.rs.
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS window_anchor_ms bigint,
    ADD COLUMN IF NOT EXISTS window_prior_balances_sha256 text,
    ADD COLUMN IF NOT EXISTS window_first_share_seq bigint,
    ADD COLUMN IF NOT EXISTS window_last_share_seq bigint,
    ADD COLUMN IF NOT EXISTS window_share_count bigint,
    ADD COLUMN IF NOT EXISTS window_snapshot_sha256 text,
    -- The block leaves the JSON document for this column, so the document
    -- stays small and keeps only block_sha256, which candidate_sha256 covers.
    -- Nullable: pre-007 rows and every terminal row carry no block, and the
    -- terminal UPDATE NULLs it with the six window columns and `candidate`.
    ADD COLUMN IF NOT EXISTS block_bytes bytea;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'qbit_block_candidate_outbox'::regclass
                     AND conname = 'qbit_block_candidate_outbox_window_check') THEN
        ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_window_check CHECK (
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
END $$;

-- B273's balance-snapshot GC scans for the digests no remaining job row and no
-- non-terminal candidate references. The terminal UPDATE NULLs all six window
-- columns atomically, so `window_prior_balances_sha256 IS NOT NULL` is exactly
-- the set of live candidate references and this partial index covers it.
CREATE INDEX IF NOT EXISTS qbit_block_candidate_outbox_window_balances_idx
    ON qbit_block_candidate_outbox (window_prior_balances_sha256)
    WHERE window_prior_balances_sha256 IS NOT NULL;
