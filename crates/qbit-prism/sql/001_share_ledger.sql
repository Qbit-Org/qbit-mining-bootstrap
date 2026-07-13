-- Canonical ordered share ledger for qbit PRISM mining.
--
-- Invariant: only one logical writer inserts into qbit_share_ledger. Stratum
-- frontends may scale horizontally, but they must feed that writer through a
-- queue instead of inserting shares independently.

CREATE TABLE IF NOT EXISTS qbit_ledger_writer_lease (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    writer_id text NOT NULL,
    writer_epoch bigint NOT NULL CHECK (writer_epoch >= 0),
    writer_session_token text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE qbit_ledger_writer_lease
    ADD COLUMN IF NOT EXISTS writer_session_token text;

UPDATE qbit_ledger_writer_lease
SET writer_session_token = writer_id || ':' || writer_epoch::text
WHERE writer_session_token IS NULL;

ALTER TABLE qbit_ledger_writer_lease
    ALTER COLUMN writer_session_token SET NOT NULL;

CREATE TABLE IF NOT EXISTS qbit_share_ledger (
    share_seq bigserial PRIMARY KEY,
    share_id text NOT NULL UNIQUE,
    miner_id text NOT NULL,
    payout_order_key text NOT NULL,
    p2mr_program bytea NOT NULL CHECK (octet_length(p2mr_program) = 32),
    share_difficulty numeric(78, 0) NOT NULL CHECK (share_difficulty > 0),
    network_difficulty numeric(78, 0) NOT NULL CHECK (network_difficulty > 0),
    template_height bigint NOT NULL CHECK (template_height >= 0),
    job_id text NOT NULL,
    job_issued_at timestamptz NOT NULL,
    ntime bigint NOT NULL CHECK (ntime >= 0),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    accepted boolean NOT NULL DEFAULT true,
    reject_reason text CHECK (
        reject_reason IS NULL OR reject_reason IN (
            'stale-job',
            'duplicate-share',
            'low-difficulty',
            'malformed-submit',
            'unauthorized-worker',
            'unknown-job',
            'invalid-extranonce',
            'invalid-ntime-or-nonce',
            'candidate-audit-mismatch',
            'submitblock-rejected',
            'backend-rpc-unavailable',
            'internal-error',
            'pool-closed',
            'block-stale',
            'ledger-confirmation-failed'
        )
    ),
    writer_id text NOT NULL,
    writer_epoch bigint NOT NULL CHECK (writer_epoch >= 0),
    credit_policy text,
    CONSTRAINT qbit_share_ledger_credit_policy_check
        CHECK (credit_policy IS NULL OR credit_policy IN ('stale-grace')),
    CHECK (accepted OR reject_reason IS NOT NULL)
);

-- A block-worthy share and the information needed to finish submitting its
-- block are committed in the same transaction.  The coordinator's in-memory
-- queue is only a low-latency wakeup; this outbox is the source of truth after
-- a process or host restart.
CREATE TABLE IF NOT EXISTS qbit_block_candidate_outbox (
    block_hash text PRIMARY KEY,
    share_id text UNIQUE REFERENCES qbit_share_ledger(share_id),
    candidate jsonb,
    candidate_sha256 text NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'submitted', 'abandoned')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK (
        (state = 'pending' AND completed_at IS NULL AND candidate IS NOT NULL)
        OR (
            state IN ('submitted', 'abandoned')
            AND completed_at IS NOT NULL
            AND candidate IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS qbit_block_candidate_outbox_pending_idx
    ON qbit_block_candidate_outbox (created_at, block_hash)
    WHERE state = 'pending';

ALTER TABLE qbit_share_ledger
    ADD COLUMN IF NOT EXISTS credit_policy text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'qbit_share_ledger'::regclass
          AND conname = 'qbit_share_ledger_credit_policy_check'
    ) THEN
        ALTER TABLE qbit_share_ledger
            ADD CONSTRAINT qbit_share_ledger_credit_policy_check
            CHECK (credit_policy IS NULL OR credit_policy IN ('stale-grace'))
            NOT VALID;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS qbit_payout_carry_forward (
    carry_forward_seq bigserial PRIMARY KEY,
    block_height bigint NOT NULL CHECK (block_height >= 0),
    block_hash text,
    miner_id text NOT NULL,
    payout_order_key text NOT NULL,
    p2mr_program bytea NOT NULL CHECK (octet_length(p2mr_program) = 32),
    gross_amount_sats bigint NOT NULL CHECK (gross_amount_sats >= 0),
    prior_balance_sats numeric(78, 0) NOT NULL,
    candidate_balance_sats numeric(78, 0) NOT NULL,
    onchain_amount_sats bigint NOT NULL CHECK (onchain_amount_sats >= 0),
    settlement_fee_sats bigint NOT NULL DEFAULT 0 CHECK (settlement_fee_sats >= 0),
    carry_forward_balance_sats numeric(78, 0) NOT NULL,
    action text NOT NULL CHECK (action IN ('onchain', 'accrued')),
    maturity_state text NOT NULL DEFAULT 'immature'
        CHECK (maturity_state IN ('immature', 'mature', 'reversed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE qbit_payout_carry_forward
    ADD COLUMN IF NOT EXISTS settlement_fee_sats bigint NOT NULL DEFAULT 0 CHECK (settlement_fee_sats >= 0);

CREATE TABLE IF NOT EXISTS qbit_pool_blocks (
    block_hash text PRIMARY KEY,
    block_height bigint NOT NULL CHECK (block_height >= 0),
    parent_hash text NOT NULL,
    coinbase_txid text NOT NULL,
    payout_manifest_sha256 text NOT NULL,
    found_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    chain_state text NOT NULL DEFAULT 'prepared'
        CHECK (chain_state IN ('prepared', 'confirmed', 'inactive', 'rejected', 'reversed')),
    maturity_state text NOT NULL DEFAULT 'immature'
        CHECK (maturity_state IN ('immature', 'mature', 'reversed')),
    matured_at timestamptz,
    disconnected_at timestamptz,
    CHECK ((maturity_state = 'mature') = (matured_at IS NOT NULL)),
    CHECK ((maturity_state = 'reversed') = (disconnected_at IS NOT NULL))
);

ALTER TABLE qbit_pool_blocks
    ADD COLUMN IF NOT EXISTS chain_state text;

UPDATE qbit_pool_blocks
SET chain_state = 'confirmed'
WHERE chain_state IS NULL;

ALTER TABLE qbit_pool_blocks
    ALTER COLUMN chain_state SET DEFAULT 'prepared',
    ALTER COLUMN chain_state SET NOT NULL;

ALTER TABLE qbit_pool_blocks
    DROP CONSTRAINT IF EXISTS qbit_pool_blocks_chain_state_check;

ALTER TABLE qbit_pool_blocks
    ADD CONSTRAINT qbit_pool_blocks_chain_state_check
    CHECK (chain_state IN ('prepared', 'confirmed', 'inactive', 'rejected', 'reversed'));

CREATE TABLE IF NOT EXISTS qbit_pool_audit_bundles (
    block_hash text PRIMARY KEY REFERENCES qbit_pool_blocks(block_hash),
    audit_bundle jsonb NOT NULL,
    audit_bundle_sha256 text NOT NULL,
    coinbase_tx_hex text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Audit-body externalization. The audit_bundle JSONB re-embeds the full
-- accepted-share snapshot per found block, so hot Postgres grows ~quadratically.
-- New blocks store the body in an external content artifact (body_uri) and keep
-- only metadata plus the scalar/array fields the read model and the
-- by-commitment lookup need. Existing rows keep their inline audit_bundle; every
-- reader falls back to it, so this migration loses no data.
ALTER TABLE qbit_pool_audit_bundles
    ADD COLUMN IF NOT EXISTS body_uri text,
    ADD COLUMN IF NOT EXISTS audit_body_byte_len bigint,
    ADD COLUMN IF NOT EXISTS schema_version text,
    ADD COLUMN IF NOT EXISTS found_block_network_difficulty numeric(78,0),
    ADD COLUMN IF NOT EXISTS found_block_bits text,
    ADD COLUMN IF NOT EXISTS found_block_coinbase_value_sats bigint,
    ADD COLUMN IF NOT EXISTS audit_commitment_leaves_hex jsonb,
    ADD COLUMN IF NOT EXISTS witness_merkle_leaves_hex jsonb;

ALTER TABLE qbit_pool_audit_bundles
    ALTER COLUMN audit_bundle DROP NOT NULL;

ALTER TABLE qbit_pool_audit_bundles
    DROP CONSTRAINT IF EXISTS qbit_pool_audit_bundles_body_present_check;

ALTER TABLE qbit_pool_audit_bundles
    ADD CONSTRAINT qbit_pool_audit_bundles_body_present_check
    CHECK (audit_bundle IS NOT NULL OR body_uri IS NOT NULL);

ALTER TABLE qbit_pool_audit_bundles
    DROP CONSTRAINT IF EXISTS qbit_pool_audit_bundles_audit_body_byte_len_check;

ALTER TABLE qbit_pool_audit_bundles
    ADD CONSTRAINT qbit_pool_audit_bundles_audit_body_byte_len_check
    CHECK (audit_body_byte_len IS NULL OR audit_body_byte_len >= 0);

CREATE INDEX IF NOT EXISTS qbit_pool_audit_bundles_commitment_leaves_idx
    ON qbit_pool_audit_bundles USING gin (audit_commitment_leaves_hex);

CREATE INDEX IF NOT EXISTS qbit_pool_audit_bundles_witness_leaves_idx
    ON qbit_pool_audit_bundles USING gin (witness_merkle_leaves_hex);

CREATE INDEX IF NOT EXISTS qbit_pool_audit_bundles_legacy_commitment_leaves_idx
    ON qbit_pool_audit_bundles USING gin ((audit_bundle->'audit_commitment_leaves_hex'))
    WHERE audit_bundle IS NOT NULL;

CREATE INDEX IF NOT EXISTS qbit_pool_audit_bundles_legacy_witness_leaves_idx
    ON qbit_pool_audit_bundles USING gin ((audit_bundle->'witness_merkle_leaves_hex'))
    WHERE audit_bundle IS NOT NULL;

CREATE TABLE IF NOT EXISTS qbit_ctv_fanout_sets (
    block_hash text PRIMARY KEY REFERENCES qbit_pool_blocks(block_hash),
    manifest_set_json text NOT NULL,
    manifest_set jsonb NOT NULL,
    manifest_set_sha256 text NOT NULL,
    settlement_mode text NOT NULL
        CHECK (settlement_mode IN ('hybrid_coinbase_ctv_fanout', 'ctv_fanout')),
    parent_coinbase_txid text NOT NULL,
    parent_coinbase_tx_hex text NOT NULL,
    fanout_count integer NOT NULL CHECK (fanout_count > 0),
    fanout_output_sum_sats bigint NOT NULL CHECK (fanout_output_sum_sats >= 0),
    covenant_output_value_sats bigint NOT NULL CHECK (covenant_output_value_sats >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS qbit_ctv_fanout_artifacts (
    fanout_txid text PRIMARY KEY,
    block_hash text NOT NULL REFERENCES qbit_ctv_fanout_sets(block_hash),
    manifest_set_sha256 text NOT NULL,
    manifest_json text NOT NULL,
    manifest jsonb NOT NULL,
    manifest_sha256 text NOT NULL,
    precommitment_sha256 text NOT NULL,
    ctv_hash text NOT NULL,
    commitment_witness_leaf_hex text NOT NULL,
    chunk_index integer NOT NULL CHECK (chunk_index >= 0),
    chunk_count integer NOT NULL CHECK (chunk_count > 0),
    parent_coinbase_txid text NOT NULL,
    parent_coinbase_vout integer NOT NULL CHECK (parent_coinbase_vout >= 0),
    fanout_tx_template_hex text NOT NULL,
    fanout_tx_hex text NOT NULL,
    anchor_vout integer CHECK (anchor_vout >= 0),
    covenant_output_value_sats bigint NOT NULL CHECK (covenant_output_value_sats >= 0),
    fanout_output_sum_sats bigint NOT NULL CHECK (fanout_output_sum_sats >= 0),
    settlement_status text NOT NULL DEFAULT 'awaiting_maturity'
        CHECK (
            settlement_status IN (
                'awaiting_maturity',
                'broadcastable',
                'broadcast_submitted',
                'confirmed',
                'reorged',
                'failed'
            )
        ),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (block_hash, chunk_index),
    CHECK (chunk_index < chunk_count)
);

ALTER TABLE qbit_ctv_fanout_artifacts
    ADD COLUMN IF NOT EXISTS broadcast_attempt_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS broadcast_attempt_detail_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_broadcast_attempt_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_broadcast_attempt_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_broadcast_attempt_status text,
    ADD COLUMN IF NOT EXISTS last_broadcast_package_tx_hexes jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS last_broadcast_package_txids jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS last_broadcast_submit_result jsonb,
    ADD COLUMN IF NOT EXISTS last_broadcast_error text,
    ADD COLUMN IF NOT EXISTS broadcast_attempt_status_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS next_broadcast_attempt_at timestamptz,
    ADD COLUMN IF NOT EXISTS broadcast_retry_backoff_seconds bigint NOT NULL DEFAULT 0;

ALTER TABLE qbit_ctv_fanout_artifacts
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_broadcast_attempt_count_check,
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_broadcast_attempt_detail_count_check,
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_last_broadcast_attempt_status_check,
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_last_broadcast_package_tx_hexes_check,
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_last_broadcast_package_txids_check,
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_broadcast_attempt_status_counts_check,
    DROP CONSTRAINT IF EXISTS qbit_ctv_fanout_artifacts_broadcast_retry_backoff_seconds_check;

ALTER TABLE qbit_ctv_fanout_artifacts
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_broadcast_attempt_count_check
        CHECK (broadcast_attempt_count >= 0),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_broadcast_attempt_detail_count_check
        CHECK (broadcast_attempt_detail_count >= 0),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_last_broadcast_attempt_status_check
        CHECK (
            last_broadcast_attempt_status IS NULL
            OR last_broadcast_attempt_status IN ('planned', 'submitted', 'accepted', 'rejected', 'failed')
        ),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_last_broadcast_package_tx_hexes_check
        CHECK (jsonb_typeof(last_broadcast_package_tx_hexes) = 'array'),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_last_broadcast_package_txids_check
        CHECK (jsonb_typeof(last_broadcast_package_txids) = 'array'),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_broadcast_attempt_status_counts_check
        CHECK (jsonb_typeof(broadcast_attempt_status_counts) = 'object'),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_broadcast_retry_backoff_seconds_check
        CHECK (broadcast_retry_backoff_seconds >= 0);

-- Anchorless, fee-bearing CTV fanouts store NULL here. Existing
-- deployments created before the nullable schema need the startup repair.
ALTER TABLE qbit_ctv_fanout_artifacts
    ALTER COLUMN anchor_vout DROP NOT NULL;

CREATE TABLE IF NOT EXISTS qbit_ctv_fanout_broadcast_attempts (
    attempt_seq bigserial PRIMARY KEY,
    fanout_txid text NOT NULL REFERENCES qbit_ctv_fanout_artifacts(fanout_txid),
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_status text NOT NULL
        CHECK (attempt_status IN ('planned', 'submitted', 'accepted', 'rejected', 'failed')),
    package_tx_hexes jsonb NOT NULL DEFAULT '[]'::jsonb,
    package_txids jsonb NOT NULL DEFAULT '[]'::jsonb,
    submit_result jsonb,
    error text,
    CHECK (jsonb_typeof(package_tx_hexes) = 'array'),
    CHECK (jsonb_typeof(package_txids) = 'array')
);

CREATE TABLE IF NOT EXISTS qbit_pool_payout_entries (
    payout_entry_seq bigserial PRIMARY KEY,
    block_hash text NOT NULL REFERENCES qbit_pool_blocks(block_hash),
    block_height bigint NOT NULL CHECK (block_height >= 0),
    miner_id text NOT NULL,
    payout_order_key text NOT NULL,
    p2mr_program bytea NOT NULL CHECK (octet_length(p2mr_program) = 32),
    onchain_amount_sats bigint NOT NULL CHECK (onchain_amount_sats >= 0),
    carry_forward_balance_sats numeric(78, 0) NOT NULL,
    action text NOT NULL CHECK (action IN ('onchain', 'accrued')),
    maturity_state text NOT NULL DEFAULT 'immature'
        CHECK (maturity_state IN ('immature', 'mature', 'reversed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS qbit_payout_carry_forward_miner_idx
    ON qbit_payout_carry_forward (miner_id, payout_order_key, carry_forward_seq DESC);

CREATE INDEX IF NOT EXISTS qbit_payout_carry_forward_current_idx
    ON qbit_payout_carry_forward (
        miner_id,
        payout_order_key,
        p2mr_program,
        block_height DESC,
        carry_forward_seq DESC
    )
    WHERE maturity_state <> 'reversed';

CREATE INDEX IF NOT EXISTS qbit_payout_carry_forward_maturity_idx
    ON qbit_payout_carry_forward (maturity_state, block_height);

CREATE INDEX IF NOT EXISTS qbit_pool_blocks_maturity_idx
    ON qbit_pool_blocks (maturity_state, block_height);

CREATE INDEX IF NOT EXISTS qbit_pool_audit_bundles_created_idx
    ON qbit_pool_audit_bundles (created_at DESC);

CREATE INDEX IF NOT EXISTS qbit_ctv_fanout_artifacts_block_idx
    ON qbit_ctv_fanout_artifacts (block_hash, chunk_index);

CREATE INDEX IF NOT EXISTS qbit_ctv_fanout_artifacts_status_idx
    ON qbit_ctv_fanout_artifacts (settlement_status, block_hash);

CREATE INDEX IF NOT EXISTS qbit_ctv_fanout_artifacts_broadcast_candidate_idx
    ON qbit_ctv_fanout_artifacts (settlement_status, next_broadcast_attempt_at, block_hash, chunk_index);

CREATE INDEX IF NOT EXISTS qbit_ctv_fanout_broadcast_attempts_txid_idx
    ON qbit_ctv_fanout_broadcast_attempts (fanout_txid, attempt_seq DESC);

CREATE INDEX IF NOT EXISTS qbit_pool_payout_entries_block_idx
    ON qbit_pool_payout_entries (block_hash, payout_entry_seq);

CREATE INDEX IF NOT EXISTS qbit_pool_payout_entries_maturity_idx
    ON qbit_pool_payout_entries (maturity_state, block_height);

CREATE INDEX IF NOT EXISTS qbit_pool_blocks_public_recent_idx
    ON qbit_pool_blocks (block_height DESC, found_at DESC)
    INCLUDE (block_hash, payout_manifest_sha256, coinbase_txid, chain_state, maturity_state)
    WHERE chain_state <> 'reversed';

CREATE INDEX IF NOT EXISTS qbit_pool_payout_entries_miner_public_history_idx
    ON qbit_pool_payout_entries (miner_id, block_height DESC, payout_entry_seq DESC)
    INCLUDE (
        block_hash,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action,
        maturity_state,
        created_at
    )
    WHERE maturity_state <> 'reversed';

CREATE INDEX IF NOT EXISTS qbit_share_ledger_accepted_window_idx
    ON qbit_share_ledger (job_issued_at, share_seq DESC)
    WHERE accepted;

CREATE INDEX IF NOT EXISTS qbit_share_ledger_template_height_idx
    ON qbit_share_ledger (template_height, share_seq)
    WHERE accepted;

CREATE INDEX IF NOT EXISTS qbit_share_ledger_accepted_recent_idx
    ON qbit_share_ledger (accepted_at DESC)
    INCLUDE (share_difficulty, miner_id, share_seq)
    WHERE accepted;

CREATE INDEX IF NOT EXISTS qbit_share_ledger_accepted_miner_recent_idx
    ON qbit_share_ledger (miner_id, accepted_at DESC)
    INCLUDE (share_difficulty, share_seq, share_id, payout_order_key)
    WHERE accepted;

CREATE INDEX IF NOT EXISTS qbit_share_ledger_accepted_seq_window_idx
    ON qbit_share_ledger (share_seq DESC)
    INCLUDE (
        job_issued_at,
        accepted_at,
        miner_id,
        payout_order_key,
        p2mr_program,
        share_difficulty,
        share_id
    )
    WHERE accepted;

CREATE INDEX IF NOT EXISTS qbit_share_ledger_accepted_block_suffix_idx
    ON qbit_share_ledger ((lower(right(share_id, 64))), accepted_at DESC, share_seq DESC)
    INCLUDE (miner_id, share_difficulty, network_difficulty)
    WHERE accepted AND length(share_id) >= 65;

CREATE INDEX IF NOT EXISTS qbit_payout_carry_forward_miner_public_history_idx
    ON qbit_payout_carry_forward (miner_id, block_height DESC, carry_forward_seq DESC)
    INCLUDE (
        block_hash,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action,
        maturity_state,
        created_at
    )
    WHERE maturity_state <> 'reversed';

CREATE INDEX IF NOT EXISTS qbit_payout_carry_forward_block_amount_idx
    ON qbit_payout_carry_forward (block_hash)
    INCLUDE (gross_amount_sats);

DROP FUNCTION IF EXISTS qbit_audit_share_window(timestamptz, numeric);
DROP FUNCTION IF EXISTS qbit_prism_window(timestamptz, numeric);

CREATE OR REPLACE FUNCTION qbit_prism_window(
    anchor_job_issued_at timestamptz,
    window_weight numeric
)
RETURNS TABLE (
    share_seq bigint,
    share_id text,
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    share_difficulty numeric,
    counted_difficulty numeric,
    job_issued_at timestamptz,
    accepted_at timestamptz,
    credit_policy text
)
LANGUAGE sql
STABLE
AS $$
    WITH RECURSIVE eligible AS (
        (
            SELECT
                ledger.*,
                ledger.share_difficulty::numeric AS cumulative_difficulty
            FROM qbit_share_ledger ledger
            WHERE ledger.accepted
              AND ledger.job_issued_at <= anchor_job_issued_at
              AND ledger.accepted_at <= anchor_job_issued_at
            ORDER BY ledger.share_seq DESC
            LIMIT 1
        )
        UNION ALL
        SELECT
            next_ledger.*,
            eligible.cumulative_difficulty + next_ledger.share_difficulty AS cumulative_difficulty
        FROM eligible
        CROSS JOIN LATERAL (
            SELECT ledger.*
            FROM qbit_share_ledger ledger
            WHERE ledger.accepted
              AND ledger.job_issued_at <= anchor_job_issued_at
              AND ledger.accepted_at <= anchor_job_issued_at
              AND ledger.share_seq < eligible.share_seq
            ORDER BY ledger.share_seq DESC
            LIMIT 1
        ) next_ledger
        WHERE eligible.cumulative_difficulty < window_weight
    )
    SELECT
        eligible.share_seq,
        eligible.share_id,
        eligible.miner_id,
        eligible.payout_order_key,
        eligible.p2mr_program,
        eligible.share_difficulty,
        CASE
            WHEN eligible.cumulative_difficulty <= window_weight THEN eligible.share_difficulty
            ELSE eligible.share_difficulty - (eligible.cumulative_difficulty - window_weight)
        END AS counted_difficulty,
        eligible.job_issued_at,
        eligible.accepted_at,
        eligible.credit_policy
    FROM eligible
    WHERE eligible.cumulative_difficulty - eligible.share_difficulty < window_weight
    ORDER BY eligible.share_seq DESC;
$$;

CREATE OR REPLACE FUNCTION qbit_shares_since_template_height(
    min_template_height bigint
)
RETURNS SETOF qbit_share_ledger
LANGUAGE sql
STABLE
AS $$
    SELECT *
    FROM qbit_share_ledger
    WHERE accepted
      AND template_height >= min_template_height
    ORDER BY share_seq ASC;
$$;

CREATE OR REPLACE FUNCTION qbit_current_carry_forward_balances()
RETURNS TABLE (
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    balance_sats numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH balances AS (
        SELECT
            (array_agg(ledger.miner_id ORDER BY ledger.payout_order_key, ledger.miner_id))[1] AS miner_id,
            (array_agg(ledger.payout_order_key ORDER BY ledger.payout_order_key, ledger.miner_id))[1] AS payout_order_key,
            ledger.p2mr_program,
            SUM(ledger.gross_amount_sats::numeric - ledger.onchain_amount_sats::numeric) AS balance_sats
        FROM qbit_payout_carry_forward ledger
        JOIN qbit_pool_blocks block
          ON block.block_hash = ledger.block_hash
        WHERE ledger.maturity_state <> 'reversed'
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
        GROUP BY
            ledger.p2mr_program
        HAVING SUM(ledger.gross_amount_sats::numeric - ledger.onchain_amount_sats::numeric) <> 0
    )
    SELECT
        balances.miner_id,
        balances.payout_order_key,
        balances.p2mr_program,
        balances.balance_sats
    FROM balances
    ORDER BY
        balances.payout_order_key,
        balances.miner_id,
        balances.p2mr_program;
$$;

CREATE OR REPLACE FUNCTION qbit_current_owed_balances()
RETURNS TABLE (
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    owed_balance_sats numeric
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        balances.miner_id,
        balances.payout_order_key,
        balances.p2mr_program,
        GREATEST(balances.balance_sats, 0) AS owed_balance_sats
    FROM qbit_current_carry_forward_balances() AS balances;
$$;

CREATE OR REPLACE FUNCTION qbit_carry_forward_integrity_mismatches()
RETURNS TABLE (
    carry_forward_seq bigint,
    block_hash text,
    block_height bigint,
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    prior_balance_sats numeric,
    expected_prior_balance_sats numeric,
    gross_amount_sats bigint,
    candidate_balance_sats numeric,
    expected_candidate_balance_sats numeric,
    onchain_amount_sats bigint,
    settlement_fee_sats bigint,
    carry_forward_balance_sats numeric,
    expected_carry_forward_balance_sats numeric,
    action text,
    mismatch_reason text
)
LANGUAGE sql
STABLE
AS $$
    WITH active AS (
        SELECT ledger.*
        FROM qbit_payout_carry_forward ledger
        JOIN qbit_pool_blocks block
          ON block.block_hash = ledger.block_hash
        WHERE ledger.maturity_state <> 'reversed'
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
    ),
    checked AS (
        SELECT
            active.*,
            COALESCE(
                SUM(active.gross_amount_sats::numeric - active.onchain_amount_sats::numeric)
                OVER (
                    PARTITION BY active.miner_id, active.payout_order_key, active.p2mr_program
                    ORDER BY active.block_height ASC, active.carry_forward_seq ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0::numeric
            ) AS expected_prior_balance_sats
        FROM active
    ),
    expected AS (
        SELECT
            checked.*,
            checked.expected_prior_balance_sats + checked.gross_amount_sats::numeric
                AS expected_candidate_balance_sats,
            checked.expected_prior_balance_sats + checked.gross_amount_sats::numeric
                - checked.onchain_amount_sats::numeric
                AS expected_carry_forward_balance_sats
        FROM checked
    )
    SELECT
        expected.carry_forward_seq,
        expected.block_hash,
        expected.block_height,
        expected.miner_id,
        expected.payout_order_key,
        expected.p2mr_program,
        expected.prior_balance_sats,
        expected.expected_prior_balance_sats,
        expected.gross_amount_sats,
        expected.candidate_balance_sats,
        expected.expected_candidate_balance_sats,
        expected.onchain_amount_sats,
        expected.settlement_fee_sats,
        expected.carry_forward_balance_sats,
        expected.expected_carry_forward_balance_sats,
        expected.action,
        concat_ws(
            ',',
            CASE
                WHEN expected.prior_balance_sats <> expected.expected_prior_balance_sats
                THEN 'prior_balance'
            END,
            CASE
                WHEN expected.candidate_balance_sats <> expected.expected_candidate_balance_sats
                THEN 'candidate_balance'
            END,
            CASE
                WHEN expected.carry_forward_balance_sats <> expected.expected_carry_forward_balance_sats
                THEN 'carry_forward_balance'
            END
        ) AS mismatch_reason
    FROM expected
    WHERE expected.prior_balance_sats <> expected.expected_prior_balance_sats
       OR expected.candidate_balance_sats <> expected.expected_candidate_balance_sats
       OR expected.carry_forward_balance_sats <> expected.expected_carry_forward_balance_sats
    ORDER BY expected.block_height ASC, expected.carry_forward_seq ASC;
$$;

CREATE OR REPLACE FUNCTION qbit_carry_forward_integrity_report()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    SELECT jsonb_build_object(
        'schema', 'qbit.prism.carry-forward-integrity.v1',
        'checked_active_rows', (
            SELECT count(*)
            FROM qbit_payout_carry_forward ledger
            JOIN qbit_pool_blocks block
              ON block.block_hash = ledger.block_hash
            WHERE ledger.maturity_state <> 'reversed'
              AND block.chain_state = 'confirmed'
              AND block.maturity_state <> 'reversed'
        ),
        'mismatch_count', (SELECT count(*) FROM qbit_carry_forward_integrity_mismatches()),
        'mismatches', COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'carry_forward_seq', mismatch.carry_forward_seq,
                        'block_hash', mismatch.block_hash,
                        'block_height', mismatch.block_height,
                        'recipient_id', mismatch.miner_id,
                        'order_key', mismatch.payout_order_key,
                        'p2mr_program_hex', encode(mismatch.p2mr_program, 'hex'),
                        'prior_balance_sats', mismatch.prior_balance_sats::text,
                        'expected_prior_balance_sats', mismatch.expected_prior_balance_sats::text,
                        'gross_amount_sats', mismatch.gross_amount_sats,
                        'candidate_balance_sats', mismatch.candidate_balance_sats::text,
                        'expected_candidate_balance_sats', mismatch.expected_candidate_balance_sats::text,
                        'onchain_amount_sats', mismatch.onchain_amount_sats,
                        'settlement_fee_sats', mismatch.settlement_fee_sats,
                        'carry_forward_balance_sats', mismatch.carry_forward_balance_sats::text,
                        'expected_carry_forward_balance_sats',
                            mismatch.expected_carry_forward_balance_sats::text,
                        'action', mismatch.action,
                        'mismatch_reason', mismatch.mismatch_reason
                    )
                    ORDER BY mismatch.block_height ASC, mismatch.carry_forward_seq ASC
                )
                FROM qbit_carry_forward_integrity_mismatches() mismatch
            ),
            '[]'::jsonb
        )
    );
$$;

CREATE OR REPLACE FUNCTION qbit_audit_share_window(
    anchor_job_issued_at timestamptz,
    network_difficulty numeric
)
RETURNS TABLE (
    window_multiplier numeric,
    requested_window_weight numeric,
    share_seq bigint,
    share_id text,
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    share_difficulty numeric,
    counted_difficulty numeric,
    job_issued_at timestamptz,
    accepted_at timestamptz,
    credit_policy text
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        8::numeric AS window_multiplier,
        network_difficulty * 8::numeric AS requested_window_weight,
        ledger_window.share_seq,
        ledger_window.share_id,
        ledger_window.miner_id,
        ledger_window.payout_order_key,
        ledger_window.p2mr_program,
        ledger_window.share_difficulty,
        ledger_window.counted_difficulty,
        ledger_window.job_issued_at,
        ledger_window.accepted_at,
        ledger_window.credit_policy
    FROM qbit_prism_window(anchor_job_issued_at, network_difficulty * 8::numeric) AS ledger_window;
$$;

DROP FUNCTION IF EXISTS qbit_audit_block_payouts(text);

CREATE OR REPLACE FUNCTION qbit_audit_block_payouts(
    target_block_hash text
)
RETURNS TABLE (
    block_hash text,
    block_height bigint,
    coinbase_txid text,
    payout_manifest_sha256 text,
    chain_state text,
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    onchain_amount_sats bigint,
    carry_forward_balance_sats numeric,
    action text,
    maturity_state text
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        block.block_hash,
        block.block_height,
        block.coinbase_txid,
        block.payout_manifest_sha256,
        block.chain_state,
        payout.miner_id,
        payout.payout_order_key,
        payout.p2mr_program,
        payout.onchain_amount_sats,
        payout.carry_forward_balance_sats,
        payout.action,
        payout.maturity_state
    FROM qbit_pool_blocks block
    JOIN qbit_pool_payout_entries payout
      ON payout.block_hash = block.block_hash
    WHERE block.block_hash = target_block_hash
    ORDER BY payout.payout_order_key, payout.miner_id, payout.p2mr_program;
$$;

CREATE OR REPLACE FUNCTION qbit_audit_block_fanouts(
    target_block_hash text
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        (
            SELECT jsonb_build_object(
                'schema', 'qbit.prism.ctv-fanout-recovery.v1',
                'block_hash', fanout_set.block_hash,
                'block_height', block.block_height,
                'parent_hash', block.parent_hash,
                'chain_state', block.chain_state,
                'maturity_state', block.maturity_state,
                'coinbase_txid', block.coinbase_txid,
                'payout_manifest_sha256', block.payout_manifest_sha256,
                'audit_bundle_sha256', bundle.audit_bundle_sha256,
                'manifest_set_sha256', fanout_set.manifest_set_sha256,
                'manifest_set_json', fanout_set.manifest_set_json,
                'settlement_mode', fanout_set.settlement_mode,
                'parent_coinbase_txid', fanout_set.parent_coinbase_txid,
                'parent_coinbase_tx_hex', fanout_set.parent_coinbase_tx_hex,
                'fanout_count', fanout_set.fanout_count,
                'fanout_output_sum_sats', fanout_set.fanout_output_sum_sats,
                'covenant_output_value_sats', fanout_set.covenant_output_value_sats,
                'manifest_set', fanout_set.manifest_set,
                'artifacts', COALESCE(
                    (
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'fanout_txid', artifact.fanout_txid,
                                'manifest_json', artifact.manifest_json,
                                'manifest_sha256', artifact.manifest_sha256,
                                'manifest', artifact.manifest,
                                'precommitment_sha256', artifact.precommitment_sha256,
                                'ctv_hash', artifact.ctv_hash,
                                'commitment_witness_leaf_hex', artifact.commitment_witness_leaf_hex,
                                'chunk_index', artifact.chunk_index,
                                'chunk_count', artifact.chunk_count,
                                'parent_coinbase_txid', artifact.parent_coinbase_txid,
                                'parent_coinbase_vout', artifact.parent_coinbase_vout,
                                'fanout_tx_template_hex', artifact.fanout_tx_template_hex,
                                'fanout_tx_hex', artifact.fanout_tx_hex,
                                'anchor_vout', artifact.anchor_vout,
                                'covenant_output_value_sats', artifact.covenant_output_value_sats,
                                'fanout_output_sum_sats', artifact.fanout_output_sum_sats,
                                'settlement_status', artifact.settlement_status,
                                'updated_at', artifact.updated_at::text,
                                'broadcast_attempt_count', artifact.broadcast_attempt_count,
                                'broadcast_attempt_detail_count', artifact.broadcast_attempt_detail_count,
                                'first_broadcast_attempt_at', artifact.first_broadcast_attempt_at::text,
                                'last_broadcast_attempt_at', artifact.last_broadcast_attempt_at::text,
                                'last_broadcast_attempt_status', artifact.last_broadcast_attempt_status,
                                'last_broadcast_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
                                'last_broadcast_package_txids', artifact.last_broadcast_package_txids,
                                'last_broadcast_submit_result', artifact.last_broadcast_submit_result,
                                'last_broadcast_error', artifact.last_broadcast_error,
                                'broadcast_attempt_status_counts', artifact.broadcast_attempt_status_counts,
                                'next_broadcast_attempt_at', artifact.next_broadcast_attempt_at::text,
                                'broadcast_retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds,
                                'broadcast_attempt_summary', jsonb_build_object(
                                    'attempt_count', artifact.broadcast_attempt_count,
                                    'detail_count', artifact.broadcast_attempt_detail_count,
                                    'first_attempt_at', artifact.first_broadcast_attempt_at::text,
                                    'last_attempt_at', artifact.last_broadcast_attempt_at::text,
                                    'last_attempt_status', artifact.last_broadcast_attempt_status,
                                    'last_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
                                    'last_package_txids', artifact.last_broadcast_package_txids,
                                    'last_submit_result', artifact.last_broadcast_submit_result,
                                    'last_error', artifact.last_broadcast_error,
                                    'status_counts', artifact.broadcast_attempt_status_counts,
                                    'next_attempt_at', artifact.next_broadcast_attempt_at::text,
                                    'retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds
                                )
                            )
                            ORDER BY artifact.chunk_index
                        )
                        FROM qbit_ctv_fanout_artifacts artifact
                        WHERE artifact.block_hash = fanout_set.block_hash
                    ),
                    '[]'::jsonb
                )
            )
            FROM qbit_ctv_fanout_sets fanout_set
            JOIN qbit_pool_blocks block
              ON block.block_hash = fanout_set.block_hash
            LEFT JOIN qbit_pool_audit_bundles bundle
              ON bundle.block_hash = fanout_set.block_hash
            WHERE fanout_set.block_hash = target_block_hash
        ),
        'null'::jsonb
    );
$$;

CREATE OR REPLACE FUNCTION qbit_fanout_status(
    target_fanout_txid text
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        (
            SELECT jsonb_build_object(
                'schema', 'qbit.prism.ctv-fanout-status.v1',
                'fanout_txid', artifact.fanout_txid,
                'block_hash', artifact.block_hash,
                'block_height', block.block_height,
                'parent_hash', block.parent_hash,
                'chain_state', block.chain_state,
                'maturity_state', block.maturity_state,
                'coinbase_txid', block.coinbase_txid,
                'payout_manifest_sha256', block.payout_manifest_sha256,
                'audit_bundle_sha256', bundle.audit_bundle_sha256,
                'manifest_set_sha256', artifact.manifest_set_sha256,
                'manifest_json', artifact.manifest_json,
                'manifest_sha256', artifact.manifest_sha256,
                'manifest', artifact.manifest,
                'precommitment_sha256', artifact.precommitment_sha256,
                'ctv_hash', artifact.ctv_hash,
                'commitment_witness_leaf_hex', artifact.commitment_witness_leaf_hex,
                'chunk_index', artifact.chunk_index,
                'chunk_count', artifact.chunk_count,
                'parent_coinbase_txid', artifact.parent_coinbase_txid,
                'parent_coinbase_vout', artifact.parent_coinbase_vout,
                'fanout_tx_template_hex', artifact.fanout_tx_template_hex,
                'fanout_tx_hex', artifact.fanout_tx_hex,
                'anchor_vout', artifact.anchor_vout,
                'covenant_output_value_sats', artifact.covenant_output_value_sats,
                'fanout_output_sum_sats', artifact.fanout_output_sum_sats,
                'settlement_status', artifact.settlement_status,
                'updated_at', artifact.updated_at::text,
                'broadcast_attempt_count', artifact.broadcast_attempt_count,
                'broadcast_attempt_detail_count', artifact.broadcast_attempt_detail_count,
                'first_broadcast_attempt_at', artifact.first_broadcast_attempt_at::text,
                'last_broadcast_attempt_at', artifact.last_broadcast_attempt_at::text,
                'last_broadcast_attempt_status', artifact.last_broadcast_attempt_status,
                'last_broadcast_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
                'last_broadcast_package_txids', artifact.last_broadcast_package_txids,
                'last_broadcast_submit_result', artifact.last_broadcast_submit_result,
                'last_broadcast_error', artifact.last_broadcast_error,
                'broadcast_attempt_status_counts', artifact.broadcast_attempt_status_counts,
                'next_broadcast_attempt_at', artifact.next_broadcast_attempt_at::text,
                'broadcast_retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds,
                'broadcast_attempt_summary', jsonb_build_object(
                    'attempt_count', artifact.broadcast_attempt_count,
                    'detail_count', artifact.broadcast_attempt_detail_count,
                    'first_attempt_at', artifact.first_broadcast_attempt_at::text,
                    'last_attempt_at', artifact.last_broadcast_attempt_at::text,
                    'last_attempt_status', artifact.last_broadcast_attempt_status,
                    'last_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
                    'last_package_txids', artifact.last_broadcast_package_txids,
                    'last_submit_result', artifact.last_broadcast_submit_result,
                    'last_error', artifact.last_broadcast_error,
                    'status_counts', artifact.broadcast_attempt_status_counts,
                    'next_attempt_at', artifact.next_broadcast_attempt_at::text,
                    'retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds
                ),
                'broadcast_attempts', COALESCE(
                    (
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'attempt_seq', attempt.attempt_seq,
                                'attempted_at', attempt.attempted_at::text,
                                'attempt_status', attempt.attempt_status,
                                'package_tx_hexes', attempt.package_tx_hexes,
                                'package_txids', attempt.package_txids,
                                'submit_result', attempt.submit_result,
                                'error', attempt.error
                            )
                            ORDER BY attempt.attempt_seq ASC
                        )
                        FROM qbit_ctv_fanout_broadcast_attempts attempt
                        WHERE attempt.fanout_txid = artifact.fanout_txid
                    ),
                    '[]'::jsonb
                )
            )
            FROM qbit_ctv_fanout_artifacts artifact
            JOIN qbit_pool_blocks block
              ON block.block_hash = artifact.block_hash
            LEFT JOIN qbit_pool_audit_bundles bundle
              ON bundle.block_hash = artifact.block_hash
            WHERE artifact.fanout_txid = target_fanout_txid
        ),
        'null'::jsonb
    );
$$;

CREATE OR REPLACE FUNCTION qbit_mark_mature_pool_payouts(
    active_tip_height bigint
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    payout_count integer;
BEGIN
    UPDATE qbit_pool_blocks
    SET maturity_state = 'mature',
        matured_at = clock_timestamp()
    WHERE maturity_state = 'immature'
      AND chain_state = 'confirmed'
      AND active_tip_height >= block_height + 1000;

    UPDATE qbit_pool_payout_entries payout
    SET maturity_state = 'mature'
    FROM qbit_pool_blocks block
    WHERE payout.block_hash = block.block_hash
      AND block.chain_state = 'confirmed'
      AND payout.maturity_state = 'immature'
      AND active_tip_height >= payout.block_height + 1000;

    GET DIAGNOSTICS payout_count = ROW_COUNT;

    UPDATE qbit_payout_carry_forward carry
    SET maturity_state = 'mature'
    FROM qbit_pool_blocks block
    WHERE carry.block_hash = block.block_hash
      AND block.chain_state = 'confirmed'
      AND carry.maturity_state = 'immature'
      AND active_tip_height >= carry.block_height + 1000;

    UPDATE qbit_ctv_fanout_artifacts artifact
    SET settlement_status = 'broadcastable',
        updated_at = clock_timestamp()
    FROM qbit_pool_blocks block
    WHERE artifact.block_hash = block.block_hash
      AND block.chain_state = 'confirmed'
      AND block.maturity_state = 'mature'
      AND artifact.settlement_status = 'awaiting_maturity'
      AND active_tip_height >= block.block_height + 1000;

    RETURN payout_count;
END;
$$;

DROP FUNCTION IF EXISTS qbit_confirm_pool_block(text, bigint, text, bigint, text);

CREATE OR REPLACE FUNCTION qbit_confirm_pool_block(
    confirmed_block_hash text,
    active_tip_height bigint,
    active_writer_id text,
    active_writer_epoch bigint,
    active_writer_session_token text,
    lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    lease_count integer;
    confirmed_count integer;
BEGIN
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + lease_duration,
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = active_writer_id
      AND writer_epoch = active_writer_epoch
      AND writer_session_token = active_writer_session_token;
    GET DIAGNOSTICS lease_count = ROW_COUNT;

    IF lease_count = 0 THEN
        RAISE EXCEPTION 'writer lease is not active';
    END IF;

    UPDATE qbit_pool_blocks
    SET chain_state = 'confirmed'
    WHERE block_hash = confirmed_block_hash
      AND block_height = active_tip_height
      AND chain_state = 'prepared'
      AND maturity_state = 'immature';
    GET DIAGNOSTICS confirmed_count = ROW_COUNT;

    IF confirmed_count = 0
       AND EXISTS (
           SELECT 1
           FROM qbit_pool_blocks
           WHERE block_hash = confirmed_block_hash
             AND block_height = active_tip_height
             AND chain_state = 'confirmed'
             AND maturity_state <> 'reversed'
       ) THEN
        RETURN 1;
    END IF;

    RETURN confirmed_count;
END;
$$;

DROP FUNCTION IF EXISTS qbit_mark_pool_block_inactive(text, bigint, text, bigint, text);

CREATE OR REPLACE FUNCTION qbit_mark_pool_block_inactive(
    disconnected_block_hash text,
    active_tip_height bigint,
    active_writer_id text,
    active_writer_epoch bigint,
    active_writer_session_token text,
    lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    lease_count integer;
    inactive_count integer;
BEGIN
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + lease_duration,
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = active_writer_id
      AND writer_epoch = active_writer_epoch
      AND writer_session_token = active_writer_session_token;
    GET DIAGNOSTICS lease_count = ROW_COUNT;

    IF lease_count = 0 THEN
        RAISE EXCEPTION 'writer lease is not active';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM qbit_pool_blocks
        WHERE block_hash = disconnected_block_hash
          AND chain_state IN ('confirmed', 'inactive')
          AND maturity_state = 'mature'
    ) THEN
        RAISE EXCEPTION 'refusing to mark mature pool block inactive %', disconnected_block_hash;
    END IF;

    UPDATE qbit_pool_blocks
    SET chain_state = 'inactive'
    WHERE block_hash = disconnected_block_hash
      AND chain_state = 'confirmed'
      AND maturity_state = 'immature';
    GET DIAGNOSTICS inactive_count = ROW_COUNT;

    RETURN inactive_count;
END;
$$;

DROP FUNCTION IF EXISTS qbit_reactivate_pool_block(text, bigint, text, bigint, text);

CREATE OR REPLACE FUNCTION qbit_reactivate_pool_block(
    reconnected_block_hash text,
    active_tip_height bigint,
    active_writer_id text,
    active_writer_epoch bigint,
    active_writer_session_token text,
    lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    lease_count integer;
    reactivated_count integer;
BEGIN
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + lease_duration,
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = active_writer_id
      AND writer_epoch = active_writer_epoch
      AND writer_session_token = active_writer_session_token;
    GET DIAGNOSTICS lease_count = ROW_COUNT;

    IF lease_count = 0 THEN
        RAISE EXCEPTION 'writer lease is not active';
    END IF;

    UPDATE qbit_pool_blocks
    SET chain_state = 'confirmed'
    WHERE block_hash = reconnected_block_hash
      AND block_height <= active_tip_height
      AND chain_state = 'inactive'
      AND maturity_state = 'immature';
    GET DIAGNOSTICS reactivated_count = ROW_COUNT;

    RETURN reactivated_count;
END;
$$;

DROP FUNCTION IF EXISTS qbit_reject_prepared_pool_block(text, bigint, text, bigint, text);

CREATE OR REPLACE FUNCTION qbit_reject_prepared_pool_block(
    rejected_block_hash text,
    active_tip_height bigint,
    active_writer_id text,
    active_writer_epoch bigint,
    active_writer_session_token text,
    lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    lease_count integer;
    block_count integer;
    payout_count integer;
    carry_count integer;
    fanout_count integer;
BEGIN
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + lease_duration,
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = active_writer_id
      AND writer_epoch = active_writer_epoch
      AND writer_session_token = active_writer_session_token;
    GET DIAGNOSTICS lease_count = ROW_COUNT;

    IF lease_count = 0 THEN
        RAISE EXCEPTION 'writer lease is not active';
    END IF;

    UPDATE qbit_pool_blocks
    SET chain_state = 'rejected',
        maturity_state = 'reversed',
        disconnected_at = clock_timestamp()
    WHERE block_hash = rejected_block_hash
      AND chain_state = 'prepared'
      AND maturity_state = 'immature';
    GET DIAGNOSTICS block_count = ROW_COUNT;

    IF block_count = 0 THEN
        RETURN 0;
    END IF;

    UPDATE qbit_pool_payout_entries
    SET maturity_state = 'reversed'
    WHERE block_hash = rejected_block_hash
      AND maturity_state = 'immature';
    GET DIAGNOSTICS payout_count = ROW_COUNT;

    UPDATE qbit_payout_carry_forward
    SET maturity_state = 'reversed'
    WHERE block_hash = rejected_block_hash
      AND maturity_state = 'immature';
    GET DIAGNOSTICS carry_count = ROW_COUNT;

    UPDATE qbit_ctv_fanout_artifacts
    SET settlement_status = 'reorged',
        updated_at = clock_timestamp()
    WHERE block_hash = rejected_block_hash
      AND settlement_status <> 'confirmed';
    GET DIAGNOSTICS fanout_count = ROW_COUNT;

    RETURN block_count + payout_count + carry_count + fanout_count;
END;
$$;

DROP FUNCTION IF EXISTS qbit_reverse_immature_pool_block(text, bigint);
DROP FUNCTION IF EXISTS qbit_reverse_immature_pool_block(text, bigint, text, bigint, text);

CREATE OR REPLACE FUNCTION qbit_reverse_immature_pool_block(
    disconnected_block_hash text,
    active_tip_height bigint,
    active_writer_id text,
    active_writer_epoch bigint,
    active_writer_session_token text,
    lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    lease_count integer;
    block_count integer;
    payout_count integer;
    carry_count integer;
    fanout_count integer;
BEGIN
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + lease_duration,
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = active_writer_id
      AND writer_epoch = active_writer_epoch
      AND writer_session_token = active_writer_session_token;
    GET DIAGNOSTICS lease_count = ROW_COUNT;

    IF lease_count = 0 THEN
        RAISE EXCEPTION 'writer lease is not active';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM qbit_pool_blocks
        WHERE block_hash = disconnected_block_hash
          AND chain_state IN ('confirmed', 'inactive')
          AND (
              maturity_state = 'mature'
              OR active_tip_height >= block_height + 1000
          )
    ) THEN
        RAISE EXCEPTION 'refusing to reverse mature pool block %', disconnected_block_hash;
    END IF;

    UPDATE qbit_pool_blocks
    SET chain_state = 'reversed',
        maturity_state = 'reversed',
        disconnected_at = clock_timestamp()
    WHERE block_hash = disconnected_block_hash
      AND chain_state IN ('prepared', 'confirmed', 'inactive')
      AND maturity_state = 'immature';
    GET DIAGNOSTICS block_count = ROW_COUNT;

    UPDATE qbit_pool_payout_entries
    SET maturity_state = 'reversed'
    WHERE block_hash = disconnected_block_hash
      AND maturity_state = 'immature';
    GET DIAGNOSTICS payout_count = ROW_COUNT;

    UPDATE qbit_payout_carry_forward
    SET maturity_state = 'reversed'
    WHERE block_hash = disconnected_block_hash
      AND maturity_state = 'immature';
    GET DIAGNOSTICS carry_count = ROW_COUNT;

    UPDATE qbit_ctv_fanout_artifacts
    SET settlement_status = 'reorged',
        updated_at = clock_timestamp()
    WHERE block_hash = disconnected_block_hash
      AND settlement_status <> 'confirmed';
    GET DIAGNOSTICS fanout_count = ROW_COUNT;

    RETURN block_count + payout_count + carry_count + fanout_count;
END;
$$;
