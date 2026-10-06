-- #708: check each legacy (unmarked) carry-forward chain per payout program,
-- the key 2.x keeps carry balances by, instead of per case-sensitive label.
--
-- 2.x carries a miner's balance per p2mr_program. qbit_current_carry_forward_
-- balances() sums a program's per-label summary rows, and the payout policy
-- looks the prior balance up by program. Stratum keeps the address as the
-- miner typed it, and bech32 is case-insensitive, so the uppercase and
-- lowercase spellings of one address are two labels of one program. On
-- union, one program carries both: uppercase on 5,527 rows (heights 11341 to
-- 118151) and lowercase on 116 (11470 to 12772), one running balance between
-- them.
--
-- 001's and 011's sequential rule partitioned the running sum by
-- (miner_id, payout_order_key, p2mr_program). On the restored union ledger it
-- found 5,506 mismatches, all in that one program. The same rule partitioned
-- by p2mr_program alone found 0 over all 2,753,849 active rows, and
-- qbit_carry_forward_current_drift() found none. self-check and fatal-state
-- clear refuse any mismatch, so the false alarm would refuse the cutover.
--
-- This replaces the validator in place with 011's body, changing only the
-- legacy rule's window partition. The marked (as-issued) rule, the report
-- that wraps it, the per-label summary and its drift check are unchanged. A
-- validator replacement changes no stored row or format, so it needs no
-- capability or shutdown proof. An earlier native binary that meets the
-- database accepts the unknown migration with a warning and reads the
-- corrected report.
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
        SELECT ledger.*, block.as_issued_audit_sha256
        FROM qbit_payout_carry_forward ledger
        JOIN qbit_pool_blocks block
          ON block.block_hash = ledger.block_hash
        WHERE ledger.maturity_state <> 'reversed'
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
    ),
    -- A program's chain runs across every label it was paid under (#708),
    -- as qbit_current_carry_forward_balances() sums it.
    sequential AS (
        SELECT
            active.*,
            COALESCE(
                SUM(active.gross_amount_sats::numeric - active.onchain_amount_sats::numeric)
                OVER (
                    PARTITION BY active.p2mr_program
                    ORDER BY active.block_height ASC, active.carry_forward_seq ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0::numeric
            ) AS running_prior_balance_sats
        FROM active
    ),
    legacy AS (
        SELECT
            sequential.*,
            sequential.running_prior_balance_sats + sequential.gross_amount_sats::numeric
                AS running_candidate_balance_sats,
            sequential.running_prior_balance_sats + sequential.gross_amount_sats::numeric
                - sequential.onchain_amount_sats::numeric
                AS running_carry_forward_balance_sats
        FROM sequential
        WHERE sequential.as_issued_audit_sha256 IS NULL
    ),
    legacy_mismatch AS (
        SELECT
            legacy.carry_forward_seq,
            legacy.block_hash,
            legacy.block_height,
            legacy.miner_id,
            legacy.payout_order_key,
            legacy.p2mr_program,
            legacy.prior_balance_sats,
            legacy.running_prior_balance_sats AS expected_prior_balance_sats,
            legacy.gross_amount_sats,
            legacy.candidate_balance_sats,
            legacy.running_candidate_balance_sats AS expected_candidate_balance_sats,
            legacy.onchain_amount_sats,
            legacy.settlement_fee_sats,
            legacy.carry_forward_balance_sats,
            legacy.running_carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            legacy.action,
            concat_ws(
                ',',
                CASE WHEN legacy.prior_balance_sats <> legacy.running_prior_balance_sats
                     THEN 'prior_balance' END,
                CASE WHEN legacy.candidate_balance_sats <> legacy.running_candidate_balance_sats
                     THEN 'candidate_balance' END,
                CASE WHEN legacy.carry_forward_balance_sats <> legacy.running_carry_forward_balance_sats
                     THEN 'carry_forward_balance' END
            ) AS mismatch_reason
        FROM legacy
        WHERE legacy.prior_balance_sats <> legacy.running_prior_balance_sats
           OR legacy.candidate_balance_sats <> legacy.running_candidate_balance_sats
           OR legacy.carry_forward_balance_sats <> legacy.running_carry_forward_balance_sats
    ),
    -- Every marked, active block beside the audit its marker names, or
    -- beside nothing when that audit is missing. The manifest is read as a
    -- whole so a missing or malformed one is a block-level finding, whether
    -- or not the block has any carry or payout row at all.
    marked_blocks AS (
        SELECT
            block.block_hash,
            block.block_height,
            bundle.block_hash IS NOT NULL AS audit_present,
            bundle.audit_bundle -> 'payout_policy_manifest' AS manifest,
            CASE WHEN jsonb_typeof(bundle.audit_bundle -> 'payout_policy_manifest' -> 'accounts') = 'array'
                 THEN bundle.audit_bundle -> 'payout_policy_manifest' -> 'accounts' END AS accounts
        FROM qbit_pool_blocks block
        LEFT JOIN qbit_pool_audit_bundles bundle
          ON bundle.block_hash = block.block_hash
         AND bundle.audit_bundle_sha256 = block.as_issued_audit_sha256
        WHERE block.as_issued_audit_sha256 IS NOT NULL
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
    ),
    block_mismatch AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            marked_blocks.block_hash,
            marked_blocks.block_height,
            NULL::text AS miner_id,
            NULL::text AS payout_order_key,
            NULL::bytea AS p2mr_program,
            NULL::numeric AS prior_balance_sats,
            NULL::numeric AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            NULL::numeric AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            NULL::numeric AS expected_carry_forward_balance_sats,
            NULL::text AS action,
            concat_ws(
                ',',
                CASE WHEN NOT marked_blocks.audit_present THEN 'audit_missing' END,
                CASE WHEN marked_blocks.audit_present
                      AND jsonb_typeof(marked_blocks.manifest) IS DISTINCT FROM 'object'
                     THEN 'manifest_missing' END,
                CASE WHEN jsonb_typeof(marked_blocks.manifest) = 'object'
                      AND marked_blocks.accounts IS NULL
                     THEN 'manifest_accounts_missing' END,
                CASE WHEN jsonb_typeof(marked_blocks.manifest) = 'object'
                      AND (CASE WHEN marked_blocks.manifest ->> 'block_height' ~ '^[0-9]{1,18}$'
                                THEN (marked_blocks.manifest ->> 'block_height')::bigint END)
                          IS DISTINCT FROM marked_blocks.block_height
                     THEN 'manifest_block_height' END
            ) AS mismatch_reason
        FROM marked_blocks
    ),
    -- The manifest's accounts, each field taken only when well formed, so
    -- a missing or malformed field is NULL here and a finding below rather
    -- than a cast error or a comparison that quietly passes.
    manifest_accounts AS (
        SELECT
            marked_blocks.block_hash,
            marked_blocks.block_height,
            account ->> 'recipient_id' AS miner_id,
            account ->> 'order_key' AS payout_order_key,
            CASE WHEN account ->> 'p2mr_program_hex' ~ '^[0-9a-f]{64}$'
                 THEN decode(account ->> 'p2mr_program_hex', 'hex') END AS p2mr_program,
            COALESCE(account ->> 'account_type', 'miner') AS account_type,
            CASE WHEN account ->> 'gross_amount_sats' ~ '^[0-9]{1,18}$'
                 THEN (account ->> 'gross_amount_sats')::bigint END AS gross_amount_sats,
            CASE WHEN account ->> 'prior_balance_sats' ~ '^-?[0-9]{1,39}$'
                 THEN (account ->> 'prior_balance_sats')::numeric END AS prior_balance_sats,
            CASE WHEN account ->> 'candidate_balance_sats' ~ '^-?[0-9]{1,39}$'
                 THEN (account ->> 'candidate_balance_sats')::numeric END AS candidate_balance_sats,
            CASE WHEN account ->> 'onchain_amount_sats' ~ '^[0-9]{1,18}$'
                 THEN (account ->> 'onchain_amount_sats')::bigint END AS onchain_amount_sats,
            CASE WHEN account -> 'settlement_fee_sats' IS NULL THEN 0::bigint
                 WHEN account ->> 'settlement_fee_sats' ~ '^[0-9]{1,18}$'
                 THEN (account ->> 'settlement_fee_sats')::bigint END AS settlement_fee_sats,
            CASE WHEN account ->> 'carry_forward_balance_sats' ~ '^-?[0-9]{1,39}$'
                 THEN (account ->> 'carry_forward_balance_sats')::numeric END AS carry_forward_balance_sats,
            account ->> 'action' AS action
        FROM marked_blocks
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(marked_blocks.accounts, '[]'::jsonb)) AS account
    ),
    -- A manifest account missing a required field is a finding by itself,
    -- and so is one whose own amounts do not add up, whatever its account
    -- type: a fee recipient has no carry row, so the issued arithmetic of
    -- its manifest entry is checked here, where every entry is, rather
    -- than only against the carry rows below.
    manifest_mismatch AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            manifest.block_hash,
            manifest.block_height,
            manifest.miner_id,
            manifest.payout_order_key,
            manifest.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            manifest.prior_balance_sats AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            manifest.candidate_balance_sats AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            manifest.settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            manifest.carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            manifest.action,
            concat_ws(
                ',',
                CASE WHEN num_nulls(manifest.miner_id, manifest.payout_order_key, manifest.p2mr_program,
                                    manifest.gross_amount_sats, manifest.prior_balance_sats,
                                    manifest.candidate_balance_sats, manifest.onchain_amount_sats,
                                    manifest.settlement_fee_sats, manifest.carry_forward_balance_sats,
                                    manifest.action) > 0
                     THEN 'manifest_field_missing' END,
                CASE WHEN manifest.candidate_balance_sats
                          <> manifest.prior_balance_sats + manifest.gross_amount_sats::numeric
                     THEN 'manifest_candidate_arithmetic' END,
                CASE WHEN manifest.carry_forward_balance_sats
                          <> manifest.candidate_balance_sats - manifest.onchain_amount_sats::numeric
                     THEN 'manifest_carry_arithmetic' END
            ) AS mismatch_reason
        FROM manifest_accounts manifest
    ),
    marked_carry AS (
        SELECT
            active.*,
            marked_blocks.block_height AS marked_block_height,
            marked_blocks.accounts IS NULL AS manifest_unusable,
            count(*) OVER (
                PARTITION BY active.block_hash, active.miner_id, active.payout_order_key,
                             active.p2mr_program
            ) AS evidence_rows,
            manifest.block_hash IS NOT NULL AS account_present,
            manifest.prior_balance_sats AS manifest_prior_balance_sats,
            manifest.gross_amount_sats AS manifest_gross_amount_sats,
            manifest.candidate_balance_sats AS manifest_candidate_balance_sats,
            manifest.onchain_amount_sats AS manifest_onchain_amount_sats,
            manifest.settlement_fee_sats AS manifest_settlement_fee_sats,
            manifest.carry_forward_balance_sats AS manifest_carry_forward_balance_sats,
            manifest.action AS manifest_action
        FROM active
        JOIN marked_blocks
          ON marked_blocks.block_hash = active.block_hash
        LEFT JOIN manifest_accounts manifest
          ON manifest.block_hash = active.block_hash
         AND manifest.account_type = 'miner'
         AND manifest.miner_id = active.miner_id
         AND manifest.payout_order_key = active.payout_order_key
         AND manifest.p2mr_program = active.p2mr_program
        WHERE active.as_issued_audit_sha256 IS NOT NULL
    ),
    marked_mismatch AS (
        SELECT
            marked_carry.carry_forward_seq,
            marked_carry.block_hash,
            marked_carry.block_height,
            marked_carry.miner_id,
            marked_carry.payout_order_key,
            marked_carry.p2mr_program,
            marked_carry.prior_balance_sats,
            marked_carry.manifest_prior_balance_sats AS expected_prior_balance_sats,
            marked_carry.gross_amount_sats,
            marked_carry.candidate_balance_sats,
            marked_carry.manifest_candidate_balance_sats AS expected_candidate_balance_sats,
            marked_carry.onchain_amount_sats,
            marked_carry.settlement_fee_sats,
            marked_carry.carry_forward_balance_sats,
            marked_carry.manifest_carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            marked_carry.action,
            concat_ws(
                ',',
                CASE WHEN NOT marked_carry.manifest_unusable AND NOT marked_carry.account_present
                     THEN 'account_missing' END,
                CASE WHEN marked_carry.evidence_rows > 1 THEN 'duplicate_evidence' END,
                CASE WHEN marked_carry.block_height IS DISTINCT FROM marked_carry.marked_block_height
                     THEN 'block_height' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.prior_balance_sats IS DISTINCT FROM marked_carry.manifest_prior_balance_sats
                     THEN 'prior_balance' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.gross_amount_sats IS DISTINCT FROM marked_carry.manifest_gross_amount_sats
                     THEN 'gross_amount' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.candidate_balance_sats IS DISTINCT FROM marked_carry.manifest_candidate_balance_sats
                     THEN 'candidate_balance' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.onchain_amount_sats IS DISTINCT FROM marked_carry.manifest_onchain_amount_sats
                     THEN 'onchain_amount' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.settlement_fee_sats IS DISTINCT FROM marked_carry.manifest_settlement_fee_sats
                     THEN 'settlement_fee' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.carry_forward_balance_sats IS DISTINCT FROM marked_carry.manifest_carry_forward_balance_sats
                     THEN 'carry_forward_balance' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.action IS DISTINCT FROM marked_carry.manifest_action
                     THEN 'action' END,
                CASE WHEN marked_carry.candidate_balance_sats
                          <> marked_carry.prior_balance_sats + marked_carry.gross_amount_sats::numeric
                     THEN 'candidate_arithmetic' END,
                CASE WHEN marked_carry.carry_forward_balance_sats
                          <> marked_carry.candidate_balance_sats - marked_carry.onchain_amount_sats::numeric
                     THEN 'carry_arithmetic' END
            ) AS mismatch_reason
        FROM marked_carry
    ),
    evidence_missing AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            manifest.block_hash,
            manifest.block_height,
            manifest.miner_id,
            manifest.payout_order_key,
            manifest.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            manifest.prior_balance_sats AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            manifest.candidate_balance_sats AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            manifest.carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            NULL::text AS action,
            'evidence_missing'::text AS mismatch_reason
        FROM manifest_accounts manifest
        WHERE manifest.account_type = 'miner'
          AND NOT EXISTS (
              SELECT 1
              FROM active
              WHERE active.block_hash = manifest.block_hash
                AND active.miner_id IS NOT DISTINCT FROM manifest.miner_id
                AND active.payout_order_key IS NOT DISTINCT FROM manifest.payout_order_key
                AND active.p2mr_program IS NOT DISTINCT FROM manifest.p2mr_program)
    ),
    payout_entries AS (
        SELECT
            manifest.block_hash,
            manifest.block_height,
            manifest.miner_id,
            manifest.payout_order_key,
            manifest.p2mr_program,
            manifest.prior_balance_sats,
            manifest.candidate_balance_sats,
            manifest.carry_forward_balance_sats,
            count(payout.block_hash) AS entries,
            COALESCE(bool_or(
                payout.block_height IS DISTINCT FROM manifest.block_height
                OR payout.onchain_amount_sats IS DISTINCT FROM manifest.onchain_amount_sats
                OR payout.carry_forward_balance_sats IS DISTINCT FROM manifest.carry_forward_balance_sats
                OR payout.action IS DISTINCT FROM manifest.action), false) AS differs
        FROM manifest_accounts manifest
        LEFT JOIN qbit_pool_payout_entries payout
          ON payout.block_hash = manifest.block_hash
         AND payout.miner_id = manifest.miner_id
         AND payout.payout_order_key = manifest.payout_order_key
         AND payout.p2mr_program = manifest.p2mr_program
        GROUP BY
            manifest.block_hash, manifest.block_height, manifest.miner_id,
            manifest.payout_order_key, manifest.p2mr_program, manifest.prior_balance_sats,
            manifest.candidate_balance_sats, manifest.carry_forward_balance_sats
    ),
    payout_mismatch AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            payout_entries.block_hash,
            payout_entries.block_height,
            payout_entries.miner_id,
            payout_entries.payout_order_key,
            payout_entries.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            payout_entries.prior_balance_sats AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            payout_entries.candidate_balance_sats AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            payout_entries.carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            NULL::text AS action,
            concat_ws(
                ',',
                CASE WHEN payout_entries.entries = 0 THEN 'payout_entry_missing' END,
                CASE WHEN payout_entries.entries > 1 THEN 'payout_entry_duplicate' END,
                CASE WHEN payout_entries.differs THEN 'payout_entry' END
            ) AS mismatch_reason
        FROM payout_entries
        WHERE payout_entries.entries <> 1 OR payout_entries.differs
    ),
    payout_unexpected AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            payout.block_hash,
            payout.block_height,
            payout.miner_id,
            payout.payout_order_key,
            payout.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            NULL::numeric AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            NULL::numeric AS expected_candidate_balance_sats,
            payout.onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            payout.carry_forward_balance_sats,
            NULL::numeric AS expected_carry_forward_balance_sats,
            payout.action,
            'payout_entry_unexpected'::text AS mismatch_reason
        FROM qbit_pool_payout_entries payout
        JOIN marked_blocks
          ON marked_blocks.block_hash = payout.block_hash
        WHERE NOT EXISTS (
            SELECT 1
            FROM manifest_accounts manifest
            WHERE manifest.block_hash = payout.block_hash
              AND manifest.miner_id = payout.miner_id
              AND manifest.payout_order_key = payout.payout_order_key
              AND manifest.p2mr_program IS NOT DISTINCT FROM payout.p2mr_program)
    ),
    every_mismatch AS (
        SELECT * FROM legacy_mismatch
        UNION ALL
        SELECT * FROM block_mismatch WHERE block_mismatch.mismatch_reason <> ''
        UNION ALL
        SELECT * FROM manifest_mismatch WHERE manifest_mismatch.mismatch_reason <> ''
        UNION ALL
        SELECT * FROM marked_mismatch WHERE marked_mismatch.mismatch_reason <> ''
        UNION ALL
        SELECT * FROM evidence_missing
        UNION ALL
        SELECT * FROM payout_mismatch
        UNION ALL
        SELECT * FROM payout_unexpected
    )
    SELECT *
    FROM every_mismatch
    ORDER BY every_mismatch.block_height ASC,
             every_mismatch.carry_forward_seq ASC NULLS LAST,
             every_mismatch.payout_order_key ASC,
             every_mismatch.miner_id ASC;
$$;
