-- #737: compute each of the carry-forward integrity report's two expensive
-- sets once instead of twice.
--
-- 001's qbit_carry_forward_integrity_report() counts
-- qbit_carry_forward_integrity_mismatches() and
-- qbit_carry_forward_current_drift() in two scalar subqueries and lists them
-- in two more, so each function runs twice. Both are CPU-bound. On the
-- restored union ledger, 2,753,849 active carry rows on a standby with replay
-- paused, EXPLAIN ANALYZE put the validator at 20.29 s and the drift check at
-- 9.14 s, and the report took 57.42 s: nearly all of it those four runs. A
-- larger work_mem saved about 4% (1 GB: 54.98 s). self-check, fatal-state
-- clear and the import admission gate all run the report, and a clear holds
-- its transaction open for as long while the chain tip may move.
--
-- This replaces the report in place. It keeps 001's subqueries word for
-- word, the active row count and the two listings that aggregate each set,
-- and evaluates each once in a MATERIALIZED CTE: inlined, every reference
-- to a listing would evaluate it again. They run in 001's order, the count,
-- the validator, then the drift check, so a run that fails stops where
-- 001's would, after the same work. Each set's count is its listing's
-- length, where 001 runs the set again to count it. So the report is
-- evaluated as 001's is, by the same subqueries, which the recovery
-- evidence needs: a 2.x source's summary carries 001's report, and it must
-- equal its migrated copy's.
--
-- Listing a MATERIALIZED CTE of each set would not be. The listing orders
-- findings by block height and carry_forward_seq, and findings without a
-- carry row have a NULL seq, so those at one height tie. 001 lists them in
-- the order the validator's own sort leaves them, which its aggregate reads
-- as already sorted. Rows read back from a CTE are sorted again, and a sort
-- that spills to disk can reorder ties: with 2,000 of them at one height and
-- work_mem at 64kB, that design listed them in another order than 001.
--
-- Only 001 defines the report, and 001 runs only on a database without 3,
-- before every native migration; 025 replaces the validator, not the report.
-- migrate runs 026 again whenever it runs 001, as it runs 025 again after
-- 011. A report replacement changes no stored row or format, so it needs no
-- capability or shutdown proof. An earlier native binary that meets the
-- database accepts the unknown migration with a warning and reads the same
-- report.
CREATE OR REPLACE FUNCTION qbit_carry_forward_integrity_report()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
    WITH listed AS MATERIALIZED (
        SELECT
            (
                SELECT count(*)
                FROM qbit_payout_carry_forward ledger
                JOIN qbit_pool_blocks block
                  ON block.block_hash = ledger.block_hash
                WHERE ledger.maturity_state <> 'reversed'
                  AND block.chain_state = 'confirmed'
                  AND block.maturity_state <> 'reversed'
            ) AS checked_active_rows,
            COALESCE(
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
            ) AS mismatches,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'p2mr_program_hex', encode(drift.p2mr_program, 'hex'),
                            'current_recipient_id', drift.current_miner_id,
                            'current_order_key', drift.current_payout_order_key,
                            'current_balance_sats', drift.current_balance_sats::text,
                            'recomputed_recipient_id', drift.recomputed_miner_id,
                            'recomputed_order_key', drift.recomputed_payout_order_key,
                            'recomputed_balance_sats', drift.recomputed_balance_sats::text
                        )
                        ORDER BY encode(drift.p2mr_program, 'hex')
                    )
                    FROM qbit_carry_forward_current_drift() drift
                ),
                '[]'::jsonb
            ) AS current_drift
    )
    SELECT jsonb_build_object(
        'schema', 'qbit.prism.carry-forward-integrity.v1',
        'checked_active_rows', listed.checked_active_rows,
        'mismatch_count', jsonb_array_length(listed.mismatches),
        'current_drift_count', jsonb_array_length(listed.current_drift),
        'current_drift', listed.current_drift,
        'mismatches', listed.mismatches
    )
    FROM listed;
$$;
