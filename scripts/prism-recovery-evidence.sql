-- Read-only reconciliation evidence shared by both migration runbooks.
-- Run with psql -XqAt -v ON_ERROR_STOP=1; feed stdout to
-- scripts/prism-recovery-evidence.py. Works on frozen 2.x and native schemas.
-- Writers must be stopped: the transaction gives this export one snapshot,
-- but cannot make separately taken backups or exports contemporaneous.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL bytea_output = 'hex';

SELECT jsonb_build_object('kind', 'shares', 'row', to_jsonb(s))
FROM qbit_share_ledger s ORDER BY share_seq;

SELECT jsonb_build_object('kind', 'blocks', 'row', jsonb_build_object(
    'block_hash', block_hash, 'block_height', block_height,
    'parent_hash', parent_hash, 'coinbase_txid', coinbase_txid,
    'payout_manifest_sha256', payout_manifest_sha256,
    'audit_publication_sequence', audit_publication_sequence,
    'chain_state', chain_state, 'maturity_state', maturity_state))
FROM qbit_pool_blocks ORDER BY block_hash COLLATE "C";

-- Import adds body storage and derived metadata, never changes these identities.
SELECT jsonb_build_object('kind', 'audits', 'row', jsonb_build_object(
    'block_hash', block_hash, 'audit_bundle_sha256', audit_bundle_sha256,
    'coinbase_tx_hex', coinbase_tx_hex))
FROM qbit_pool_audit_bundles ORDER BY block_hash COLLATE "C";

SELECT jsonb_build_object('kind', 'carry', 'row', to_jsonb(c))
FROM qbit_payout_carry_forward c ORDER BY carry_forward_seq;
SELECT jsonb_build_object('kind', 'payouts', 'row', to_jsonb(p))
FROM qbit_pool_payout_entries p ORDER BY payout_entry_seq;
SELECT jsonb_build_object('kind', 'candidates', 'row', jsonb_build_object(
    'block_hash', block_hash, 'share_id', share_id,
    'candidate_sha256', candidate_sha256, 'state', state))
FROM qbit_block_candidate_outbox ORDER BY block_hash COLLATE "C";
SELECT jsonb_build_object('kind', 'ctv_sets', 'row', jsonb_build_object(
    'block_hash', block_hash, 'manifest_set_sha256', manifest_set_sha256,
    'manifest_set_json', manifest_set_json, 'manifest_set', manifest_set,
    'settlement_mode', settlement_mode,
    'parent_coinbase_txid', parent_coinbase_txid, 'parent_coinbase_tx_hex', parent_coinbase_tx_hex,
    'fanout_count', fanout_count, 'fanout_output_sum_sats', fanout_output_sum_sats,
    'covenant_output_value_sats', covenant_output_value_sats))
FROM qbit_ctv_fanout_sets ORDER BY block_hash COLLATE "C";
-- Keep the immutable payout payload in the fingerprint while excluding native
-- claim/confirmation metadata that does not exist in the frozen 2.x schema.
SELECT jsonb_build_object('kind', 'ctv_artifacts', 'row', jsonb_build_object(
    'fanout_txid', fanout_txid, 'block_hash', block_hash,
    'manifest_set_sha256', manifest_set_sha256,
    'manifest_json', manifest_json, 'manifest', manifest,
    'manifest_sha256', manifest_sha256, 'precommitment_sha256', precommitment_sha256,
    'ctv_hash', ctv_hash, 'commitment_witness_leaf_hex', commitment_witness_leaf_hex,
    'chunk_index', chunk_index, 'chunk_count', chunk_count,
    'parent_coinbase_txid', parent_coinbase_txid, 'parent_coinbase_vout', parent_coinbase_vout,
    'fanout_tx_template_hex', fanout_tx_template_hex, 'fanout_tx_hex', fanout_tx_hex,
    'anchor_vout', anchor_vout, 'covenant_output_value_sats', covenant_output_value_sats,
    'fanout_output_sum_sats', fanout_output_sum_sats,
    'settlement_status', settlement_status))
FROM qbit_ctv_fanout_artifacts ORDER BY fanout_txid COLLATE "C";

SELECT jsonb_build_object('kind', 'ctv_broadcast_attempts', 'row', to_jsonb(a))
FROM qbit_ctv_fanout_broadcast_attempts a ORDER BY attempt_seq;

-- Frozen 2.x has no native reservations or deferred credit. Skip absent
-- tables before parsing their queries; empty native tables hash identically.
SELECT to_regclass('qbit_prism_cpfp_packages') IS NOT NULL AS has_cpfp_packages,
       to_regclass('qbit_prism_cpfp_retired_funding') IS NOT NULL AS has_cpfp_retired_funding,
       to_regclass('qbit_prism_deferred_shares') IS NOT NULL AS has_deferred_shares
\gset
\if :has_cpfp_packages
SELECT jsonb_build_object('kind', 'cpfp_packages', 'row', to_jsonb(p))
FROM qbit_prism_cpfp_packages p ORDER BY fanout_txid COLLATE "C";
\endif
\if :has_cpfp_retired_funding
SELECT jsonb_build_object('kind', 'cpfp_retired_funding', 'row', to_jsonb(r))
FROM qbit_prism_cpfp_retired_funding r
ORDER BY funding_txid COLLATE "C", funding_vout;
\endif
\if :has_deferred_shares
SELECT jsonb_build_object('kind', 'deferred_shares', 'row', to_jsonb(d))
FROM qbit_prism_deferred_shares d ORDER BY block_hash COLLATE "C";
\endif

-- Exact row shape/order used by 2.x _carry_forward_audit_head_locked.
-- Stream rows rather than constructing one unbounded json_agg value.
SELECT jsonb_build_object('kind', 'active_carry', 'row', jsonb_build_object(
    'carry_forward_seq', ledger.carry_forward_seq,
    'block_hash', ledger.block_hash, 'block_height', ledger.block_height,
    'recipient_id', ledger.miner_id, 'order_key', ledger.payout_order_key,
    'p2mr_program_hex', encode(ledger.p2mr_program, 'hex'),
    'gross_amount_sats', ledger.gross_amount_sats,
    'prior_balance_sats', ledger.prior_balance_sats::text,
    'candidate_balance_sats', ledger.candidate_balance_sats::text,
    'onchain_amount_sats', ledger.onchain_amount_sats,
    'settlement_fee_sats', ledger.settlement_fee_sats,
    'carry_forward_balance_sats', ledger.carry_forward_balance_sats::text,
    'action', ledger.action, 'maturity_state', ledger.maturity_state))
FROM qbit_payout_carry_forward ledger
JOIN qbit_pool_blocks block ON block.block_hash = ledger.block_hash
WHERE ledger.maturity_state <> 'reversed' AND block.chain_state = 'confirmed'
  AND block.maturity_state <> 'reversed'
ORDER BY ledger.block_height, ledger.carry_forward_seq;

SELECT jsonb_build_object('kind', 'integrity', 'row', qbit_carry_forward_integrity_report());
-- A final marker makes an interrupted/failed psql export fail closed.
SELECT jsonb_build_object('kind', 'complete', 'row', true);
COMMIT;
