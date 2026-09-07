-- Parameterized shared-database read model.

SELECT COALESCE(json_agg(json_build_object(
    'window_multiplier', window_multiplier::text,
    'requested_window_weight', requested_window_weight::text,
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'counted_difficulty', counted_difficulty::text,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'credit_policy', credit_policy
) ORDER BY share_seq DESC), '[]'::json)
FROM qbit_audit_share_window(
    to_timestamp(($1::double precision / 1000.0)),
    $2::text::numeric
);
