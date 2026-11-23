-- The pending CTV fanouts both pending feeds list, one page and their count.
-- The settlement status list names every pending status (#688), so
-- qbit_ctv_fanout_artifacts_status_idx serves it and the settled fanouts are
-- never read. A test pins it to the schema's statuses.

WITH eligible AS (
    SELECT a.fanout_txid, a.chunk_index, b.block_height
    FROM qbit_ctv_fanout_artifacts a
    JOIN qbit_pool_blocks b USING (block_hash)
    WHERE b.chain_state = 'confirmed'
      AND a.settlement_status IN ('awaiting_maturity', 'broadcastable', 'broadcast_submitted', 'failed')
      AND (a.next_broadcast_attempt_at IS NULL OR a.next_broadcast_attempt_at <= clock_timestamp())
), page AS (
    SELECT * FROM eligible ORDER BY block_height, chunk_index, fanout_txid LIMIT $1 OFFSET $2
)
SELECT jsonb_build_object(
    'total_count', (SELECT count(*) FROM eligible),
    'rows', COALESCE((SELECT jsonb_agg(qbit_fanout_status(fanout_txid) ORDER BY block_height, chunk_index, fanout_txid) FROM page), '[]'::jsonb)
)
