-- #668: the CTV fanout claim lane reads only the fanouts due now, never the
-- settled history behind them.
--
-- Settled fanouts accumulate with every found block and stay in
-- qbit_ctv_fanout_artifacts. Before 023 the lane's selection matched them on
-- its status and schedule predicates and dropped them only after reading
-- them: a sequential scan of the whole table on every claim, about 65 ms at
-- 100,000 settled fanouts and 400 ms at 1,000,000 on a warm cache.
--
-- This index holds exactly the fanouts the lane may still claim apart from
-- one: every fanout still to broadcast or check (`broadcastable`,
-- `broadcast_submitted`, `failed`) and every confirmed one under 1,000
-- confirmations deep. The one it leaves out, the newest deep fanout that
-- reconciliation keeps watching, is found through
-- qbit_prism_fanout_checkpoint_idx. It is keyed by the schedule, so the lane
-- reads the due ones as two ranges of it, never attempted (`NULL`) and
-- scheduled at or before the claim statement's start, and never the watched
-- fanouts waiting for their next check. `Ledger::fanout_lane_sql` repeats
-- this predicate, which must stay identical to it, or the planner cannot use
-- the index.
--
-- Additive: no capability and no shutdown proof, as 019 and 020. A binary
-- that does not know the index never reads it. Applied online, as 013 is
-- (`ONLINE_MIGRATIONS`): on an existing ledger the runner builds it with
-- CREATE INDEX CONCURRENTLY after the migration transaction commits, so no
-- write to qbit_ctv_fanout_artifacts waits for the build, a found block's
-- landing included, and records 23 once it is valid. A fresh or empty
-- source applies this file inside the transaction.
CREATE INDEX qbit_ctv_fanout_artifacts_lane_idx
    ON qbit_ctv_fanout_artifacts (next_broadcast_attempt_at, fanout_txid)
    WHERE settlement_status IN ('broadcastable', 'broadcast_submitted', 'failed')
       OR (settlement_status = 'confirmed' AND confirmed_depth < 1000);
