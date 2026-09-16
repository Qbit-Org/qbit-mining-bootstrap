-- Trim the qbit_share_ledger secondary indexes to what the native readers
-- use (#153). The table is append-only and every insert maintains every
-- index, so INCLUDE payload nobody reads and indexes nobody scans are pure
-- write amplification. Each index, mapped against the native query set:
--
-- * qbit_share_ledger_accepted_seq_window_idx, (share_seq DESC) with a
--   seven-column INCLUDE: no consumer. Every share_seq walk (the payout
--   page walk, the audit range reads, the landing count, max(share_seq))
--   is planned on the primary key, so the payload never earns an
--   index-only read. Replaced by qbit_share_ledger_accepted_seq_walk_idx,
--   whose INCLUDE is exactly what the newest-first page walk of
--   qbit_prism_window (001, still called by the pool snapshot and the
--   reward leaderboard) and the landing durable-range count read; both
--   become index-only on it.
-- * qbit_share_ledger_accepted_miner_recent_idx, (miner_id, accepted_at
--   DESC) INCLUDE (share_difficulty, share_seq, share_id,
--   payout_order_key): the miner share summary, worker rows, hashrate
--   series and hashrate rollups read the first three; nothing reads
--   payout_order_key. Replaced by
--   qbit_share_ledger_accepted_miner_history_idx without it.
-- * qbit_share_ledger_accepted_window_idx, (job_issued_at, share_seq
--   DESC): no consumer; job_issued_at is only ever a filter on a share_seq
--   walk. Dropped.
-- * qbit_share_ledger_template_height_idx, (template_height, share_seq):
--   no native consumer. qbit_shares_since_template_height (001, operator
--   replay) is its only caller and reads the primary key without it; if a
--   native consumer appears, restoring it is one CREATE INDEX CONCURRENTLY
--   ON qbit_share_ledger (template_height, share_seq) WHERE accepted. Dropped.
-- * qbit_share_ledger_accepted_recent_idx: the pool hashrate series,
--   leaderboard, pool snapshot, miner summary, rollup boundaries and
--   evidence counts read all three INCLUDE columns. Kept.
-- * qbit_share_ledger_accepted_block_suffix_idx: the block-solver lookup
--   in the blocks, leaderboard, reward leaderboard and pool snapshot reads
--   all three INCLUDE columns. Kept.
--
-- WHERE accepted stays on every index. The native writers only insert
-- accepted rows, but the readers and their tests exclude rejected rows by
-- contract, and the predicate is what lets the frozen 001 function's walk
-- stay index-only without carrying the column.
--
-- Applied online for existing native ledgers and populated 2.x.x sources
-- (ONLINE_MIGRATIONS in ledger/migration.rs). Inside its transaction the
-- migrator runs this file against the scratch schema to learn the index
-- definitions. Fresh or empty 2.x.x sources also apply it transactionally
-- under the cutover locks that exclude writers. Every native upgrade runs
-- each change after the commit as CREATE INDEX CONCURRENTLY or DROP INDEX
-- CONCURRENTLY, even without visible shares, so appends continue while the
-- replacements are built, and it records 13
-- after the last drop. A replacement takes a new name: CREATE INDEX IF NOT
-- EXISTS under the old name would keep the old definition.
CREATE INDEX qbit_share_ledger_accepted_seq_walk_idx
    ON qbit_share_ledger (share_seq DESC)
    INCLUDE (job_issued_at, accepted_at, share_difficulty)
    WHERE accepted;

CREATE INDEX qbit_share_ledger_accepted_miner_history_idx
    ON qbit_share_ledger (miner_id, accepted_at DESC)
    INCLUDE (share_difficulty, share_seq, share_id)
    WHERE accepted;

DROP INDEX qbit_share_ledger_accepted_seq_window_idx;
DROP INDEX qbit_share_ledger_accepted_miner_recent_idx;
DROP INDEX qbit_share_ledger_accepted_window_idx;
DROP INDEX qbit_share_ledger_template_height_idx;
