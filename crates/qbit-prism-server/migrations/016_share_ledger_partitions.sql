-- Convert qbit_share_ledger into a partitioned table, RANGE (share_seq),
-- with the release table attached as its first partition (#144). The work
-- is the two functions migration 015 defined; this file is the order they
-- run in, and it is applied as written only where the ledger is empty (a
-- fresh deployment, or an empty 2.x.x source under the cutover locks),
-- inside the migration transaction. Existing native ledgers and populated
-- 2.x.x sources run the same three steps after the commit, each in its own
-- transaction on a dedicated connection (ONLINE_MIGRATIONS in
-- ledger/migration.rs, the partition runner in ledger/migration/partition.rs):
--
--   1. prepare: CHECK (share_seq < bound) NOT VALID on the release table,
--      bound two partition widths above the sequence on a grid boundary;
--   2. validate: one scan of the table per NOT VALID CHECK it carries (the
--      bound, and the release's pending credit_policy_check where 001 left
--      it so) under SHARE UPDATE EXCLUSIVE, appends and reads continue,
--      hours on a large ledger;
--   3. swap: milliseconds of catalog work (17 ms measured, none of it a
--      function of the row count) under ACCESS EXCLUSIVE, taken with a
--      short lock timeout and retried: rename, parent, indexes adopted by
--      ATTACH (nothing is copied or rebuilt), lead partitions.
--
-- The version is recorded after the swap. Until then every start refuses
-- the database, as for every other required migration. The migration is
-- one-way (decision D5): the revert is a second cutover that has to rebuild
-- a global share_id index over the whole table, and it fails on the first
-- duplicate share_id accepted across two partitions, so none is shipped.
SELECT qbit_prism_share_ledger_convert_prepare(0);
SELECT qbit_prism_share_ledger_convert_validate();
SELECT qbit_prism_share_ledger_convert_swap();
