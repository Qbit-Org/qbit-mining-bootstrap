-- 3.1 dual writer, window cuts (D2): a dual-writer payout window records, for
-- each node, the highest share_seq of that node's rows it may include
-- (qbit_prism::WindowCut). A peer's row that reaches this database after the
-- window was taken is above the peer's entry, so it stays outside the window
-- and joins later ones, and every proof of the window (landing, its in-lock
-- count, the #619 holding probe, audit reconstruction) reads the same rows on
-- either node.
--
-- Window references carry the cut inside the documents that already hold
-- them: the candidate document (candidate_sha256) and the prepared-job
-- payload, both digest-checked. Only the audit share snapshot, which has no
-- document, needs columns for it.
--
-- Additive only: no capability and no shutdown proof. A binary that does not
-- know these columns never names them, and both are NULL on every existing
-- row and on every row it writes: no cut, which means exactly what it meant
-- in 3.0. The ALTER is catalog-only. The CHECK is validated under the
-- migration's lock against the existing rows, every one of them NULL; the
-- table holds one row per distinct landed window.
--
-- cut_seq_0 and cut_seq_1 are both NULL (no cut) or both set. 0 is the JSON's
-- null: the window includes no row of that node. share_seq starts at 1, so 0
-- admits nothing and a predicate needs no special case for it. A window
-- without a share range (the bootstrap window's inline share is not a ledger
-- row) may have a cut below its synthetic share.
ALTER TABLE qbit_prism_audit_snapshots
    ADD COLUMN IF NOT EXISTS cut_seq_0 bigint,
    ADD COLUMN IF NOT EXISTS cut_seq_1 bigint;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'qbit_prism_audit_snapshots'::regclass
                     AND conname = 'qbit_prism_audit_snapshots_cut_check') THEN
        ALTER TABLE qbit_prism_audit_snapshots
            ADD CONSTRAINT qbit_prism_audit_snapshots_cut_check CHECK (
                num_nulls(cut_seq_0, cut_seq_1) IN (0, 2)
                AND (cut_seq_0 IS NULL
                     OR (cut_seq_0 >= 0 AND cut_seq_1 >= 0
                         AND (inline_shares IS NOT NULL
                              OR last_share_seq <= GREATEST(cut_seq_0, cut_seq_1)))));
    END IF;
END $$;
