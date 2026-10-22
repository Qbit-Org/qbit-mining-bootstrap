-- Normal dispatch prefers newly found blocks; periodic oldest-due slots keep
-- both recovery attempts and older unattempted work from starving.
CREATE SEQUENCE IF NOT EXISTS qbit_prism_candidate_dispatch_sequence AS bigint;

CREATE INDEX IF NOT EXISTS qbit_prism_candidate_fresh_idx
    ON qbit_block_candidate_outbox(created_at DESC,block_hash)
    WHERE state='pending' AND attempt_count=0;
