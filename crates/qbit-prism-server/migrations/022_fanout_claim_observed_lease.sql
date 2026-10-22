-- #654: a CTV fanout claim's lease is timed by the frontend that takes it
-- over, on that frontend's own monotonic clock, as a candidate claim's is
-- since 021 (#581), never by comparing `claim_expires_at` with the database
-- clock.
--
-- STOP EVERY pre-022 FRONTEND AND TOOL BEFORE APPLYING THIS MIGRATION, keep
-- them stopped until it has committed, and restart only upgraded binaries.
-- The runner refuses to apply it while any registered instance has not
-- reported `drained` or `stopped`, and the capability declared at the end is
-- read when a binary connects, never while it runs.
--
-- Until now `claim_expires_at` (`clock_timestamp() + lease`) decided both
-- sides of a fanout lease: the holder's writes required it to be in the
-- future and the claim lane took a row once it was in the past. A database
-- clock step moved every such deadline by the step: forward, a live claim
-- looked expired, a second frontend took over a fanout the first was still
-- broadcasting and the first's settlement write was refused; backward, a dead
-- holder's row waited lease + step. A pre-022 binary still takes over by the
-- database clock and never writes the columns below, so it must not run
-- beside a post-022 one.
--
-- From 022, exactly as 021 for qbit_block_candidate_outbox:
-- * `claim_renewals` counts the renewals of the current claim token: 0 when
--   a claim is taken, plus one on every renewal. A takeover is a compare and
--   set on (`claim_token`, `claim_renewals`), so any renewal invalidates
--   every observation of the claim before it.
-- * `claim_lease_seconds` is the lease the holder took or last renewed
--   (1 to 600). A frontend may take a claimed fanout over only after it has
--   itself observed the same (`claim_token`, `claim_renewals`) for that many
--   seconds of its own monotonic time, measured from the reply that showed
--   it. NULL, a claim taken before this migration, is timed as the 600-second
--   maximum. 0 revokes the claim: the next observer takes it over at once.
--   Nothing the server writes is 0; it is an operator and test revocation.
-- * `claim_expires_at` is still written, as the database clock's estimate of
--   the lease's end, for operators. No decision reads it.
-- Both describe the current claim and are read only while `claim_token` is
-- set: every claim writes them, and every statement the server runs that
-- clears `claim_token` resets them too.
ALTER TABLE qbit_ctv_fanout_artifacts
    ADD COLUMN claim_renewals bigint NOT NULL DEFAULT 0,
    ADD COLUMN claim_lease_seconds integer,
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_claim_renewals_check
        CHECK (claim_renewals >= 0),
    ADD CONSTRAINT qbit_ctv_fanout_artifacts_claim_lease_seconds_check
        CHECK (claim_lease_seconds IS NULL OR claim_lease_seconds BETWEEN 0 AND 600);

-- Every claim poll reads the claimed fanouts to time them, so they are found
-- without walking the table: a claim is held by at most one fanout per
-- frontend attempt, while settled fanouts accumulate with every block.
CREATE INDEX qbit_ctv_fanout_artifacts_claimed_idx
    ON qbit_ctv_fanout_artifacts (fanout_txid)
    WHERE claim_token IS NOT NULL;

-- Declared last. A pre-022 binary that checks after this commit refuses the
-- database; nothing here stops one that is already running, which is why
-- the runner requires every earlier instance to have reported shutdown.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('fanout_claim_observed_lease', 1);
