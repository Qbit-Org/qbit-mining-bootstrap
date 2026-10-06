-- #581: a candidate claim's lease is timed by the frontend that takes it
-- over, on that frontend's own monotonic clock, never by comparing
-- `claim_expires_at` with the database clock.
--
-- STOP EVERY pre-021 FRONTEND AND TOOL BEFORE APPLYING THIS MIGRATION, keep
-- them stopped until it has committed, and restart only upgraded binaries.
-- The runner refuses to apply it while any registered instance has not
-- reported `drained` or `stopped`, and the capability declared at the end is
-- read when a binary connects, never while it runs.
--
-- Until now `claim_expires_at` (`clock_timestamp() + lease`) decided both
-- sides of a lease: the holder's writes required it to be in the future and
-- the claim lanes took a row once it was in the past. A database clock step
-- moved every such deadline by the step: forward, a live claim looked
-- expired and a second frontend took a row the first was still offering;
-- backward, a dead holder's row waited lease + step. A pre-021 binary still
-- takes over by the database clock and never writes the columns below, so it
-- must not run beside a post-021 one.
--
-- From 021:
-- * `claim_renewals` counts the renewals of the current claim token: 0 when
--   a claim is taken, plus one on every renewal. A takeover is a compare and
--   set on (`claim_token`, `claim_renewals`), so any renewal invalidates
--   every observation of the claim before it.
-- * `claim_lease_seconds` is the lease the holder took or last renewed
--   (1 to 600). A frontend may take a claimed row over only after it has
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
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN claim_renewals bigint NOT NULL DEFAULT 0,
    ADD COLUMN claim_lease_seconds integer,
    ADD CONSTRAINT qbit_block_candidate_outbox_claim_renewals_check
        CHECK (claim_renewals >= 0),
    ADD CONSTRAINT qbit_block_candidate_outbox_claim_lease_seconds_check
        CHECK (claim_lease_seconds IS NULL OR claim_lease_seconds BETWEEN 0 AND 600);

-- Every claim poll reads the claimed rows to time them, so they are found
-- without walking the unfinished set: a claim is held by at most one row
-- per frontend attempt, while a storm can leave thousands unfinished.
CREATE INDEX qbit_block_candidate_outbox_claimed_idx
    ON qbit_block_candidate_outbox (block_hash)
    WHERE claim_token IS NOT NULL;

-- Declared last. A pre-021 binary that checks after this commit refuses the
-- database; nothing here stops one that is already running, which is why
-- the runner requires every earlier instance to have reported shutdown.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('candidate_claim_observed_lease', 1);
