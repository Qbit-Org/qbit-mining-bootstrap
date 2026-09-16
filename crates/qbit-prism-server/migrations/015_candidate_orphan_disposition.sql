-- #415: a terminal disposition for a proven orphan, `orphaned`, beside the
-- `submitted` and `abandoned` states 011 left the outbox with.
--
-- STOP EVERY pre-015 FRONTEND BEFORE APPLYING THIS MIGRATION, keep them
-- stopped until it has committed, and restart only upgraded binaries. The
-- runner refuses to apply it while any registered instance has not reported
-- `drained` or `stopped` (the shutdown proof 011 and 012 require), and the
-- capability declared at the end is read when a binary connects, never
-- while it runs: a pre-015 frontend that is already running is not evicted
-- by the declaration, only refused by a capability check made after the
-- commit.
--
-- 011 keeps every offered block automation cannot finish in
-- `reconciliation`, retried with read-only chain observations only and never
-- abandoned, and the pending-candidate gauges count every unfinished row. A
-- block that lost a tip race (the 2026-09-16 mainnet orphan stall, #413: our
-- block was the node's best block for 61 ms, then a same-height competitor
-- replaced it) is such a row,
-- and a handful of them would keep the gauges nonzero and the migrated
-- pending-candidate alerts firing forever. This migration lets the post-offer
-- settlement mark a row terminal once the chain has proven the orphan: a
-- DIFFERENT block is active at the candidate's height, with at least
-- PRISM_CANDIDATE_ORPHAN_CONFIRMATIONS confirmations (default 6), observed on
-- one coherent tip, after the row's audit has landed. `orphaned` is a
-- processing disposition, not a permanent chain status: the row is terminal
-- like `submitted` and `abandoned`, so its document, block bytes, window
-- reference and body are cleared exactly as theirs are, and it is never
-- claimed again (the claim lanes and the unfinished index select the four
-- unfinished states, which it is not in) and the gauges no longer count it.
-- It keeps its offer record and its `last_error`, which names the
-- competitor, the height, the confirmations and the tip; the block itself
-- keeps its landed audit and its `qbit_pool_blocks` row, marked `inactive`,
-- so a later reorg that reactivates the block is credited through the
-- ordinary reorg reconciler (`Ledger::reconcile_blocks_in`), deferred share
-- included, without the outbox row ever reopening. `abandoned` stays
-- reachable from `pending` only; `orphaned` is reachable from the three
-- offer states only.
--
-- The three lifecycle CHECKs 011 wrote are replaced under their own names,
-- and only if they still carry exactly the definitions 011 wrote (deparsed
-- by this server and compared, never a hand-written string): a definition
-- that differs was edited by an operator or written by another release, and
-- is refused with nothing changed. Every other CHECK on the outbox is
-- classified as 011 classified it, by the columns the catalog says it
-- references (pg_constraint.conkey): one that references `state` or
-- `completed_at` is refused by name, because 015 cannot know whether it
-- admits an orphaned row, and one that references neither is kept exactly
-- as it is. A refusal RAISEs with every offending constraint named, so this
-- transaction changes nothing. The unfinished index needs no change: its
-- predicate is the four unfinished states, and an orphaned row leaves it the
-- way a submitted one does.
DO $$
DECLARE
    outbox constant regclass := 'qbit_block_candidate_outbox'::regclass;
    -- 011's three rules, verbatim, for the probe comparison below.
    known_state constant text := $def$
        state IN ('pending', 'offer_reserved', 'offered', 'reconciliation',
                  'submitted', 'abandoned')$def$;
    known_payload_dual constant text := $def$
        (state = 'pending' AND completed_at IS NULL
            AND ((storage_version = 1 AND candidate IS NOT NULL AND body_id IS NULL)
                 OR (storage_version = 2 AND candidate IS NULL AND body_id IS NOT NULL)))
        OR (state IN ('offer_reserved', 'offered', 'reconciliation')
            AND completed_at IS NULL
            AND body_id IS NULL
            AND num_nonnulls(candidate, block_bytes, window_anchor_ms,
                             window_prior_balances_sha256) = 4)
        OR (state IN ('submitted', 'abandoned')
            AND completed_at IS NOT NULL
            AND body_id IS NULL
            AND num_nulls(candidate, block_bytes, window_anchor_ms,
                          window_prior_balances_sha256, window_first_share_seq,
                          window_last_share_seq, window_share_count,
                          window_snapshot_sha256) = 8)$def$;
    known_payload_plain constant text := $def$
        (state = 'pending' AND completed_at IS NULL AND candidate IS NOT NULL)
        OR (state IN ('offer_reserved', 'offered', 'reconciliation')
            AND completed_at IS NULL
            AND num_nonnulls(candidate, block_bytes, window_anchor_ms,
                             window_prior_balances_sha256) = 4)
        OR (state IN ('submitted', 'abandoned')
            AND completed_at IS NOT NULL
            AND num_nulls(candidate, block_bytes, window_anchor_ms,
                          window_prior_balances_sha256, window_first_share_seq,
                          window_last_share_seq, window_share_count,
                          window_snapshot_sha256) = 8)$def$;
    known_offer constant text := $def$
        (offer_outcome IS NULL OR offer_outcome IN ('accepted', 'rejected', 'unknown'))
        AND (offer_outcome IS DISTINCT FROM 'rejected' OR offer_reply IS NOT NULL)
        AND (
            (state IN ('pending', 'abandoned')
                AND num_nulls(offer_reserved_at, offer_reserved_by, offered_at_ms,
                              offer_outcome, offer_reply) = 5)
            OR (state = 'offer_reserved'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by) = 2
                AND num_nulls(offered_at_ms, offer_outcome, offer_reply) = 3)
            OR (state = 'offered'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by, offered_at_ms,
                                 offer_outcome) = 4)
            OR (state = 'reconciliation'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by, offer_outcome,
                                 last_error) = 4
                AND btrim(last_error) <> '')
            OR state = 'submitted')$def$;
    -- 015's rules. An orphaned row is terminal (`completed_at` set) and its
    -- payload is a terminal row's: the document, the block, all six window
    -- columns and any body are NULL together. Its offer columns are a
    -- reconciliation row's: the reservation, an outcome and the reason in
    -- `last_error`. An outcome the node gave (`accepted` or `rejected`) also
    -- keeps the call time it was recorded with; a reservation whose call was
    -- lost stays `unknown`, and no submission is invented for it.
    lifecycle_state constant text := $def$
        state IN ('pending', 'offer_reserved', 'offered', 'reconciliation',
                  'submitted', 'abandoned', 'orphaned')$def$;
    lifecycle_payload_dual constant text := $def$
        (state = 'pending' AND completed_at IS NULL
            AND ((storage_version = 1 AND candidate IS NOT NULL AND body_id IS NULL)
                 OR (storage_version = 2 AND candidate IS NULL AND body_id IS NOT NULL)))
        OR (state IN ('offer_reserved', 'offered', 'reconciliation')
            AND completed_at IS NULL
            AND body_id IS NULL
            AND num_nonnulls(candidate, block_bytes, window_anchor_ms,
                             window_prior_balances_sha256) = 4)
        OR (state IN ('submitted', 'abandoned', 'orphaned')
            AND completed_at IS NOT NULL
            AND body_id IS NULL
            AND num_nulls(candidate, block_bytes, window_anchor_ms,
                          window_prior_balances_sha256, window_first_share_seq,
                          window_last_share_seq, window_share_count,
                          window_snapshot_sha256) = 8)$def$;
    lifecycle_payload_plain constant text := $def$
        (state = 'pending' AND completed_at IS NULL AND candidate IS NOT NULL)
        OR (state IN ('offer_reserved', 'offered', 'reconciliation')
            AND completed_at IS NULL
            AND num_nonnulls(candidate, block_bytes, window_anchor_ms,
                             window_prior_balances_sha256) = 4)
        OR (state IN ('submitted', 'abandoned', 'orphaned')
            AND completed_at IS NOT NULL
            AND num_nulls(candidate, block_bytes, window_anchor_ms,
                          window_prior_balances_sha256, window_first_share_seq,
                          window_last_share_seq, window_share_count,
                          window_snapshot_sha256) = 8)$def$;
    offer_rule constant text := $def$
        (offer_outcome IS NULL OR offer_outcome IN ('accepted', 'rejected', 'unknown'))
        AND (offer_outcome IS DISTINCT FROM 'rejected' OR offer_reply IS NOT NULL)
        AND (
            (state IN ('pending', 'abandoned')
                AND num_nulls(offer_reserved_at, offer_reserved_by, offered_at_ms,
                              offer_outcome, offer_reply) = 5)
            OR (state = 'offer_reserved'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by) = 2
                AND num_nulls(offered_at_ms, offer_outcome, offer_reply) = 3)
            OR (state = 'offered'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by, offered_at_ms,
                                 offer_outcome) = 4)
            OR (state = 'reconciliation'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by, offer_outcome,
                                 last_error) = 4
                AND btrim(last_error) <> '')
            OR (state = 'orphaned'
                AND num_nonnulls(offer_reserved_at, offer_reserved_by, offer_outcome,
                                 last_error) = 4
                AND btrim(last_error) <> ''
                AND (offer_outcome = 'unknown' OR offered_at_ms IS NOT NULL))
            OR state = 'submitted')$def$;
    lifecycle_names constant text[] := ARRAY[
        'qbit_block_candidate_outbox_lifecycle_payload_check',
        'qbit_block_candidate_outbox_lifecycle_state_check',
        'qbit_block_candidate_outbox_offer_check'];
    dual boolean;
    known_payload text;
    lifecycle_payload text;
    expected_known_state text;
    expected_known_payload text;
    expected_known_offer text;
    item record;
    present text[] := '{}';
    mismatched text[] := '{}';
    refused text[] := '{}';
    missing text[];
BEGIN
    -- Every read below and the replacement at the end see one catalog. The
    -- lock waits no longer than the session's lock_timeout (the runner's
    -- connections always set one), so a session still holding the outbox
    -- fails this migration instead of stalling it.
    LOCK TABLE qbit_block_candidate_outbox IN ACCESS EXCLUSIVE MODE;
    dual := EXISTS (SELECT 1 FROM pg_attribute
                    WHERE attrelid = outbox AND attname = 'body_id' AND NOT attisdropped);
    known_payload := CASE WHEN dual THEN known_payload_dual ELSE known_payload_plain END;
    lifecycle_payload := CASE WHEN dual THEN lifecycle_payload_dual
                              ELSE lifecycle_payload_plain END;

    -- 011's definitions, deparsed by this server on a temporary, empty copy
    -- of the outbox's columns (the same names, types and collations), so the
    -- comparison below is deparse against deparse and no probe constraint
    -- is ever added to, or dropped from, the outbox itself.
    CREATE TEMPORARY TABLE qbit_block_candidate_outbox_015_probe
        (LIKE qbit_block_candidate_outbox) ON COMMIT DROP;
    EXECUTE format(
        'ALTER TABLE pg_temp.qbit_block_candidate_outbox_015_probe '
        'ADD CONSTRAINT probe_state CHECK (%s), '
        'ADD CONSTRAINT probe_payload CHECK (%s), '
        'ADD CONSTRAINT probe_offer CHECK (%s)',
        known_state, known_payload, known_offer);
    SELECT
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'probe_state'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'probe_payload'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'probe_offer')
    INTO expected_known_state, expected_known_payload, expected_known_offer
    FROM pg_constraint
    WHERE conrelid = 'pg_temp.qbit_block_candidate_outbox_015_probe'::regclass
      AND contype = 'c';
    DROP TABLE pg_temp.qbit_block_candidate_outbox_015_probe;
    IF num_nulls(expected_known_state, expected_known_payload, expected_known_offer) > 0 THEN
        RAISE EXCEPTION 'migration 015 could not deparse its probe constraints for qbit_block_candidate_outbox; nothing was changed';
    END IF;

    -- Classify every CHECK on the outbox before anything is dropped: each of
    -- 011's names must carry 011's definition, and any other CHECK that
    -- references a lifecycle column is refused.
    FOR item IN
        SELECT c.conname::text AS name,
               pg_get_expr(c.conbin, c.conrelid) AS definition,
               pg_get_constraintdef(c.oid) AS declaration,
               EXISTS (SELECT 1 FROM pg_attribute a
                       WHERE a.attrelid = c.conrelid
                         AND a.attnum = ANY (c.conkey)
                         AND a.attname IN ('state', 'completed_at')) AS lifecycle
        FROM pg_constraint c
        WHERE c.conrelid = outbox AND c.contype = 'c'
        ORDER BY c.conname
    LOOP
        IF item.name = ANY (lifecycle_names) THEN
            present := present || item.name;
            -- Parenthesised: PL/pgSQL ends an IF condition at the first
            -- unnested THEN, and the CASE has its own.
            IF item.definition IS DISTINCT FROM (CASE item.name
                    WHEN 'qbit_block_candidate_outbox_lifecycle_state_check' THEN expected_known_state
                    WHEN 'qbit_block_candidate_outbox_lifecycle_payload_check' THEN expected_known_payload
                    ELSE expected_known_offer END) THEN
                mismatched := mismatched || format('%I %s', item.name, item.declaration);
            END IF;
        ELSIF item.lifecycle THEN
            refused := refused || format('%I %s', item.name, item.declaration);
        END IF;
    END LOOP;
    missing := ARRAY(SELECT required FROM unnest(lifecycle_names) AS names(required)
                     WHERE required <> ALL (present) ORDER BY required);
    IF array_length(missing, 1) > 0 THEN
        RAISE EXCEPTION 'migration 015 refuses to run: qbit_block_candidate_outbox is missing the lifecycle CHECK constraint(s) 011 created: %. Restore them from migrations/011_offer_before_landing.sql, then migrate again; nothing was changed',
            array_to_string(missing, ', ');
    END IF;
    IF array_length(mismatched, 1) > 0 THEN
        RAISE EXCEPTION 'migration 015 refuses to replace the outbox lifecycle rules: % carries a definition 011 did not write. Restore the 011 definition (or, if a newer PRISM release wrote this database, upgrade the server), then migrate again; nothing was changed',
            array_to_string(mismatched, '; ');
    END IF;
    IF array_length(refused, 1) > 0 THEN
        RAISE EXCEPTION 'migration 015 refuses to replace the outbox lifecycle rules: qbit_block_candidate_outbox carries CHECK constraints 015 does not know that reference its lifecycle columns state or completed_at: %. Only 011''s three lifecycle rules are replaced, by name and definition; a CHECK that references neither column is kept as it is. Review each named constraint, drop it (and re-create it after the migration if it still holds for a terminal orphaned row), then migrate again; nothing was changed',
            array_to_string(refused, '; ');
    END IF;

    ALTER TABLE qbit_block_candidate_outbox
        DROP CONSTRAINT qbit_block_candidate_outbox_lifecycle_state_check,
        DROP CONSTRAINT qbit_block_candidate_outbox_lifecycle_payload_check,
        DROP CONSTRAINT qbit_block_candidate_outbox_offer_check;
    EXECUTE format(
        'ALTER TABLE qbit_block_candidate_outbox '
        'ADD CONSTRAINT qbit_block_candidate_outbox_lifecycle_state_check CHECK (%s), '
        'ADD CONSTRAINT qbit_block_candidate_outbox_lifecycle_payload_check CHECK (%s), '
        'ADD CONSTRAINT qbit_block_candidate_outbox_offer_check CHECK (%s)',
        lifecycle_state, lifecycle_payload, offer_rule);
END $$;

-- Declared last, once the lifecycle rules admit the state. The declaration
-- is read by a binary's capability check at connect (ledger/migration.rs,
-- NATIVE_CAPABILITIES), and only there: a pre-015 binary that checks after
-- this commit refuses the database, but nothing here stops one that is
-- already running, and 012's startup marker does not fence a pre-015
-- startup that passed its check before this commit and is still waiting to
-- register. The runner requires every earlier instance to have reported
-- shutdown before this file runs; old frontends must stay stopped until the
-- migration has committed, and only upgraded binaries may be restarted. A
-- pre-015 binary never claims an orphaned row (its claim lanes select the
-- four unfinished states) and its gauges never count one, but its
-- `CandidateState::parse` has no name for the state and its terminal-state
-- check after a lost lease does not know it.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('candidate_orphan_disposition', 1)
ON CONFLICT (capability) DO UPDATE
SET capability_value = EXCLUDED.capability_value,
    updated_at = clock_timestamp();
