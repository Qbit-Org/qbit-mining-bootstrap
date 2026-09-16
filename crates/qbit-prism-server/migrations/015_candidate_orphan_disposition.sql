-- #415: a terminal disposition for a proven orphan, `orphaned`, beside the
-- `submitted` and `abandoned` states 011 left the outbox with.
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
-- one coherent tip, after the row's audit has landed. The row keeps its
-- document, its block bytes, its window reference and its offer record: it
-- is never claimed again (the claim lanes and the unfinished index select
-- the four unfinished states, which it is not in), the gauges no longer
-- count it, and the block itself keeps its landed audit and its
-- `qbit_pool_blocks` row, marked `inactive`, so a later reorg that
-- reactivates the block is credited through the ordinary reorg reconciler
-- (`Ledger::reconcile_blocks_in`), deferred share included, without the
-- outbox row ever reopening. `abandoned` stays reachable from `pending`
-- only; `orphaned` is reachable from the three offer states only.
--
-- The three lifecycle CHECKs 011 wrote are replaced under their own names,
-- and only if they still carry exactly the definitions 011 wrote (deparsed
-- by this server and compared, never a hand-written string): a definition
-- that differs was edited by an operator or written by another release, and
-- is refused with nothing changed. The unfinished index needs no change: its
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
    -- 015's rules. An orphaned row is terminal (`completed_at` set) and keeps
    -- the evidence an unfinished offer-state row keeps: the document, the
    -- block bytes and the window reference. Its offer columns are a
    -- reconciliation row's: the reservation, an outcome (`unknown` for a
    -- reservation whose call was lost) and the reason in `last_error`, which
    -- names the competitor, the height, the confirmations and the tip.
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
        OR (state = 'orphaned'
            AND completed_at IS NOT NULL
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
    lifecycle_payload_plain constant text := $def$
        (state = 'pending' AND completed_at IS NULL AND candidate IS NOT NULL)
        OR (state IN ('offer_reserved', 'offered', 'reconciliation')
            AND completed_at IS NULL
            AND num_nonnulls(candidate, block_bytes, window_anchor_ms,
                             window_prior_balances_sha256) = 4)
        OR (state = 'orphaned'
            AND completed_at IS NOT NULL
            AND num_nonnulls(candidate, block_bytes, window_anchor_ms,
                             window_prior_balances_sha256) = 4)
        OR (state IN ('submitted', 'abandoned')
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
            OR (state IN ('reconciliation', 'orphaned')
                AND num_nonnulls(offer_reserved_at, offer_reserved_by, offer_outcome,
                                 last_error) = 4
                AND btrim(last_error) <> '')
            OR state = 'submitted')$def$;
    dual boolean;
    known_payload text;
    lifecycle_payload text;
    expected_known_state text;
    expected_known_payload text;
    expected_known_offer text;
    item record;
    mismatched text[] := '{}';
    missing text[] := '{}';
BEGIN
    dual := EXISTS (SELECT 1 FROM pg_attribute
                    WHERE attrelid = outbox AND attname = 'body_id' AND NOT attisdropped);
    known_payload := CASE WHEN dual THEN known_payload_dual ELSE known_payload_plain END;
    lifecycle_payload := CASE WHEN dual THEN lifecycle_payload_dual
                              ELSE lifecycle_payload_plain END;

    -- 011's definitions, deparsed by this server through temporary NOT
    -- VALID probes, so the comparison below is deparse against deparse.
    EXECUTE format(
        'ALTER TABLE qbit_block_candidate_outbox '
        'ADD CONSTRAINT qbit_block_candidate_outbox_015_probe_state CHECK (%s) NOT VALID, '
        'ADD CONSTRAINT qbit_block_candidate_outbox_015_probe_payload CHECK (%s) NOT VALID, '
        'ADD CONSTRAINT qbit_block_candidate_outbox_015_probe_offer CHECK (%s) NOT VALID',
        known_state, known_payload, known_offer);
    SELECT
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_015_probe_state'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_015_probe_payload'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_015_probe_offer')
    INTO expected_known_state, expected_known_payload, expected_known_offer
    FROM pg_constraint
    WHERE conrelid = outbox AND contype = 'c';
    ALTER TABLE qbit_block_candidate_outbox
        DROP CONSTRAINT qbit_block_candidate_outbox_015_probe_state,
        DROP CONSTRAINT qbit_block_candidate_outbox_015_probe_payload,
        DROP CONSTRAINT qbit_block_candidate_outbox_015_probe_offer;
    IF num_nulls(expected_known_state, expected_known_payload, expected_known_offer) > 0 THEN
        RAISE EXCEPTION 'migration 015 could not deparse its probe constraints on qbit_block_candidate_outbox; nothing was changed';
    END IF;

    -- Each of 011's three names must exist and carry 011's definition.
    FOR item IN
        SELECT names.name,
               c.conname IS NOT NULL AS present,
               pg_get_expr(c.conbin, c.conrelid) AS definition,
               pg_get_constraintdef(c.oid) AS declaration,
               names.expected
        FROM (VALUES
                ('qbit_block_candidate_outbox_lifecycle_state_check', expected_known_state),
                ('qbit_block_candidate_outbox_lifecycle_payload_check', expected_known_payload),
                ('qbit_block_candidate_outbox_offer_check', expected_known_offer))
             AS names(name, expected)
        LEFT JOIN pg_constraint c
               ON c.conrelid = outbox AND c.contype = 'c' AND c.conname = names.name
        ORDER BY names.name
    LOOP
        IF NOT item.present THEN
            missing := missing || item.name;
        ELSIF item.definition IS DISTINCT FROM item.expected THEN
            mismatched := mismatched || format('%I %s', item.name, item.declaration);
        END IF;
    END LOOP;
    IF array_length(missing, 1) > 0 THEN
        RAISE EXCEPTION 'migration 015 refuses to run: qbit_block_candidate_outbox is missing the lifecycle CHECK constraint(s) 011 created: %. Restore them from migrations/011_offer_before_landing.sql, then migrate again; nothing was changed',
            array_to_string(missing, ', ');
    END IF;
    IF array_length(mismatched, 1) > 0 THEN
        RAISE EXCEPTION 'migration 015 refuses to replace the outbox lifecycle rules: % carries a definition 011 did not write. Restore the 011 definition (or, if a newer PRISM release wrote this database, upgrade the server), then migrate again; nothing was changed',
            array_to_string(mismatched, '; ');
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

-- Declared last, once the lifecycle rules admit the state: a binary that
-- does not know this capability refuses the database at connect
-- (ledger/migration.rs, NATIVE_CAPABILITIES). A pre-015 binary never
-- claims an orphaned row (its claim lanes select the four unfinished
-- states) and its gauges never count one, but its `CandidateState::parse`
-- has no name for the state and its terminal-state check after a lost
-- lease does not know it, so a mixed-version rollout is refused outright
-- rather than left to those two paths.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('candidate_orphan_disposition', 1)
ON CONFLICT (capability) DO UPDATE
SET capability_value = EXCLUDED.capability_value,
    updated_at = clock_timestamp();
