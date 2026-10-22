-- A266's offer-before-landing lifecycle for qbit_block_candidate_outbox, and
-- the as-issued provenance of the payout evidence it lands.
--
-- STOP EVERY pre-011 FRONTEND BEFORE APPLYING THIS MIGRATION, and let their
-- candidate claims expire (at most the 120 s lease). A pre-011 frontend runs
-- two orders: an ordinary candidate lands its audit and then offers the
-- block, a leased candidate (#350) offers first and lands afterwards; either
-- way the row stays `pending` until the same attempt finishes it, so a block
-- that reached the node before a crash is indistinguishable from one that
-- never did. A post-011 frontend reserves the row before its one
-- `submitblock` call and never offers a reserved row again. The two cannot
-- share an outbox: a post-011 claim would reserve and re-offer a row an old
-- frontend already sent. The runner therefore refuses to apply this
-- migration while any pending row holds a live claim, quarantines every
-- pending row an old frontend already attempted (below), and the capability
-- declared at the end keeps a pre-011 binary from connecting to the migrated
-- database at all. See docs/prism-ledger-ops.md, "Durable block candidates".
--
-- States. `pending` is a durable candidate no frontend has offered. A claim
-- moves it to `offer_reserved` before the one `submitblock` call (the row is
-- the unique reservation per block hash), records the node's answer as
-- `offered`, then rebuilds and lands the audit and finishes it as
-- `submitted` once the block is proven on the active chain. `reconciliation`
-- holds every offered row automation could not finish: an ambiguous RPC
-- outcome, a reservation whose call was lost to a crash (delivery unknown),
-- a node rejection, or a landing failure after acceptance. Rows in any of
-- the three offer states are never offered again, by any frontend, and are
-- never abandoned. `abandoned` is reachable from `pending` only: a block
-- proven superseded before it was ever offered. Every unfinished state keeps
-- its candidate document, its block bytes and 007's six window-reference
-- columns; the terminal UPDATE NULLs all of them at once, as before.
ALTER TABLE qbit_block_candidate_outbox
    -- The enqueuing frontend's wall clock (UNIX ms) when the locally
    -- validated block proof reached the coordinator. Written at enqueue,
    -- NULL for rows an older binary wrote. Not the ledger clock and not
    -- `created_at`, which is the database clock at the insert.
    ADD COLUMN IF NOT EXISTS proof_observed_at_ms bigint,
    -- The reservation: when (database clock) and which instance took it.
    ADD COLUMN IF NOT EXISTS offer_reserved_at timestamptz,
    ADD COLUMN IF NOT EXISTS offer_reserved_by text,
    -- The actual `submitblock` call time on the offering frontend's wall
    -- clock (UNIX ms). NULL when the outcome commit was lost to a crash: the
    -- reservation time is never substituted for it.
    ADD COLUMN IF NOT EXISTS offered_at_ms bigint,
    -- How the node answered: accepted (null reply), rejected (a reason
    -- string, kept in offer_reply), or unknown (transport failure, timeout,
    -- or an outcome lost with the frontend).
    ADD COLUMN IF NOT EXISTS offer_outcome text,
    ADD COLUMN IF NOT EXISTS offer_reply text;

-- Replace the state and payload rules. The rules 011 supersedes are known by
-- definition, never by name: 001 wrote its two rules inline, so PostgreSQL
-- named them, and on a database that came through #258's 002 the payload
-- rule is that migration's dual-format rule, whose pending-row shape the new
-- rule keeps. Each known text is added below as a temporary NOT VALID probe,
-- deparsed by this server and dropped again, so a catalog definition is
-- compared with a deparse of the same server, never with a hand-written
-- string or a substring, and a known rule is recognised under any name.
-- Every CHECK on the outbox is then classified, in this order: one of 011's
-- three names must already carry 011's own rule, whatever else it holds, a
-- legacy rule included, it is refused as conflicting; a known legacy rule is
-- dropped and replaced; and any other CHECK is classified by the columns the
-- catalog says it references (pg_constraint.conkey): one that references
-- `state` or `completed_at` is refused by name, one that references neither
-- is kept exactly as it is, validation state included. 007's window CHECK,
-- 001's column CHECKs on candidate_sha256 and attempt_count and #258's
-- storage_version CHECK are all of the kept kind. A refusal RAISEs with
-- every offending constraint named, so this transaction rolls back the
-- columns added above, every drop below and the version row together, and
-- nothing is dropped silently. This is a conservative catalog policy: it
-- writes no synthetic row and executes no unknown predicate on purpose, and
-- it does not prove a kept CHECK compatible with every future write. A kept
-- CHECK that rejects the quarantine below, or a later lifecycle write, fails
-- that statement's transaction as any CHECK would.
DO $$
DECLARE
    outbox constant regclass := 'qbit_block_candidate_outbox'::regclass;
    -- 011's rules, written once: the probes and the final ADD use them.
    lifecycle_state constant text := $def$
        state IN ('pending', 'offer_reserved', 'offered', 'reconciliation',
                  'submitted', 'abandoned')$def$;
    -- An unfinished row keeps its evidence. The three offer states are only
    -- ever written by a post-011 frontend, so they also require the block
    -- bytes and the window reference 007 introduced; `pending` keeps the
    -- rule it had, because 007 already refuses a pending row without a
    -- reference at claim time. A terminal row is "no evidence": the
    -- document, the block and all six window columns are NULL together. On
    -- a database that came through #258's 002, a pending row keeps the
    -- dual-format shape (a version-1 document, or a version-2 body the
    -- claim lane parks and 006's drain check refuses), and a terminal or
    -- offered row carries no body.
    lifecycle_payload_dual constant text := $def$
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
    lifecycle_payload_plain constant text := $def$
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
    -- The offer columns follow the state: nothing before the reservation,
    -- the reservation alone while it is held, a recorded outcome once the
    -- call returned or was lost (a rejection carries the node's reply, a
    -- reconciliation row carries its reason), and a submitted row keeps
    -- whatever it recorded, which is nothing for a row an older binary
    -- finished. Counts are used throughout because a CHECK that evaluates
    -- to NULL passes.
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
            OR state = 'submitted')$def$;
    dual boolean;
    lifecycle_payload text;
    known_state text;
    known_payload text;
    known_dual text;
    expected_state text;
    expected_payload text;
    expected_offer text;
    known text[];
    to_drop text[] := '{}';
    reused text[] := '{}';
    refused text[] := '{}';
    item record;
    item_name text;
BEGIN
    dual := EXISTS (SELECT 1 FROM pg_attribute
                    WHERE attrelid = outbox AND attname = 'body_id' AND NOT attisdropped);
    lifecycle_payload := CASE WHEN dual THEN lifecycle_payload_dual
                              ELSE lifecycle_payload_plain END;

    -- The known and the expected definitions, deparsed by this server.
    ALTER TABLE qbit_block_candidate_outbox
        ADD CONSTRAINT qbit_block_candidate_outbox_011_probe_known_state CHECK (
            state IN ('pending', 'submitted', 'abandoned')) NOT VALID,
        ADD CONSTRAINT qbit_block_candidate_outbox_011_probe_known_payload CHECK (
            (state = 'pending' AND completed_at IS NULL AND candidate IS NOT NULL)
            OR (state IN ('submitted', 'abandoned')
                AND completed_at IS NOT NULL
                AND candidate IS NULL)) NOT VALID;
    IF dual THEN
        ALTER TABLE qbit_block_candidate_outbox
            ADD CONSTRAINT qbit_block_candidate_outbox_011_probe_known_dual CHECK (
                (state = 'pending' AND completed_at IS NULL
                    AND ((storage_version = 1 AND candidate IS NOT NULL AND body_id IS NULL)
                         OR (storage_version = 2 AND candidate IS NULL AND body_id IS NOT NULL)))
                OR (state IN ('submitted', 'abandoned')
                    AND completed_at IS NOT NULL
                    AND candidate IS NULL
                    AND body_id IS NULL)) NOT VALID;
    END IF;
    EXECUTE format(
        'ALTER TABLE qbit_block_candidate_outbox '
        'ADD CONSTRAINT qbit_block_candidate_outbox_011_probe_expected_state CHECK (%s) NOT VALID, '
        'ADD CONSTRAINT qbit_block_candidate_outbox_011_probe_expected_payload CHECK (%s) NOT VALID, '
        'ADD CONSTRAINT qbit_block_candidate_outbox_011_probe_expected_offer CHECK (%s) NOT VALID',
        lifecycle_state, lifecycle_payload, offer_rule);
    SELECT
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_011_probe_known_state'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_011_probe_known_payload'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_011_probe_known_dual'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_011_probe_expected_state'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_011_probe_expected_payload'),
        max(pg_get_expr(conbin, conrelid)) FILTER (WHERE conname = 'qbit_block_candidate_outbox_011_probe_expected_offer')
    INTO known_state, known_payload, known_dual, expected_state, expected_payload, expected_offer
    FROM pg_constraint
    WHERE conrelid = outbox AND contype = 'c';
    ALTER TABLE qbit_block_candidate_outbox
        DROP CONSTRAINT qbit_block_candidate_outbox_011_probe_known_state,
        DROP CONSTRAINT qbit_block_candidate_outbox_011_probe_known_payload,
        DROP CONSTRAINT qbit_block_candidate_outbox_011_probe_expected_state,
        DROP CONSTRAINT qbit_block_candidate_outbox_011_probe_expected_payload,
        DROP CONSTRAINT qbit_block_candidate_outbox_011_probe_expected_offer;
    IF dual THEN
        ALTER TABLE qbit_block_candidate_outbox
            DROP CONSTRAINT qbit_block_candidate_outbox_011_probe_known_dual;
    END IF;
    IF num_nulls(known_state, known_payload, expected_state, expected_payload, expected_offer) > 0
       OR (dual AND known_dual IS NULL) THEN
        RAISE EXCEPTION 'migration 011 could not deparse its probe constraints on qbit_block_candidate_outbox; nothing was changed';
    END IF;
    known := array_remove(ARRAY[known_state, known_payload, known_dual], NULL);

    -- Classify every CHECK on the outbox before anything is dropped: one of
    -- 011's names must carry 011's own rule, a known rule is replaced, and
    -- any other CHECK that references a lifecycle column is refused.
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
        IF item.name IN ('qbit_block_candidate_outbox_lifecycle_state_check',
                         'qbit_block_candidate_outbox_lifecycle_payload_check',
                         'qbit_block_candidate_outbox_offer_check') THEN
            -- Parenthesised: PL/pgSQL ends an IF condition at the first
            -- unnested THEN, and the CASE has its own.
            IF item.definition IS DISTINCT FROM (CASE item.name
                    WHEN 'qbit_block_candidate_outbox_lifecycle_state_check' THEN expected_state
                    WHEN 'qbit_block_candidate_outbox_lifecycle_payload_check' THEN expected_payload
                    ELSE expected_offer END) THEN
                reused := reused || format('%I exists with a definition 011 did not create: %s',
                                           item.name, item.declaration);
            END IF;
        ELSIF item.definition = ANY (known) THEN
            to_drop := to_drop || item.name;
        ELSIF item.lifecycle THEN
            refused := refused || format('%I %s', item.name, item.declaration);
        END IF;
    END LOOP;
    IF array_length(reused, 1) > 0 THEN
        RAISE EXCEPTION 'migration 011 refuses to reuse its constraint names on qbit_block_candidate_outbox: %. Drop or rename each named constraint, then migrate again; nothing was changed',
            array_to_string(reused, '; ');
    END IF;
    IF array_length(refused, 1) > 0 THEN
        RAISE EXCEPTION 'migration 011 refuses to replace the outbox lifecycle rules: qbit_block_candidate_outbox carries CHECK constraints 011 does not know that reference its lifecycle columns state or completed_at: %. Only 001''s state and payload rules and #258''s dual-format rule are replaced, by definition; a CHECK that references neither column is kept as it is. Review each named constraint, drop it (and re-create it after the migration if it still holds for the offer lifecycle), then migrate again; nothing was changed',
            array_to_string(refused, '; ');
    END IF;
    FOREACH item_name IN ARRAY to_drop LOOP
        EXECUTE format('ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT %I', item_name);
    END LOOP;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = outbox
                     AND conname = 'qbit_block_candidate_outbox_lifecycle_state_check') THEN
        EXECUTE format('ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_lifecycle_state_check CHECK (%s)', lifecycle_state);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = outbox
                     AND conname = 'qbit_block_candidate_outbox_lifecycle_payload_check') THEN
        EXECUTE format('ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_lifecycle_payload_check CHECK (%s)', lifecycle_payload);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = outbox
                     AND conname = 'qbit_block_candidate_outbox_offer_check') THEN
        EXECUTE format('ALTER TABLE qbit_block_candidate_outbox ADD CONSTRAINT qbit_block_candidate_outbox_offer_check CHECK (%s)', offer_rule);
    END IF;
END $$;

-- The oldest-due lane's index: every unfinished row, the pending rows
-- included, in the lane's own order. The lane selects `state IN` all four
-- unfinished states and takes the first due row ordered by next_attempt_at,
-- created_at and block_hash, so the predicate here is exactly the lane's
-- and the columns are exactly its order; the dispatch probe that asks
-- whether any unfinished row is due uses the same predicate. A partial
-- index over the three offer states alone could serve neither: the planner
-- cannot prove the lane's four-state predicate from it, so every claim would
-- scan and sort the whole outbox, the retained terminal history included.
-- A row leaves this index when the terminal UPDATE leaves the predicate.
-- 005's fresh index keeps serving the fresh lane (never-attempted pending
-- rows, newest first); 001's pending index and 002's claim index are
-- unchanged, and 007's balance-reference index is state-independent.
CREATE INDEX IF NOT EXISTS qbit_block_candidate_outbox_unfinished_idx
    ON qbit_block_candidate_outbox (next_attempt_at, created_at, block_hash)
    WHERE state IN ('pending', 'offer_reserved', 'offered', 'reconciliation');

-- Quarantine, before any post-011 frontend can claim: a pending row an old
-- frontend attempted at least once (every pre-011 claim counted an attempt)
-- may already have been offered and lost its reply, so it is never granted
-- a reservation. It becomes a reconciliation row with an unknown outcome and
-- is due at once: the recovery lane observes the chain, lands and finishes
-- an active block, and keeps an inactive one, with its evidence, for the
-- operator. A never-attempted row stays pending. The runner has already
-- refused if an attempted row lacks the evidence the payload rule requires.
UPDATE qbit_block_candidate_outbox
SET state = 'reconciliation',
    offer_reserved_at = clock_timestamp(),
    offer_reserved_by = 'migration-011-legacy-quarantine',
    offer_outcome = 'unknown',
    offer_reply = NULL,
    last_error = format(
        'quarantined by migration 011: a pre-011 frontend attempted this candidate %s time(s) and may have offered it; delivery unknown, never offered again (last error before quarantine: %s)',
        attempt_count, COALESCE(last_error, 'none')),
    claim_token = NULL,
    claim_instance_id = NULL,
    claim_expires_at = NULL,
    next_attempt_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE state = 'pending' AND attempt_count > 0;

-- As-issued provenance. A landing writes the block's payout and carry rows
-- from the immutable payout_policy_manifest of the audit the block's coinbase
-- commits to, whatever the canonical balances are at landing time, and marks
-- the block with that audit's digest in the same transaction, after the
-- audit, coinbase and window reference have been verified. The marker ties
-- the evidence to the stored audit: the validator below checks marked rows
-- against that manifest. NULL on every block landed before this migration.
ALTER TABLE qbit_pool_blocks
    ADD COLUMN IF NOT EXISTS as_issued_audit_sha256 text;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'qbit_pool_blocks'::regclass
                     AND conname = 'qbit_pool_blocks_as_issued_audit_sha256_check') THEN
        ALTER TABLE qbit_pool_blocks
            ADD CONSTRAINT qbit_pool_blocks_as_issued_audit_sha256_check
            CHECK (as_issued_audit_sha256 IS NULL OR as_issued_audit_sha256 ~ '^[0-9a-f]{64}$');
    END IF;
END $$;

-- The carry-forward integrity validator, replaced in place (its signature,
-- the report that wraps it, and fatal-state recovery's use of that report
-- are unchanged). Unmarked rows, every block landed before this migration,
-- keep 001's sequential rule: the stored prior, candidate and carry of a row
-- must equal the running sum of (gross - onchain) over the partition's
-- earlier active rows in chain order. A marked row is as-issued evidence:
-- its stored amounts are validated against the matching miner account of
-- the immutable payout_policy_manifest in the audit the block's marker
-- names, field by field with null-safe comparisons, with the issued
-- arithmetic, and the block's payout entries against every account of that
-- manifest. Findings, each named in mismatch_reason: a marked block whose
-- audit is missing or whose manifest is missing, has no accounts array or
-- names another height (found whether or not the block has any carry or
-- payout row left); a manifest account missing a required field; a carry
-- row without an account, an account without a carry row, duplicated
-- evidence, a row at another height, or any amount or action that differs
-- from the manifest or breaks the issued arithmetic; a payout entry that is
-- missing, duplicated, differs from its account, or matches no account. So
-- a corrupted marked row, and a marked block whose whole evidence set is
-- gone, both still fail. The current balances are unaffected by either
-- rule: they sum the active deltas, and qbit_carry_forward_current_drift()
-- checks that summary against its recomputation independently of this
-- validator.
CREATE OR REPLACE FUNCTION qbit_carry_forward_integrity_mismatches()
RETURNS TABLE (
    carry_forward_seq bigint,
    block_hash text,
    block_height bigint,
    miner_id text,
    payout_order_key text,
    p2mr_program bytea,
    prior_balance_sats numeric,
    expected_prior_balance_sats numeric,
    gross_amount_sats bigint,
    candidate_balance_sats numeric,
    expected_candidate_balance_sats numeric,
    onchain_amount_sats bigint,
    settlement_fee_sats bigint,
    carry_forward_balance_sats numeric,
    expected_carry_forward_balance_sats numeric,
    action text,
    mismatch_reason text
)
LANGUAGE sql
STABLE
AS $$
    WITH active AS (
        SELECT ledger.*, block.as_issued_audit_sha256
        FROM qbit_payout_carry_forward ledger
        JOIN qbit_pool_blocks block
          ON block.block_hash = ledger.block_hash
        WHERE ledger.maturity_state <> 'reversed'
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
    ),
    sequential AS (
        SELECT
            active.*,
            COALESCE(
                SUM(active.gross_amount_sats::numeric - active.onchain_amount_sats::numeric)
                OVER (
                    PARTITION BY active.miner_id, active.payout_order_key, active.p2mr_program
                    ORDER BY active.block_height ASC, active.carry_forward_seq ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0::numeric
            ) AS running_prior_balance_sats
        FROM active
    ),
    legacy AS (
        SELECT
            sequential.*,
            sequential.running_prior_balance_sats + sequential.gross_amount_sats::numeric
                AS running_candidate_balance_sats,
            sequential.running_prior_balance_sats + sequential.gross_amount_sats::numeric
                - sequential.onchain_amount_sats::numeric
                AS running_carry_forward_balance_sats
        FROM sequential
        WHERE sequential.as_issued_audit_sha256 IS NULL
    ),
    legacy_mismatch AS (
        SELECT
            legacy.carry_forward_seq,
            legacy.block_hash,
            legacy.block_height,
            legacy.miner_id,
            legacy.payout_order_key,
            legacy.p2mr_program,
            legacy.prior_balance_sats,
            legacy.running_prior_balance_sats AS expected_prior_balance_sats,
            legacy.gross_amount_sats,
            legacy.candidate_balance_sats,
            legacy.running_candidate_balance_sats AS expected_candidate_balance_sats,
            legacy.onchain_amount_sats,
            legacy.settlement_fee_sats,
            legacy.carry_forward_balance_sats,
            legacy.running_carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            legacy.action,
            concat_ws(
                ',',
                CASE WHEN legacy.prior_balance_sats <> legacy.running_prior_balance_sats
                     THEN 'prior_balance' END,
                CASE WHEN legacy.candidate_balance_sats <> legacy.running_candidate_balance_sats
                     THEN 'candidate_balance' END,
                CASE WHEN legacy.carry_forward_balance_sats <> legacy.running_carry_forward_balance_sats
                     THEN 'carry_forward_balance' END
            ) AS mismatch_reason
        FROM legacy
        WHERE legacy.prior_balance_sats <> legacy.running_prior_balance_sats
           OR legacy.candidate_balance_sats <> legacy.running_candidate_balance_sats
           OR legacy.carry_forward_balance_sats <> legacy.running_carry_forward_balance_sats
    ),
    -- Every marked, active block beside the audit its marker names, or
    -- beside nothing when that audit is missing. The manifest is read as a
    -- whole so a missing or malformed one is a block-level finding, whether
    -- or not the block has any carry or payout row at all.
    marked_blocks AS (
        SELECT
            block.block_hash,
            block.block_height,
            bundle.block_hash IS NOT NULL AS audit_present,
            bundle.audit_bundle -> 'payout_policy_manifest' AS manifest,
            CASE WHEN jsonb_typeof(bundle.audit_bundle -> 'payout_policy_manifest' -> 'accounts') = 'array'
                 THEN bundle.audit_bundle -> 'payout_policy_manifest' -> 'accounts' END AS accounts
        FROM qbit_pool_blocks block
        LEFT JOIN qbit_pool_audit_bundles bundle
          ON bundle.block_hash = block.block_hash
         AND bundle.audit_bundle_sha256 = block.as_issued_audit_sha256
        WHERE block.as_issued_audit_sha256 IS NOT NULL
          AND block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
    ),
    block_mismatch AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            marked_blocks.block_hash,
            marked_blocks.block_height,
            NULL::text AS miner_id,
            NULL::text AS payout_order_key,
            NULL::bytea AS p2mr_program,
            NULL::numeric AS prior_balance_sats,
            NULL::numeric AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            NULL::numeric AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            NULL::numeric AS expected_carry_forward_balance_sats,
            NULL::text AS action,
            concat_ws(
                ',',
                CASE WHEN NOT marked_blocks.audit_present THEN 'audit_missing' END,
                CASE WHEN marked_blocks.audit_present
                      AND jsonb_typeof(marked_blocks.manifest) IS DISTINCT FROM 'object'
                     THEN 'manifest_missing' END,
                CASE WHEN jsonb_typeof(marked_blocks.manifest) = 'object'
                      AND marked_blocks.accounts IS NULL
                     THEN 'manifest_accounts_missing' END,
                CASE WHEN jsonb_typeof(marked_blocks.manifest) = 'object'
                      AND (CASE WHEN marked_blocks.manifest ->> 'block_height' ~ '^[0-9]{1,18}$'
                                THEN (marked_blocks.manifest ->> 'block_height')::bigint END)
                          IS DISTINCT FROM marked_blocks.block_height
                     THEN 'manifest_block_height' END
            ) AS mismatch_reason
        FROM marked_blocks
    ),
    -- The manifest's accounts, each field taken only when well formed, so
    -- a missing or malformed field is NULL here and a finding below rather
    -- than a cast error or a comparison that quietly passes.
    manifest_accounts AS (
        SELECT
            marked_blocks.block_hash,
            marked_blocks.block_height,
            account ->> 'recipient_id' AS miner_id,
            account ->> 'order_key' AS payout_order_key,
            CASE WHEN account ->> 'p2mr_program_hex' ~ '^[0-9a-f]{64}$'
                 THEN decode(account ->> 'p2mr_program_hex', 'hex') END AS p2mr_program,
            COALESCE(account ->> 'account_type', 'miner') AS account_type,
            CASE WHEN account ->> 'gross_amount_sats' ~ '^[0-9]{1,18}$'
                 THEN (account ->> 'gross_amount_sats')::bigint END AS gross_amount_sats,
            CASE WHEN account ->> 'prior_balance_sats' ~ '^-?[0-9]{1,39}$'
                 THEN (account ->> 'prior_balance_sats')::numeric END AS prior_balance_sats,
            CASE WHEN account ->> 'candidate_balance_sats' ~ '^-?[0-9]{1,39}$'
                 THEN (account ->> 'candidate_balance_sats')::numeric END AS candidate_balance_sats,
            CASE WHEN account ->> 'onchain_amount_sats' ~ '^[0-9]{1,18}$'
                 THEN (account ->> 'onchain_amount_sats')::bigint END AS onchain_amount_sats,
            CASE WHEN account -> 'settlement_fee_sats' IS NULL THEN 0::bigint
                 WHEN account ->> 'settlement_fee_sats' ~ '^[0-9]{1,18}$'
                 THEN (account ->> 'settlement_fee_sats')::bigint END AS settlement_fee_sats,
            CASE WHEN account ->> 'carry_forward_balance_sats' ~ '^-?[0-9]{1,39}$'
                 THEN (account ->> 'carry_forward_balance_sats')::numeric END AS carry_forward_balance_sats,
            account ->> 'action' AS action
        FROM marked_blocks
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(marked_blocks.accounts, '[]'::jsonb)) AS account
    ),
    -- A manifest account missing a required field is a finding by itself,
    -- and so is one whose own amounts do not add up, whatever its account
    -- type: a fee recipient has no carry row, so the issued arithmetic of
    -- its manifest entry is checked here, where every entry is, rather
    -- than only against the carry rows below.
    manifest_mismatch AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            manifest.block_hash,
            manifest.block_height,
            manifest.miner_id,
            manifest.payout_order_key,
            manifest.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            manifest.prior_balance_sats AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            manifest.candidate_balance_sats AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            manifest.settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            manifest.carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            manifest.action,
            concat_ws(
                ',',
                CASE WHEN num_nulls(manifest.miner_id, manifest.payout_order_key, manifest.p2mr_program,
                                    manifest.gross_amount_sats, manifest.prior_balance_sats,
                                    manifest.candidate_balance_sats, manifest.onchain_amount_sats,
                                    manifest.settlement_fee_sats, manifest.carry_forward_balance_sats,
                                    manifest.action) > 0
                     THEN 'manifest_field_missing' END,
                CASE WHEN manifest.candidate_balance_sats
                          <> manifest.prior_balance_sats + manifest.gross_amount_sats::numeric
                     THEN 'manifest_candidate_arithmetic' END,
                CASE WHEN manifest.carry_forward_balance_sats
                          <> manifest.candidate_balance_sats - manifest.onchain_amount_sats::numeric
                     THEN 'manifest_carry_arithmetic' END
            ) AS mismatch_reason
        FROM manifest_accounts manifest
    ),
    marked_carry AS (
        SELECT
            active.*,
            marked_blocks.block_height AS marked_block_height,
            marked_blocks.accounts IS NULL AS manifest_unusable,
            count(*) OVER (
                PARTITION BY active.block_hash, active.miner_id, active.payout_order_key,
                             active.p2mr_program
            ) AS evidence_rows,
            manifest.block_hash IS NOT NULL AS account_present,
            manifest.prior_balance_sats AS manifest_prior_balance_sats,
            manifest.gross_amount_sats AS manifest_gross_amount_sats,
            manifest.candidate_balance_sats AS manifest_candidate_balance_sats,
            manifest.onchain_amount_sats AS manifest_onchain_amount_sats,
            manifest.settlement_fee_sats AS manifest_settlement_fee_sats,
            manifest.carry_forward_balance_sats AS manifest_carry_forward_balance_sats,
            manifest.action AS manifest_action
        FROM active
        JOIN marked_blocks
          ON marked_blocks.block_hash = active.block_hash
        LEFT JOIN manifest_accounts manifest
          ON manifest.block_hash = active.block_hash
         AND manifest.account_type = 'miner'
         AND manifest.miner_id = active.miner_id
         AND manifest.payout_order_key = active.payout_order_key
         AND manifest.p2mr_program = active.p2mr_program
        WHERE active.as_issued_audit_sha256 IS NOT NULL
    ),
    marked_mismatch AS (
        SELECT
            marked_carry.carry_forward_seq,
            marked_carry.block_hash,
            marked_carry.block_height,
            marked_carry.miner_id,
            marked_carry.payout_order_key,
            marked_carry.p2mr_program,
            marked_carry.prior_balance_sats,
            marked_carry.manifest_prior_balance_sats AS expected_prior_balance_sats,
            marked_carry.gross_amount_sats,
            marked_carry.candidate_balance_sats,
            marked_carry.manifest_candidate_balance_sats AS expected_candidate_balance_sats,
            marked_carry.onchain_amount_sats,
            marked_carry.settlement_fee_sats,
            marked_carry.carry_forward_balance_sats,
            marked_carry.manifest_carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            marked_carry.action,
            concat_ws(
                ',',
                CASE WHEN NOT marked_carry.manifest_unusable AND NOT marked_carry.account_present
                     THEN 'account_missing' END,
                CASE WHEN marked_carry.evidence_rows > 1 THEN 'duplicate_evidence' END,
                CASE WHEN marked_carry.block_height IS DISTINCT FROM marked_carry.marked_block_height
                     THEN 'block_height' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.prior_balance_sats IS DISTINCT FROM marked_carry.manifest_prior_balance_sats
                     THEN 'prior_balance' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.gross_amount_sats IS DISTINCT FROM marked_carry.manifest_gross_amount_sats
                     THEN 'gross_amount' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.candidate_balance_sats IS DISTINCT FROM marked_carry.manifest_candidate_balance_sats
                     THEN 'candidate_balance' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.onchain_amount_sats IS DISTINCT FROM marked_carry.manifest_onchain_amount_sats
                     THEN 'onchain_amount' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.settlement_fee_sats IS DISTINCT FROM marked_carry.manifest_settlement_fee_sats
                     THEN 'settlement_fee' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.carry_forward_balance_sats IS DISTINCT FROM marked_carry.manifest_carry_forward_balance_sats
                     THEN 'carry_forward_balance' END,
                CASE WHEN marked_carry.account_present
                      AND marked_carry.action IS DISTINCT FROM marked_carry.manifest_action
                     THEN 'action' END,
                CASE WHEN marked_carry.candidate_balance_sats
                          <> marked_carry.prior_balance_sats + marked_carry.gross_amount_sats::numeric
                     THEN 'candidate_arithmetic' END,
                CASE WHEN marked_carry.carry_forward_balance_sats
                          <> marked_carry.candidate_balance_sats - marked_carry.onchain_amount_sats::numeric
                     THEN 'carry_arithmetic' END
            ) AS mismatch_reason
        FROM marked_carry
    ),
    evidence_missing AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            manifest.block_hash,
            manifest.block_height,
            manifest.miner_id,
            manifest.payout_order_key,
            manifest.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            manifest.prior_balance_sats AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            manifest.candidate_balance_sats AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            manifest.carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            NULL::text AS action,
            'evidence_missing'::text AS mismatch_reason
        FROM manifest_accounts manifest
        WHERE manifest.account_type = 'miner'
          AND NOT EXISTS (
              SELECT 1
              FROM active
              WHERE active.block_hash = manifest.block_hash
                AND active.miner_id IS NOT DISTINCT FROM manifest.miner_id
                AND active.payout_order_key IS NOT DISTINCT FROM manifest.payout_order_key
                AND active.p2mr_program IS NOT DISTINCT FROM manifest.p2mr_program)
    ),
    payout_entries AS (
        SELECT
            manifest.block_hash,
            manifest.block_height,
            manifest.miner_id,
            manifest.payout_order_key,
            manifest.p2mr_program,
            manifest.prior_balance_sats,
            manifest.candidate_balance_sats,
            manifest.carry_forward_balance_sats,
            count(payout.block_hash) AS entries,
            COALESCE(bool_or(
                payout.block_height IS DISTINCT FROM manifest.block_height
                OR payout.onchain_amount_sats IS DISTINCT FROM manifest.onchain_amount_sats
                OR payout.carry_forward_balance_sats IS DISTINCT FROM manifest.carry_forward_balance_sats
                OR payout.action IS DISTINCT FROM manifest.action), false) AS differs
        FROM manifest_accounts manifest
        LEFT JOIN qbit_pool_payout_entries payout
          ON payout.block_hash = manifest.block_hash
         AND payout.miner_id = manifest.miner_id
         AND payout.payout_order_key = manifest.payout_order_key
         AND payout.p2mr_program = manifest.p2mr_program
        GROUP BY
            manifest.block_hash, manifest.block_height, manifest.miner_id,
            manifest.payout_order_key, manifest.p2mr_program, manifest.prior_balance_sats,
            manifest.candidate_balance_sats, manifest.carry_forward_balance_sats
    ),
    payout_mismatch AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            payout_entries.block_hash,
            payout_entries.block_height,
            payout_entries.miner_id,
            payout_entries.payout_order_key,
            payout_entries.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            payout_entries.prior_balance_sats AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            payout_entries.candidate_balance_sats AS expected_candidate_balance_sats,
            NULL::bigint AS onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            NULL::numeric AS carry_forward_balance_sats,
            payout_entries.carry_forward_balance_sats AS expected_carry_forward_balance_sats,
            NULL::text AS action,
            concat_ws(
                ',',
                CASE WHEN payout_entries.entries = 0 THEN 'payout_entry_missing' END,
                CASE WHEN payout_entries.entries > 1 THEN 'payout_entry_duplicate' END,
                CASE WHEN payout_entries.differs THEN 'payout_entry' END
            ) AS mismatch_reason
        FROM payout_entries
        WHERE payout_entries.entries <> 1 OR payout_entries.differs
    ),
    payout_unexpected AS (
        SELECT
            NULL::bigint AS carry_forward_seq,
            payout.block_hash,
            payout.block_height,
            payout.miner_id,
            payout.payout_order_key,
            payout.p2mr_program,
            NULL::numeric AS prior_balance_sats,
            NULL::numeric AS expected_prior_balance_sats,
            NULL::bigint AS gross_amount_sats,
            NULL::numeric AS candidate_balance_sats,
            NULL::numeric AS expected_candidate_balance_sats,
            payout.onchain_amount_sats,
            NULL::bigint AS settlement_fee_sats,
            payout.carry_forward_balance_sats,
            NULL::numeric AS expected_carry_forward_balance_sats,
            payout.action,
            'payout_entry_unexpected'::text AS mismatch_reason
        FROM qbit_pool_payout_entries payout
        JOIN marked_blocks
          ON marked_blocks.block_hash = payout.block_hash
        WHERE NOT EXISTS (
            SELECT 1
            FROM manifest_accounts manifest
            WHERE manifest.block_hash = payout.block_hash
              AND manifest.miner_id = payout.miner_id
              AND manifest.payout_order_key = payout.payout_order_key
              AND manifest.p2mr_program IS NOT DISTINCT FROM payout.p2mr_program)
    ),
    every_mismatch AS (
        SELECT * FROM legacy_mismatch
        UNION ALL
        SELECT * FROM block_mismatch WHERE block_mismatch.mismatch_reason <> ''
        UNION ALL
        SELECT * FROM manifest_mismatch WHERE manifest_mismatch.mismatch_reason <> ''
        UNION ALL
        SELECT * FROM marked_mismatch WHERE marked_mismatch.mismatch_reason <> ''
        UNION ALL
        SELECT * FROM evidence_missing
        UNION ALL
        SELECT * FROM payout_mismatch
        UNION ALL
        SELECT * FROM payout_unexpected
    )
    SELECT *
    FROM every_mismatch
    ORDER BY every_mismatch.block_height ASC,
             every_mismatch.carry_forward_seq ASC NULLS LAST,
             every_mismatch.payout_order_key ASC,
             every_mismatch.miner_id ASC;
$$;

-- Declared last, once every object above exists: a binary that does not
-- know this capability refuses the database at connect (ledger/migration.rs,
-- NATIVE_CAPABILITIES), so no pre-011 frontend can claim a row this
-- lifecycle owns.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('candidate_offer_lifecycle', 1)
ON CONFLICT (capability) DO UPDATE
SET capability_value = EXCLUDED.capability_value,
    updated_at = clock_timestamp();
