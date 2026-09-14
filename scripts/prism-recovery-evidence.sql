-- Read-only reconciliation evidence shared by both migration runbooks.
-- Run with psql -XqAt -v ON_ERROR_STOP=1; feed stdout to
-- scripts/prism-recovery-evidence.py. Works on frozen 2.x and native schemas.
-- Writers must be stopped: the transaction gives this export one snapshot,
-- but cannot make separately taken backups or exports contemporaneous.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL bytea_output = 'hex';

-- Startup refuses a database at migration 6 whose capability declaration or
-- source record is missing, substituted or unreadable
-- (require_known_capabilities and require_migration_source). Refuse it here
-- too, so a restore that cannot start yields no evidence. Only the current
-- schema's own tables count: search_path must not resolve a lost table to
-- another ledger's. Frozen 2.x and pre-006 native schemas, whose #258
-- declaration of 2 remains valid, are not checked. Metadata is validated,
-- never exported: routine migration provenance must not change evidence.
DO $metadata$
DECLARE
    history regclass := to_regclass('qbit_prism_schema_migrations');
    hint constant text := 'Startup refuses this database. Restore the full backup, including the metadata tables of the current schema, then export again.';
    native boolean;
    metadata text;
    relation record;
    capability_table text;
    source_table text;
    capability record;
    declared boolean := false;
    source record;
    source_rows bigint;
BEGIN
    IF history IS NULL THEN
        RETURN;
    END IF;
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s WHERE version >= 6)', history) INTO native;
    IF NOT native THEN
        RETURN;
    END IF;
    FOREACH metadata IN ARRAY ARRAY['qbit_prism_schema_capabilities', 'qbit_prism_migration_source'] LOOP
        SELECT n.nspname IS NOT DISTINCT FROM current_schema() AS in_current_schema,
               format('%I.%I', n.nspname, c.relname) AS qualified, c.relkind::text AS kind,
               c.relrowsecurity OR c.relforcerowsecurity AS row_security
        INTO relation
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = to_regclass(metadata);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'database is at schema migration 6 but has no % in the current schema %', metadata, current_schema() USING HINT = hint;
        ELSIF NOT relation.in_current_schema THEN
            RAISE EXCEPTION '% resolves to %, outside the current schema %; refusing another schema''s metadata', metadata, relation.qualified, current_schema() USING HINT = hint;
        ELSIF relation.kind <> 'r' THEN
            RAISE EXCEPTION '% must be an ordinary table (found relation kind %)', metadata, relation.kind USING HINT = hint;
        ELSIF metadata = 'qbit_prism_schema_capabilities' AND relation.row_security THEN
            RAISE EXCEPTION 'qbit_prism_schema_capabilities has row-level security enabled or forced; refusing possibly hidden capability rows' USING HINT = hint;
        END IF;
        IF metadata = 'qbit_prism_schema_capabilities' THEN
            capability_table := relation.qualified;
        ELSE
            source_table := relation.qualified;
        END IF;
    END LOOP;
    -- Startup decodes capability as text and capability_value as int4, and
    -- understands candidate_storage_version 1 only once 006 has run.
    FOR capability IN EXECUTE format('SELECT capability, capability_value, pg_typeof(capability)::text AS name_type, pg_typeof(capability_value)::text AS value_type FROM %s', capability_table) LOOP
        IF capability.name_type <> 'text' OR capability.value_type <> 'integer'
           OR capability.capability IS NULL OR capability.capability_value IS NULL THEN
            RAISE EXCEPTION 'qbit_prism_schema_capabilities has an unreadable row: capability % (%), capability_value % (%)', capability.capability, capability.name_type, capability.capability_value, capability.value_type USING HINT = hint;
        ELSIF capability.capability <> 'candidate_storage_version' THEN
            RAISE EXCEPTION 'database declares capability % = %, which this server does not understand', capability.capability, capability.capability_value USING HINT = hint;
        ELSIF capability.capability_value <> 1 THEN
            RAISE EXCEPTION 'database declares candidate_storage_version = %, but this server understands candidate_storage_version 1 to 1 only', capability.capability_value USING HINT = hint;
        END IF;
        declared := true;
    END LOOP;
    IF NOT declared THEN
        RAISE EXCEPTION 'database is at schema migration 6 but qbit_prism_schema_capabilities has no candidate_storage_version row' USING HINT = hint;
    END IF;
    -- The singleton row startup decodes into MigrationSource.
    EXECUTE format('SELECT concat_ws('','', pg_typeof(source_state), pg_typeof(source_release), pg_typeof(source_commit), pg_typeof(candidate_storage_version), pg_typeof(prior_schema_version), pg_typeof(migrated_by), pg_typeof(migrated_at)) AS types, source_state IS NULL OR prior_schema_version IS NULL OR migrated_by IS NULL OR migrated_at IS NULL AS incomplete FROM %s WHERE singleton LIMIT 1', source_table) INTO source;
    GET DIAGNOSTICS source_rows = ROW_COUNT;
    IF source_rows = 0 THEN
        RAISE EXCEPTION 'database is at schema migration 6 but qbit_prism_migration_source has no singleton row' USING HINT = hint;
    ELSIF source.types <> 'text,text,text,integer,integer,text,timestamp with time zone' OR source.incomplete THEN
        RAISE EXCEPTION 'qbit_prism_migration_source has an unreadable singleton row (column types %, required value missing: %)', source.types, source.incomplete USING HINT = hint;
    END IF;
END
$metadata$;

SELECT jsonb_build_object('kind', 'shares', 'row', to_jsonb(s))
FROM qbit_share_ledger s ORDER BY share_seq;
-- Rows alone do not preserve the next allocation, including gaps left by
-- rolled-back writes. Read sequence state without consuming a value.
SELECT jsonb_build_object('kind', 'share_sequence', 'row', jsonb_build_object(
    'last_value', last_value, 'is_called', is_called))
FROM qbit_share_ledger_share_seq_seq;

SELECT jsonb_build_object('kind', 'blocks', 'row', jsonb_build_object(
    'block_hash', block_hash, 'block_height', block_height,
    'parent_hash', parent_hash, 'coinbase_txid', coinbase_txid,
    'payout_manifest_sha256', payout_manifest_sha256,
    'audit_publication_sequence', audit_publication_sequence,
    'chain_state', chain_state, 'maturity_state', maturity_state))
FROM qbit_pool_blocks ORDER BY block_hash COLLATE "C";

-- Import authenticates added bytes against the declared digest. Normalize
-- absent bytes to that digest so frozen, migrated and imported rows agree.
-- Hash bytea directly rather than copying potentially large bodies into JSON.
-- Import derives reader metadata from those bytes. Once they authenticate,
-- export the stored metadata only if it disagrees, so valid imports and
-- pre-import rows agree. Parse text json: bundles can exceed jsonb limits.
-- No bundle field determines bits, so every schema exports them as stored.
-- Native reconstruction inputs are fingerprinted separately below.
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_attribute
    WHERE attrelid = to_regclass('qbit_pool_audit_bundles')
      AND attname = 'canonical_audit_bytes' AND NOT attisdropped
) OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL AS has_canonical_audit_bytes
\gset
\if :has_canonical_audit_bytes
SELECT jsonb_build_object('kind', 'audits', 'row', jsonb_build_object(
    'block_hash', a.block_hash, 'audit_bundle_sha256', a.audit_bundle_sha256,
    'coinbase_tx_hex', a.coinbase_tx_hex, 'found_block_bits', a.found_block_bits,
    'canonical_audit_bytes_sha256', COALESCE(a.canonical_sha256, a.audit_bundle_sha256))
    || CASE WHEN a.canonical_sha256 IS DISTINCT FROM a.audit_bundle_sha256
        OR (a.schema_version, a.found_block_network_difficulty,
            a.found_block_coinbase_value_sats::numeric,
            a.audit_commitment_leaves_hex, a.witness_merkle_leaves_hex)
        IS NOT DISTINCT FROM (c.schema, (c.found_block->>'network_difficulty')::numeric,
            (c.found_block->>'coinbase_value_sats')::numeric,
            -- Canonical JSON omits empty leaf arrays; import stores [].
            COALESCE(c.audit_commitment_leaves_hex, '[]'),
            COALESCE(c.witness_merkle_leaves_hex, '[]'))
    THEN '{}'::jsonb ELSE jsonb_build_object('mismatched_metadata', jsonb_build_object(
        'schema_version', a.schema_version,
        'found_block_network_difficulty', a.found_block_network_difficulty,
        'found_block_coinbase_value_sats', a.found_block_coinbase_value_sats,
        'audit_commitment_leaves_hex', a.audit_commitment_leaves_hex,
        'witness_merkle_leaves_hex', a.witness_merkle_leaves_hex)) END)
FROM (
    SELECT block_hash, audit_bundle_sha256, coinbase_tx_hex, found_block_bits,
        schema_version, found_block_network_difficulty, found_block_coinbase_value_sats,
        audit_commitment_leaves_hex, witness_merkle_leaves_hex, canonical_audit_bytes,
        encode(pg_catalog.sha256(canonical_audit_bytes), 'hex') AS canonical_sha256
    FROM qbit_pool_audit_bundles OFFSET 0
) a
-- Decode only authenticated bytes; corrupt bytes can be invalid UTF-8.
LEFT JOIN LATERAL json_to_record(CASE WHEN a.canonical_sha256 = a.audit_bundle_sha256
    THEN convert_from(a.canonical_audit_bytes, 'UTF8')::json END)
    AS c(schema text, found_block json, audit_commitment_leaves_hex jsonb,
        witness_merkle_leaves_hex jsonb) ON true
ORDER BY a.block_hash COLLATE "C";
\else
SELECT jsonb_build_object('kind', 'audits', 'row', jsonb_build_object(
    'block_hash', block_hash, 'audit_bundle_sha256', audit_bundle_sha256,
    'coinbase_tx_hex', coinbase_tx_hex, 'found_block_bits', found_block_bits,
    'canonical_audit_bytes_sha256', audit_bundle_sha256))
FROM qbit_pool_audit_bundles ORDER BY block_hash COLLATE "C";
\endif

SELECT jsonb_build_object('kind', 'carry', 'row', to_jsonb(c))
FROM qbit_payout_carry_forward c ORDER BY carry_forward_seq;
SELECT jsonb_build_object('kind', 'payouts', 'row', to_jsonb(p))
FROM qbit_pool_payout_entries p ORDER BY payout_entry_seq;
SELECT jsonb_build_object('kind', 'candidates', 'row', jsonb_build_object(
    'block_hash', block_hash, 'share_id', share_id,
    'candidate_sha256', candidate_sha256, 'state', state,
    'candidate', candidate,
    'block_bytes', to_jsonb(o)->'block_bytes',
    'storage_version', COALESCE(to_jsonb(o)->'storage_version', '1'::jsonb)))
FROM qbit_block_candidate_outbox o ORDER BY block_hash COLLATE "C";
SELECT jsonb_build_object('kind', 'ctv_sets', 'row', jsonb_build_object(
    'block_hash', block_hash, 'manifest_set_sha256', manifest_set_sha256,
    'manifest_set_json', manifest_set_json, 'manifest_set', manifest_set,
    'settlement_mode', settlement_mode,
    'parent_coinbase_txid', parent_coinbase_txid, 'parent_coinbase_tx_hex', parent_coinbase_tx_hex,
    'fanout_count', fanout_count, 'fanout_output_sum_sats', fanout_output_sum_sats,
    'covenant_output_value_sats', covenant_output_value_sats))
FROM qbit_ctv_fanout_sets ORDER BY block_hash COLLATE "C";
-- Keep the immutable payout payload separate from native progress below.
-- Ephemeral claim ownership is not part of recovery evidence.
SELECT jsonb_build_object('kind', 'ctv_artifacts', 'row', jsonb_build_object(
    'fanout_txid', fanout_txid, 'block_hash', block_hash,
    'manifest_set_sha256', manifest_set_sha256,
    'manifest_json', manifest_json, 'manifest', manifest,
    'manifest_sha256', manifest_sha256, 'precommitment_sha256', precommitment_sha256,
    'ctv_hash', ctv_hash, 'commitment_witness_leaf_hex', commitment_witness_leaf_hex,
    'chunk_index', chunk_index, 'chunk_count', chunk_count,
    'parent_coinbase_txid', parent_coinbase_txid, 'parent_coinbase_vout', parent_coinbase_vout,
    'fanout_tx_template_hex', fanout_tx_template_hex, 'fanout_tx_hex', fanout_tx_hex,
    'anchor_vout', anchor_vout, 'covenant_output_value_sats', covenant_output_value_sats,
    'fanout_output_sum_sats', fanout_output_sum_sats,
    'settlement_status', settlement_status))
FROM qbit_ctv_fanout_artifacts ORDER BY fanout_txid COLLATE "C";

-- Missing legacy columns and freshly migrated NULL/zero checkpoints are
-- equivalent. Any native confirmation or spend-scan progress must survive
-- recovery, even when settlement_status does not change.
SELECT jsonb_build_object('kind', 'ctv_checkpoints', 'row',
    checkpoint || jsonb_build_object('fanout_txid', fanout_txid))
FROM (
    SELECT fanout_txid, jsonb_build_object(
        'confirmed_block_hash', to_jsonb(a)->'confirmed_block_hash',
        'confirmed_block_height', to_jsonb(a)->'confirmed_block_height',
        'confirmed_depth', COALESCE(to_jsonb(a)->'confirmed_depth', '0'::jsonb),
        'spend_scan_next_height', to_jsonb(a)->'spend_scan_next_height',
        'spend_scan_anchor_height', to_jsonb(a)->'spend_scan_anchor_height',
        'spend_scan_anchor_hash', to_jsonb(a)->'spend_scan_anchor_hash') AS checkpoint
    FROM qbit_ctv_fanout_artifacts a
) progress
WHERE checkpoint <> '{"confirmed_block_hash":null,"confirmed_block_height":null,"confirmed_depth":0,"spend_scan_next_height":null,"spend_scan_anchor_height":null,"spend_scan_anchor_hash":null}'::jsonb
ORDER BY fanout_txid COLLATE "C";

SELECT jsonb_build_object('kind', 'ctv_broadcast_attempts', 'row', to_jsonb(a))
FROM qbit_ctv_fanout_broadcast_attempts a ORDER BY attempt_seq;

-- Frozen 2.x lacks these native tables. Skip absent tables before parsing
-- their queries only without native migration history. A native marker
-- requires the complete native evidence tables; loss must fail the export.
-- Empty native tables still hash identically to frozen 2.x.
SELECT (to_regclass('qbit_prism_cpfp_packages') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_cpfp_packages,
       (to_regclass('qbit_prism_cpfp_retired_funding') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_cpfp_retired_funding,
       (to_regclass('qbit_prism_deferred_shares') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_deferred_shares,
       (to_regclass('qbit_prism_audit_snapshots') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_audit_snapshots,
       (to_regclass('qbit_prism_share_hashes') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_native_share_hashes,
       (to_regclass('qbit_prism_cluster') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_cluster,
       (to_regclass('qbit_prism_fatal_state_events') IS NOT NULL
        OR to_regclass('qbit_prism_schema_migrations') IS NOT NULL) AS has_fatal_state_events
\gset
-- Native replay protection must survive recovery. Frozen 2.x exports the
-- exact mapping migration 002 will backfill, including its duplicate rule.
-- Native migration history prevents a missing table from being synthesized.
\if :has_native_share_hashes
SELECT jsonb_build_object('kind', 'share_hashes', 'row', jsonb_build_object(
    'header_hash', header_hash, 'share_id', share_id))
FROM qbit_prism_share_hashes ORDER BY header_hash COLLATE "C";
\else
SELECT jsonb_build_object('kind', 'share_hashes', 'row', jsonb_build_object(
    'header_hash', header_hash, 'share_id', share_id))
FROM (
    SELECT DISTINCT ON (lower(right(share_id,64)))
        lower(right(share_id,64)) AS header_hash, share_id
    FROM qbit_share_ledger
    WHERE accepted AND share_id ~ '[0-9a-fA-F]{64}$'
    ORDER BY lower(right(share_id,64)), share_seq
) legacy_hashes ORDER BY header_hash COLLATE "C";
\endif
\if :has_audit_snapshots
-- Imported legacy rows gain canonical bytes and normalized metadata, but no
-- snapshot reference. Fingerprint native reconstruction inputs separately.
SELECT jsonb_build_object('kind', 'audit_bodies', 'row', jsonb_build_object(
    'block_hash', block_hash, 'audit_bundle', audit_bundle,
    'share_snapshot_sha256', share_snapshot_sha256))
FROM qbit_pool_audit_bundles WHERE share_snapshot_sha256 IS NOT NULL
ORDER BY block_hash COLLATE "C";

SELECT jsonb_build_object('kind', 'audit_snapshots', 'row', jsonb_build_object(
    'snapshot_sha256', snapshot_sha256,
    'first_share_seq', first_share_seq, 'last_share_seq', last_share_seq,
    'anchor_ms', anchor_ms, 'share_count', share_count, 'inline_shares', inline_shares))
FROM qbit_prism_audit_snapshots ORDER BY snapshot_sha256 COLLATE "C";
\endif
\if :has_cpfp_packages
SELECT jsonb_build_object('kind', 'cpfp_packages', 'row', to_jsonb(p))
FROM qbit_prism_cpfp_packages p ORDER BY fanout_txid COLLATE "C";
\endif
\if :has_cpfp_retired_funding
SELECT jsonb_build_object('kind', 'cpfp_retired_funding', 'row', to_jsonb(r))
FROM qbit_prism_cpfp_retired_funding r
ORDER BY funding_txid COLLATE "C", funding_vout;
\endif
\if :has_deferred_shares
SELECT jsonb_build_object('kind', 'deferred_shares', 'row', to_jsonb(d))
FROM qbit_prism_deferred_shares d ORDER BY block_hash COLLATE "C";
\endif
\if :has_cluster
-- A fresh migration has no halt. Exclude routine cluster metadata so that
-- migrating an unchanged legacy backup still produces identical evidence.
-- Historical halts can have an unknown set time; retain that NULL value.
SELECT jsonb_build_object('kind', 'fatal_state', 'row', jsonb_build_object(
    'fatal_error', fatal_error, 'fatal_error_set_at', to_jsonb(c)->'fatal_error_set_at'))
FROM qbit_prism_cluster c WHERE fatal_error IS NOT NULL ORDER BY singleton;
\endif
\if :has_fatal_state_events
SELECT jsonb_build_object('kind', 'fatal_state_events', 'row', to_jsonb(e))
FROM qbit_prism_fatal_state_events e ORDER BY event_id;
\endif

-- Exact row shape/order used by 2.x _carry_forward_audit_head_locked.
-- Stream rows rather than constructing one unbounded json_agg value.
SELECT jsonb_build_object('kind', 'active_carry', 'row', jsonb_build_object(
    'carry_forward_seq', ledger.carry_forward_seq,
    'block_hash', ledger.block_hash, 'block_height', ledger.block_height,
    'recipient_id', ledger.miner_id, 'order_key', ledger.payout_order_key,
    'p2mr_program_hex', encode(ledger.p2mr_program, 'hex'),
    'gross_amount_sats', ledger.gross_amount_sats,
    'prior_balance_sats', ledger.prior_balance_sats::text,
    'candidate_balance_sats', ledger.candidate_balance_sats::text,
    'onchain_amount_sats', ledger.onchain_amount_sats,
    'settlement_fee_sats', ledger.settlement_fee_sats,
    'carry_forward_balance_sats', ledger.carry_forward_balance_sats::text,
    'action', ledger.action, 'maturity_state', ledger.maturity_state))
FROM qbit_payout_carry_forward ledger
JOIN qbit_pool_blocks block ON block.block_hash = ledger.block_hash
WHERE ledger.maturity_state <> 'reversed' AND block.chain_state = 'confirmed'
  AND block.maturity_state <> 'reversed'
ORDER BY ledger.block_height, ledger.carry_forward_seq;

SELECT jsonb_build_object('kind', 'integrity', 'row', qbit_carry_forward_integrity_report());
-- A final marker makes an interrupted/failed psql export fail closed.
SELECT jsonb_build_object('kind', 'complete', 'row', true);
COMMIT;
