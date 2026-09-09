-- Issue #255: chunked immutable block-candidate bodies.
--
-- Additive migration. Every statement is idempotent so the ledger can apply
-- it on each start after 001_share_ledger.sql, and nothing here rewrites or
-- drops the legacy ``candidate`` jsonb column: a v1 row stays readable by
-- the compatibility reader for as long as it is pending.
--
-- Layout:
--   qbit_block_candidate_body        one scalar manifest per body (staging/
--                                    sealed/retired), owned by the exact
--                                    producer session while staging;
--   qbit_block_candidate_body_chunk  fixed-offset byte chunks, at most
--                                    262144 bytes each, with a digest the
--                                    seal statement re-verifies server-side;
--   qbit_block_candidate_body_span   one row per large top-level field of
--                                    the body (byte span, kind, counts);
--   qbit_block_candidate_body_page   one row per bounded page of a large
--                                    array field (body offset, record index);
--   qbit_block_candidate_outbox      gains storage_version, body_id, a small
--                                    typed replay header and retired_body_id;
--   qbit_prism_schema_capabilities   what this schema declares, so a process
--                                    can refuse a database newer than itself.
--
-- Immutability is enforced here, not by caller discipline: the part tables
-- accept inserts only while their manifest is ``staging``, never accept
-- updates, and accept deletes only once the manifest is ``retired``; the
-- manifest itself may only move staging -> sealed, staging -> retired and
-- sealed -> retired, with every scalar frozen. The seal statement locks the
-- manifest row, so an upload racing a seal serializes behind it and is then
-- refused.
--
-- Rollback floor: once a storage_version = 2 outbox row exists, a writer
-- that predates this migration must not run. The terminal-state CHECK below
-- is the schema-level refusal for its terminal writes (a legacy writer never
-- detaches body_id, so it cannot abandon or submit a v2 row); it cannot stop
-- such a process from acquiring the lease and offering blocks before it
-- fails, which is why the compatibility reader ships before v2 writers and
-- why the capability row below lets every later process refuse a database
-- newer than itself at startup.

CREATE TABLE IF NOT EXISTS qbit_prism_schema_capabilities (
    capability text PRIMARY KEY,
    capability_value integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS qbit_block_candidate_body (
    body_id text PRIMARY KEY CHECK (body_id ~ '^[0-9a-f]{32}$'),
    storage_version integer NOT NULL CHECK (storage_version = 2),
    block_hash text NOT NULL,
    candidate_sha256 text NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    chunk_count integer NOT NULL CHECK (chunk_count >= 0),
    chunk_bytes integer NOT NULL CHECK (chunk_bytes > 0 AND chunk_bytes <= 262144),
    share_count bigint NOT NULL CHECK (share_count >= 0),
    shares_offset bigint NOT NULL CHECK (shares_offset >= 0),
    shares_end bigint NOT NULL CHECK (shares_end >= shares_offset),
    span_count integer NOT NULL DEFAULT 0 CHECK (span_count >= 0),
    page_count bigint NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    state text NOT NULL DEFAULT 'staging'
        CHECK (state IN ('staging', 'sealed', 'retired')),
    staging_writer_id text NOT NULL,
    staging_writer_epoch bigint NOT NULL CHECK (staging_writer_epoch >= 0),
    staging_session_token text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sealed_at timestamptz,
    retired_at timestamptz,
    CHECK (
        (state = 'staging' AND sealed_at IS NULL AND retired_at IS NULL)
        OR (state = 'sealed' AND sealed_at IS NOT NULL AND retired_at IS NULL)
        OR (state = 'retired' AND retired_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS qbit_block_candidate_body_orphan_idx
    ON qbit_block_candidate_body (created_at, body_id)
    WHERE state IN ('staging', 'sealed');

CREATE INDEX IF NOT EXISTS qbit_block_candidate_body_retired_idx
    ON qbit_block_candidate_body (retired_at, body_id)
    WHERE state = 'retired';

CREATE TABLE IF NOT EXISTS qbit_block_candidate_body_chunk (
    body_id text NOT NULL REFERENCES qbit_block_candidate_body(body_id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    chunk bytea NOT NULL
        CHECK (octet_length(chunk) > 0 AND octet_length(chunk) <= 262144),
    chunk_sha256 text NOT NULL CHECK (chunk_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (body_id, ordinal)
);

CREATE TABLE IF NOT EXISTS qbit_block_candidate_body_span (
    body_id text NOT NULL REFERENCES qbit_block_candidate_body(body_id),
    field text NOT NULL CHECK (octet_length(field) <= 256),
    kind text NOT NULL CHECK (kind IN ('array', 'string', 'value')),
    start_offset bigint NOT NULL CHECK (start_offset >= 0),
    end_offset bigint NOT NULL CHECK (end_offset >= start_offset),
    item_count bigint NOT NULL CHECK (item_count >= 0),
    page_count bigint NOT NULL CHECK (page_count >= 0),
    pages_exact boolean NOT NULL DEFAULT true,
    PRIMARY KEY (body_id, field)
);

CREATE TABLE IF NOT EXISTS qbit_block_candidate_body_page (
    body_id text NOT NULL REFERENCES qbit_block_candidate_body(body_id),
    field text NOT NULL CHECK (octet_length(field) <= 256),
    page_ordinal bigint NOT NULL CHECK (page_ordinal >= 0),
    body_offset bigint NOT NULL CHECK (body_offset >= 0),
    record_index bigint NOT NULL CHECK (record_index >= 0),
    PRIMARY KEY (body_id, field, page_ordinal)
);

-- Part tables (chunks, spans, pages): insert only while staging, no
-- updates, delete only once retired. FOR SHARE on the manifest serializes an
-- insert behind a concurrent seal, which then leaves the body sealed and
-- the insert refused.
CREATE OR REPLACE FUNCTION qbit_prism_candidate_body_part_guard() RETURNS trigger AS $$
DECLARE
    body_state text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'candidate body parts are immutable (%)', TG_TABLE_NAME
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT state INTO body_state
        FROM qbit_block_candidate_body
        WHERE body_id = NEW.body_id
        FOR SHARE;
        IF body_state IS DISTINCT FROM 'staging' THEN
            RAISE EXCEPTION 'candidate body % is % and accepts no parts', NEW.body_id, COALESCE(body_state, 'missing')
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
    END IF;
    SELECT state INTO body_state
    FROM qbit_block_candidate_body
    WHERE body_id = OLD.body_id;
    IF body_state IS DISTINCT FROM 'retired' THEN
        RAISE EXCEPTION 'candidate body % is % and its parts cannot be deleted', OLD.body_id, COALESCE(body_state, 'missing')
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN OLD;
END
$$ LANGUAGE plpgsql;

-- Manifest: staging -> sealed, staging -> retired, sealed -> retired only;
-- every scalar is frozen; a retired manifest can only be deleted.
CREATE OR REPLACE FUNCTION qbit_prism_candidate_body_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'retired' THEN
            RAISE EXCEPTION 'candidate body % is % and cannot be deleted', OLD.body_id, OLD.state
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN OLD;
    END IF;
    IF OLD.state = 'retired' THEN
        RAISE EXCEPTION 'candidate body % is retired', OLD.body_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.state = 'sealed' AND NEW.state <> 'retired' THEN
        RAISE EXCEPTION 'candidate body % is sealed', OLD.body_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    -- A body an outbox row references cannot be retired by anything but
    -- the fenced terminal write that detaches it first. Evaluated here,
    -- after any row-lock wait, with a fresh snapshot: a publication that
    -- attached the body while this retirement waited for its FOR UPDATE
    -- lock is visible now, and the retirement fails closed.
    IF NEW.state = 'retired' AND EXISTS (
        SELECT 1 FROM qbit_block_candidate_outbox outbox
        WHERE outbox.body_id = OLD.body_id
    ) THEN
        RAISE EXCEPTION 'candidate body % is referenced by a pending outbox row', OLD.body_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.state = 'sealed' AND OLD.state <> 'staging' THEN
        RAISE EXCEPTION 'candidate body % cannot be sealed from %', OLD.body_id, OLD.state
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.body_id <> OLD.body_id
       OR NEW.storage_version <> OLD.storage_version
       OR NEW.block_hash <> OLD.block_hash
       OR NEW.candidate_sha256 <> OLD.candidate_sha256
       OR NEW.byte_count <> OLD.byte_count
       OR NEW.chunk_count <> OLD.chunk_count
       OR NEW.chunk_bytes <> OLD.chunk_bytes
       OR NEW.share_count <> OLD.share_count
       OR NEW.shares_offset <> OLD.shares_offset
       OR NEW.shares_end <> OLD.shares_end
       OR NEW.span_count <> OLD.span_count
       OR NEW.page_count <> OLD.page_count
       OR NEW.staging_writer_id <> OLD.staging_writer_id
       OR NEW.staging_writer_epoch <> OLD.staging_writer_epoch
       OR NEW.staging_session_token <> OLD.staging_session_token
       OR NEW.created_at <> OLD.created_at
    THEN
        RAISE EXCEPTION 'candidate body % manifest is immutable', OLD.body_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS qbit_block_candidate_body_chunk_guard ON qbit_block_candidate_body_chunk;
CREATE TRIGGER qbit_block_candidate_body_chunk_guard
    BEFORE INSERT OR UPDATE OR DELETE ON qbit_block_candidate_body_chunk
    FOR EACH ROW EXECUTE FUNCTION qbit_prism_candidate_body_part_guard();

DROP TRIGGER IF EXISTS qbit_block_candidate_body_span_guard ON qbit_block_candidate_body_span;
CREATE TRIGGER qbit_block_candidate_body_span_guard
    BEFORE INSERT OR UPDATE OR DELETE ON qbit_block_candidate_body_span
    FOR EACH ROW EXECUTE FUNCTION qbit_prism_candidate_body_part_guard();

DROP TRIGGER IF EXISTS qbit_block_candidate_body_page_guard ON qbit_block_candidate_body_page;
CREATE TRIGGER qbit_block_candidate_body_page_guard
    BEFORE INSERT OR UPDATE OR DELETE ON qbit_block_candidate_body_page
    FOR EACH ROW EXECUTE FUNCTION qbit_prism_candidate_body_part_guard();

DROP TRIGGER IF EXISTS qbit_block_candidate_body_manifest_guard ON qbit_block_candidate_body;
CREATE TRIGGER qbit_block_candidate_body_manifest_guard
    BEFORE UPDATE OR DELETE ON qbit_block_candidate_body
    FOR EACH ROW EXECUTE FUNCTION qbit_prism_candidate_body_guard();

CREATE OR REPLACE FUNCTION qbit_prism_bounded_fact(value jsonb, expected text, byte_limit integer) RETURNS jsonb AS $$
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN jsonb_typeof(value) = expected AND octet_length(value::text) <= byte_limit THEN value
        ELSE NULL
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION qbit_prism_fact_oversized(value jsonb, expected text, byte_limit integer) RETURNS boolean AS $$
    SELECT value IS NOT NULL
        AND jsonb_typeof(value) <> 'null'
        AND NOT (jsonb_typeof(value) = expected AND octet_length(value::text) <= byte_limit);
$$ LANGUAGE sql IMMUTABLE;

-- Bounded replay header: an explicit projection of typed facts, each one
-- within a byte bound or replaced by null with ``oversized`` set. Mirrors
-- ``replay_header_from_fields`` in lab/prism/candidate_codec.py; a page of
-- these has a provable maximum size regardless of what the body holds.
CREATE OR REPLACE FUNCTION qbit_prism_bounded_replay_header(header jsonb) RETURNS jsonb AS $$
DECLARE
    text_limit constant integer := 256;
    number_limit constant integer := 128;
    hash_limit constant integer := 66;
    oversized boolean := false;
    pending jsonb := NULL;
    bounded_pending jsonb := NULL;
    entry record;
    template jsonb;
    found jsonb;
BEGIN
    IF header IS NULL OR jsonb_typeof(header) <> 'object' THEN
        RETURN jsonb_build_object('oversized', true);
    END IF;
    pending := header->'pending_share';
    IF pending IS NOT NULL AND jsonb_typeof(pending) = 'object' THEN
        bounded_pending := '{}'::jsonb;
        FOR entry IN SELECT key, value FROM jsonb_each(pending) LOOP
            IF octet_length(entry.key) > 64 THEN
                oversized := true;
                CONTINUE;
            END IF;
            IF jsonb_typeof(entry.value) = 'string' THEN
                IF octet_length(entry.value::text) <= text_limit THEN
                    bounded_pending := bounded_pending || jsonb_build_object(entry.key, entry.value);
                ELSE
                    oversized := true;
                    bounded_pending := bounded_pending || jsonb_build_object(entry.key, NULL);
                END IF;
            ELSIF jsonb_typeof(entry.value) = 'number' THEN
                IF octet_length(entry.value::text) <= number_limit THEN
                    bounded_pending := bounded_pending || jsonb_build_object(entry.key, entry.value);
                ELSE
                    oversized := true;
                    bounded_pending := bounded_pending || jsonb_build_object(entry.key, NULL);
                END IF;
            ELSIF jsonb_typeof(entry.value) IN ('null', 'boolean') THEN
                bounded_pending := bounded_pending || jsonb_build_object(entry.key, entry.value);
            ELSE
                oversized := true;
                bounded_pending := bounded_pending || jsonb_build_object(entry.key, NULL);
            END IF;
            IF (SELECT count(*) FROM jsonb_object_keys(bounded_pending)) > 32 THEN
                oversized := true;
                EXIT;
            END IF;
        END LOOP;
    ELSIF pending IS NOT NULL AND jsonb_typeof(pending) <> 'null' THEN
        oversized := true;
    END IF;
    template := header->'template';
    found := header->'found_block';
    RETURN jsonb_build_object(
        'schema', qbit_prism_bounded_fact(header->'schema', 'string', text_limit),
        'block_hash_hex', qbit_prism_bounded_fact(header->'block_hash_hex', 'string', hash_limit),
        'parent_hash', qbit_prism_bounded_fact(header->'parent_hash', 'string', hash_limit),
        'expected_height', qbit_prism_bounded_fact(header->'expected_height', 'number', number_limit),
        'template', CASE
            WHEN template IS NOT NULL AND jsonb_typeof(template) = 'object' THEN jsonb_build_object(
                'previousblockhash', qbit_prism_bounded_fact(template->'previousblockhash', 'string', hash_limit),
                'height', qbit_prism_bounded_fact(template->'height', 'number', number_limit),
                'coinbasevalue', qbit_prism_bounded_fact(template->'coinbasevalue', 'number', number_limit)
            )
            ELSE NULL
        END,
        'found_block', CASE
            WHEN found IS NOT NULL AND jsonb_typeof(found) = 'object' THEN jsonb_build_object(
                'network_difficulty', qbit_prism_bounded_fact(found->'network_difficulty', 'number', number_limit)
            )
            ELSE NULL
        END,
        'pending_share', bounded_pending,
        'credit_share_on_accept', CASE
            WHEN jsonb_typeof(header->'credit_share_on_accept') = 'boolean' THEN header->'credit_share_on_accept'
            ELSE NULL
        END,
        'collection_only', CASE
            WHEN jsonb_typeof(header->'collection_only') = 'boolean' THEN header->'collection_only'
            ELSE NULL
        END,
        'username', qbit_prism_bounded_fact(header->'username', 'string', text_limit),
        'accepted_at_present', COALESCE(
            CASE WHEN jsonb_typeof(header->'accepted_at_present') = 'boolean' THEN (header->>'accepted_at_present')::boolean END,
            pending IS NOT NULL AND jsonb_typeof(pending) = 'object' AND pending ? 'accepted_at_ms'
        ),
        'oversized', oversized
            OR qbit_prism_fact_oversized(header->'schema', 'string', text_limit)
            OR qbit_prism_fact_oversized(header->'block_hash_hex', 'string', hash_limit)
            OR qbit_prism_fact_oversized(header->'parent_hash', 'string', hash_limit)
            OR qbit_prism_fact_oversized(header->'expected_height', 'number', number_limit)
            OR qbit_prism_fact_oversized(header->'username', 'string', text_limit)
            OR (template IS NOT NULL AND jsonb_typeof(template) = 'object' AND (
                qbit_prism_fact_oversized(template->'previousblockhash', 'string', hash_limit)
                OR qbit_prism_fact_oversized(template->'height', 'number', number_limit)
                OR qbit_prism_fact_oversized(template->'coinbasevalue', 'number', number_limit)))
            OR (found IS NOT NULL AND jsonb_typeof(found) = 'object' AND
                qbit_prism_fact_oversized(found->'network_difficulty', 'number', number_limit))
    );
END
$$ LANGUAGE plpgsql IMMUTABLE;

ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS storage_version integer NOT NULL DEFAULT 1;
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS body_id text REFERENCES qbit_block_candidate_body(body_id);
-- Only pending v2 rows carry a body reference; the manifest trigger, the
-- orphan retirement and the manifest reads all ask "does any outbox row
-- reference this body", and none of them may scan the terminal history.
CREATE INDEX IF NOT EXISTS qbit_block_candidate_outbox_body_idx
    ON qbit_block_candidate_outbox (body_id)
    WHERE body_id IS NOT NULL;
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS retired_body_id text;
-- Small typed replay header: explicit projection written by the v2 writer.
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS replay_header jsonb;
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS parent_hash text;
ALTER TABLE qbit_block_candidate_outbox
    ADD COLUMN IF NOT EXISTS expected_height bigint;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'qbit_block_candidate_outbox'::regclass
          AND conname = 'qbit_block_candidate_outbox_storage_version_check'
    ) THEN
        ALTER TABLE qbit_block_candidate_outbox
            ADD CONSTRAINT qbit_block_candidate_outbox_storage_version_check
            CHECK (storage_version IN (1, 2));
    END IF;
END
$$;

-- The pending/terminal CHECK from 001 requires a pending row to carry the
-- legacy jsonb. Replace it with the dual-format rule: a pending v1 row
-- carries jsonb, a pending v2 row carries a body reference, and a terminal
-- row carries neither. Detaching the body at terminalization is what makes
-- the legacy writer's terminal UPDATE fail on a v2 row (it never clears
-- body_id), which is the version refusal the rollback floor relies on.
DO $$
DECLARE
    legacy_check text;
BEGIN
    SELECT conname INTO legacy_check
    FROM pg_constraint
    WHERE conrelid = 'qbit_block_candidate_outbox'::regclass
      AND contype = 'c'
      AND conname NOT IN (
          'qbit_block_candidate_outbox_storage_version_check',
          'qbit_block_candidate_outbox_candidate_sha256_check',
          'qbit_block_candidate_outbox_state_check',
          'qbit_block_candidate_outbox_attempt_count_check',
          'qbit_block_candidate_outbox_dual_format_check'
      )
      AND pg_get_constraintdef(oid) LIKE '%candidate IS NOT NULL%'
    LIMIT 1;
    IF legacy_check IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE qbit_block_candidate_outbox DROP CONSTRAINT %I',
            legacy_check
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'qbit_block_candidate_outbox'::regclass
          AND conname = 'qbit_block_candidate_outbox_dual_format_check'
    ) THEN
        ALTER TABLE qbit_block_candidate_outbox
            ADD CONSTRAINT qbit_block_candidate_outbox_dual_format_check
            CHECK (
                (
                    state = 'pending'
                    AND completed_at IS NULL
                    AND (
                        (storage_version = 1 AND candidate IS NOT NULL AND body_id IS NULL)
                        OR (storage_version = 2 AND candidate IS NULL AND body_id IS NOT NULL)
                    )
                )
                OR (
                    state IN ('submitted', 'abandoned')
                    AND completed_at IS NOT NULL
                    AND candidate IS NULL
                    AND body_id IS NULL
                )
            );
    END IF;
END
$$;

-- Declare the capability last, once every object above exists.
INSERT INTO qbit_prism_schema_capabilities (capability, capability_value)
VALUES ('candidate_storage_version', 2)
ON CONFLICT (capability) DO UPDATE
SET capability_value = GREATEST(qbit_prism_schema_capabilities.capability_value, EXCLUDED.capability_value),
    updated_at = clock_timestamp();
