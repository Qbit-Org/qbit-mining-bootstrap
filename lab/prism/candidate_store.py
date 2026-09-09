#!/usr/bin/env python3
"""Chunked candidate body storage: statements, hydration, legacy lane, janitor.

Issue #255. The ledger's candidate rows used to carry the whole intent as
one ``jsonb`` value, so every durable write inlined a payout-window-sized
literal, every replay page ``json_agg``-ed complete candidates, and psycopg
decoded them in one GIL-held call. This module owns the storage side of the
bounded replacement:

* **Staging.** A body is uploaded as fixed byte chunks into
  ``qbit_block_candidate_body_chunk`` under a manifest row whose owner is
  the exact producer session, with its page index in
  ``qbit_block_candidate_body_span``/``_page`` written in bounded batches.
  Staging grants nothing: no share credit, no replay eligibility, no
  lease-row touch. The seal statement locks the manifest row, proves
  completeness from the chunk and index rows themselves and re-hashes
  every chunk on the server; from then on the database's own triggers
  refuse any chunk insert, update or delete until authoritative
  retirement.
* **Publication.** The ledger's fenced statements (share append, standalone
  intent persist) reference a sealed manifest by id and compare-and-swap on
  the outbox row's observed version; the exact-body precomparison runs
  here, outside that critical section, as a paged server-side chunk digest
  check.
* **Metadata-first replay.** ``header_page_sql`` projects only a bounded
  typed summary per row (``qbit_prism_bounded_replay_header``), at most
  :data:`HEADER_PAGE_MAX_ROWS` rows and :data:`HEADER_PAGE_MAX_BYTES` per
  page, and answers exhaustion explicitly.
* **Hydration.** :class:`CandidateBodyHydrator` reads the scalar manifest,
  the span rows and the page rows in bounded pages (the page index goes
  straight to disk), then at most :data:`BODY_READ_MAX_CHUNKS` chunks per
  statement, verifying state, digests and a rolling body digest as it
  goes. A body that turns terminal or corrupt mid-read is discarded.
* **Legacy lane.** Pending v1 ``jsonb`` rows are converted by an isolated
  helper process (``python -m lab.prism.candidate_store``) that owns its
  own database connection, cannot take the writer lease, is memory- and
  deadline-limited, and hands back a spool/index pair plus a scalar
  manifest, never a Python object graph.
* **Janitor.** Two separate statements: a tiny fenced compare-and-swap that
  retires orphaned staging/sealed bodies (``retire_orphan_bodies_sql``),
  and a lease-free bounded deletion of one retired body's chunks a page at
  a time (``reap_retired_chunks_sql``). ``retired`` is permanent, and a
  publication can only reference a ``sealed`` body, so nothing revives a
  body once it is reapable.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lab.prism.candidate_codec import (
    CANDIDATE_BODY_CHUNK_BYTES,
    CANDIDATE_BODY_STORAGE_VERSION,
    INDEX_PAGE_ENTRIES,
    INDEX_SPAN_ROWS,
    LEGACY_CANDIDATE_STORAGE_VERSION,
    BodyChunk,
    CandidateBody,
    CandidateBodyIntegrityError,
    CandidateBodyManifest,
    FieldSpan,
    PageEntry,
    PreparedCandidateIntent,
    SpoolWriter,
    jsonb_equivalent,
    legacy_json_to_spool,
    prepared_intent_from_spool,
)

# Chunks per body read statement; three 256 KiB chunks base64-encode to
# exactly 1 MiB of returned text, which is the transport ceiling.
BODY_READ_MAX_CHUNKS = 3
BODY_READ_MAX_BYTES = 1024 * 1024
# Base64 text for one chunk: 4/3 of the bytes plus one newline per 76 chars.
CHUNK_BASE64_MAX_CHARS = ((CANDIDATE_BODY_CHUNK_BYTES + 2) // 3) * 4 + CANDIDATE_BODY_CHUNK_BYTES // 57 + 2
# Header enumeration page: rows and running header bytes. The byte cap is
# the bound that matters for the native decoder (every header is a bounded
# projection, so a 256 KiB page holds a few hundred of them at most); the
# row cap matches the configured replay page maximum rather than a smaller
# number, because every page costs the collapse walk two chain tip reads and
# a probe budget, and a 64-row cap multiplied a storm's chain reads by
# sixteen in the existing cost contracts.
HEADER_PAGE_MAX_ROWS = 1024
HEADER_PAGE_MAX_BYTES = 256 * 1024
# Per-header ceiling enforced on read; the bounded projection is far below.
HEADER_MAX_BYTES = 8 * 1024
# Local spool admission for hydrated bodies (operator-reviewed reservation).
DEFAULT_SPOOL_RESERVATION_BYTES = 4 * 1024 * 1024 * 1024
# Chunk deletions per janitor statement and the age after which an
# unreferenced staging/sealed body is an orphan.
JANITOR_CHUNKS_PER_STEP = 64
STALE_STAGING_SECONDS = 15 * 60
# Legacy helper process limits.
LEGACY_HELPER_MEMORY_BYTES = 4 * 1024 * 1024 * 1024
LEGACY_HELPER_TIMEOUT_SECONDS = 600.0
LEGACY_HELPER_POLL_SECONDS = 0.2
# The schema capability this code understands; a database that declares a
# higher one is newer than this process and must not be written by it.
CANDIDATE_SCHEMA_CAPABILITY = "candidate_storage_version"

CHUNK_LITERAL_TAG = "qbit_prism_chunk"


class CandidateStorageError(RuntimeError):
    """A storage operation failed in a way that is not a transport retry."""


class CandidateBodyUnavailable(CandidateStorageError):
    """The body no longer exists as a sealed, referenced manifest."""


class SpoolAdmissionExhausted(CandidateStorageError):
    """Local spool reservation would be exceeded; backpressure, not loss."""


class IncompatibleCandidateSchema(CandidateStorageError):
    """The database declares candidate storage newer than this process."""


def new_body_id() -> str:
    return uuid.uuid4().hex


# --------------------------------------------------------------------------
# typed rows
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CandidateBodyRef:
    """Small reference to a stored body, carried by a header row."""

    body_id: str
    storage_version: int
    candidate_sha256: str
    byte_count: int
    chunk_count: int
    chunk_bytes: int
    share_count: int

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> CandidateBodyRef:
        body_id = row.get("body_id")
        if not isinstance(body_id, str) or len(body_id) != 32:
            raise ValueError("candidate body reference carries no body id")
        return cls(
            body_id=body_id,
            storage_version=int(row.get("storage_version", CANDIDATE_BODY_STORAGE_VERSION)),
            candidate_sha256=str(row["candidate_sha256"]),
            byte_count=int(row["byte_count"]),
            chunk_count=int(row["chunk_count"]),
            chunk_bytes=int(row["chunk_bytes"]),
            share_count=int(row["share_count"]),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "body_id": self.body_id,
            "storage_version": self.storage_version,
            "candidate_sha256": self.candidate_sha256,
            "byte_count": self.byte_count,
            "chunk_count": self.chunk_count,
            "chunk_bytes": self.chunk_bytes,
            "share_count": self.share_count,
        }


@dataclass(frozen=True)
class CandidateHeaderPage:
    """One metadata-first replay page with an explicit completeness fact."""

    rows: tuple[dict[str, Any], ...]
    next_cursor: Any
    exhausted: bool
    fetched: int
    truncated_by_bytes: bool

    def __iter__(self):  # noqa: ANN204 - Sequence-like convenience
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> dict[str, Any]:
        return self.rows[index]


@dataclass(frozen=True)
class DurableCandidateDescriptor:
    """Everything replay needs about a pending row before its body exists.

    Built from one header row after stale collapse and strict typing. The
    descriptor is what the bounded replay window holds; the body is
    hydrated only when the descriptor is dequeued.
    """

    block_hash: str
    block_height: int
    parent_hash: str
    candidate_sha256: str
    storage_version: int
    credit_share_on_accept: bool
    collection_only: bool
    username: str
    pending_share: dict[str, Any]
    accepted_at_present: bool
    accepted_at_ms: Any
    body: CandidateBodyRef | None
    cursor: Any
    # The header row this descriptor was built from, handed back to the
    # ledger at hydration time (it carries the body reference and the
    # original pending stamp).
    row: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)
    header_oversized: bool = False
    attempts: int = 0
    not_before_monotonic: float = 0.0

    @property
    def submission(self) -> Any:
        """Duck-typed hash carrier for registries that read ``.submission``."""
        from types import SimpleNamespace

        return SimpleNamespace(block_hash_hex=self.block_hash)

    @property
    def body_key(self) -> str:
        return self.body.body_id if self.body is not None else "legacy"


# --------------------------------------------------------------------------
# statement builders
# --------------------------------------------------------------------------

Literal = Callable[[str], str]
JsonbLiteral = Callable[[object], str]


def chunk_hex_literal(data: bytes) -> str:
    return f"decode(${CHUNK_LITERAL_TAG}${data.hex()}${CHUNK_LITERAL_TAG}$, 'hex')"


def schema_capability_sql() -> str:
    """Read the candidate storage version the schema declares."""
    return f"""
SELECT json_build_object(
    'declared', (
        SELECT capability_value
        FROM qbit_prism_schema_capabilities
        WHERE capability = '{CANDIDATE_SCHEMA_CAPABILITY}'
    )
);
"""


def stage_body_sql(
    payload: Mapping[str, Any],
    *,
    jsonb: JsonbLiteral,
) -> str:
    """Create one staging manifest; idempotent on ``body_id``.

    No lease CTE: staging is private preparation and must not touch the
    lease row. The producer identity is recorded so chunk uploads and the
    seal can require the same session, and so an orphan can be identified
    by the janitor.
    """
    return f"""
WITH input AS (
    SELECT {jsonb(payload)} AS data
),
inserted AS (
    INSERT INTO qbit_block_candidate_body (
        body_id, storage_version, block_hash, candidate_sha256,
        byte_count, chunk_count, chunk_bytes, share_count,
        shares_offset, shares_end, span_count, page_count,
        staging_writer_id, staging_writer_epoch, staging_session_token
    )
    SELECT
        data->>'body_id', (data->>'storage_version')::integer,
        data->>'block_hash', data->>'candidate_sha256',
        (data->>'byte_count')::bigint, (data->>'chunk_count')::integer,
        (data->>'chunk_bytes')::integer, (data->>'share_count')::bigint,
        (data->>'shares_offset')::bigint, (data->>'shares_end')::bigint,
        (data->>'span_count')::integer, (data->>'page_count')::bigint,
        data->>'writer_id', (data->>'writer_epoch')::bigint,
        data->>'writer_session_token'
    FROM input
    ON CONFLICT (body_id) DO NOTHING
    RETURNING body_id
)
SELECT json_build_object(
    'staged', (SELECT count(*) FROM inserted),
    -- A row this statement inserted is invisible to its own final SELECT
    -- (one snapshot per statement), so the fresh case is answered from the
    -- RETURNING CTE.
    'state', COALESCE(
        (
            SELECT state FROM qbit_block_candidate_body body, input
            WHERE body.body_id = input.data->>'body_id'
        ),
        CASE WHEN EXISTS (SELECT 1 FROM inserted) THEN 'staging' END
    )
);
"""


def _owner_cte(alias: str = "owner") -> str:
    return f"""
{alias} AS (
    SELECT body.body_id
    FROM qbit_block_candidate_body body, input
    WHERE body.body_id = input.data->>'body_id'
      AND body.state = 'staging'
      AND body.staging_writer_id = input.data->>'writer_id'
      AND body.staging_writer_epoch = (input.data->>'writer_epoch')::bigint
      AND body.staging_session_token = input.data->>'writer_session_token'
)"""


def body_chunk_sql(
    payload: Mapping[str, Any],
    data: bytes,
    *,
    jsonb: JsonbLiteral,
) -> str:
    """Upload one chunk; idempotent on ``(body_id, ordinal)``.

    The insert is guarded by the manifest still being in ``staging`` under
    the uploading session (and, independently, by the chunk table's
    trigger), so a sealed or retired body can never gain a chunk and a
    foreign session cannot fill someone else's staging.
    """
    return f"""
WITH input AS (
    SELECT {jsonb(payload)} AS data
),{_owner_cte()},
inserted AS (
    INSERT INTO qbit_block_candidate_body_chunk (body_id, ordinal, chunk, chunk_sha256)
    SELECT
        owner.body_id, (input.data->>'ordinal')::integer,
        {chunk_hex_literal(data)}, input.data->>'chunk_sha256'
    FROM owner, input
    ON CONFLICT (body_id, ordinal) DO NOTHING
    RETURNING ordinal
)
SELECT json_build_object(
    'owned', EXISTS (SELECT 1 FROM owner),
    'inserted', (SELECT count(*) FROM inserted)
);
"""


def body_index_rows_sql(
    payload: Mapping[str, Any],
    *,
    jsonb: JsonbLiteral,
) -> str:
    """Upload one bounded batch of span rows and/or page rows.

    ``payload['spans']`` holds at most :data:`INDEX_SPAN_ROWS` span objects
    and ``payload['pages']`` at most :data:`INDEX_PAGE_ENTRIES` page arrays
    ``[field, ordinal, offset, record_index]``. Idempotent on the primary
    keys, and owner-guarded exactly like a chunk upload.
    """
    return f"""
WITH input AS (
    SELECT {jsonb(payload)} AS data
),{_owner_cte()},
inserted_spans AS (
    INSERT INTO qbit_block_candidate_body_span (
        body_id, field, kind, start_offset, end_offset, item_count, page_count, pages_exact
    )
    SELECT
        owner.body_id, span->>'field', span->>'kind',
        (span->>'start')::bigint, (span->>'end')::bigint,
        (span->>'item_count')::bigint, (span->>'page_count')::bigint,
        (span->>'pages_exact')::boolean
    FROM owner, input, jsonb_array_elements(COALESCE(input.data->'spans', '[]'::jsonb)) AS span
    ON CONFLICT (body_id, field) DO NOTHING
    RETURNING field
),
inserted_pages AS (
    INSERT INTO qbit_block_candidate_body_page (
        body_id, field, page_ordinal, body_offset, record_index
    )
    SELECT
        owner.body_id, page->>0, (page->>1)::bigint, (page->>2)::bigint, (page->>3)::bigint
    FROM owner, input, jsonb_array_elements(COALESCE(input.data->'pages', '[]'::jsonb)) AS page
    ON CONFLICT (body_id, field, page_ordinal) DO NOTHING
    RETURNING page_ordinal
)
SELECT json_build_object(
    'owned', EXISTS (SELECT 1 FROM owner),
    'spans', (SELECT count(*) FROM inserted_spans),
    'pages', (SELECT count(*) FROM inserted_pages)
);
"""


def seal_body_sql(
    payload: Mapping[str, Any],
    *,
    jsonb: JsonbLiteral,
) -> str:
    """Seal a complete, verified staging body under its producer session.

    The manifest row is locked (``FOR UPDATE``) so a concurrent chunk or
    index upload serializes behind the seal and then finds the body sealed
    (its trigger refuses the insert). Completeness is proven from the rows
    themselves (chunk count, byte sum, ordinal range, span and page counts)
    and integrity by re-hashing every stored chunk on the server against
    the digest the uploader recorded. Nothing here trusts a client-side
    upload count.
    """
    return f"""
WITH input AS (
    SELECT {jsonb(payload)} AS data
),
target AS (
    SELECT body.*
    FROM qbit_block_candidate_body body, input
    WHERE body.body_id = input.data->>'body_id'
    FOR UPDATE
),
observed AS (
    SELECT
        count(*) AS chunk_count,
        COALESCE(sum(octet_length(chunk)), 0) AS byte_count,
        COALESCE(min(ordinal), 0) AS min_ordinal,
        COALESCE(max(ordinal), -1) AS max_ordinal,
        COALESCE(bool_and(sha256(chunk) = decode(chunk_sha256, 'hex')), true) AS digests_ok,
        COALESCE(bool_and(
            octet_length(chunk) = (SELECT chunk_bytes FROM target)
            OR ordinal = (SELECT chunk_count FROM target) - 1
        ), true) AS lengths_ok
    FROM qbit_block_candidate_body_chunk chunk
    WHERE chunk.body_id = (SELECT body_id FROM target)
),
observed_index AS (
    SELECT
        (SELECT count(*) FROM qbit_block_candidate_body_span span
         WHERE span.body_id = (SELECT body_id FROM target)) AS span_count,
        (SELECT count(*) FROM qbit_block_candidate_body_page page
         WHERE page.body_id = (SELECT body_id FROM target)) AS page_count
),
complete AS (
    SELECT target.body_id
    FROM target, observed, observed_index, input
    WHERE target.state = 'staging'
      AND target.staging_writer_id = input.data->>'writer_id'
      AND target.staging_writer_epoch = (input.data->>'writer_epoch')::bigint
      AND target.staging_session_token = input.data->>'writer_session_token'
      AND target.candidate_sha256 = input.data->>'candidate_sha256'
      AND observed.chunk_count = target.chunk_count
      AND observed.byte_count = target.byte_count
      AND (target.chunk_count = 0 OR observed.min_ordinal = 0)
      AND observed.max_ordinal = target.chunk_count - 1
      AND observed.digests_ok
      AND observed.lengths_ok
      AND observed_index.span_count = target.span_count
      AND observed_index.page_count = target.page_count
),
sealed AS (
    UPDATE qbit_block_candidate_body
    SET state = 'sealed', sealed_at = clock_timestamp()
    FROM complete
    WHERE qbit_block_candidate_body.body_id = complete.body_id
    RETURNING qbit_block_candidate_body.body_id
)
SELECT json_build_object(
    'sealed', (SELECT count(*) FROM sealed),
    'state', (SELECT state FROM target),
    'observed_chunk_count', (SELECT chunk_count FROM observed),
    'observed_byte_count', (SELECT byte_count FROM observed),
    'observed_span_count', (SELECT span_count FROM observed_index),
    'observed_page_count', (SELECT page_count FROM observed_index),
    'digests_ok', (SELECT digests_ok FROM observed),
    'lengths_ok', (SELECT lengths_ok FROM observed)
);
"""


def retire_staging_body_sql(body_id: str, *, text: Literal) -> str:
    """Hand a failed staging body to the janitor (no lease; own body only)."""
    return f"""
WITH retired AS (
    UPDATE qbit_block_candidate_body
    SET state = 'retired', retired_at = clock_timestamp()
    WHERE body_id = {text(body_id)}
      AND state = 'staging'
    RETURNING body_id
)
SELECT json_build_object('retired', (SELECT count(*) FROM retired));
"""


def body_manifest_sql(body_id: str, *, text: Literal) -> str:
    """Read one scalar manifest with its live state and reference fact."""
    return f"""
SELECT json_build_object(
    'found', EXISTS (SELECT 1 FROM qbit_block_candidate_body WHERE body_id = {text(body_id)}),
    'manifest', (
        SELECT json_build_object(
            'body_id', body_id,
            'storage_version', storage_version,
            'block_hash', block_hash,
            'candidate_sha256', candidate_sha256,
            'byte_count', byte_count,
            'chunk_count', chunk_count,
            'chunk_bytes', chunk_bytes,
            'share_count', share_count,
            'shares_offset', shares_offset,
            'shares_end', shares_end,
            'span_count', span_count,
            'page_count', page_count,
            'state', state,
            'referenced', EXISTS (
                SELECT 1 FROM qbit_block_candidate_outbox outbox
                WHERE outbox.body_id = body.body_id AND outbox.state = 'pending'
            )
        )
        FROM qbit_block_candidate_body body
        WHERE body_id = {text(body_id)}
    )
);
"""


def body_spans_sql(body_id: str, after_field: str | None, *, text: Literal) -> str:
    """Read at most :data:`INDEX_SPAN_ROWS` span rows, keyset by field name."""
    after = "" if after_field is None else f"\n      AND field > {text(after_field)}"
    return f"""
SELECT json_build_object(
    'state', (SELECT state FROM qbit_block_candidate_body WHERE body_id = {text(body_id)}),
    'spans', (
        SELECT COALESCE(json_agg(json_build_object(
            'field', span.field, 'kind', span.kind,
            'start', span.start_offset, 'end', span.end_offset,
            'item_count', span.item_count, 'page_count', span.page_count,
            'pages_exact', span.pages_exact
        ) ORDER BY span.field), '[]'::json)
        FROM (
            SELECT * FROM qbit_block_candidate_body_span
            WHERE body_id = {text(body_id)}{after}
            ORDER BY field
            LIMIT {INDEX_SPAN_ROWS}
        ) span
    )
);
"""


def body_pages_sql(body_id: str, field_name: str, from_ordinal: int, *, text: Literal) -> str:
    """Read at most :data:`INDEX_PAGE_ENTRIES` page rows of one field."""
    return f"""
SELECT json_build_object(
    'state', (SELECT state FROM qbit_block_candidate_body WHERE body_id = {text(body_id)}),
    'pages', (
        SELECT COALESCE(json_agg(json_build_array(
            page.page_ordinal, page.body_offset, page.record_index
        ) ORDER BY page.page_ordinal), '[]'::json)
        FROM (
            SELECT * FROM qbit_block_candidate_body_page
            WHERE body_id = {text(body_id)}
              AND field = {text(field_name)}
              AND page_ordinal >= {int(from_ordinal)}
            ORDER BY page_ordinal
            LIMIT {INDEX_PAGE_ENTRIES}
        ) page
    )
);
"""


def body_page_sql(
    body_id: str,
    from_ordinal: int,
    max_chunks: int,
    *,
    text: Literal,
) -> str:
    """Read at most ``max_chunks`` consecutive chunks of one sealed body.

    Bounded by the SELECT itself (``LIMIT``), so the PGresult is bounded
    too -- not merely the client's fetch size. Base64 keeps the transport
    text at four thirds of the raw bytes.
    """
    max_chunks = max(1, min(int(max_chunks), BODY_READ_MAX_CHUNKS))
    return f"""
SELECT json_build_object(
    'state', (SELECT state FROM qbit_block_candidate_body WHERE body_id = {text(body_id)}),
    'referenced', EXISTS (
        SELECT 1 FROM qbit_block_candidate_outbox outbox
        WHERE outbox.body_id = {text(body_id)} AND outbox.state = 'pending'
    ),
    'chunks', (
        SELECT COALESCE(json_agg(json_build_object(
            'ordinal', page.ordinal,
            'sha256', page.chunk_sha256,
            'base64', encode(page.chunk, 'base64')
        ) ORDER BY page.ordinal), '[]'::json)
        FROM (
            SELECT ordinal, chunk_sha256, chunk
            FROM qbit_block_candidate_body_chunk
            WHERE body_id = {text(body_id)}
              AND ordinal >= {int(from_ordinal)}
            ORDER BY ordinal
            LIMIT {max_chunks}
        ) page
    )
);
"""


def header_page_sql(
    limit: int,
    after_cursor: tuple[str, str] | None,
    max_bytes: int,
    *,
    text: Literal,
) -> str:
    """Metadata-first pending page with explicit exhaustion.

    Projects the bounded typed replay header for both storage versions
    through ``qbit_prism_bounded_replay_header``: v2 rows carry the
    writer's projection in ``replay_header``; v1 rows have the same named
    keys projected out of the legacy jsonb (explicit field projection,
    never ``- 'shares_json'`` over an arbitrary document). Every fact is
    bounded on the server, so a header has a provable maximum size and
    the byte cap is a real bound even for the page's first row. The inner
    query fetches one row beyond the row cap so the page can prove
    exhaustion, and the running byte total cuts the page at ``max_bytes``
    while reporting that it did.
    """
    limit = max(1, min(int(limit), HEADER_PAGE_MAX_ROWS))
    max_bytes = max(1, min(int(max_bytes), HEADER_PAGE_MAX_BYTES))
    after_predicate = ""
    if after_cursor is not None:
        created_at_text, cursor_block_hash = after_cursor
        after_predicate = (
            "\n      AND (created_at, block_hash) > "
            f"({text(created_at_text)}::timestamptz, {text(cursor_block_hash)})"
        )
    return f"""
WITH fetched AS (
    SELECT
        block_hash,
        storage_version,
        candidate_sha256,
        body_id,
        created_at,
        to_char(
            created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ) AS cursor_created_at,
        qbit_prism_bounded_replay_header(
            CASE
                WHEN storage_version = 2 THEN replay_header
                ELSE jsonb_build_object(
                    'schema', candidate->'schema',
                    'block_hash_hex', candidate->'block_hash_hex',
                    'parent_hash', candidate->'parent_hash',
                    'expected_height', candidate->'expected_height',
                    'template', candidate->'template',
                    'found_block', jsonb_build_object(
                        'network_difficulty', candidate->'found_block'->'network_difficulty'
                    ),
                    'pending_share', candidate->'pending_share',
                    'credit_share_on_accept', candidate->'credit_share_on_accept',
                    'collection_only', candidate->'collection_only',
                    'username', candidate->'username',
                    'accepted_at_present', (candidate->'pending_share') ? 'accepted_at_ms'
                )
            END
        ) AS header,
        EXISTS (
            SELECT 1
            FROM qbit_pool_blocks pool
            WHERE pool.block_hash = qbit_block_candidate_outbox.block_hash
        ) AS pool_block_exists
    FROM qbit_block_candidate_outbox
    WHERE state = 'pending'{after_predicate}
    ORDER BY created_at, block_hash
    LIMIT {limit + 1}
),
measured AS (
    SELECT
        fetched.*,
        row_number() OVER (ORDER BY created_at, block_hash) AS row_position,
        sum(octet_length(header::text)) OVER (
            ORDER BY created_at, block_hash
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_bytes
    FROM fetched
),
page AS (
    SELECT measured.*
    FROM measured
    WHERE row_position <= {limit}
      AND (row_position = 1 OR running_bytes <= {max_bytes})
)
SELECT json_build_object(
    'fetched', (SELECT count(*) FROM fetched),
    'returned', (SELECT count(*) FROM page),
    'rows', (
        SELECT COALESCE(json_agg(json_build_object(
            'block_hash', page.block_hash,
            'storage_version', page.storage_version,
            'candidate_sha256', page.candidate_sha256,
            'header', page.header,
            'header_bytes', octet_length(page.header::text),
            'body', (
                SELECT json_build_object(
                    'body_id', body.body_id,
                    'storage_version', body.storage_version,
                    'candidate_sha256', body.candidate_sha256,
                    'byte_count', body.byte_count,
                    'chunk_count', body.chunk_count,
                    'chunk_bytes', body.chunk_bytes,
                    'share_count', body.share_count,
                    'state', body.state
                )
                FROM qbit_block_candidate_body body
                WHERE body.body_id = page.body_id
            ),
            'pool_block_exists', page.pool_block_exists,
            'cursor', json_build_array(page.cursor_created_at, page.block_hash)
        ) ORDER BY page.created_at, page.block_hash), '[]'::json)
        FROM page
    )
);
"""


def retire_orphan_bodies_sql(payload: Mapping[str, Any], *, jsonb: JsonbLiteral) -> str:
    """Tiny fenced compare-and-swap: orphaned bodies become ``retired``.

    Under the writer fence (a deposed session retires nothing), but the
    only row this touches besides the lease is one manifest row: a
    ``staging`` body older than ``stale_staging_seconds`` owned by another
    session, or a ``sealed`` body older than that age that no outbox row
    references. ``retired`` is permanent -- the manifest trigger permits no
    transition out of it -- and publication references only ``sealed``
    bodies, so a retired body can never be revived.

    The race with a publication in flight is settled by the database, not
    by this statement's snapshot: publication locks the body row ``FOR
    SHARE`` while it checks the body is sealed, this statement locks it
    ``FOR UPDATE``, and the manifest trigger refuses to retire a body any
    outbox row references (its check runs with a fresh snapshot after the
    lock wait). Whichever side loses the lock re-evaluates and fails
    closed; in-process, both statements also serialize behind the ledger's
    writer gate.
    """
    return f"""
WITH input AS (
    SELECT {jsonb(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET updated_at = clock_timestamp()
    FROM input
    WHERE singleton
      AND writer_id = input.data->>'writer_id'
      AND writer_epoch = (input.data->>'writer_epoch')::bigint
      AND writer_session_token = input.data->>'writer_session_token'
    RETURNING writer_id
),
target AS (
    SELECT body.body_id
    FROM qbit_block_candidate_body body, input
    WHERE EXISTS (SELECT 1 FROM lease)
      AND (
          (
              body.state = 'staging'
              AND body.created_at < clock_timestamp()
                  - (input.data->>'stale_staging_seconds')::double precision * interval '1 second'
              AND NOT (
                  body.staging_writer_id = input.data->>'writer_id'
                  AND body.staging_session_token = input.data->>'writer_session_token'
              )
          )
          OR (
              body.state = 'sealed'
              AND body.sealed_at < clock_timestamp()
                  - (input.data->>'stale_staging_seconds')::double precision * interval '1 second'
          )
      )
      AND NOT EXISTS (
          SELECT 1 FROM qbit_block_candidate_outbox outbox
          WHERE outbox.body_id = body.body_id
      )
    ORDER BY body.created_at, body.body_id
    LIMIT 1
    FOR UPDATE OF body
),
retired AS (
    UPDATE qbit_block_candidate_body
    SET state = 'retired', retired_at = clock_timestamp()
    FROM target
    WHERE qbit_block_candidate_body.body_id = target.body_id
      AND qbit_block_candidate_body.state IN ('staging', 'sealed')
      AND NOT EXISTS (
          SELECT 1 FROM qbit_block_candidate_outbox outbox
          WHERE outbox.body_id = qbit_block_candidate_body.body_id
      )
    RETURNING qbit_block_candidate_body.body_id
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    ELSE json_build_object('retired', COALESCE((SELECT json_agg(body_id) FROM retired), '[]'::json))
END;
"""


def reap_retired_chunks_sql(payload: Mapping[str, Any], *, jsonb: JsonbLiteral) -> str:
    """Delete one bounded page of one retired body's parts; no lease touch.

    Runs entirely outside the lease row: ``retired`` is a permanent state
    that nothing publishes from, so reclaiming its bytes needs no fence
    and a deposed session doing it cannot harm anyone.

    Every step is bounded and every probe is an existence test, never a
    count: at most ``max_chunks`` chunk rows go, then (once the statement's
    snapshot shows no chunk left) at most ``max_chunks`` page rows, then
    span rows, and the manifest itself only on a later step whose snapshot
    finds every child table empty. The returned ``remaining`` flags let the
    caller keep stepping until the body is gone, so a body with zero chunks
    but many page rows never stalls.
    """
    return f"""
WITH input AS (
    SELECT {jsonb(payload)} AS data
),
target AS (
    SELECT body.body_id
    FROM qbit_block_candidate_body body
    WHERE body.state = 'retired'
      AND NOT EXISTS (
          SELECT 1 FROM qbit_block_candidate_outbox outbox
          WHERE outbox.body_id = body.body_id
      )
    ORDER BY body.retired_at, body.body_id
    LIMIT 1
),
before AS (
    SELECT
        EXISTS (SELECT 1 FROM qbit_block_candidate_body_chunk chunk, target
                WHERE chunk.body_id = target.body_id) AS chunks,
        EXISTS (SELECT 1 FROM qbit_block_candidate_body_page page, target
                WHERE page.body_id = target.body_id) AS pages,
        EXISTS (SELECT 1 FROM qbit_block_candidate_body_span span, target
                WHERE span.body_id = target.body_id) AS spans
),
deleted_chunks AS (
    DELETE FROM qbit_block_candidate_body_chunk
    WHERE ctid IN (
        SELECT chunk.ctid
        FROM qbit_block_candidate_body_chunk chunk, target
        WHERE chunk.body_id = target.body_id
        ORDER BY chunk.ordinal
        LIMIT (SELECT (data->>'max_chunks')::integer FROM input)
    )
    RETURNING body_id
),
deleted_pages AS (
    DELETE FROM qbit_block_candidate_body_page
    WHERE NOT (SELECT chunks FROM before)
      AND ctid IN (
          SELECT page.ctid
          FROM qbit_block_candidate_body_page page, target
          WHERE page.body_id = target.body_id
          ORDER BY page.field, page.page_ordinal
          LIMIT (SELECT (data->>'max_chunks')::integer FROM input)
      )
    RETURNING body_id
),
deleted_spans AS (
    DELETE FROM qbit_block_candidate_body_span
    WHERE NOT (SELECT chunks FROM before)
      AND NOT (SELECT pages FROM before)
      AND ctid IN (
          SELECT span.ctid
          FROM qbit_block_candidate_body_span span, target
          WHERE span.body_id = target.body_id
          ORDER BY span.field
          LIMIT (SELECT (data->>'max_chunks')::integer FROM input)
      )
    RETURNING body_id
),
deleted_bodies AS (
    DELETE FROM qbit_block_candidate_body
    WHERE body_id IN (SELECT body_id FROM target)
      AND state = 'retired'
      AND NOT (SELECT chunks FROM before)
      AND NOT (SELECT pages FROM before)
      AND NOT (SELECT spans FROM before)
    RETURNING body_id
)
SELECT json_build_object(
    'body_id', (SELECT body_id FROM target),
    'deleted_chunks', (SELECT count(*) FROM deleted_chunks),
    'deleted_pages', (SELECT count(*) FROM deleted_pages),
    'deleted_spans', (SELECT count(*) FROM deleted_spans),
    'deleted_bodies', (SELECT count(*) FROM deleted_bodies),
    'remaining', (SELECT json_build_object('chunks', chunks, 'pages', pages, 'spans', spans) FROM before)
);
"""


# --------------------------------------------------------------------------
# spool admission
# --------------------------------------------------------------------------


class SpoolAdmission:
    """Global byte accounting for hydrated spool files."""

    def __init__(
        self,
        directory: str | Path | None = None,
        *,
        limit_bytes: int = DEFAULT_SPOOL_RESERVATION_BYTES,
    ) -> None:
        self._lock = threading.Lock()
        self._limit = int(limit_bytes)
        self._reserved = 0
        self._directory = (
            Path(directory)
            if directory is not None
            else Path(tempfile.gettempdir()) / "qbit-prism-candidate-spool"
        )

    @property
    def directory(self) -> Path:
        return self._directory

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {"reserved_bytes": self._reserved, "limit_bytes": self._limit}

    def reserve(self, byte_count: int) -> Callable[[], None]:
        """Reserve ``byte_count``; returns the idempotent release callable."""
        byte_count = max(0, int(byte_count))
        with self._lock:
            if self._reserved + byte_count > self._limit:
                raise SpoolAdmissionExhausted(
                    f"candidate spool reservation exhausted: {self._reserved + byte_count} "
                    f"> {self._limit} bytes"
                )
            self._reserved += byte_count
        released = False
        lock = self._lock

        def release() -> None:
            nonlocal released
            with lock:
                if not released:
                    released = True
                    self._reserved -= byte_count

        return release

    def new_spool_paths(self, block_hash: str) -> tuple[str, str]:
        """A fresh ``(body path, index path)`` pair in the spool directory."""
        self._directory.mkdir(parents=True, exist_ok=True)
        handle, path = tempfile.mkstemp(
            prefix=f"candidate-{block_hash[:16]}-",
            suffix=".body",
            dir=str(self._directory),
        )
        os.close(handle)
        index_path = path[: -len(".body")] + ".idx"
        try:
            with open(index_path, "wb"):
                pass
        except BaseException:
            os.unlink(path)
            raise
        return path, index_path


# --------------------------------------------------------------------------
# hydration
# --------------------------------------------------------------------------


def parse_manifest_row(row: Mapping[str, Any]) -> tuple[CandidateBodyManifest, str, bool]:
    """Typed manifest, live state and referenced flag from a manifest read."""
    if not isinstance(row, Mapping) or not row.get("found"):
        raise CandidateBodyUnavailable("candidate body manifest does not exist")
    manifest_json = row.get("manifest")
    if not isinstance(manifest_json, Mapping):
        raise CandidateBodyUnavailable("candidate body manifest is unreadable")
    manifest = CandidateBodyManifest.from_json(manifest_json)
    state = str(manifest_json.get("state"))
    referenced = bool(manifest_json.get("referenced"))
    return manifest, state, referenced


def _strict_base64(encoded: str, *, max_chars: int) -> bytes:
    """Decode PostgreSQL ``encode(..., 'base64')`` output strictly.

    The size is checked before any decoding allocates. PostgreSQL wraps at
    76 characters with ``\\n``; only that whitespace is tolerated, and the
    alphabet is validated rather than silently filtered.
    """
    if not isinstance(encoded, str):
        raise CandidateBodyIntegrityError("candidate body page chunk carries no bytes")
    if len(encoded) > max_chars:
        raise CandidateBodyIntegrityError("candidate body page chunk exceeds the transport bound")
    compact = encoded.replace("\n", "")
    if "\r" in compact or " " in compact:
        raise CandidateBodyIntegrityError("candidate body page chunk carries unexpected whitespace")
    try:
        return base64.b64decode(compact.encode("ascii"), validate=True)
    except (binascii.Error, ValueError, UnicodeEncodeError) as exc:
        raise CandidateBodyIntegrityError("candidate body page chunk is not valid base64") from exc


def parse_body_page(row: Mapping[str, Any], *, max_chunks: int = BODY_READ_MAX_CHUNKS) -> tuple[str | None, bool, list[BodyChunk]]:
    """Decode one page read: state, referenced flag and verified chunks.

    The chunk count and every chunk's encoded length are checked against
    the page bound before any base64 allocation; the decoded total is
    checked against the read bound as well.
    """
    if not isinstance(row, Mapping):
        raise CandidateBodyIntegrityError("candidate body page is not an object")
    state = row.get("state")
    chunks_json = row.get("chunks")
    if not isinstance(chunks_json, list):
        raise CandidateBodyIntegrityError("candidate body page carries no chunk list")
    if len(chunks_json) > max(1, min(int(max_chunks), BODY_READ_MAX_CHUNKS)):
        raise CandidateBodyIntegrityError("candidate body page exceeds the chunk bound")
    chunks: list[BodyChunk] = []
    total = 0
    for entry in chunks_json:
        if not isinstance(entry, Mapping):
            raise CandidateBodyIntegrityError("candidate body page chunk is malformed")
        data = _strict_base64(entry.get("base64"), max_chars=CHUNK_BASE64_MAX_CHARS)
        total += len(data)
        if len(data) > CANDIDATE_BODY_CHUNK_BYTES or total > BODY_READ_MAX_BYTES:
            raise CandidateBodyIntegrityError("candidate body page exceeds the read bound")
        chunks.append(
            BodyChunk(
                ordinal=int(entry.get("ordinal", -1)),
                data=data,
                sha256=str(entry.get("sha256", "")),
            )
        )
    return (None if state is None else str(state)), bool(row.get("referenced")), chunks


def parse_span_rows(row: Mapping[str, Any]) -> tuple[str | None, list[FieldSpan]]:
    if not isinstance(row, Mapping) or not isinstance(row.get("spans"), list):
        raise CandidateBodyIntegrityError("candidate body span page is malformed")
    spans = row["spans"]
    if len(spans) > INDEX_SPAN_ROWS:
        raise CandidateBodyIntegrityError("candidate body span page exceeds its bound")
    state = row.get("state")
    return (None if state is None else str(state)), [FieldSpan.from_json(span) for span in spans]


def parse_page_rows(row: Mapping[str, Any], field_name: str) -> tuple[str | None, list[PageEntry]]:
    if not isinstance(row, Mapping) or not isinstance(row.get("pages"), list):
        raise CandidateBodyIntegrityError("candidate body page-index page is malformed")
    pages = row["pages"]
    if len(pages) > INDEX_PAGE_ENTRIES:
        raise CandidateBodyIntegrityError("candidate body page-index page exceeds its bound")
    entries: list[PageEntry] = []
    for page in pages:
        if not isinstance(page, list) or len(page) != 3:
            raise CandidateBodyIntegrityError("candidate body page-index entry is malformed")
        entries.append(
            PageEntry(field=field_name, ordinal=int(page[0]), offset=int(page[1]), record_index=int(page[2]))
        )
    state = row.get("state")
    return (None if state is None else str(state)), entries


class CandidateBodyHydrator:
    """Hydrate one sealed body to a spool/index pair in bounded pages."""

    def __init__(
        self,
        *,
        read_manifest: Callable[[str], Any],
        read_spans: Callable[[str, str | None], Any],
        read_pages: Callable[[str, str, int], Any],
        read_page: Callable[[str, int, int], Any],
        admission: SpoolAdmission,
        cancelled: Callable[[], bool] | None = None,
        max_chunks_per_page: int = BODY_READ_MAX_CHUNKS,
    ) -> None:
        self._read_manifest = read_manifest
        self._read_spans = read_spans
        self._read_pages = read_pages
        self._read_page = read_page
        self._admission = admission
        self._cancelled = cancelled or (lambda: False)
        self._max_chunks = max(1, min(int(max_chunks_per_page), BODY_READ_MAX_CHUNKS))

    def hydrate(
        self,
        ref: CandidateBodyRef,
        *,
        block_hash: str,
        accepted_at_present: bool,
        accepted_at_ms: Any,
    ) -> PreparedCandidateIntent:
        manifest, state, referenced = parse_manifest_row(self._read_manifest(ref.body_id))
        if state != "sealed" or not referenced:
            raise CandidateBodyUnavailable(
                f"candidate body {ref.body_id} is {state} and "
                f"{'referenced' if referenced else 'unreferenced'}"
            )
        if manifest.candidate_sha256 != ref.candidate_sha256:
            raise CandidateBodyIntegrityError("candidate body manifest digest differs from its outbox row")
        if manifest.byte_count != ref.byte_count or manifest.chunk_count != ref.chunk_count:
            raise CandidateBodyIntegrityError("candidate body manifest disagrees with its outbox row")
        release = self._admission.reserve(manifest.byte_count + manifest.page_count * 16)
        paths: tuple[str, str] = ()
        writer = None
        body = None
        try:
            paths = self._admission.new_spool_paths(block_hash)
            writer = SpoolWriter(*paths, manifest)
            self._spool_index(ref.body_id, manifest, writer)
            self._spool_chunks(ref.body_id, manifest, writer)
            body = writer.finish(release=release)
            return prepared_intent_from_spool(
                body,
                accepted_at_present=accepted_at_present,
                accepted_at_ms=accepted_at_ms,
            )
        except BaseException:
            if body is not None:
                body.close()
            elif writer is not None:
                writer.abort()
            else:
                for path in paths:
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass
            release()
            raise

    def _check_state(self, state: str | None) -> None:
        if state != "sealed":
            raise CandidateBodyUnavailable("candidate body turned terminal during hydration")

    def _spool_index(self, body_id: str, manifest: CandidateBodyManifest, writer: SpoolWriter) -> None:
        after: str | None = None
        seen = 0
        while True:
            if self._cancelled():
                raise CandidateStorageError("candidate body hydration cancelled")
            state, spans = parse_span_rows(self._read_spans(body_id, after))
            self._check_state(state)
            for span in spans:
                writer.span(span)
                self._spool_pages(body_id, span, writer)
            seen += len(spans)
            if len(spans) < INDEX_SPAN_ROWS:
                break
            after = spans[-1].field
        if seen != manifest.span_count:
            raise CandidateBodyIntegrityError("candidate body span rows disagree with the manifest")

    def _spool_pages(self, body_id: str, span: FieldSpan, writer: SpoolWriter) -> None:
        ordinal = 0
        while ordinal < span.page_count:
            if self._cancelled():
                raise CandidateStorageError("candidate body hydration cancelled")
            state, entries = parse_page_rows(self._read_pages(body_id, span.field, ordinal), span.field)
            self._check_state(state)
            if not entries:
                raise CandidateBodyIntegrityError("candidate body page index is incomplete")
            for entry in entries:
                if entry.ordinal != ordinal:
                    raise CandidateBodyIntegrityError("candidate body page index is not contiguous")
                ordinal += 1
            writer.pages(entries)
        if ordinal != span.page_count:
            raise CandidateBodyIntegrityError("candidate body page index disagrees with its span")

    def _spool_chunks(self, body_id: str, manifest: CandidateBodyManifest, writer: SpoolWriter) -> None:
        ordinal = 0
        while ordinal < manifest.chunk_count:
            if self._cancelled():
                raise CandidateStorageError("candidate body hydration cancelled")
            state, referenced, chunks = parse_body_page(
                self._read_page(body_id, ordinal, self._max_chunks),
                max_chunks=self._max_chunks,
            )
            if state != "sealed" or not referenced:
                raise CandidateBodyUnavailable("candidate body turned terminal during hydration")
            if not chunks:
                raise CandidateBodyIntegrityError("candidate body page returned no chunks")
            for chunk in chunks:
                if chunk.ordinal != ordinal:
                    raise CandidateBodyIntegrityError("candidate body chunks are not contiguous")
                writer.chunk(chunk)
                ordinal += 1


# --------------------------------------------------------------------------
# legacy helper (isolated process)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class LegacyTransport:
    """How the helper reaches PostgreSQL: a psql command or a DSN."""

    psql_command: str | None = None
    database_url: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {"psql_command": self.psql_command, "database_url": self.database_url}


@dataclass
class LegacyHelperResult:
    manifest: CandidateBodyManifest
    spans: list[FieldSpan]
    accepted_at_present: bool
    accepted_at_ms: Any
    row_candidate_sha256: str | None
    identity_matches_row: bool


class LegacyCandidateHelper:
    """Run the isolated legacy conversion/comparison child process."""

    def __init__(
        self,
        transport: LegacyTransport,
        *,
        python: str | None = None,
        memory_limit_bytes: int = LEGACY_HELPER_MEMORY_BYTES,
        timeout_seconds: float = LEGACY_HELPER_TIMEOUT_SECONDS,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        self._transport = transport
        self._python = python or sys.executable
        self._memory_limit = int(memory_limit_bytes)
        self._timeout = float(timeout_seconds)
        self._cancelled = cancelled or (lambda: False)
        self._lock = threading.Lock()  # admit at most one conversion

    def with_cancellation(self, cancelled: Callable[[], bool]) -> LegacyCandidateHelper:
        helper = LegacyCandidateHelper(
            self._transport,
            python=self._python,
            memory_limit_bytes=self._memory_limit,
            timeout_seconds=self._timeout,
            cancelled=cancelled,
        )
        # Cancellation wrappers share the same admission slot.
        helper._lock = self._lock
        return helper

    def _run(
        self,
        request: Mapping[str, Any],
        body: CandidateBody | None = None,
    ) -> dict[str, Any]:
        """Supervise input, output, cancellation and retirement together.

        A helper may wait on PostgreSQL before reading stdin. The deadline
        therefore starts before sending body bytes, and a blocked pipe writer
        runs alongside the supervisor. Diagnostic pipes are drained with fixed
        retention limits so a failed child cannot deadlock or grow the parent.
        """
        deadline = time.monotonic() + self._timeout

        def check_deadline() -> None:
            if self._cancelled():
                raise CandidateStorageError("legacy candidate helper cancelled")
            if time.monotonic() >= deadline:
                raise CandidateStorageError("legacy candidate helper exceeded its deadline")

        while not self._lock.acquire(timeout=LEGACY_HELPER_POLL_SECONDS):
            check_deadline()
        process = None
        threads: list[threading.Thread] = []
        output = bytearray()
        errors = bytearray()
        io_errors: list[BaseException] = []
        limit = 64 * 1024
        try:
            check_deadline()
            repo_root = Path(__file__).resolve().parents[2]
            env = dict(os.environ)
            env["PYTHONPATH"] = str(repo_root) + (
                os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
            )
            # Apply limits in the freshly exec'd child. preexec_fn is unsafe
            # in this multithreaded process, before Python locks are reset.
            process = subprocess.Popen(
                [self._python, "-m", "lab.prism.candidate_store"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=str(repo_root), env=env,
            )
            assert process.stdin and process.stdout and process.stderr

            def drain(pipe: Any, target: bytearray) -> None:
                try:
                    while chunk := pipe.read(8192):
                        target.extend(chunk)
                        if len(target) > limit:
                            del target[:-limit]
                except BaseException as exc:
                    io_errors.append(exc)

            def produce() -> None:
                try:
                    assert process is not None and process.stdin is not None
                    process.stdin.write((json.dumps({
                        **request, "memory_limit_bytes": self._memory_limit,
                    }) + "\n").encode("utf-8"))
                    process.stdin.flush()
                    if body is not None:
                        def write_chunk(chunk: BodyChunk) -> None:
                            check_deadline()
                            process.stdin.write(chunk.data)
                        body.write_chunks(write_chunk)
                    process.stdin.flush()
                except BaseException as exc:
                    io_errors.append(exc)
                finally:
                    try:
                        process.stdin.close()
                    except OSError:
                        pass

            for target, args, name in (
                (drain, (process.stdout, output), "stdout"),
                (drain, (process.stderr, errors), "stderr"),
                (produce, (), "input"),
            ):
                thread = threading.Thread(target=target, args=args, name=f"prism-legacy-helper-{name}")
                threads.append(thread)
                thread.start()
            while process.poll() is None:
                check_deadline()
                if io_errors:
                    raise CandidateStorageError("legacy candidate helper transport failed") from io_errors[0]
                time.sleep(LEGACY_HELPER_POLL_SECONDS)
            for thread in threads:
                thread.join()
            if process.returncode != 0:
                raise CandidateStorageError(
                    "legacy candidate helper failed: " + errors.decode("utf-8", "replace")[-2000:]
                )
            if io_errors:
                raise CandidateStorageError("legacy candidate helper transport failed") from io_errors[0]
            try:
                result = json.loads(output.decode("utf-8").strip().splitlines()[-1])
            except (ValueError, IndexError) as exc:
                raise CandidateStorageError("legacy candidate helper returned no verdict") from exc
            if not isinstance(result, dict):
                raise CandidateStorageError("legacy candidate helper verdict is malformed")
            if "error" in result:
                raise CandidateStorageError(f"legacy candidate helper: {result['error']}")
            return result
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait()
                for thread in threads:
                    thread.join()
                for pipe in (process.stdin, process.stdout, process.stderr):
                    if pipe is not None:
                        pipe.close()
            self._lock.release()

    def convert(self, block_hash: str, spool_path: str, index_path: str, *, spool_limit_bytes: int | None = None) -> LegacyHelperResult:
        result = self._run({
            "mode": "convert",
            "transport": self._transport.to_json(),
            "block_hash": block_hash,
            "spool_path": spool_path,
            "index_path": index_path,
            "chunk_bytes": CANDIDATE_BODY_CHUNK_BYTES,
            "spool_limit_bytes": spool_limit_bytes,
        })
        manifest = CandidateBodyManifest.from_json(result["manifest"])
        spans_json = result.get("spans")
        if not isinstance(spans_json, list) or len(spans_json) > INDEX_SPAN_ROWS * 64:
            raise CandidateStorageError("legacy candidate helper returned malformed spans")
        return LegacyHelperResult(
            manifest=manifest,
            spans=[FieldSpan.from_json(span) for span in spans_json],
            accepted_at_present=bool(result.get("accepted_at_present")),
            accepted_at_ms=result.get("accepted_at_ms"),
            row_candidate_sha256=result.get("row_candidate_sha256"),
            identity_matches_row=bool(result.get("identity_matches_row")),
        )

    def compare(self, block_hash: str, body: CandidateBody) -> bool:
        """jsonb-equivalence of the legacy row's identity and ``body``."""
        return self._compare("compare", block_hash, body, body_id=None)

    def compare_stored_body(self, body_id: str, block_hash: str, body: CandidateBody) -> bool:
        """jsonb-equivalence of a stored chunked body and ``body``.

        The fallback behind the byte comparison in
        ``PsqlShareLedger.compare_candidate_body``: the helper reads the
        stored chunks itself (page by page, its own connection), decodes
        both documents whole -- isolated, memory- and deadline-limited --
        and answers PostgreSQL's jsonb equality.
        """
        return self._compare("compare_stored", block_hash, body, body_id=body_id)

    def _compare(
        self,
        mode: str,
        block_hash: str,
        body: CandidateBody,
        *,
        body_id: str | None,
    ) -> bool:
        result = self._run({
            "mode": mode,
            "transport": self._transport.to_json(),
            "block_hash": block_hash,
            "body_id": body_id,
            "body_bytes": body.manifest.byte_count,
        }, body)
        return bool(result.get("equal"))


def _helper_query_json(transport: Mapping[str, Any], sql: str) -> Any:
    """Run one statement that yields a single JSON value, in the helper."""
    database_url = transport.get("database_url")
    psql_command = transport.get("psql_command")
    if database_url:
        try:
            import psycopg
        except ImportError:
            psycopg = None
        if psycopg is not None:
            with psycopg.connect(database_url, autocommit=True) as connection:
                row = connection.execute(sql).fetchone()
            if row is None:
                return None
            value = row[0]
            return json.loads(value) if isinstance(value, str) else value
    if not psql_command:
        raise CandidateStorageError("legacy helper has no database transport")
    completed = subprocess.run(
        [*shlex.split(psql_command), "--tuples-only", "--no-align", "--quiet"],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise CandidateStorageError(f"helper psql read failed: {completed.stderr.strip()}")
    output = completed.stdout.strip()
    if not output:
        return None
    return json.loads(output.splitlines()[-1])


def _helper_fetch_legacy_text(transport: Mapping[str, Any], block_hash: str) -> tuple[str | None, str | None]:
    """Fetch ``candidate::text`` and ``candidate_sha256`` for one v1 row."""
    quoted = block_hash.lower().replace(chr(39), chr(39) * 2)
    document = _helper_query_json(
        transport,
        "SELECT json_build_object('candidate', candidate::text, 'sha', candidate_sha256) "
        "FROM qbit_block_candidate_outbox "
        f"WHERE block_hash = '{quoted}' "
        f"AND storage_version = {LEGACY_CANDIDATE_STORAGE_VERSION} AND candidate IS NOT NULL;",
    )
    if not isinstance(document, dict):
        return None, None
    return document.get("candidate"), document.get("sha")


def _helper_fetch_stored_body(transport: Mapping[str, Any], body_id: str) -> bytes | None:
    """Read one stored chunked body in the helper, three chunks per statement."""
    quoted = body_id.replace(chr(39), chr(39) * 2)
    header = _helper_query_json(
        transport,
        "SELECT json_build_object('state', state, 'chunk_count', chunk_count) "
        f"FROM qbit_block_candidate_body WHERE body_id = '{quoted}';",
    )
    if not isinstance(header, dict) or header.get("state") != "sealed":
        return None
    chunk_count = int(header.get("chunk_count") or 0)
    pieces: list[bytes] = []
    ordinal = 0
    while ordinal < chunk_count:
        page = _helper_query_json(
            transport,
            "SELECT COALESCE(json_agg(json_build_object('ordinal', ordinal, 'base64', encode(chunk, 'base64')) "
            "ORDER BY ordinal), '[]'::json) FROM (SELECT ordinal, chunk FROM qbit_block_candidate_body_chunk "
            f"WHERE body_id = '{quoted}' AND ordinal >= {ordinal} ORDER BY ordinal LIMIT {BODY_READ_MAX_CHUNKS}) page;",
        )
        if not isinstance(page, list) or not page:
            return None
        for entry in page:
            if int(entry.get("ordinal", -1)) != ordinal:
                return None
            pieces.append(_strict_base64(entry.get("base64"), max_chars=CHUNK_BASE64_MAX_CHARS))
            ordinal += 1
    return b"".join(pieces)


def helper_main(stdin_stream: Any = None, stdout_stream: Any = None) -> int:
    """Entry point of the isolated helper process."""
    stdin_buffer = (stdin_stream or sys.stdin.buffer)
    stdout_text = stdout_stream or sys.stdout
    header = stdin_buffer.readline()
    try:
        request = json.loads(header.decode("utf-8"))
        if os.name == "posix":
            import resource
            memory_limit = int(request.get("memory_limit_bytes", LEGACY_HELPER_MEMORY_BYTES))
            resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
        mode = request["mode"]
        transport = request["transport"]
        block_hash = str(request["block_hash"]).lower()
        if mode == "compare_stored":
            from decimal import Decimal

            stored = _helper_fetch_stored_body(transport, str(request["body_id"]))
            if stored is None:
                raise CandidateBodyUnavailable("stored candidate body is not sealed")
            ours_bytes = stdin_buffer.read()
            theirs = json.loads(stored.decode("utf-8"), parse_float=Decimal)
            ours = json.loads(ours_bytes.decode("utf-8"), parse_float=Decimal)
            result = {"equal": jsonb_equivalent(theirs, ours)}
            stdout_text.write(json.dumps(result) + "\n")
            stdout_text.flush()
            return 0
        legacy_text, row_sha = _helper_fetch_legacy_text(transport, block_hash)
        if legacy_text is None:
            raise CandidateBodyUnavailable("legacy candidate row is not pending")
        if mode == "convert":
            manifest, spans, present, stamp = legacy_json_to_spool(
                legacy_text,
                str(request["spool_path"]),
                str(request["index_path"]),
                chunk_bytes=int(request.get("chunk_bytes", CANDIDATE_BODY_CHUNK_BYTES)),
                spool_limit_bytes=request.get("spool_limit_bytes"),
            )
            # The spans are a handful of rows (one per large field); the
            # page entries stay in the index file.
            result = {
                "manifest": manifest.to_json(),
                "spans": [span.to_json() for span in spans],
                "accepted_at_present": present,
                "accepted_at_ms": stamp,
                "row_candidate_sha256": row_sha,
                "identity_matches_row": manifest.candidate_sha256 == row_sha,
            }
        elif mode == "compare":
            from decimal import Decimal

            ours_bytes = stdin_buffer.read()
            legacy = json.loads(legacy_text, parse_float=Decimal)
            ours = json.loads(ours_bytes.decode("utf-8"), parse_float=Decimal)
            for document in (legacy, ours):
                pending = document.get("pending_share") if isinstance(document, dict) else None
                if isinstance(pending, dict) and "accepted_at_ms" in pending:
                    pending["accepted_at_ms"] = None
            result = {"equal": jsonb_equivalent(legacy, ours)}
        else:
            raise CandidateStorageError(f"unknown helper mode {mode!r}")
    except Exception as exc:  # noqa: BLE001 - reported to the parent
        stdout_text.write(json.dumps({"error": f"{type(exc).__name__}: {exc}"}) + "\n")
        stdout_text.flush()
        return 1
    stdout_text.write(json.dumps(result) + "\n")
    stdout_text.flush()
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    sys.exit(helper_main())


__all__ = [
    "BODY_READ_MAX_BYTES",
    "BODY_READ_MAX_CHUNKS",
    "CANDIDATE_SCHEMA_CAPABILITY",
    "HEADER_MAX_BYTES",
    "HEADER_PAGE_MAX_BYTES",
    "HEADER_PAGE_MAX_ROWS",
    "JANITOR_CHUNKS_PER_STEP",
    "STALE_STAGING_SECONDS",
    "CandidateBodyHydrator",
    "CandidateBodyRef",
    "CandidateBodyUnavailable",
    "CandidateHeaderPage",
    "CandidateStorageError",
    "DurableCandidateDescriptor",
    "IncompatibleCandidateSchema",
    "LegacyCandidateHelper",
    "LegacyHelperResult",
    "LegacyTransport",
    "SpoolAdmission",
    "SpoolAdmissionExhausted",
    "body_chunk_sql",
    "body_index_rows_sql",
    "body_manifest_sql",
    "body_page_sql",
    "body_pages_sql",
    "body_spans_sql",
    "chunk_hex_literal",
    "header_page_sql",
    "helper_main",
    "new_body_id",
    "parse_body_page",
    "parse_manifest_row",
    "parse_page_rows",
    "parse_span_rows",
    "reap_retired_chunks_sql",
    "retire_orphan_bodies_sql",
    "retire_staging_body_sql",
    "schema_capability_sql",
    "seal_body_sql",
    "stage_body_sql",
]
