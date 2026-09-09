#!/usr/bin/env python3
"""Bounded codec and immutable body handles for durable block candidates.

Issue #255. A block-candidate intent carries the whole payout window under
``shares_json`` (375,000 shares and ~232 MB of JSON at the stress size), and
the historical path materialized it three times in the lease-bearing
coordinator: ``list(context.shares_json)``, a whole ``json.dumps`` to
validate it, and a second whole ``json.dumps`` to inline it as a ``jsonb``
literal. Each of those is one C call that holds the GIL for the duration
while the writer-lease monitor waits to run.

This module replaces the whole-body calls with a streaming encoder whose
individual C calls are bounded (at most :data:`CODEC_BATCH_RECORDS` records
or roughly :data:`CODEC_BATCH_TARGET_BYTES` of output per ``json.dumps``;
strings over :data:`CODEC_STRING_SLICE_CHARS` code points are escaped in
slices) and hands the result to storage as fixed
:data:`CANDIDATE_BODY_CHUNK_BYTES` byte chunks with per-chunk digests.

Nothing the encoder keeps scales with the body. It emits three kinds of
events -- chunks, per-field *spans* (one per large top-level field) and
*page* entries (one per bounded batch of a large array) -- and retains only
a scalar :class:`CandidateBodyManifest`. A reader gets the same facts back
from storage in bounded pages and keeps the page index on disk next to the
spool file, bisecting it with ``pread`` instead of holding a list.

Byte identity is the contract. The body bytes are exactly the historical
v1 identity JSON -- ``json.dumps(block_candidate_identity(intent),
sort_keys=True, separators=(",", ":"))`` with the pending acknowledgment
stamp neutralized -- so ``candidate_sha256`` is unchanged for every existing
row and every existing oracle. Chunk boundaries, spans, pages and the
storage version never enter that digest.

Three body shapes exist:

* :class:`SourceCandidateBody` re-encodes on demand from the immutable
  share sequence the live job pinned (a page-backed window, wave B's lazy
  daemon sequence, or a plain list). It retains no encoded copy.
* :class:`SpoolCandidateBody` is a durable replay body hydrated to a local
  spool file in bounded pages; :class:`SpoolJsonArraySequence` decodes one
  bounded page per ``json.loads`` from it and digests the share array
  without decoding it at all.
* :class:`PreparedCandidateIntent` wraps either one behind the historical
  mapping interface (``intent["shares_json"]`` is the immutable sequence,
  never a list copy) so dict-facing callers keep working.

This module is a leaf: standard library only.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import struct
import threading
import weakref
from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Callable

# Storage format version of a chunked body. Distinct from the v1 mining
# intent schema string (``qbit.prism.block-candidate-intent.v1``), which is
# neither renamed nor re-hashed.
CANDIDATE_BODY_STORAGE_VERSION = 2
LEGACY_CANDIDATE_STORAGE_VERSION = 1
# Fixed byte size of every body chunk but the last. Storage, transport and
# the reader all enforce it independently.
CANDIDATE_BODY_CHUNK_BYTES = 256 * 1024
# Records per ``json.dumps`` call on the fast path, and the output size the
# adaptive batch aims for.
CODEC_BATCH_RECORDS = 256
CODEC_BATCH_TARGET_BYTES = 256 * 1024
# Output of one batch above which its items are re-encoded one by one
# through the incremental path (a giant field inside a record).
CODEC_BATCH_OVERSIZED_BYTES = 4 * 1024 * 1024
# Ordinary per-value ``json.dumps``/``json.loads`` input ceiling. Longer
# strings are escaped and decoded slice by slice; a top-level field whose
# encoding is larger than this becomes a *large field* with its own span.
# Nothing is rejected for size alone.
CODEC_STRING_SLICE_CHARS = 4096
CODEC_FAST_PATH_BYTES = 64 * 1024
# Historical reader ceilings, retained for callers that import them. The
# reader no longer decodes a page or a record whole above
# ``candidate_spool_view.SPOOL_VIEW_DECODE_BYTES``: an oversized record is a
# lazy view, and no record size is rejected.
CODEC_PAGE_DECODE_BYTES = 16 * 1024 * 1024
CODEC_WALK_RECORD_MAX_BYTES = 256 * 1024 * 1024
# Records decoded per page when a body carries no page index for a span.
SPOOL_WALK_BATCH_RECORDS = 256
# Bytes read from a spool per refill while walking records.
SPOOL_READ_BYTES = 256 * 1024
# Page entries written per index statement / read per index page.
INDEX_PAGE_ENTRIES = 256
# Span rows read per statement.
INDEX_SPAN_ROWS = 64

CANDIDATE_SHARES_KEY = "shares_json"
CANDIDATE_INTENT_SCHEMA = "qbit.prism.block-candidate-intent.v1"

_COMPACT_SEPARATORS = (",", ":")
# One on-disk page index record: body offset, record index (u64, u64).
_INDEX_RECORD = struct.Struct("<QQ")

# Replay-header field bounds (bytes of the JSON text of each fact). The
# header is an explicit projection of a few typed facts; anything longer
# than this is not a fact the header needs and is deferred to the body.
HEADER_TEXT_FIELD_BYTES = 256
HEADER_NUMERIC_FIELD_BYTES = 128
HEADER_HASH_FIELD_BYTES = 64


class CandidateCodecError(ValueError):
    """The intent cannot be encoded as durable JSON (bad field or value)."""


class CandidateBodyIntegrityError(RuntimeError):
    """A body's bytes do not match its manifest or its source changed."""


# --------------------------------------------------------------------------
# manifest, spans, pages, chunks
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class BodyChunk:
    """One fixed-offset slice of a body with its own digest."""

    ordinal: int
    data: bytes
    sha256: str

    @property
    def length(self) -> int:
        return len(self.data)


@dataclass(frozen=True)
class FieldSpan:
    """Byte span of one large top-level field inside the body.

    ``kind`` is ``array`` (decoded page by page), ``string`` (decoded slice
    by slice) or ``value`` (any other JSON value, decoded in one call bounded
    by the field's own size). ``item_count`` is the array length for arrays.
    ``pages_exact`` is False when the page entries are boundary *hints*
    (verbatim daemon bytes scanned rather than encoded record by record); a
    reader validates every hint by decoding and falls back to the walker.
    """

    field: str
    kind: str
    start: int
    end: int
    item_count: int
    page_count: int
    pages_exact: bool

    def to_json(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "kind": self.kind,
            "start": self.start,
            "end": self.end,
            "item_count": self.item_count,
            "page_count": self.page_count,
            "pages_exact": self.pages_exact,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> FieldSpan:
        span = cls(
            field=str(payload["field"]),
            kind=str(payload["kind"]),
            start=int(payload["start"]),
            end=int(payload["end"]),
            item_count=int(payload.get("item_count") or 0),
            page_count=int(payload.get("page_count") or 0),
            pages_exact=bool(payload.get("pages_exact", True)),
        )
        if span.kind not in {"array", "string", "value"}:
            raise CandidateBodyIntegrityError("body span has an unknown kind")
        if span.start < 0 or span.end < span.start or span.item_count < 0 or span.page_count < 0:
            raise CandidateBodyIntegrityError("body span is out of range")
        return span


@dataclass(frozen=True)
class PageEntry:
    """Where one bounded page of a large array starts inside the body."""

    field: str
    ordinal: int
    offset: int
    record_index: int


@dataclass(frozen=True)
class CandidateBodyManifest:
    """Scalar facts about a body; nothing here scales with its size."""

    storage_version: int
    candidate_sha256: str
    byte_count: int
    chunk_count: int
    chunk_bytes: int
    share_count: int
    shares_offset: int
    shares_end: int
    span_count: int
    page_count: int

    def to_json(self) -> dict[str, Any]:
        return {
            "storage_version": self.storage_version,
            "candidate_sha256": self.candidate_sha256,
            "byte_count": self.byte_count,
            "chunk_count": self.chunk_count,
            "chunk_bytes": self.chunk_bytes,
            "share_count": self.share_count,
            "shares_offset": self.shares_offset,
            "shares_end": self.shares_end,
            "span_count": self.span_count,
            "page_count": self.page_count,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> CandidateBodyManifest:
        try:
            manifest = cls(
                storage_version=int(payload["storage_version"]),
                candidate_sha256=str(payload["candidate_sha256"]),
                byte_count=int(payload["byte_count"]),
                chunk_count=int(payload["chunk_count"]),
                chunk_bytes=int(payload["chunk_bytes"]),
                share_count=int(payload["share_count"]),
                shares_offset=int(payload["shares_offset"]),
                shares_end=int(payload["shares_end"]),
                span_count=int(payload.get("span_count") or 0),
                page_count=int(payload.get("page_count") or 0),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CandidateBodyIntegrityError("manifest is malformed") from exc
        manifest.validate()
        return manifest

    def validate(self) -> None:
        if self.storage_version != CANDIDATE_BODY_STORAGE_VERSION:
            raise CandidateBodyIntegrityError(
                f"unsupported candidate body storage version {self.storage_version}"
            )
        if self.chunk_bytes <= 0 or self.chunk_bytes > CANDIDATE_BODY_CHUNK_BYTES:
            raise CandidateBodyIntegrityError("manifest chunk size is out of range")
        if self.byte_count < 0 or self.chunk_count < 0:
            raise CandidateBodyIntegrityError("manifest counts are negative")
        if self.chunk_count != chunk_ordinals_for(self.byte_count, self.chunk_bytes):
            raise CandidateBodyIntegrityError(
                "manifest chunk count disagrees with its byte count"
            )
        # A body without a share array (a candidate-only intent, as some
        # tools and tests persist) carries an empty span at offset 0.
        if not (0 <= self.shares_offset <= self.shares_end <= self.byte_count):
            raise CandidateBodyIntegrityError("manifest share span is out of range")
        if self.shares_offset == self.shares_end and self.share_count:
            raise CandidateBodyIntegrityError("manifest declares shares without a span")
        if len(self.candidate_sha256) != 64:
            raise CandidateBodyIntegrityError("manifest digest is malformed")
        if self.span_count < 0 or self.page_count < 0:
            raise CandidateBodyIntegrityError("manifest index counts are negative")


def chunk_ordinals_for(byte_count: int, chunk_bytes: int = CANDIDATE_BODY_CHUNK_BYTES) -> int:
    return -(-int(byte_count) // int(chunk_bytes))


# --------------------------------------------------------------------------
# jsonb compatibility
# --------------------------------------------------------------------------

# PostgreSQL ``jsonb`` rejects two things CPython's encoder happily emits:
# the ``\\u0000`` escape (text cannot hold NUL) and an unpaired UTF-16
# surrogate escape. The historical route inlined the intent as a jsonb
# literal, so both failed on the server before any credit; a chunked body
# never reaches a jsonb parser, so the same rejection happens here, at
# preparation. Escape-aware: a backslash run of even length before ``\u``
# is escaped backslashes, not an escape start.
_ESCAPED_NUL_RE = re.compile(r"(?<!\\)(?:\\\\)*\\u0000")
_SURROGATE_ESCAPE_RE = re.compile(r"(?<!\\)(?:\\\\)*\\u(d[89ab][0-9a-f]{2}|d[c-f][0-9a-f]{2})")
_LOW_SURROGATE_ESCAPE_RE = re.compile(r"\\ud[c-f][0-9a-f]{2}")
_LONE_SURROGATE_RE = re.compile(
    "[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]"
)


def _reject_jsonb_incompatible_text(text: str) -> None:
    """Refuse encoded JSON text PostgreSQL's jsonb parser would refuse."""
    if "\\u0000" in text and _ESCAPED_NUL_RE.search(text):
        raise CandidateCodecError(
            "unsupported Unicode escape sequence: \\u0000 cannot be converted to text"
        )
    if "\\ud" not in text:
        return
    expected_low_at = -1
    for match in _SURROGATE_ESCAPE_RE.finditer(text):
        unit = match.group(1)
        escape_start = match.end() - 6
        if unit[1] in "89ab":
            if expected_low_at == escape_start:
                raise CandidateCodecError("Unicode high surrogate must not follow a high surrogate")
            following = text[match.end() : match.end() + 6]
            if not _LOW_SURROGATE_ESCAPE_RE.fullmatch(following):
                raise CandidateCodecError("Unicode high surrogate must not be alone")
            expected_low_at = match.end()
        elif expected_low_at == escape_start:
            expected_low_at = -1
        else:
            raise CandidateCodecError("Unicode low surrogate must follow a high surrogate")


def _reject_jsonb_incompatible_str(value: str) -> None:
    """The Python-string form of :func:`_reject_jsonb_incompatible_text`."""
    if "\x00" in value:
        raise CandidateCodecError(
            "unsupported Unicode escape sequence: \\u0000 cannot be converted to text"
        )
    if _LONE_SURROGATE_RE.search(value):
        raise CandidateCodecError("unpaired UTF-16 surrogate cannot be stored as jsonb")


# --------------------------------------------------------------------------
# streaming encoder
# --------------------------------------------------------------------------


class _ChunkSink:
    """Turns encoded text into fixed-size chunks and a running digest."""

    __slots__ = (
        "_chunk_bytes",
        "_on_chunk",
        "_buffer",
        "_buffered",
        "offset",
        "_digest",
        "_ordinal",
    )

    def __init__(
        self,
        chunk_bytes: int,
        on_chunk: Callable[[BodyChunk], None] | None,
    ) -> None:
        self._chunk_bytes = int(chunk_bytes)
        self._on_chunk = on_chunk
        self._buffer: list[bytes] = []
        self._buffered = 0
        self.offset = 0
        self._digest = hashlib.sha256()
        self._ordinal = 0

    @property
    def chunk_count(self) -> int:
        return self._ordinal

    def write_text(self, text: str) -> None:
        if text:
            self.write_bytes(text.encode("utf-8"))

    def write_bytes(self, data: bytes) -> None:
        if not data:
            return
        self._digest.update(data)
        self.offset += len(data)
        # Fill the current chunk to exactly its size before emitting, and
        # never re-join a remainder: a verbatim page of any size is consumed
        # in one linear pass.
        view = memoryview(data)
        while len(view):
            room = self._chunk_bytes - self._buffered
            piece = view[:room]
            self._buffer.append(bytes(piece))
            self._buffered += len(piece)
            view = view[room:]
            if self._buffered >= self._chunk_bytes:
                self._emit()

    def _emit(self) -> None:
        chunk_data = self._buffer[0] if len(self._buffer) == 1 else b"".join(self._buffer)
        self._buffer = []
        self._buffered = 0
        chunk = BodyChunk(
            ordinal=self._ordinal,
            data=chunk_data,
            sha256=hashlib.sha256(chunk_data).hexdigest(),
        )
        self._ordinal += 1
        if self._on_chunk is not None:
            self._on_chunk(chunk)

    def finish(self) -> str:
        if self._buffered:
            self._emit()
        return self._digest.hexdigest()


class _IndexEvents:
    """Collects span/page events for the field being encoded, bounded.

    Page entries are handed to ``on_page`` in batches of at most
    :data:`INDEX_PAGE_ENTRIES`; the span is handed to ``on_span`` when the
    field ends. Only the current batch is ever held.
    """

    __slots__ = ("on_span", "on_page", "_field", "_pages", "_page_ordinal", "_start", "span_count", "page_count", "_kind")

    def __init__(
        self,
        on_span: Callable[[FieldSpan], None] | None,
        on_page: Callable[[list[PageEntry]], None] | None,
    ) -> None:
        self.on_span = on_span
        self.on_page = on_page
        self._field = ""
        self._kind = ""
        self._pages: list[PageEntry] = []
        self._page_ordinal = 0
        self._start = 0
        self.span_count = 0
        self.page_count = 0

    def begin(self, field_name: str, kind: str, start: int) -> None:
        self._field = field_name
        self._kind = kind
        self._start = start
        self._pages = []
        self._page_ordinal = 0

    def page(self, offset: int, record_index: int) -> None:
        self._pages.append(
            PageEntry(
                field=self._field,
                ordinal=self._page_ordinal,
                offset=offset,
                record_index=record_index,
            )
        )
        self._page_ordinal += 1
        if len(self._pages) >= INDEX_PAGE_ENTRIES:
            self._flush_pages()

    def _flush_pages(self) -> None:
        if not self._pages:
            return
        if self.on_page is not None:
            self.on_page(self._pages)
        self.page_count += len(self._pages)
        self._pages = []

    def end(self, end: int, *, item_count: int, pages_exact: bool, emit: bool) -> None:
        if not emit:
            # A small field: no span, no pages -- decoded from the skeleton.
            self._pages = []
            self._page_ordinal = 0
            return
        self._flush_pages()
        span = FieldSpan(
            field=self._field,
            kind=self._kind,
            start=self._start,
            end=end,
            item_count=item_count,
            page_count=self._page_ordinal,
            pages_exact=pages_exact,
        )
        self.span_count += 1
        if self.on_span is not None:
            self.on_span(span)


def _dumps_scalar(value: Any) -> str:
    # ``allow_nan=False`` is the deliberate difference from the historical
    # ``json.dumps``: NaN/Infinity used to pass the client-side validation
    # and then fail as a jsonb literal on the server, after the share was
    # already inside the fenced statement. A chunked body never reaches a
    # jsonb parser, so the rejection has to happen here, before credit.
    text = json.dumps(value, allow_nan=False, sort_keys=True, separators=_COMPACT_SEPARATORS)
    if isinstance(value, (dict, list, tuple)):
        _reject_jsonb_incompatible_text(text)
    return text


def _dumps_key(key: str) -> str:
    _reject_jsonb_incompatible_str(key)
    return json.dumps(key)


def _write_string(value: str, sink: _ChunkSink) -> None:
    if len(value) <= CODEC_STRING_SLICE_CHARS:
        _reject_jsonb_incompatible_str(value)
        sink.write_text(json.dumps(value))
        return
    # ``ensure_ascii`` escapes code point by code point (a non-BMP character
    # is one code point and becomes one surrogate pair), so slicing by code
    # point and escaping each slice reproduces the whole-string escape.
    sink.write_text('"')
    start = 0
    while start < len(value):
        end = min(len(value), start + CODEC_STRING_SLICE_CHARS)
        if end < len(value) and "\ud800" <= value[end - 1] <= "\udbff" and "\udc00" <= value[end] <= "\udfff":
            end += 1
        piece = value[start:end]
        _reject_jsonb_incompatible_str(piece)
        sink.write_text(json.dumps(piece)[1:-1])
        start = end
    sink.write_text('"')


def _write_value(value: Any, sink: _ChunkSink) -> None:
    """Write ``value`` exactly as ``json.dumps(value, sort_keys=True)`` would."""
    encoded_chunks = getattr(value, "iter_byte_chunks", None)
    if callable(encoded_chunks):
        for chunk in encoded_chunks():
            sink.write_bytes(chunk)
        return
    if value is None or value is True or value is False:
        sink.write_text(_dumps_scalar(value))
        return
    if isinstance(value, str):
        _write_string(value, sink)
        return
    if isinstance(value, (int, float)):
        sink.write_text(_dumps_scalar(value))
        return
    if isinstance(value, dict):
        keys = list(value)
        if not keys:
            sink.write_text("{}")
            return
        # Sort before coercion, matching CPython's failure on incomparable
        # key types. Even a numeric-key extension can contain a giant value;
        # it must use the same streamed value encoder as ordinary fields.
        keys.sort()
        sink.write_text("{")
        first = True
        for key in keys:
            if not first:
                sink.write_text(",")
            if isinstance(key, str):
                encoded_key = key
            elif key is None or isinstance(key, (bool, int, float)):
                encoded_key = _dumps_scalar(key)
            else:
                raise TypeError(f"keys must be str, int, float, bool or None, not {type(key).__name__}")
            _write_string(encoded_key, sink)
            sink.write_text(":")
            first = False
            _write_value(value[key], sink)
        sink.write_text("}")
        return
    if isinstance(value, (list, tuple)):
        _write_array(value, sink, on_page=None)
        return
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _batch_text(batch: list[Any]) -> str:
    # ``_dumps_scalar`` already ran the jsonb-compatibility scan over the
    # batch text (whole records, so escape runs never straddle a boundary).
    return _dumps_scalar(batch)[1:-1]


def _encoded_size_bound(value: Any, budget: int) -> int | None:
    """Conservatively bound an ordinary value *before* calling the C codec.

    Stop inspecting once the byte budget is spent. In particular, an oversized
    string needs only its length checked; measuring it must not encode it first.
    Twelve ASCII bytes per code point covers ensure_ascii's surrogate pairs.
    """
    if budget < 2:
        return None
    if isinstance(value, str):
        size = 2 + 12 * len(value)
    elif value is None or isinstance(value, bool):
        size = 5
    elif isinstance(value, int):
        size = 2 + value.bit_length() // 3
    elif isinstance(value, float):
        size = 32
    elif isinstance(value, (dict, list, tuple)):
        size = 2
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    return None
                key_size = 2 + 12 * len(key)
                item_size = _encoded_size_bound(item, budget - size - key_size - 2)
                if item_size is None:
                    return None
                size += key_size + item_size + 2
        else:
            for item in value:
                item_size = _encoded_size_bound(item, budget - size - 1)
                if item_size is None:
                    return None
                size += item_size + 1
    else:
        return None
    return size if size <= budget else None


def _write_array(
    items: Iterable[Any],
    sink: _ChunkSink,
    *,
    on_page: Callable[[int, int], None] | None,
) -> int:
    """Write one JSON array in bounded batches; returns the item count.

    ``on_page(offset, record_index)`` fires at the start of every batch.
    """
    sink.write_text("[")
    batch: list[Any] = []
    batch_bytes = 0
    first = True
    record_index = 0

    def flush(batch: list[Any]) -> None:
        nonlocal first, record_index
        if not first:
            sink.write_text(",")
        first = False
        if on_page is not None:
            on_page(sink.offset, record_index)
        text = _batch_text(batch)
        sink.write_text(text)
        record_index += len(batch)

    for item in items:
        size = _encoded_size_bound(item, CODEC_BATCH_TARGET_BYTES - 2)
        if batch and (size is None or batch_bytes + size + 1 > CODEC_BATCH_TARGET_BYTES - 2):
            flush(batch)
            batch = []
            batch_bytes = 0
        if size is None:
            if not first:
                sink.write_text(",")
            first = False
            if on_page is not None:
                on_page(sink.offset, record_index)
            _write_value(item, sink)
            record_index += 1
            continue
        batch.append(item)
        batch_bytes += size + 1
        if len(batch) >= CODEC_BATCH_RECORDS:
            flush(batch)
            batch = []
            batch_bytes = 0
    if batch:
        flush(batch)
    sink.write_text("]")
    return record_index


def _canonical_share_pages(shares: Any) -> Iterator[tuple[bytes, int | None]] | None:
    """Already-encoded share pages, when the sequence can supply them.

    Returns ``None`` for a sequence that has to be iterated and re-encoded.
    Three shapes are consumed verbatim:

    * the ledger's page-backed window (``pages[*].canonical_json_items``),
      whose bytes CPython's own encoder produced from the very records the
      sequence iterates;
    * a spool body written by this codec (``canonical_share_pages``);
    * the daemon mirror (``canonical_items`` + ``record_count``), whose
      items stream is the canonical encoding the mirror reconciled against
      its record count and digest at construction. Copying it is the only
      way to stage a daemon-owned window without the sequence's whole
      parse (its ``__iter__`` on the current tree materializes every
      record); wave B of #254 keeps the same bytes contract. Record
      boundaries are not known for this shape, so its page entries are
      scanned *hints* that a reader validates by decoding.
    """
    pages_method = getattr(shares, "canonical_share_pages", None)
    if callable(pages_method):
        return pages_method()
    canonical_items = getattr(shares, "canonical_items", None)
    if isinstance(canonical_items, bytes) and isinstance(
        getattr(shares, "record_count", None), int
    ):

        def iter_daemon_items() -> Iterator[tuple[bytes, int | None]]:
            # One raw slice: the sink consumes it linearly and the digest
            # update releases the GIL for a buffer this size. No copy of
            # the stream is made beyond the chunk pieces themselves.
            yield canonical_items, None

        return iter_daemon_items()
    pages = getattr(shares, "pages", None)
    if isinstance(pages, tuple) and all(
        isinstance(getattr(page, "canonical_json_items", None), bytes)
        and isinstance(getattr(page, "prism_json_records", None), tuple)
        for page in pages
    ):

        def iter_pages() -> Iterator[tuple[bytes, int | None]]:
            for page in pages:
                if page.canonical_json_items:
                    yield page.canonical_json_items, len(page.prism_json_records)

        return iter_pages()
    return None


_JSON_STRUCTURE = re.compile(rb'["\\{}\[\],]')


class _RecordPageScanner:
    """Locate exact top-level separators using bounded byte scans.

    Quoted text and nested arrays/objects cannot become page boundaries.
    State persists across source chunks, including a split escape pair.
    """

    def __init__(self, on_page: Callable[[int, int], None]) -> None:
        self.on_page = on_page
        self.started = False
        self.in_string = False
        self.depth = 0
        self.escape_at = -2
        self.record_index = 0
        self.page_index = 0
        self.page_start = 0

    def feed(self, data: bytes, base_offset: int) -> None:
        if not data:
            return
        if not self.started:
            self.started = True
            self.page_start = base_offset
            self.on_page(base_offset, 0)
        for offset in range(0, len(data), SPOOL_READ_BYTES):
            piece = data[offset:offset + SPOOL_READ_BYTES]
            for match in _JSON_STRUCTURE.finditer(piece):
                position = base_offset + offset + match.start()
                token = match[0]
                if self.in_string:
                    if position == self.escape_at + 1:
                        self.escape_at = -2
                    elif token == b"\\":
                        self.escape_at = position
                    elif token == b'"':
                        self.in_string = False
                elif token == b'"':
                    self.in_string = True
                elif token in (b"{", b"["):
                    self.depth += 1
                elif token in (b"}", b"]"):
                    self.depth -= 1
                    if self.depth < 0:
                        raise CandidateBodyIntegrityError("array source has unmatched delimiters")
                elif token == b"," and self.depth == 0:
                    self.record_index += 1
                    if (self.record_index - self.page_index >= CODEC_BATCH_RECORDS
                            or position + 1 - self.page_start >= CODEC_BATCH_TARGET_BYTES):
                        self.page_start = position + 1
                        self.page_index = self.record_index
                        self.on_page(self.page_start, self.page_index)

    def finish(self) -> int:
        if self.in_string or self.depth:
            raise CandidateBodyIntegrityError("array source ends inside a record")
        return self.record_index + int(self.started)


def _write_sequence(sequence: Any, sink: _ChunkSink, events: _IndexEvents) -> tuple[int, bool]:
    """Copy canonical items with exact page boundaries and no decoded mirror."""
    pages = _canonical_share_pages(sequence)
    if pages is None:
        return _write_array(iter(sequence), sink, on_page=events.page), True
    sink.write_text("[")
    scanner = _RecordPageScanner(events.page)
    first = True
    raw_mode = False
    for data, record_count in pages:
        if not data:
            continue
        if not first and (record_count is not None or not raw_mode):
            scanner.feed(b",", sink.offset)
            sink.write_text(",")
        raw_mode = record_count is None
        first = False
        scanner.feed(data, sink.offset)
        sink.write_bytes(data)
    count = scanner.finish()
    if count != len(sequence):
        raise CandidateBodyIntegrityError("array pages disagree with the sequence length")
    sink.write_text("]")
    return count, True


def _write_shares(shares: Any, sink: _ChunkSink, events: _IndexEvents) -> tuple[int, int, int, bool]:
    """Write the share array; returns (start, end, count, pages_exact)."""
    start = sink.offset
    events.begin(CANDIDATE_SHARES_KEY, "array", start)
    count, pages_exact = _write_sequence(shares, sink, events)
    end = sink.offset
    events.end(end, item_count=count, pages_exact=pages_exact, emit=True)
    return start, end, count, pages_exact


def _write_top_level_field(key: str, value: Any, sink: _ChunkSink, events: _IndexEvents) -> None:
    """Write one non-share top-level field, spanning it when it is large.

    Any sequence that is not a string is an array here -- a list, a tuple,
    or a spool-backed array adapter from a hydrated body -- so a hydrated
    intent re-stages without materializing its large fields.
    """
    start = sink.offset
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        events.begin(key, "array", start)
        count, pages_exact = _write_sequence(value, sink, events)
        end = sink.offset
        events.end(end, item_count=count, pages_exact=pages_exact, emit=(end - start) > CODEC_FAST_PATH_BYTES)
        return
    if isinstance(value, str):
        events.begin(key, "string", start)
        _write_string(value, sink)
        end = sink.offset
        events.end(end, item_count=0, pages_exact=True, emit=(end - start) > CODEC_FAST_PATH_BYTES)
        return
    events.begin(key, "value", start)
    _write_value(value, sink)
    end = sink.offset
    events.end(end, item_count=0, pages_exact=True, emit=(end - start) > CODEC_FAST_PATH_BYTES)


def encode_identity_body(
    identity: Mapping[str, Any],
    shares: Any,
    *,
    chunk_bytes: int = CANDIDATE_BODY_CHUNK_BYTES,
    on_chunk: Callable[[BodyChunk], None] | None = None,
    on_span: Callable[[FieldSpan], None] | None = None,
    on_page: Callable[[list[PageEntry]], None] | None = None,
) -> CandidateBodyManifest:
    """Encode ``{**identity, "shares_json": shares}`` in bounded pieces.

    ``identity`` is the small-field mapping with the acknowledgment stamp
    already neutralized. The concatenated chunks handed to ``on_chunk`` are
    byte-identical to ``json.dumps({**identity, "shares_json":
    list(shares)}, sort_keys=True, separators=(",", ":"))``. ``on_span`` and
    ``on_page`` receive the index events; the returned manifest is scalar.
    """
    if shares is not None and (
        isinstance(shares, (str, bytes)) or not isinstance(shares, Sequence)
    ):
        raise CandidateCodecError("shares_json must be a sequence of records")
    keys = list(identity)
    if CANDIDATE_SHARES_KEY in keys:
        raise CandidateCodecError("identity fields must not carry shares_json")
    if any(not isinstance(key, str) for key in keys):
        raise CandidateCodecError("candidate intent keys must be strings")
    if shares is not None:
        keys.append(CANDIDATE_SHARES_KEY)
    keys.sort()
    sink = _ChunkSink(chunk_bytes, on_chunk)
    events = _IndexEvents(on_span, on_page)
    sink.write_text("{")
    first = True
    shares_start = shares_end = 0
    share_count = 0
    for key in keys:
        sink.write_text(("" if first else ",") + _dumps_key(key) + ":")
        first = False
        if key == CANDIDATE_SHARES_KEY:
            shares_start, shares_end, share_count, _exact = _write_shares(shares, sink, events)
        else:
            _write_top_level_field(key, identity[key], sink, events)
    sink.write_text("}")
    digest = sink.finish()
    manifest = CandidateBodyManifest(
        storage_version=CANDIDATE_BODY_STORAGE_VERSION,
        candidate_sha256=digest,
        byte_count=sink.offset,
        chunk_count=sink.chunk_count,
        chunk_bytes=int(chunk_bytes),
        share_count=share_count,
        shares_offset=shares_start,
        shares_end=shares_end,
        span_count=events.span_count,
        page_count=events.page_count,
    )
    manifest.validate()
    return manifest


# --------------------------------------------------------------------------
# identity normalization and replay header
# --------------------------------------------------------------------------


def split_candidate_fields(
    fields: Mapping[str, Any],
) -> tuple[dict[str, Any], Any]:
    """Return ``(small facts, shares sequence or None)`` without copying.

    ``None`` means the intent carries no ``shares_json`` key at all (a
    candidate-only intent as some tools persist); the historical identity
    JSON of such a document has no share member and is reproduced as is.
    """
    if not isinstance(fields, Mapping):
        raise TypeError("block candidate intent must be an object")
    facts = {
        str(key): value
        for key, value in fields.items()
        if key != CANDIDATE_SHARES_KEY
    }
    return facts, fields.get(CANDIDATE_SHARES_KEY) if CANDIDATE_SHARES_KEY in fields else None


def neutralized_identity_facts(facts: Mapping[str, Any]) -> tuple[dict[str, Any], bool, Any]:
    """Apply ``block_candidate_identity``'s stamp rule to the small facts.

    Returns ``(identity facts, stamp present, original stamp)``. The rule
    is exactly the historical one: only when ``pending_share`` is a mapping
    that carries ``accepted_at_ms`` is that one value replaced by ``None``.
    """
    pending_share = facts.get("pending_share")
    if isinstance(pending_share, dict) and "accepted_at_ms" in pending_share:
        identity = {
            **facts,
            "pending_share": {**pending_share, "accepted_at_ms": None},
        }
        return identity, True, pending_share["accepted_at_ms"]
    return dict(facts), False, None


def restore_pending_stamp(
    facts: Mapping[str, Any],
    *,
    present: bool,
    accepted_at_ms: Any,
) -> dict[str, Any]:
    """Undo :func:`neutralized_identity_facts` for a body read back."""
    restored = dict(facts)
    pending_share = restored.get("pending_share")
    if present and isinstance(pending_share, dict):
        restored["pending_share"] = {**pending_share, "accepted_at_ms": accepted_at_ms}
    return restored


def _bounded_text(value: Any, limit: int) -> Any:
    """A string fact within ``limit`` bytes of JSON text, else None."""
    if not isinstance(value, str) or len(value) > limit:
        return None
    if len(json.dumps(value)) > limit:
        return None
    return value


def _bounded_number(value: Any, limit: int) -> Any:
    """An int/float fact within ``limit`` bytes of JSON text, else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if len(json.dumps(value)) > limit:
        return None
    return value


_PENDING_SHARE_TEXT_FIELDS = ("share_id", "miner_id", "order_key", "p2mr_program_hex", "job_id", "credit_policy")
_PENDING_SHARE_NUMBER_FIELDS = (
    "share_difficulty",
    "network_difficulty",
    "template_height",
    "job_issued_at_ms",
    "accepted_at_ms",
    "ntime",
)


def replay_header_from_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """The small typed summary an outbox row carries beside its body.

    Explicit projection with per-fact byte bounds, never subtraction:
    every key is named and every fact is either within its bound or
    replaced by ``None`` with ``oversized`` set, so a page of headers has a
    provable maximum size regardless of what the body holds. An oversized
    fact is deferred to the body (hydration reads the real value); nothing
    is rejected here. ``pending_share`` keeps the original acknowledgment
    stamp. Mirrors ``qbit_prism_bounded_replay_header`` in SQL.
    """
    template = fields.get("template")
    found_block = fields.get("found_block")
    pending_share = fields.get("pending_share")
    oversized = False

    def text(value: Any, limit: int = HEADER_TEXT_FIELD_BYTES) -> Any:
        nonlocal oversized
        bounded = _bounded_text(value, limit)
        if bounded is None and value is not None:
            oversized = True
        return bounded

    def number(value: Any, limit: int = HEADER_NUMERIC_FIELD_BYTES) -> Any:
        nonlocal oversized
        bounded = _bounded_number(value, limit)
        if bounded is None and value is not None:
            oversized = True
        return bounded

    bounded_pending: dict[str, Any] | None = None
    if isinstance(pending_share, Mapping):
        bounded_pending = {}
        for key, value in pending_share.items():
            if not isinstance(key, str) or len(key) > 64 or len(key.encode("utf-8")) > 64:
                oversized = True
                continue
            if key in _PENDING_SHARE_NUMBER_FIELDS:
                bounded_pending[str(key)] = number(value)
            elif key in _PENDING_SHARE_TEXT_FIELDS:
                bounded_pending[str(key)] = text(value)
            elif isinstance(value, str):
                bounded_pending[str(key)] = text(value)
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                bounded_pending[str(key)] = number(value)
            elif value is None or isinstance(value, bool):
                bounded_pending[str(key)] = value
            else:
                oversized = True
                bounded_pending[str(key)] = None
            if len(bounded_pending) > 32:
                oversized = True
                break
    elif pending_share is not None:
        oversized = True
    header = {
        "schema": text(fields.get("schema")),
        "block_hash_hex": text(fields.get("block_hash_hex"), HEADER_HASH_FIELD_BYTES + 2),
        "parent_hash": text(fields.get("parent_hash"), HEADER_HASH_FIELD_BYTES + 2),
        "expected_height": number(fields.get("expected_height")),
        "template": (
            {
                "previousblockhash": text(template.get("previousblockhash"), HEADER_HASH_FIELD_BYTES + 2),
                "height": number(template.get("height")),
                "coinbasevalue": number(template.get("coinbasevalue")),
            }
            if isinstance(template, Mapping)
            else None
        ),
        "found_block": (
            {"network_difficulty": number(found_block.get("network_difficulty"))}
            if isinstance(found_block, Mapping)
            else None
        ),
        "pending_share": bounded_pending,
        "credit_share_on_accept": (
            fields.get("credit_share_on_accept")
            if isinstance(fields.get("credit_share_on_accept"), bool)
            else None
        ),
        "collection_only": (
            fields.get("collection_only")
            if isinstance(fields.get("collection_only"), bool)
            else None
        ),
        "username": text(fields.get("username")),
        "accepted_at_present": bool(
            isinstance(pending_share, Mapping) and "accepted_at_ms" in pending_share
        ),
        "oversized": oversized,
    }
    return header


# --------------------------------------------------------------------------
# on-disk page index
# --------------------------------------------------------------------------


class SpoolFieldIndex:
    """Spans and page entries of one spool body, kept on disk.

    The index file holds every field's page entries as fixed 16-byte
    records (``<QQ``: body offset, record index) in one contiguous region
    per field. In memory there is one small :class:`FieldSpan` plus a region
    start per large field -- a handful of entries -- never the page list.
    """

    __slots__ = ("path", "_spans", "_regions", "_lock")

    def __init__(self, path: str) -> None:
        self.path = path
        self._spans: dict[str, FieldSpan] = {}
        self._regions: dict[str, int] = {}
        self._lock = threading.Lock()

    @classmethod
    def create(cls, path: str) -> SpoolFieldIndex:
        with open(path, "wb"):
            pass
        return cls(path)

    @classmethod
    def open_written(cls, path: str, spans_in_order: Sequence[FieldSpan]) -> SpoolFieldIndex:
        """Attach to an index file another process wrote span by span.

        The writer appended each field's region in the order it began the
        spans, so the regions are recovered from the page counts alone and
        the file size is checked against them.
        """
        index = cls(path)
        position = 0
        for span in spans_in_order:
            index._spans[span.field] = span
            index._regions[span.field] = position
            position += span.page_count * _INDEX_RECORD.size
        if os.path.getsize(path) != position:
            raise CandidateBodyIntegrityError("page index size disagrees with its spans")
        return index

    def spans(self) -> dict[str, FieldSpan]:
        with self._lock:
            return dict(self._spans)

    def span(self, field_name: str) -> FieldSpan | None:
        with self._lock:
            return self._spans.get(field_name)

    def begin_field(self, span: FieldSpan) -> None:
        """Declare a field's span.

        The encoder emits a field's page batches *before* its span (the
        span is known only when the field ends), so the region start is
        pinned by whichever comes first: the first page append, or this
        declaration for a field with no pages.
        """
        with self._lock:
            if span.field in self._spans:
                raise CandidateBodyIntegrityError("duplicate body span")
            self._spans[span.field] = span
            self._regions.setdefault(span.field, os.path.getsize(self.path))

    def append_pages(self, field_name: str, entries: Iterable[tuple[int, int]]) -> None:
        """Append ``(offset, record_index)`` records for the field being written."""
        with self._lock:
            self._regions.setdefault(field_name, os.path.getsize(self.path))
            packed = b"".join(_INDEX_RECORD.pack(int(offset), int(index)) for offset, index in entries)
            with open(self.path, "ab") as handle:
                handle.write(packed)

    def page_count(self, field_name: str) -> int:
        return self._spans[field_name].page_count

    def entry(self, field_name: str, ordinal: int) -> tuple[int, int]:
        span = self._spans[field_name]
        if not (0 <= ordinal < span.page_count):
            raise IndexError(ordinal)
        position = self._regions[field_name] + ordinal * _INDEX_RECORD.size
        with open(self.path, "rb") as handle:
            handle.seek(position)
            raw = handle.read(_INDEX_RECORD.size)
        if len(raw) != _INDEX_RECORD.size:
            raise CandidateBodyIntegrityError("page index is truncated")
        offset, record_index = _INDEX_RECORD.unpack(raw)
        return int(offset), int(record_index)

    def set_record_index(self, field_name: str, ordinal: int, record_index: int) -> None:
        """Correct one hint's record index after an exact decode."""
        offset, _ = self.entry(field_name, ordinal)
        position = self._regions[field_name] + ordinal * _INDEX_RECORD.size
        with self._lock, open(self.path, "r+b") as handle:
            handle.seek(position)
            handle.write(_INDEX_RECORD.pack(int(offset), int(record_index)))

    def bisect_record(self, field_name: str, record_index: int) -> int:
        """The page ordinal holding ``record_index`` (exact pages only)."""
        low, high = 0, self._spans[field_name].page_count
        while high - low > 1:
            middle = (low + high) // 2
            if self.entry(field_name, middle)[1] <= record_index:
                low = middle
            else:
                high = middle
        return low

    def mark_exact(self, field_name: str) -> None:
        with self._lock:
            span = self._spans[field_name]
            self._spans[field_name] = replace(span, pages_exact=True)

    def close(self) -> None:
        try:
            os.unlink(self.path)
        except OSError:
            pass


# --------------------------------------------------------------------------
# body handles
# --------------------------------------------------------------------------


class CandidateBody:
    """Replayable immutable body bytes plus the manifest describing them.

    Chunks are delivered push-style, one at a time, to a consumer callable:
    every consumer this codebase has (validation, storage upload, spool
    write, helper pipe) is push-shaped, and a push interface keeps at most
    one chunk plus the encoder's bounded batch alive at any instant. Index
    events (spans, page batches) ride the same pass.
    """

    manifest: CandidateBodyManifest

    def write_chunks(
        self,
        consumer: Callable[[BodyChunk], None],
        *,
        on_span: Callable[[FieldSpan], None] | None = None,
        on_page: Callable[[list[PageEntry]], None] | None = None,
    ) -> None:
        raise NotImplementedError

    def close(self) -> None:
        """Release what this handle owns; idempotent."""


class SourceCandidateBody(CandidateBody):
    """A live body: re-encoded from the pinned source sequence on demand.

    Nothing encoded is retained between passes. Every pass re-derives the
    bytes, and the rolling digest of what was handed out must equal the
    manifest's digest, so a source that changed underneath is detected
    before the caller seals anything.
    """

    __slots__ = ("manifest", "_identity", "_shares")

    def __init__(
        self,
        identity: Mapping[str, Any],
        shares: Any,
        manifest: CandidateBodyManifest,
    ) -> None:
        self.manifest = manifest
        self._identity = identity
        self._shares = shares

    def write_chunks(
        self,
        consumer: Callable[[BodyChunk], None],
        *,
        on_span: Callable[[FieldSpan], None] | None = None,
        on_page: Callable[[list[PageEntry]], None] | None = None,
    ) -> None:
        manifest = encode_identity_body(
            self._identity,
            self._shares,
            chunk_bytes=self.manifest.chunk_bytes,
            on_chunk=consumer,
            on_span=on_span,
            on_page=on_page,
        )
        if manifest != self.manifest:
            raise CandidateBodyIntegrityError(
                "candidate body source changed after validation"
            )

    def close(self) -> None:
        self._shares = ()
        self._identity = {}


class SpoolCandidateBody(CandidateBody):
    """A durable body materialized to a local spool file with a page index.

    The spool holds exactly the body bytes and the index its spans and
    pages. Chunk iteration re-reads and re-hashes the file, so a reader
    never trusts a stale manifest over the bytes on disk. Both files are
    unlinked when the last owner releases the handle (a ``weakref.finalize``
    on the handle, plus explicit ``close``), never while a sequence still
    reads them.
    """

    __slots__ = ("manifest", "path", "index", "_finalizer")

    def __init__(
        self,
        path: str,
        manifest: CandidateBodyManifest,
        index: SpoolFieldIndex,
        *,
        release: Callable[[], None] | None = None,
    ) -> None:
        self.manifest = manifest
        self.path = path
        self.index = index
        self._finalizer = weakref.finalize(self, _unlink_spool, path, index.path, release)

    def iter_chunks(self) -> Iterator[BodyChunk]:
        chunk_bytes = self.manifest.chunk_bytes
        digest = hashlib.sha256()
        with open(self.path, "rb") as handle:
            ordinal = 0
            while True:
                data = handle.read(chunk_bytes)
                if not data:
                    break
                if ordinal >= self.manifest.chunk_count:
                    raise CandidateBodyIntegrityError("spool body is longer than its manifest")
                digest.update(data)
                yield BodyChunk(ordinal=ordinal, data=data, sha256=hashlib.sha256(data).hexdigest())
                ordinal += 1
        if ordinal != self.manifest.chunk_count or digest.hexdigest() != self.manifest.candidate_sha256:
            raise CandidateBodyIntegrityError("spool body disagrees with its manifest")

    def write_chunks(
        self,
        consumer: Callable[[BodyChunk], None],
        *,
        on_span: Callable[[FieldSpan], None] | None = None,
        on_page: Callable[[list[PageEntry]], None] | None = None,
    ) -> None:
        for chunk in self.iter_chunks():
            consumer(chunk)
        if on_span is None and on_page is None:
            return
        for field_name, span in sorted(self.index.spans().items()):
            if on_span is not None:
                on_span(span)
            if on_page is None:
                continue
            batch: list[PageEntry] = []
            for ordinal in range(span.page_count):
                offset, record_index = self.index.entry(field_name, ordinal)
                batch.append(PageEntry(field=field_name, ordinal=ordinal, offset=offset, record_index=record_index))
                if len(batch) >= INDEX_PAGE_ENTRIES:
                    on_page(batch)
                    batch = []
            if batch:
                on_page(batch)

    def read_span(self, start: int, end: int) -> bytes:
        if start < 0 or end < start or end > self.manifest.byte_count:
            raise ValueError("spool span out of range")
        with open(self.path, "rb") as handle:
            handle.seek(start)
            data = handle.read(end - start)
        if len(data) != end - start:
            raise CandidateBodyIntegrityError("spool body is shorter than its manifest")
        return data

    def close(self) -> None:
        self._finalizer()

    @property
    def alive(self) -> bool:
        return self._finalizer.alive


def _unlink_spool(path: str, index_path: str, release: Callable[[], None] | None) -> None:
    for target in (path, index_path):
        try:
            os.unlink(target)
        except OSError:
            pass
    if release is not None:
        try:
            release()
        except Exception:  # noqa: BLE001 - finalizers must not raise
            pass


# --------------------------------------------------------------------------
# spool decoding
# --------------------------------------------------------------------------


def _views() -> Any:
    """The lazy-view module (``candidate_spool_view``), imported on first use.

    That module builds on this one and on the audit adapters' retryable
    resource-pressure class, so the import is deferred to keep this module
    a standard-library leaf at import time.
    """
    from lab.prism import candidate_spool_view

    return candidate_spool_view


def _walk_record_batches(
    body: SpoolCandidateBody,
    start: int,
    end: int,
    *,
    batch_records: int = SPOOL_WALK_BATCH_RECORDS,
) -> Iterator[tuple[int, int, tuple[Any, ...]]]:
    """``(batch_start, batch_end, records)`` over ``body[start:end]``.

    The range holds records joined by ``,``. Record boundaries come from the
    structural byte scanner, so no record is decoded to find its end, and
    each record follows the view module's decode policy: one bounded
    ``json.loads`` when it fits :data:`~lab.prism.candidate_spool_view.SPOOL_VIEW_DECODE_BYTES`,
    a lazy view when it does not. There is no per-record size ceiling.
    """
    views = _views()
    batch: list[Any] = []
    batch_start = start
    batch_end = start
    for item_start, item_end in views.iter_item_spans(body, start, end):
        if not batch:
            batch_start = item_start
        batch.append(views.decode_body_span(body, item_start, item_end))
        batch_end = item_end
        if len(batch) >= batch_records:
            yield batch_start, batch_end, tuple(batch)
            batch = []
    if batch:
        yield batch_start, batch_end, tuple(batch)


def _walk_records(
    body: SpoolCandidateBody,
    start: int,
    end: int,
    *,
    batch_records: int = SPOOL_WALK_BATCH_RECORDS,
) -> Iterator[tuple[Any, ...]]:
    """Decode ``body[start:end]`` (records joined by ``,``) in bounded batches."""
    for _batch_start, _batch_end, records in _walk_record_batches(
        body, start, end, batch_records=batch_records
    ):
        yield records


class SpoolJsonArraySequence(Sequence):
    """One large JSON array field decoded page by page from a spool body.

    Iteration decodes one page per ``json.loads`` from the on-disk page
    index while the page fits the view module's decode threshold; a larger
    page (one holding an oversized record) is split by the structural
    scanner and each record follows the decode policy, so an oversized
    record is a lazy view rather than a whole decode or a rejection. Random
    access bisects the index with ``pread``. A span whose page entries are
    not exact (legacy scanned hints) is walked sequentially once, ignoring
    the hints, and the exact page starts recovered by that walk serve random
    access from then on. Only the page last decoded is cached.
    """

    __slots__ = ("_body", "_span", "_lock", "_cached_page", "_cached_records", "_walk_index")

    def __init__(self, body: SpoolCandidateBody, span: FieldSpan) -> None:
        self._body = body
        self._span = span
        self._lock = threading.Lock()
        self._cached_page = -1
        self._cached_records: tuple[Any, ...] = ()
        self._walk_index: Any = None

    @property
    def body(self) -> SpoolCandidateBody:
        return self._body

    @property
    def span(self) -> FieldSpan:
        return self._body.index.span(self._span.field) or self._span

    @property
    def byte_range(self) -> tuple[int, int]:
        return self._span.start, self._span.end

    @property
    def byte_length(self) -> int:
        return self._span.end - self._span.start

    def __len__(self) -> int:
        return self._span.item_count

    # -- pages ------------------------------------------------------------------

    def _indexed(self) -> bool:
        """True when the on-disk page index can serve random access."""
        span = self.span
        return span.page_count > 0 and span.pages_exact

    def _index_entry(self, ordinal: int) -> tuple[int, int]:
        try:
            return self._body.index.entry(self._span.field, ordinal)
        except OSError as exc:
            raise CandidateBodyIntegrityError(f"spool page index is unreadable: {exc}") from exc

    def _page_span(self, page: int) -> tuple[int, int, int]:
        """(start, end, record_index) of one page; ``end`` excludes the separator."""
        start, record_index = self._index_entry(page)
        if page + 1 < self._span.page_count:
            end = self._index_entry(page + 1)[0] - 1
        else:
            end = self._span.end - 1
        if not (self._span.start < start <= end < self._span.end):
            raise CandidateBodyIntegrityError("spool page index points outside its span")
        return start, end, record_index

    def _decode_records(self, start: int, end: int) -> tuple[Any, ...]:
        """Records in ``body[start:end]``: one bounded call, or the walk."""
        views = _views()
        if end <= start:
            return ()
        if end - start <= views.SPOOL_VIEW_DECODE_BYTES:
            data = views.read_body_span(self._body, start, end)
            try:
                records = json.loads(b"[" + data + b"]")
            except ValueError as exc:
                raise CandidateBodyIntegrityError("spool page is not valid JSON") from exc
            return tuple(records)
        return tuple(
            record
            for _batch_start, _batch_end, batch in _walk_record_batches(self._body, start, end)
            for record in batch
        )

    def _decode_page(self, page: int) -> tuple[Any, ...]:
        with self._lock:
            if page == self._cached_page:
                return self._cached_records
        start, end, _ = self._page_span(page)
        decoded = self._decode_records(start, end)
        with self._lock:
            self._cached_page = page
            self._cached_records = decoded
        return decoded

    def _walk_pages(self, span: FieldSpan) -> Iterator[tuple[Any, ...]]:
        """Sequential walk of a span without a usable page index.

        Records the exact batch starts in a private bounded index so later
        random access bisects instead of walking again.
        """
        views = _views()
        with self._lock:
            index = None if self._walk_index is not None else views._OffsetIndex(2)
        running = 0
        try:
            for batch_start, _batch_end, records in _walk_record_batches(
                self._body, span.start + 1, span.end - 1
            ):
                if index is not None:
                    index.append(batch_start, running)
                running += len(records)
                yield records
            if running != span.item_count:
                raise CandidateBodyIntegrityError(
                    f"spool array holds {running} records where {span.item_count} were declared"
                )
        except BaseException:
            if index is not None:
                index.close()
            raise
        if index is not None:
            with self._lock:
                if self._walk_index is None:
                    self._walk_index = index
                    index = None
            if index is not None:
                index.close()

    def iter_pages(self) -> Iterator[tuple[Any, ...]]:
        """Decoded pages in order; each one bounded C call (or a walk)."""
        span = self.span
        if not self._indexed():
            yield from self._walk_pages(span)
            return
        running = 0
        for page in range(span.page_count):
            records = self._decode_page(page)
            running += len(records)
            yield records
        if running != span.item_count:
            raise CandidateBodyIntegrityError(
                f"spool array holds {running} records where {span.item_count} were declared"
            )

    # -- Sequence protocol ----------------------------------------------------------

    def __iter__(self) -> Iterator[Any]:
        for page in self.iter_pages():
            yield from page

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            return tuple(self[position] for position in range(start, stop, step))
        if isinstance(index, bool) or not isinstance(index, int):
            raise TypeError("spool array indices must be integers or slices")
        resolved = index + len(self) if index < 0 else index
        if not 0 <= resolved < len(self):
            raise IndexError(index)
        span = self.span
        if self._indexed():
            try:
                page = self._body.index.bisect_record(self._span.field, resolved)
            except OSError as exc:
                raise CandidateBodyIntegrityError(f"spool page index is unreadable: {exc}") from exc
            records = self._decode_page(page)
            first = self._index_entry(page)[1]
            return records[resolved - first]
        walk = self._walk_index
        if walk is None:
            for _page in self.iter_pages():
                pass
            walk = self._walk_index
            if walk is None:
                raise IndexError(index)
        low, high = 0, walk.count
        while high - low > 1:
            middle = (low + high) // 2
            if walk.get(middle)[1] <= resolved:
                low = middle
            else:
                high = middle
        batch_start, first = walk.get(low)
        batch_end = walk.get(low + 1)[0] - 1 if low + 1 < walk.count else span.end - 1
        records = self._decode_records(batch_start, batch_end)
        return records[resolved - first]

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        if isinstance(other, (str, bytes, bytearray)) or not isinstance(other, Sequence):
            return NotImplemented
        if len(self) != len(other):
            return False
        other_iter = iter(other)
        for item in self:
            try:
                candidate = next(other_iter)
            except StopIteration:
                return False
            if item != candidate:
                return False
        return True

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f"SpoolJsonArraySequence(field={self._span.field!r}, items={len(self)}, bytes={self.byte_length})"

    # -- verbatim bytes --------------------------------------------------------------

    def iter_byte_chunks(self, *, chunk_bytes: int | None = None) -> Iterator[bytes]:
        """The array's exact encoding as stored, brackets included."""
        return _views().iter_body_bytes(self._body, self._span.start, self._span.end, chunk_bytes=chunk_bytes)

    def iter_encoded_chunks(self) -> Iterator[str]:
        return _views().iter_body_text(self._body, self._span.start, self._span.end)

    def canonical_json_sha256(self) -> str:
        digest = hashlib.sha256()
        for chunk in self.iter_byte_chunks():
            digest.update(chunk)
        return digest.hexdigest()

    def canonical_share_pages(self) -> Iterator[tuple[bytes, int | None]]:
        """The encoded pages, verbatim, for re-staging this body.

        A page within the read size is one complete page with its record
        count; a larger page (an oversized record) and a span without a
        usable index stream as raw continuation pieces, which the encoder's
        structural scanner re-pages exactly. Nothing page-sized is held.
        """
        views = _views()
        span = self.span
        if not self._indexed():
            for chunk in views.iter_body_bytes(self._body, span.start + 1, span.end - 1):
                yield chunk, None
            return
        for page in range(span.page_count):
            start, end, record_index = self._page_span(page)
            if end - start > views.SPOOL_VIEW_READ_BYTES:
                for chunk in views.iter_body_bytes(self._body, start, end):
                    yield chunk, None
                continue
            next_index = (
                self._index_entry(page + 1)[1]
                if page + 1 < span.page_count
                else span.item_count
            )
            yield views.read_body_span(self._body, start, end), next_index - record_index

    def close(self) -> None:
        """Release the private walk index, if one was built; the body is untouched."""
        with self._lock:
            walk = self._walk_index
        if walk is not None:
            walk.close()


# Historical name kept for the share array.
SpoolShareJsonSequence = SpoolJsonArraySequence


def decode_json_string_span(body: SpoolCandidateBody, span: FieldSpan) -> str:
    """The whole text of one large JSON string field, decoded in slices.

    Every C call is one slice; the join is bounded only by the field, which
    is why hydration uses this for the consensus-bounded metadata fields
    alone and hands any other oversized string out as a streamed view.
    """
    return _views().decode_json_string(body, span.start, span.end)


def decode_spool_small_fields(body: SpoolCandidateBody) -> dict[str, Any]:
    """The body's fields other than the share array, bounded per field.

    Builds a *skeleton* of the body in which every spanned (large) field is
    replaced by an empty placeholder and decodes it with one ``json.loads``
    bounded by the number of small fields times the fast-path ceiling. Each
    large field then follows the view module's policy: an array becomes a
    :class:`SpoolJsonArraySequence`; a string in
    :data:`~lab.prism.candidate_spool_view.SPOOL_DECODED_METADATA_FIELDS`
    is decoded whole in slices (its size is bounded by consensus and its
    consumers need the text); any other value is a plain value within the
    decode threshold and a lazy view above it. Nothing is rejected for size.
    """
    views = _views()
    manifest = body.manifest
    spans = sorted(body.index.spans().values(), key=lambda span: span.start)
    skeleton = io.BytesIO()
    cursor = 0
    for span in spans:
        if span.start < cursor or span.end > manifest.byte_count:
            raise CandidateBodyIntegrityError("spool spans overlap or exceed the body")
        _copy_span(body, cursor, span.start, skeleton)
        skeleton.write(b"[]" if span.kind == "array" else b'""' if span.kind == "string" else b"null")
        cursor = span.end
    _copy_span(body, cursor, manifest.byte_count, skeleton)
    try:
        fields = json.loads(skeleton.getvalue())
    except ValueError as exc:
        raise CandidateBodyIntegrityError("spool body skeleton is not valid JSON") from exc
    if not isinstance(fields, dict):
        raise CandidateBodyIntegrityError("spool body does not decode to an intent")
    for span in spans:
        if span.field == CANDIDATE_SHARES_KEY:
            continue
        if span.kind == "array":
            fields[span.field] = SpoolJsonArraySequence(body, span)
        elif span.kind == "string" and span.field in views.SPOOL_DECODED_METADATA_FIELDS:
            fields[span.field] = views.decode_json_string(body, span.start, span.end)
        else:
            fields[span.field] = views.decode_body_span(body, span.start, span.end)
    fields.pop(CANDIDATE_SHARES_KEY, None)
    return fields


def _copy_span(body: SpoolCandidateBody, start: int, end: int, sink: io.BytesIO) -> None:
    for chunk in _views().iter_body_bytes(body, start, end):
        sink.write(chunk)


# --------------------------------------------------------------------------
# prepared intent
# --------------------------------------------------------------------------


class PreparedCandidateIntent(MutableMapping):
    """A validated candidate intent behind the historical mapping interface.

    ``facts`` holds every field but ``shares_json`` with the pending share's
    original acknowledgment stamp; ``shares`` is the immutable sequence
    (never a list copy); ``body`` streams the v1 identity bytes; the
    remaining attributes are the small typed facts storage needs. Mapping
    access (``intent["shares_json"]``, ``dict(intent)``, ``intent.get``)
    behaves as the historical dict did. Mutation (``pop``, item
    assignment) is supported for dict-facing callers and tools; it marks
    the intent *dirty*, and the next preparation re-derives the manifest
    from the mutated fields rather than trusting the stale one.
    """

    __slots__ = (
        "facts",
        "shares",
        "body",
        "block_hash",
        "candidate_sha256",
        "accepted_at_present",
        "accepted_at_ms",
        "_key_order",
        "dirty",
    )

    def __init__(
        self,
        *,
        facts: dict[str, Any],
        shares: Any,
        body: CandidateBody,
        accepted_at_present: bool,
        accepted_at_ms: Any,
        key_order: tuple[str, ...],
    ) -> None:
        self.facts = facts
        self.shares = shares
        self.body = body
        self.block_hash = str(facts.get("block_hash_hex", "")).lower()
        self.candidate_sha256 = body.manifest.candidate_sha256
        self.accepted_at_present = accepted_at_present
        self.accepted_at_ms = accepted_at_ms
        self._key_order = key_order
        self.dirty = False

    def __setitem__(self, key: str, value: Any) -> None:
        key = str(key)
        if key == CANDIDATE_SHARES_KEY:
            self.shares = value
        else:
            self.facts[key] = value
        if key not in self._key_order:
            self._key_order = (*self._key_order, key)
        self.dirty = True

    def __delitem__(self, key: str) -> None:
        if key == CANDIDATE_SHARES_KEY:
            if self.shares is None:
                raise KeyError(key)
            self.shares = None
        else:
            del self.facts[key]
        self._key_order = tuple(name for name in self._key_order if name != key)
        self.dirty = True

    @property
    def manifest(self) -> CandidateBodyManifest:
        return self.body.manifest

    @property
    def storage_version(self) -> int:
        return self.body.manifest.storage_version

    @property
    def has_shares(self) -> bool:
        return self.shares is not None

    def __getitem__(self, key: str) -> Any:
        if key == CANDIDATE_SHARES_KEY:
            if self.shares is None:
                raise KeyError(key)
            return self.shares
        return self.facts[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._key_order)

    def __len__(self) -> int:
        return len(self._key_order)

    def __contains__(self, key: object) -> bool:
        if key == CANDIDATE_SHARES_KEY:
            return self.shares is not None
        return key in self.facts

    def __repr__(self) -> str:
        return (
            f"PreparedCandidateIntent(block_hash={self.block_hash!r}, "
            f"candidate_sha256={self.candidate_sha256!r}, "
            f"shares={self.manifest.share_count}, bytes={self.manifest.byte_count})"
        )

    def replay_header(self) -> dict[str, Any]:
        """The bounded typed summary the outbox row carries beside the body."""
        return replay_header_from_fields({**self.facts, "block_hash_hex": self.block_hash})

    def release(self) -> None:
        """Drop the body's hold on its source; the facts stay readable."""
        self.body.close()


def prepare_candidate_intent(
    fields: Mapping[str, Any],
    *,
    chunk_bytes: int = CANDIDATE_BODY_CHUNK_BYTES,
) -> PreparedCandidateIntent:
    """Validate and index an intent mapping without materializing its body.

    One bounded encode pass runs now -- the synchronous validation that
    used to be a whole ``json.dumps`` -- producing the scalar manifest and
    discarding the bytes and index events. The returned intent re-encodes
    from the same immutable source whenever storage asks for chunks, and
    refuses to publish if the source changed.
    """
    if isinstance(fields, PreparedCandidateIntent):
        if not fields.dirty:
            return fields
        # Mutated after preparation: re-derive from the current fields.
        fields = dict(fields)
    facts, shares = split_candidate_fields(fields)
    block_hash = str(facts.get("block_hash_hex", "")).lower()
    if not block_hash:
        raise CandidateCodecError("block candidate is missing block_hash_hex")
    facts["block_hash_hex"] = block_hash
    identity, present, stamp = neutralized_identity_facts(facts)
    try:
        manifest = encode_identity_body(identity, shares, chunk_bytes=chunk_bytes)
    except CandidateCodecError:
        raise
    except TypeError:
        # Exactly the exception the historical whole-document json.dumps
        # raised for an unsupported value type; callers key on it.
        raise
    except ValueError as exc:
        raise CandidateCodecError(str(exc)) from exc
    key_order = tuple(str(key) for key in fields.keys())
    return PreparedCandidateIntent(
        facts=facts,
        shares=shares,
        body=SourceCandidateBody(identity, shares, manifest),
        accepted_at_present=present,
        accepted_at_ms=stamp,
        key_order=key_order,
    )


def prepared_intent_from_spool(
    body: SpoolCandidateBody,
    *,
    accepted_at_present: bool,
    accepted_at_ms: Any,
) -> PreparedCandidateIntent:
    """Rebuild the mapping view of a hydrated body.

    Facts stay a plain dict; each value follows the view module's decode
    policy. ``pending_share`` alone is shallow-copied into a dict when it
    hydrates as a lazy object, so the acknowledgment stamp is restored the
    historical way (its members keep the policy: an oversized ``share_id``
    is a streamed string).
    """
    identity = decode_spool_small_fields(body)
    pending_share = identity.get("pending_share")
    if (
        accepted_at_present
        and isinstance(pending_share, Mapping)
        and not isinstance(pending_share, dict)
    ):
        try:
            identity["pending_share"] = dict(pending_share.items())
        except BaseException:
            # The view's scratch index must not outlive this failure even
            # while a retained traceback keeps the view itself reachable.
            pending_share.close()
            raise
    facts = restore_pending_stamp(
        identity,
        present=accepted_at_present,
        accepted_at_ms=accepted_at_ms,
    )
    shares_span = body.index.span(CANDIDATE_SHARES_KEY)
    if shares_span is None:
        if body.manifest.share_count or body.manifest.shares_end > body.manifest.shares_offset:
            raise CandidateBodyIntegrityError("spool body carries no share span")
        shares = None
        keys = sorted(facts.keys())
    else:
        shares = SpoolJsonArraySequence(body, shares_span)
        keys = sorted([*facts.keys(), CANDIDATE_SHARES_KEY])
    return PreparedCandidateIntent(
        facts=facts,
        shares=shares,
        body=body,
        accepted_at_present=accepted_at_present,
        accepted_at_ms=accepted_at_ms,
        key_order=tuple(keys),
    )


class SpoolWriter:
    """Write verified chunks and index events to a spool + index pair."""

    def __init__(self, path: str, index_path: str, manifest: CandidateBodyManifest) -> None:
        self.manifest = manifest
        self.path = path
        self.index = SpoolFieldIndex.create(index_path)
        self._handle = open(path, "wb")
        self._digest = hashlib.sha256()
        self._written = 0
        self._ordinal = 0
        self._span_count = 0
        self._page_count = 0
        self._open_field: str | None = None

    def chunk(self, chunk: BodyChunk) -> None:
        manifest = self.manifest
        if chunk.ordinal != self._ordinal:
            raise CandidateBodyIntegrityError("spool chunks arrived out of order")
        if self._ordinal >= manifest.chunk_count:
            raise CandidateBodyIntegrityError("more chunks than the manifest declares")
        expected_length = (
            manifest.chunk_bytes
            if self._ordinal + 1 < manifest.chunk_count
            else manifest.byte_count - manifest.chunk_bytes * (manifest.chunk_count - 1)
        )
        if len(chunk.data) != expected_length:
            raise CandidateBodyIntegrityError("spool chunk length disagrees with the manifest")
        if hashlib.sha256(chunk.data).hexdigest() != chunk.sha256:
            raise CandidateBodyIntegrityError("spool chunk digest disagrees with its bytes")
        self._handle.write(chunk.data)
        self._digest.update(chunk.data)
        self._written += len(chunk.data)
        self._ordinal += 1

    def span(self, span: FieldSpan) -> None:
        self.index.begin_field(span)
        self._span_count += 1

    def pages(self, entries: Iterable[PageEntry]) -> None:
        batch = list(entries)
        if not batch:
            return
        field_name = batch[0].field
        if any(entry.field != field_name for entry in batch):
            raise CandidateBodyIntegrityError("page batch spans several fields")
        self.index.append_pages(field_name, ((entry.offset, entry.record_index) for entry in batch))
        self._page_count += len(batch)

    def finish(self, *, release: Callable[[], None] | None = None) -> SpoolCandidateBody:
        """Verify and hand over the one body handle that owns the files.

        Exactly one :class:`SpoolCandidateBody` is created here: its
        finalizer unlinks the spool pair, so a second handle on the same
        path would delete the files as soon as the first was dropped.
        """
        self._handle.close()
        manifest = self.manifest
        if self._ordinal != manifest.chunk_count or self._written != manifest.byte_count:
            raise CandidateBodyIntegrityError("spool body is incomplete")
        if self._digest.hexdigest() != manifest.candidate_sha256:
            raise CandidateBodyIntegrityError("spool body digest disagrees with the manifest")
        if self._span_count != manifest.span_count or self._page_count != manifest.page_count:
            raise CandidateBodyIntegrityError("spool index disagrees with the manifest")
        return SpoolCandidateBody(self.path, manifest, self.index, release=release)

    def abort(self) -> None:
        try:
            self._handle.close()
        finally:
            for target in (self.path, self.index.path):
                try:
                    os.unlink(target)
                except OSError:
                    pass


def write_spool_from_body(
    path: str,
    index_path: str,
    body: CandidateBody,
) -> SpoolCandidateBody:
    """Materialize any body (with its index events) to a spool pair."""
    writer = SpoolWriter(path, index_path, body.manifest)
    try:
        body.write_chunks(writer.chunk, on_span=writer.span, on_page=writer.pages)
        return writer.finish()
    except BaseException:
        writer.abort()
        raise


# --------------------------------------------------------------------------
# legacy (v1 jsonb) normalization -- runs in the isolated helper only
# --------------------------------------------------------------------------


def legacy_json_to_spool(
    text: str,
    path: str,
    index_path: str,
    *,
    chunk_bytes: int = CANDIDATE_BODY_CHUNK_BYTES,
    spool_limit_bytes: int | None = None,
) -> tuple[CandidateBodyManifest, list[FieldSpan], bool, Any]:
    """Normalize one legacy ``candidate`` JSON text into a spool body pair.

    Whole-document work by design: this runs only inside the compatibility
    helper process, never in the lease-bearing coordinator. Returns the
    scalar manifest, the span rows (one per large field), and the pending
    stamp's presence and original value; the page entries are in the index
    file.
    """
    document = json.loads(text)
    if not isinstance(document, dict):
        raise CandidateCodecError("legacy candidate is not an object")
    facts, shares = split_candidate_fields(document)
    identity, present, stamp = neutralized_identity_facts(facts)
    index = SpoolFieldIndex.create(index_path)
    spans: list[FieldSpan] = []
    written = 0

    def admit(size: int) -> None:
        nonlocal written
        if spool_limit_bytes is not None and written + size > spool_limit_bytes:
            raise OSError("legacy candidate spool reservation exhausted")
        written += size

    def on_span(span: FieldSpan) -> None:
        index.begin_field(span)
        spans.append(span)

    with open(path, "wb") as handle:
        def write_chunk(chunk: BodyChunk) -> None:
            admit(len(chunk.data))
            handle.write(chunk.data)

        def write_pages(batch: list[PageEntry]) -> None:
            admit(len(batch) * _INDEX_RECORD.size)
            index.append_pages(batch[0].field, ((entry.offset, entry.record_index) for entry in batch))

        manifest = encode_identity_body(
            identity,
            shares,
            chunk_bytes=chunk_bytes,
            on_chunk=write_chunk,
            on_span=on_span,
            on_page=write_pages,
        )
    return manifest, spans, present, stamp


def jsonb_equivalent(left: Any, right: Any) -> bool:
    """PostgreSQL ``jsonb`` equality over two decoded documents.

    Numbers compare as exact decimals (``1.0 = 1``, ``1e16 = 10^16``), the
    boolean/null/number/string/array/object type classes never cross, and
    objects compare as key sets regardless of order. Decode both sides with
    ``parse_float=decimal.Decimal`` for exactness.
    """
    from decimal import Decimal

    def kind(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, (int, float, Decimal)):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        raise TypeError(f"not a JSON value: {type(value).__name__}")

    left_kind = kind(left)
    if left_kind != kind(right):
        return False
    if left_kind == "number":
        return Decimal(str(left)) == Decimal(str(right))
    if left_kind in {"null", "bool", "string"}:
        return left == right
    if left_kind == "array":
        return len(left) == len(right) and all(
            jsonb_equivalent(a, b) for a, b in zip(left, right, strict=True)
        )
    if set(left) != set(right):
        return False
    return all(jsonb_equivalent(left[key], right[key]) for key in left)


__all__ = [
    "CANDIDATE_BODY_CHUNK_BYTES",
    "CANDIDATE_BODY_STORAGE_VERSION",
    "CANDIDATE_INTENT_SCHEMA",
    "CANDIDATE_SHARES_KEY",
    "CODEC_BATCH_RECORDS",
    "CODEC_BATCH_TARGET_BYTES",
    "CODEC_FAST_PATH_BYTES",
    "CODEC_STRING_SLICE_CHARS",
    "HEADER_TEXT_FIELD_BYTES",
    "INDEX_PAGE_ENTRIES",
    "INDEX_SPAN_ROWS",
    "LEGACY_CANDIDATE_STORAGE_VERSION",
    "BodyChunk",
    "CandidateBody",
    "CandidateBodyIntegrityError",
    "CandidateBodyManifest",
    "CandidateCodecError",
    "FieldSpan",
    "PageEntry",
    "PreparedCandidateIntent",
    "SourceCandidateBody",
    "SpoolCandidateBody",
    "SpoolFieldIndex",
    "SpoolJsonArraySequence",
    "SpoolShareJsonSequence",
    "SpoolWriter",
    "chunk_ordinals_for",
    "decode_json_string_span",
    "decode_spool_small_fields",
    "encode_identity_body",
    "jsonb_equivalent",
    "legacy_json_to_spool",
    "neutralized_identity_facts",
    "prepare_candidate_intent",
    "prepared_intent_from_spool",
    "replay_header_from_fields",
    "restore_pending_stamp",
    "split_candidate_fields",
    "write_spool_from_body",
]
