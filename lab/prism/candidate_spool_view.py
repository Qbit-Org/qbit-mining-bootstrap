#!/usr/bin/env python3
"""Bounded lazy views over a hydrated candidate spool body (issue #255).

A hydrated block-candidate body is the v1 identity JSON on a local spool
file. The historical layout puts no bound on any single value: a share's
``share_id``, the miner ``username`` or a ``recipient_id`` may be of any
length, and ``found_block`` may carry nested arrays. The first reader
decoded every value whole, so one oversized field became one unbounded
``json.loads`` call in the lease-bearing coordinator, and the size ceilings
that guarded those calls rejected valid bodies as corruption.

This module replaces the whole-value decode with one policy applied to
every JSON value in the body:

* a value whose encoding is at most :data:`SPOOL_VIEW_DECODE_BYTES` is
  decoded with one ``json.loads`` bounded by that size and returned as the
  plain Python value it always was;
* a larger value becomes a lazy view over its own byte range: a
  :class:`SpoolObjectView` (a ``Mapping``), a :class:`SpoolArrayView` (a
  ``Sequence``), a :class:`SpoolStringView` (a streamed string) or a
  :class:`SpoolRawValue` (an oversized scalar token). Members and items of
  a lazy container are decoded on access under the same policy, so the
  largest C call the reader ever makes is bounded by the threshold, at any
  nesting depth, for a body of any size.

Nothing is rejected for size. Every view exposes its original encoding
verbatim (``iter_byte_chunks``, ``iter_encoded_chunks``) so digests and
re-staging never need the decoded value, and a streamed string exposes its
text through ``iter_text_chunks`` in pieces that never split an escape or a
surrogate pair. The streaming protocol is the audit adapters' one
(``StreamedJsonString``, ``RawJsonDocument`` in ``audit_bundle_view``), so
the compact-body encoder embeds these views without materializing them.

Structure is located with a stateful byte scanner (quoted text and nested
containers are skipped, escape pairs and multi-byte UTF-8 sequences are
carried across read boundaries), and per-container item indices live in
memory only up to a small cap, spilling to an unlinked scratch file beyond
it. A scratch file that cannot be created is
:class:`~lab.prism.audit_bundle_view.ArtifactResourcePressure` (retryable,
never corruption); a corrupt, truncated or closed body is
:class:`~lab.prism.candidate_codec.CandidateBodyIntegrityError`.

Ownership: every view holds the :class:`SpoolCandidateBody` it reads and
nothing holds a view back, so there are no reference cycles and a dropped
view releases its scratch descriptor by reference count alone, with the
cyclic collector disabled. ``PreparedCandidateIntent.release()`` closes
the body; views read after that fail closed.
"""

from __future__ import annotations

import codecs
import hashlib
import json
import os
import re
import struct
import tempfile
import threading
import weakref
from collections.abc import ItemsView, Iterator, Mapping, Sequence, ValuesView
from typing import Any

from lab.prism.audit_bundle_view import ArtifactResourcePressure
from lab.prism.candidate_codec import (
    SPOOL_READ_BYTES,
    CandidateBodyIntegrityError,
    SpoolCandidateBody,
)

# Hard ceiling on the encoded size of any value decoded in-process with one
# ``json.loads``. A larger value is a lazy view, never a Python object.
SPOOL_VIEW_DECODE_BYTES = 1024 * 1024
# Bytes of encoded string text decoded per ``json.loads`` while streaming.
SPOOL_VIEW_STRING_SLICE_BYTES = 64 * 1024
# Bytes read from the spool per positional read while scanning or copying.
SPOOL_VIEW_READ_BYTES = SPOOL_READ_BYTES
# Item-index entries held in memory before the index spills to a scratch file.
SPOOL_VIEW_INDEX_MEMORY_ENTRIES = 256
# Object member keys are cached in memory only while the object has at most
# this many members, each key at most this many encoded bytes.
SPOOL_VIEW_KEY_CACHE_ENTRIES = 1024
SPOOL_VIEW_KEY_CACHE_BYTES = 4096
# Top-level string fields hydrated as ``str`` whatever their size, each
# decoded in bounded slices and joined. Every one has a bound of its own
# that is not this reader's to impose -- the coinbase transaction and the
# hashes by consensus (the block), the extranonces by the Stratum session,
# the schema by the protocol -- and its consumers need the text whole (hex
# parsing, txid computation, identity checks). The raw block is absent on
# purpose: consensus bounds it too, but a submitter can stream it, so above
# the threshold it stays a streamed view. This is the explicit decoded
# fixed-metadata boundary; nothing outside it is ever joined implicitly.
SPOOL_DECODED_METADATA_FIELDS = frozenset(
    {
        "schema",
        "block_hash_hex",
        "parent_hash",
        "coinbase_tx_hex",
        "extranonce1_hex",
        "extranonce2_hex",
    }
)
# Pending index bytes buffered before they are written to the scratch file.
_INDEX_FLUSH_BYTES = 64 * 1024

_STRUCTURAL = re.compile(rb'["\\{}\[\],:]')
_HIGH_SURROGATE_TAIL = re.compile(r"\\u[dD][89abAB][0-9a-fA-F]{2}$")

_QUOTE = 0x22
_BACKSLASH = 0x5C
_COMMA = 0x2C
_COLON = 0x3A
_OPEN = (0x7B, 0x5B)
_CLOSE = (0x7D, 0x5D)


# --------------------------------------------------------------------------
# bounded body access
# --------------------------------------------------------------------------


def read_body_span(body: SpoolCandidateBody, start: int, end: int) -> bytes:
    """Body bytes ``[start, end)``; a closed or unreadable body fails closed."""
    if end <= start:
        return b""
    if not body.alive:
        raise CandidateBodyIntegrityError("spool body is closed")
    try:
        return body.read_span(start, end)
    except ValueError as exc:
        raise CandidateBodyIntegrityError(f"spool span is out of range: {exc}") from exc
    except OSError as exc:
        raise CandidateBodyIntegrityError(f"spool body is unreadable: {exc}") from exc


def iter_body_bytes(
    body: SpoolCandidateBody,
    start: int,
    end: int,
    *,
    chunk_bytes: int | None = None,
) -> Iterator[bytes]:
    """Body bytes ``[start, end)`` in bounded pieces."""
    size = max(1, int(chunk_bytes if chunk_bytes is not None else SPOOL_VIEW_READ_BYTES))
    position = start
    while position < end:
        stop = min(end, position + size)
        yield read_body_span(body, position, stop)
        position = stop


def iter_body_text(body: SpoolCandidateBody, start: int, end: int) -> Iterator[str]:
    """Body bytes ``[start, end)`` decoded as UTF-8 text, chunk by chunk."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        for chunk in iter_body_bytes(body, start, end):
            text = decoder.decode(chunk)
            if text:
                yield text
        tail = decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise CandidateBodyIntegrityError(f"spool body is not valid UTF-8: {exc}") from exc
    if tail:
        yield tail


def _loads(data: bytes) -> Any:
    try:
        return json.loads(data)
    except ValueError as exc:
        raise CandidateBodyIntegrityError(f"spool value is not valid JSON: {exc}") from exc


# --------------------------------------------------------------------------
# structural scanner
# --------------------------------------------------------------------------


def _iter_top_level_tokens(
    body: SpoolCandidateBody,
    start: int,
    end: int,
) -> Iterator[tuple[int, int]]:
    """``(position, byte)`` of every ``,`` and ``:`` at depth 0 in ``[start, end)``.

    Quoted text (escape-aware, including an escape pair split by a read
    boundary) and nested containers are skipped; multi-byte UTF-8 never
    matches a structural byte, so raw UTF-8 text is safe. The range must
    end outside any string or container.
    """
    in_string = False
    escape_at = -2
    depth = 0
    position = start
    read_bytes = max(1, int(SPOOL_VIEW_READ_BYTES))
    while position < end:
        stop = min(end, position + read_bytes)
        piece = read_body_span(body, position, stop)
        for match in _STRUCTURAL.finditer(piece):
            at = position + match.start()
            token = piece[match.start()]
            if in_string:
                if at == escape_at + 1:
                    escape_at = -2
                elif token == _BACKSLASH:
                    escape_at = at
                elif token == _QUOTE:
                    in_string = False
            elif token == _QUOTE:
                in_string = True
            elif token in _OPEN:
                depth += 1
            elif token in _CLOSE:
                depth -= 1
                if depth < 0:
                    raise CandidateBodyIntegrityError(
                        f"spool value has unmatched delimiters at byte {at}"
                    )
            elif depth == 0:
                yield at, token
        position = stop
    if in_string or depth:
        raise CandidateBodyIntegrityError("spool value ends inside a string or container")


def iter_item_spans(
    body: SpoolCandidateBody,
    start: int,
    end: int,
) -> Iterator[tuple[int, int]]:
    """``(item_start, item_end)`` of the comma-joined values in ``[start, end)``.

    The range is an array body without its brackets or a page of records
    (the codec's page index points at exactly such ranges). An empty range
    holds no items.
    """
    if end <= start:
        return
    item_start = start
    for at, token in _iter_top_level_tokens(body, start, end):
        if token == _COMMA:
            yield item_start, at
            item_start = at + 1
        else:
            raise CandidateBodyIntegrityError(f"spool array holds a stray colon at byte {at}")
    yield item_start, end


def iter_member_spans(
    body: SpoolCandidateBody,
    start: int,
    end: int,
) -> Iterator[tuple[int, int, int, int]]:
    """``(key_start, key_end, value_start, value_end)`` of the members in ``[start, end)``.

    The range is an object body without its braces. An empty range holds
    no members.
    """
    if end <= start:
        return
    member_start = start
    colon = -1
    for at, token in _iter_top_level_tokens(body, start, end):
        if token == _COLON:
            if colon >= 0:
                raise CandidateBodyIntegrityError(f"spool object member has two colons at byte {at}")
            colon = at
        else:
            if colon < 0:
                raise CandidateBodyIntegrityError(f"spool object member has no colon at byte {at}")
            yield member_start, colon, colon + 1, at
            member_start = at + 1
            colon = -1
    if colon < 0:
        raise CandidateBodyIntegrityError("spool object member has no colon")
    yield member_start, colon, colon + 1, end


# --------------------------------------------------------------------------
# disk-spilling offset index
# --------------------------------------------------------------------------


class _OffsetIndex:
    """Append-only fixed-width offset entries, bounded in memory.

    Entries stay in a bytearray up to :data:`SPOOL_VIEW_INDEX_MEMORY_ENTRIES`
    and move to an unlinked scratch file beyond that, read back with
    ``pread``. The scratch descriptor closes with the last owner and on
    :meth:`close`; a scratch file that cannot be created or written is
    resource pressure, not corruption.
    """

    __slots__ = (
        "__weakref__",
        "_closed",
        "_count",
        "_file",
        "_finalizer",
        "_memory",
        "_memory_entries",
        "_pending",
        "_struct",
        "_written",
    )

    def __init__(self, fields: int, *, memory_entries: int | None = None) -> None:
        self._struct = struct.Struct("<" + "Q" * int(fields))
        self._memory_entries = int(
            SPOOL_VIEW_INDEX_MEMORY_ENTRIES if memory_entries is None else memory_entries
        )
        self._memory = bytearray()
        self._pending = bytearray()
        self._count = 0
        self._written = 0
        self._file: Any = None
        self._finalizer: weakref.finalize | None = None
        self._closed = False

    @property
    def count(self) -> int:
        return self._count

    @property
    def spilled(self) -> bool:
        return self._file is not None

    def fileno(self) -> int | None:
        return None if self._file is None else self._file.fileno()

    def append(self, *values: int) -> None:
        if self._closed:
            raise CandidateBodyIntegrityError("spool view index is closed")
        packed = self._struct.pack(*values)
        if self._file is None:
            if self._count < self._memory_entries:
                self._memory += packed
                self._count += 1
                return
            self._spill()
        self._pending += packed
        self._count += 1
        if len(self._pending) >= _INDEX_FLUSH_BYTES:
            self._flush()

    def _spill(self) -> None:
        try:
            handle = tempfile.TemporaryFile()
        except OSError as exc:
            raise ArtifactResourcePressure(
                f"cannot create the spool view index scratch file: {exc}"
            ) from exc
        self._file = handle
        self._finalizer = weakref.finalize(self, handle.close)
        self._pending = bytearray(self._memory)
        self._memory = bytearray()
        self._flush()

    def _flush(self) -> None:
        if not self._pending:
            return
        if self._file is None or self._file.closed:
            raise CandidateBodyIntegrityError("spool view index is closed")
        data = memoryview(bytes(self._pending))
        offset = self._written * self._struct.size
        fd = self._file.fileno()
        while data:
            try:
                written = os.pwrite(fd, data, offset)
            except InterruptedError:
                continue
            except OSError as exc:
                raise ArtifactResourcePressure(
                    f"cannot write the spool view index scratch file: {exc}"
                ) from exc
            data = data[written:]
            offset += written
        self._written += len(self._pending) // self._struct.size
        self._pending = bytearray()

    def get(self, index: int) -> tuple[int, ...]:
        if not 0 <= index < self._count:
            raise IndexError(index)
        if self._closed:
            raise CandidateBodyIntegrityError("spool view index is closed")
        if self._file is None:
            return self._struct.unpack_from(self._memory, index * self._struct.size)
        self._flush()
        if self._file.closed:
            raise CandidateBodyIntegrityError("spool view index is closed")
        position = index * self._struct.size
        try:
            raw = os.pread(self._file.fileno(), self._struct.size, position)
        except OSError as exc:
            raise ArtifactResourcePressure(
                f"cannot read the spool view index scratch file: {exc}"
            ) from exc
        if len(raw) != self._struct.size:
            raise CandidateBodyIntegrityError("spool view index is truncated")
        return self._struct.unpack(raw)

    def close(self) -> None:
        self._closed = True
        self._memory = bytearray()
        self._pending = bytearray()
        if self._finalizer is not None:
            self._finalizer()

    @property
    def closed(self) -> bool:
        return self._closed


# --------------------------------------------------------------------------
# views
# --------------------------------------------------------------------------


class _SpoolView:
    """One byte range of a body, exposed verbatim."""

    __slots__ = ("_body", "_end", "_start")

    def __init__(self, body: SpoolCandidateBody, start: int, end: int) -> None:
        self._body = body
        self._start = int(start)
        self._end = int(end)

    @property
    def body(self) -> SpoolCandidateBody:
        return self._body

    @property
    def byte_range(self) -> tuple[int, int]:
        return self._start, self._end

    @property
    def byte_length(self) -> int:
        return self._end - self._start

    def iter_byte_chunks(self, *, chunk_bytes: int | None = None) -> Iterator[bytes]:
        """The value's exact encoding as stored, in bounded pieces."""
        return iter_body_bytes(self._body, self._start, self._end, chunk_bytes=chunk_bytes)

    def iter_encoded_chunks(self) -> Iterator[str]:
        """The value's JSON encoding as text, in bounded pieces."""
        return iter_body_text(self._body, self._start, self._end)

    def canonical_json_sha256(self) -> str:
        digest = hashlib.sha256()
        for chunk in self.iter_byte_chunks():
            digest.update(chunk)
        return digest.hexdigest()

    def close(self) -> None:
        """Release scratch resources this view owns; the body is untouched."""

    __hash__ = None  # type: ignore[assignment]


def _text_stream_equals_str(chunks: Iterator[str], other: str) -> bool:
    offset = 0
    for chunk in chunks:
        stop = offset + len(chunk)
        if stop > len(other) or other[offset:stop] != chunk:
            return False
        offset = stop
    return offset == len(other)


def _text_streams_equal(left: Iterator[str], right: Iterator[str]) -> bool:
    left_buffer = ""
    right_buffer = ""
    left_done = right_done = False
    while True:
        while not left_buffer and not left_done:
            try:
                left_buffer = next(left)
            except StopIteration:
                left_done = True
        while not right_buffer and not right_done:
            try:
                right_buffer = next(right)
            except StopIteration:
                right_done = True
        if left_done or right_done:
            return left_done and right_done and not left_buffer and not right_buffer
        size = min(len(left_buffer), len(right_buffer))
        if left_buffer[:size] != right_buffer[:size]:
            return False
        left_buffer = left_buffer[size:]
        right_buffer = right_buffer[size:]


class SpoolStringView(_SpoolView):
    """A JSON string above the decode threshold, streamed from the body.

    ``iter_text_chunks`` yields the decoded value in pieces cut only at
    points that split neither an escape sequence nor a UTF-16 surrogate
    pair; ``iter_encoded_chunks`` and ``iter_byte_chunks`` yield the stored
    encoding. Equality with a ``str`` streams the comparison. ``str()`` is
    refused deliberately: a consumer that needs the text whole must say so
    through :func:`materialize_spool_views` (or join the chunks itself) so
    a repr can never be stored where the value belongs.
    """

    __slots__ = ()

    def iter_text_chunks(self, *, slice_bytes: int | None = None) -> Iterator[str]:
        return iter_json_string_text_chunks(
            self._body, self._start, self._end, slice_bytes=slice_bytes
        )

    def text_length(self) -> int:
        """Length in code points, computed by streaming."""
        return sum(len(chunk) for chunk in self.iter_text_chunks())

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        if isinstance(other, str):
            return _text_stream_equals_str(self.iter_text_chunks(), other)
        if isinstance(other, SpoolStringView):
            return _text_streams_equal(self.iter_text_chunks(), other.iter_text_chunks())
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    def __bool__(self) -> bool:
        return True

    def __str__(self) -> str:
        raise TypeError(
            "SpoolStringView is a streamed string; iterate iter_text_chunks() "
            "or call materialize_spool_views() to obtain the text"
        )

    def __repr__(self) -> str:
        return f"SpoolStringView(bytes={self.byte_length})"

    __hash__ = None  # type: ignore[assignment]


class SpoolRawValue(_SpoolView):
    """An oversized non-string scalar token (a number or literal).

    This encoder never produces one (integers are bounded by the
    interpreter's digit limit), so it exists only so an unexpected body is
    reported faithfully rather than refused: the token is reachable through
    its encoding and equality compares encodings.
    """

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        if isinstance(other, SpoolRawValue):
            return _text_streams_equal(self.iter_encoded_chunks(), other.iter_encoded_chunks())
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    def __repr__(self) -> str:
        return f"SpoolRawValue(bytes={self.byte_length})"

    __hash__ = None  # type: ignore[assignment]


class _ObjectItemsView(ItemsView):
    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return self._mapping._iter_items()  # type: ignore[attr-defined]


class _ObjectValuesView(ValuesView):
    def __iter__(self) -> Iterator[Any]:
        for _key, value in self._mapping._iter_items():  # type: ignore[attr-defined]
            yield value


class SpoolObjectView(_SpoolView, Mapping):
    """A JSON object above the decode threshold, decoded member by member.

    The member index (key and value byte spans) is built by one structural
    scan on first access and kept bounded in memory; keys are cached only
    for objects with few, short keys, and otherwise looked up by scanning.
    Each member value follows the module's decode policy, so a member can
    itself be a view. Equality with a mapping compares member by member.
    """

    __slots__ = ("_index", "_keys", "_lock")

    def __init__(self, body: SpoolCandidateBody, start: int, end: int) -> None:
        super().__init__(body, start, end)
        self._index: _OffsetIndex | None = None
        self._keys: dict[str, int] | None = None
        self._lock = threading.Lock()

    def _ensure_indexed(self) -> _OffsetIndex:
        index = self._index
        if index is not None:
            return index
        with self._lock:
            if self._index is not None:
                return self._index
            index = _OffsetIndex(4)
            keys: dict[str, int] | None = {}
            try:
                for ordinal, (key_start, key_end, value_start, value_end) in enumerate(
                    iter_member_spans(self._body, self._start + 1, self._end - 1)
                ):
                    index.append(key_start, key_end, value_start, value_end)
                    if keys is not None:
                        if (
                            ordinal >= SPOOL_VIEW_KEY_CACHE_ENTRIES
                            or key_end - key_start > SPOOL_VIEW_KEY_CACHE_BYTES
                        ):
                            keys = None
                        else:
                            keys[self._decode_key(key_start, key_end)] = ordinal
            except BaseException:
                index.close()
                raise
            self._index = index
            self._keys = keys
            return index

    def _decode_key(self, start: int, end: int) -> str:
        return decode_json_string(self._body, start, end)

    def _value_at(self, index: _OffsetIndex, ordinal: int) -> Any:
        _key_start, _key_end, value_start, value_end = index.get(ordinal)
        return decode_body_span(self._body, value_start, value_end)

    def _ordinal_of(self, key: str) -> int | None:
        index = self._ensure_indexed()
        if self._keys is not None:
            return self._keys.get(key)
        found: int | None = None
        for ordinal in range(index.count):
            key_start, key_end, _value_start, _value_end = index.get(ordinal)
            if SpoolStringView(self._body, key_start, key_end) == key:
                found = ordinal
        return found

    def _iter_items(self) -> Iterator[tuple[str, Any]]:
        index = self._ensure_indexed()
        for ordinal in range(index.count):
            key_start, key_end, value_start, value_end = index.get(ordinal)
            yield self._decode_key(key_start, key_end), decode_body_span(
                self._body, value_start, value_end
            )

    def __getitem__(self, key: str) -> Any:
        if not isinstance(key, str):
            raise KeyError(key)
        ordinal = self._ordinal_of(key)
        if ordinal is None:
            raise KeyError(key)
        return self._value_at(self._ensure_indexed(), ordinal)

    def __iter__(self) -> Iterator[str]:
        index = self._ensure_indexed()
        for ordinal in range(index.count):
            key_start, key_end, _value_start, _value_end = index.get(ordinal)
            yield self._decode_key(key_start, key_end)

    def __len__(self) -> int:
        return self._ensure_indexed().count

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self._ordinal_of(key) is not None

    def items(self) -> ItemsView:
        return _ObjectItemsView(self)

    def values(self) -> ValuesView:
        return _ObjectValuesView(self)

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        if not isinstance(other, Mapping):
            return NotImplemented
        if len(self) != len(other):
            return False
        for key, value in self._iter_items():
            if key not in other:
                return False
            if value != other[key]:
                return False
        return True

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    def __repr__(self) -> str:
        members = "?" if self._index is None else str(self._index.count)
        return f"SpoolObjectView(bytes={self.byte_length}, members={members})"

    def close(self) -> None:
        with self._lock:
            index = self._index
        if index is not None:
            index.close()

    __hash__ = None  # type: ignore[assignment]


class SpoolArrayView(_SpoolView, Sequence):
    """A JSON array above the decode threshold, decoded item by item.

    The item index is built by one structural scan on first access and
    kept bounded in memory (spilling to a scratch file). Items follow the
    module's decode policy. ``canonical_share_pages`` hands the stored item
    bytes to the codec's verbatim re-staging path, and equality with a
    sequence compares item by item.
    """

    __slots__ = ("_index", "_lock")

    def __init__(self, body: SpoolCandidateBody, start: int, end: int) -> None:
        super().__init__(body, start, end)
        self._index: _OffsetIndex | None = None
        self._lock = threading.Lock()

    def _ensure_indexed(self) -> _OffsetIndex:
        index = self._index
        if index is not None:
            return index
        with self._lock:
            if self._index is not None:
                return self._index
            index = _OffsetIndex(2)
            try:
                for item_start, item_end in iter_item_spans(
                    self._body, self._start + 1, self._end - 1
                ):
                    index.append(item_start, item_end)
            except BaseException:
                index.close()
                raise
            self._index = index
            return index

    def __len__(self) -> int:
        return self._ensure_indexed().count

    def __iter__(self) -> Iterator[Any]:
        index = self._ensure_indexed()
        for ordinal in range(index.count):
            item_start, item_end = index.get(ordinal)
            yield decode_body_span(self._body, item_start, item_end)

    def __getitem__(self, index: int | slice) -> Any:
        count = len(self)
        if isinstance(index, slice):
            start, stop, step = index.indices(count)
            return tuple(self[position] for position in range(start, stop, step))
        if isinstance(index, bool) or not isinstance(index, int):
            raise TypeError("spool array indices must be integers or slices")
        resolved = index + count if index < 0 else index
        if not 0 <= resolved < count:
            raise IndexError(index)
        item_start, item_end = self._ensure_indexed().get(resolved)
        return decode_body_span(self._body, item_start, item_end)

    def canonical_share_pages(self) -> Iterator[tuple[bytes, int | None]]:
        """The stored item bytes, verbatim, for the codec's re-staging path."""
        for chunk in iter_body_bytes(self._body, self._start + 1, self._end - 1):
            yield chunk, None

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

    def __repr__(self) -> str:
        items = "?" if self._index is None else str(self._index.count)
        return f"SpoolArrayView(bytes={self.byte_length}, items={items})"

    def close(self) -> None:
        with self._lock:
            index = self._index
        if index is not None:
            index.close()

    __hash__ = None  # type: ignore[assignment]


# --------------------------------------------------------------------------
# decode policy
# --------------------------------------------------------------------------


def decode_body_span(body: SpoolCandidateBody, start: int, end: int) -> Any:
    """One JSON value at ``[start, end)``: plain when small, a view when not.

    A value within :data:`SPOOL_VIEW_DECODE_BYTES` is one bounded
    ``json.loads``; a larger one is classified by its first and last byte
    without reading anything in between.
    """
    size = end - start
    if size <= 0:
        raise CandidateBodyIntegrityError("spool value is empty")
    if size <= SPOOL_VIEW_DECODE_BYTES:
        return _loads(read_body_span(body, start, end))
    first = read_body_span(body, start, start + 1)[0]
    last = read_body_span(body, end - 1, end)[0]
    if first == 0x7B and last == 0x7D:
        return SpoolObjectView(body, start, end)
    if first == 0x5B and last == 0x5D:
        return SpoolArrayView(body, start, end)
    if first == _QUOTE and last == _QUOTE:
        return SpoolStringView(body, start, end)
    if first in (0x7B, 0x5B, _QUOTE) or last in (0x7D, 0x5D, _QUOTE):
        raise CandidateBodyIntegrityError("spool value delimiters do not match")
    return SpoolRawValue(body, start, end)


def _escape_is_real(text: str, at: int) -> bool:
    """The backslash at ``at`` starts an escape (an even run precedes it)."""
    preceding = at - len(text[:at].rstrip("\\"))
    return preceding % 2 == 0


def _safe_cut(text: str) -> int:
    """Largest prefix of encoded string text that ends no escape mid-way.

    Three cuts, applied in order: an odd trailing backslash run leaves its
    last backslash for the next piece; an incomplete ``\\uXXXX`` in the last
    five characters moves whole; and a complete high-surrogate escape at the
    end moves whole so its low half is decoded in the same ``json.loads``
    (two lone surrogates concatenated are not the code point they encode).
    """
    cut = len(text)
    run = cut - len(text.rstrip("\\"))
    if run % 2 == 1:
        cut -= 1
    else:
        at = text.rfind("\\u", max(0, cut - 5))
        if at >= 0 and _escape_is_real(text, at):
            cut = at
    head = text[:cut]
    match = _HIGH_SURROGATE_TAIL.search(head)
    if match is not None and _escape_is_real(head, match.start()):
        cut = match.start()
    return cut


def iter_json_string_text_chunks(
    body: SpoolCandidateBody,
    start: int,
    end: int,
    *,
    slice_bytes: int | None = None,
) -> Iterator[str]:
    """Decoded text of the JSON string at ``[start, end)`` (quotes included).

    Each piece is one ``json.loads`` over at most ``slice_bytes`` of encoded
    text plus a short carry; raw UTF-8 is decoded incrementally so a read
    boundary never splits a code point either.
    """
    # Clamped to the decode threshold so no call exceeds it however the
    # knobs are set; at least 32 so a carried escape always makes progress.
    size = max(
        32,
        min(
            int(slice_bytes if slice_bytes is not None else SPOOL_VIEW_STRING_SLICE_BYTES),
            int(SPOOL_VIEW_DECODE_BYTES),
        ),
    )
    if end - start < 2:
        raise CandidateBodyIntegrityError("spool string span is too short")
    if read_body_span(body, start, start + 1) != b'"' or read_body_span(body, end - 1, end) != b'"':
        raise CandidateBodyIntegrityError("spool string span is not quoted")
    decoder = codecs.getincrementaldecoder("utf-8")()
    position = start + 1
    stop = end - 1
    carry = ""
    while position < stop:
        read_to = min(stop, position + size)
        try:
            text = carry + decoder.decode(read_body_span(body, position, read_to), final=read_to >= stop)
        except UnicodeDecodeError as exc:
            raise CandidateBodyIntegrityError(f"spool string is not valid UTF-8: {exc}") from exc
        position = read_to
        if position < stop:
            cut = _safe_cut(text)
            carry = text[cut:]
            text = text[:cut]
        else:
            carry = ""
        if text:
            try:
                piece = json.loads('"' + text + '"')
            except ValueError as exc:
                raise CandidateBodyIntegrityError(f"spool string is not valid JSON: {exc}") from exc
            yield piece
    if carry:
        raise CandidateBodyIntegrityError("spool string ends inside an escape")


def decode_json_string(body: SpoolCandidateBody, start: int, end: int) -> str:
    """The whole text of the JSON string at ``[start, end)``.

    The explicit materialization boundary for callers that need the value
    whole (for example a bounded metadata field); every C call stays a slice.
    """
    return "".join(iter_json_string_text_chunks(body, start, end))


# --------------------------------------------------------------------------
# consumer helpers
# --------------------------------------------------------------------------


def is_spool_view(value: object) -> bool:
    return isinstance(value, _SpoolView)


def iter_string_text_chunks(value: Any) -> Iterator[str]:
    """Text of a ``str`` or a :class:`SpoolStringView`, chunk by chunk.

    The one call a consumer needs to accept either shape (a COPY writer
    streaming ``share_id`` values, a hashing loop).
    """
    if isinstance(value, str):
        if value:
            yield value
        return
    if isinstance(value, SpoolStringView):
        yield from value.iter_text_chunks()
        return
    raise TypeError(f"expected a string or a SpoolStringView, not {type(value).__name__}")


def materialize_spool_views(value: Any) -> Any:
    """A plain Python object graph with every view decoded whole.

    The explicit boundary for consumers that must hold a value entire.
    Strings are joined from their chunks, containers rebuilt member by
    member; nothing here is bounded by anything but the value's own size,
    which is why the call is explicit.
    """
    if isinstance(value, SpoolStringView):
        return "".join(value.iter_text_chunks())
    if isinstance(value, SpoolRawValue):
        return json.loads("".join(value.iter_encoded_chunks()))
    if isinstance(value, Mapping):
        return {str(key): materialize_spool_views(item) for key, item in value.items()}
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        return value
    return [materialize_spool_views(item) for item in value]


def close_spool_views(value: Any) -> None:
    """Release the scratch resources of every view reachable from ``value``.

    Walks mappings and non-string sequences shallowly enough to find the
    views a hydrated intent's facts hold; a page-indexed share sequence is
    left alone (it owns nothing but the body reference).
    """
    if isinstance(value, _SpoolView):
        value.close()
        return
    if isinstance(value, Mapping):
        for item in list(value.values()) if isinstance(value, dict) else ():
            close_spool_views(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            close_spool_views(item)


__all__ = [
    "SPOOL_DECODED_METADATA_FIELDS",
    "SPOOL_VIEW_DECODE_BYTES",
    "SPOOL_VIEW_INDEX_MEMORY_ENTRIES",
    "SPOOL_VIEW_KEY_CACHE_BYTES",
    "SPOOL_VIEW_KEY_CACHE_ENTRIES",
    "SPOOL_VIEW_READ_BYTES",
    "SPOOL_VIEW_STRING_SLICE_BYTES",
    "SpoolArrayView",
    "SpoolObjectView",
    "SpoolRawValue",
    "SpoolStringView",
    "close_spool_views",
    "decode_body_span",
    "decode_json_string",
    "is_spool_view",
    "iter_body_bytes",
    "iter_body_text",
    "iter_item_spans",
    "iter_json_string_text_chunks",
    "iter_member_spans",
    "iter_string_text_chunks",
    "materialize_spool_views",
    "read_body_span",
]
