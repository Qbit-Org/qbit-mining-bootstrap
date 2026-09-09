#!/usr/bin/env python3
"""Bounded views over an immutable canonical audit artifact (issue #255).

The canonical audit bundle the Rust builder writes for a found block carries
two window-sized arrays: the top-level ``shares`` (one record per accepted
share in the payout window) and ``reward_manifest.shares`` (one record per
counted share). Loading that artifact with ``json.load`` materialized both
as Python dictionaries inside the lease-bearing coordinator process, and
every downstream consumer -- the logical comparison against the candidate
file, compact-body segmentation, reconstruction verification, and the
accepted-block persistence payload -- parsed or encoded the whole window
again.

This module keeps the verified artifact file as the sole authority and
exposes it through bounded objects:

* :class:`CanonicalAuditBundleView` is an immutable mapping of the bundle's
  top-level members. Small members (found block, balances, policy and
  coinbase manifests) are parsed eagerly; the window-sized arrays are
  exposed as :class:`LazyRecordSequence` objects that decode records on
  demand from the file through bounded read windows.
* :class:`LazyRecordSequence` is a replayable ``collections.abc.Sequence``
  of dictionaries: every iteration re-reads the artifact, random access
  seeks to a checkpoint and scans at most one stride of records, and no
  window-sized list, tuple, string or dictionary is ever built. Equality
  against a list or another lazy sequence streams both sides record by
  record.
* :func:`iter_json_chunks` encodes a payload that embeds such sequences into
  the exact ``json.dumps(payload, separators=(",", ":"))`` text in bounded
  chunks, so compact bodies and share segments can be written and digested
  without a whole-window string.

The scanner never rejects a valid document because of its size: a record
larger than the scan window grows that one decode to the record's own size
and the window is trimmed again afterwards, so an oversized single field is
isolated to its own decode rather than refused.

The byte contract is the Rust typed canonical field order. Nothing here
sorts keys or otherwise re-canonicalizes; the v1 candidate-identity JSON
(sorted keys, ``default=str``) is a different encoder with a different
digest and is never derived from these views.
"""

from __future__ import annotations

from array import array
import codecs
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import tempfile
from typing import Any, Callable, Iterable, Iterator
import weakref

from lab.prism.share_json_stream import iter_json_array_text_chunks


# Bytes read per positional read while scanning or replaying an artifact.
# Also the ceiling on decoded text retained between records for ordinary
# records; one oversized record grows the window to its own size only.
SCAN_CHUNK_BYTES = 256 * 1024
# Records between byte-offset checkpoints of a lazy sequence. Random access
# seeks to the nearest checkpoint and skips at most this many records.
RECORD_INDEX_STRIDE = 256
# Records per ``json.dumps`` call inside the streaming encoder.
JSON_BATCH_RECORDS = 256
# Target size of one yielded encoder chunk.
JSON_CHUNK_CHARS = 64 * 1024
# Member paths of a canonical audit bundle that are window-sized (one entry
# per accepted or counted share) or transaction-sized (one entry per block
# transaction). Everything else is bounded by recipient cardinality or is a
# fixed-size manifest and is parsed eagerly.
CANONICAL_BUNDLE_LAZY_PATHS: tuple[tuple[str, ...], ...] = (
    ("shares",),
    ("reward_manifest", "shares"),
    ("witness_merkle_leaves_hex",),
    ("audit_commitment_leaves_hex",),
)
# One JSON value (a record of a lazy array or an eagerly parsed member) is
# always decoded by one ``raw_decode`` call: that is the unit the standard
# decoder offers, so a value's own size is the decoder-argument maximum.
# Values above this size are counted in the scan statistics so an oversized
# record is observable; they are never rejected.
RECORD_DECODE_SOFT_LIMIT_BYTES = 1024 * 1024

_WHITESPACE = re.compile(r"[ \t\n\r]*")
# One complete JSON object with no nested containers: every share record
# in a canonical bundle. Matching it skips a record at C speed without
# building a dictionary; anything else is skipped by the bounded
# structural walker below.
_FLAT_OBJECT = re.compile(r'\{(?:"(?:[^"\\]|\\.)*"|[^"{}\[\]])*\}')
# Characters that change structural state while walking a container or a
# string window by window.
_STRUCTURAL = re.compile(r'["\\{}\[\]]')
_STRING_STRUCTURAL = re.compile(r'["\\]')
_SCALAR_END = re.compile(r"[,\]} \t\n\r]")
_DECODER = json.JSONDecoder()
_COMPACT_SEPARATORS = (",", ":")
# Checkpoint entries buffered before they are written to the index file.
_INDEX_FLUSH_BYTES = 4096


def _close_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


class _CheckpointIndex:
    """Fixed-width record-offset checkpoints for the lazy arrays of one scan.

    Entries are 8-byte little-endian byte offsets appended while scanning
    and read back with positional reads, so the index scales with the
    window on disk, never in memory: an unlinked temporary file holds it
    (the share-window spool's approach) and its descriptor closes with the
    last owner. If no temporary file can be created the index degrades to
    an in-memory array and reports that in the scan statistics.
    """

    __slots__ = (
        "__weakref__",
        "_count",
        "_fd",
        "_finalizer",
        "_memory",
        "_pending",
        "_written",
    )

    ENTRY = struct.Struct("<Q")

    def __init__(self) -> None:
        self._count = 0
        self._written = 0
        self._pending = bytearray()
        self._memory: array | None = None
        self._fd: int | None = None
        self._finalizer: Any = None
        try:
            handle = tempfile.TemporaryFile()
            try:
                self._fd = os.dup(handle.fileno())
            finally:
                handle.close()
        except OSError:
            self._memory = array("Q")
        if self._fd is not None:
            self._finalizer = weakref.finalize(self, _close_fd, self._fd)

    @property
    def count(self) -> int:
        return self._count

    @property
    def in_memory(self) -> bool:
        return self._memory is not None

    def append(self, offset: int) -> None:
        if self._memory is not None:
            self._memory.append(int(offset))
        else:
            self._pending += self.ENTRY.pack(int(offset))
            if len(self._pending) >= _INDEX_FLUSH_BYTES:
                self.flush()
        self._count += 1

    def flush(self) -> None:
        if not self._pending or self._fd is None:
            return
        data = bytes(self._pending)
        offset = self._written * self.ENTRY.size
        while data:
            written = os.pwrite(self._fd, data, offset)
            data = data[written:]
            offset += written
        self._written += len(self._pending) // self.ENTRY.size
        self._pending = bytearray()

    def get(self, index: int) -> int:
        if not 0 <= index < self._count:
            raise IndexError("checkpoint index out of range")
        if self._memory is not None:
            return int(self._memory[index])
        self.flush()
        if self._fd is None or (self._finalizer is not None and not self._finalizer.alive):
            raise CanonicalArtifactError("canonical audit artifact index is closed")
        entry = os.pread(self._fd, self.ENTRY.size, index * self.ENTRY.size)
        if len(entry) != self.ENTRY.size:
            raise CanonicalArtifactError("canonical audit artifact index is truncated")
        return int(self.ENTRY.unpack(entry)[0])

    def close(self) -> None:
        if self._finalizer is not None:
            self._finalizer()


class CanonicalArtifactError(ValueError):
    """The artifact cannot be used: identity changed, closed, or not a file."""


class CanonicalArtifactSyntaxError(CanonicalArtifactError, json.JSONDecodeError):
    """The artifact is not a well-formed JSON document.

    Also a :class:`json.JSONDecodeError`, so callers that classified the
    historical ``json.load`` failure keep classifying this one the same way.
    """

    def __init__(self, msg: str, doc: str = "", pos: int = 0) -> None:
        json.JSONDecodeError.__init__(self, msg, doc, pos)

    def __str__(self) -> str:
        return str(self.msg)


@dataclass(frozen=True)
class ArtifactIdentity:
    """The stat identity a view binds to; mirrors the store's file identity."""

    device: int
    inode: int
    mode: int
    size: int
    mtime_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> ArtifactIdentity:
        return cls(
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
        )

    def matches(self, value: os.stat_result) -> bool:
        return (
            stat.S_ISREG(value.st_mode)
            and self.device == value.st_dev
            and self.inode == value.st_ino
            and self.mode == value.st_mode
            and self.size == value.st_size
            and self.mtime_ns == value.st_mtime_ns
        )


class ArtifactSource:
    """One open regular file read only through bounded positional reads.

    Owns its descriptor: the file stays readable after its path is unlinked
    or replaced, and every read re-checks the descriptor's identity so a
    truncated or rewritten inode is reported rather than silently reread.
    The descriptor closes when the source is closed or garbage collected.
    """

    def __init__(self, fd: int, *, path: Path | None = None) -> None:
        value = os.fstat(fd)
        if not stat.S_ISREG(value.st_mode):
            os.close(fd)
            raise CanonicalArtifactError("canonical audit artifact is not a regular file")
        self._fd = fd
        self.path = path
        self.identity = ArtifactIdentity.from_stat(value)
        self._finalizer = weakref.finalize(self, os.close, fd)

    @property
    def closed(self) -> bool:
        return not self._finalizer.alive

    def close(self) -> None:
        self._finalizer()

    def fileno(self) -> int:
        if self.closed:
            raise CanonicalArtifactError("canonical audit artifact source is closed")
        return self._fd

    def verify_identity(self) -> None:
        if self.closed:
            raise CanonicalArtifactError("canonical audit artifact source is closed")
        if not self.identity.matches(os.fstat(self._fd)):
            raise CanonicalArtifactError("canonical audit artifact changed after it was scanned")

    def pread(self, offset: int, size: int) -> bytes:
        """Read up to ``size`` bytes at ``offset``; empty at end of file."""
        fd = self.fileno()
        pieces: list[bytes] = []
        remaining = size
        while remaining > 0:
            try:
                chunk = os.pread(fd, remaining, offset)
            except InterruptedError:
                continue
            if not chunk:
                break
            pieces.append(chunk)
            offset += len(chunk)
            remaining -= len(chunk)
        return b"".join(pieces) if len(pieces) != 1 else pieces[0]

    def iter_bytes(self, *, chunk_bytes: int = SCAN_CHUNK_BYTES) -> Iterator[bytes]:
        """The complete file in bounded chunks; identity checked at both ends."""
        self.verify_identity()
        offset = 0
        while True:
            chunk = self.pread(offset, chunk_bytes)
            if not chunk:
                break
            offset += len(chunk)
            yield chunk
        self.verify_identity()
        if offset != self.identity.size:
            raise CanonicalArtifactError("canonical audit artifact size changed while reading")

    def sha256_hex(self, *, chunk_bytes: int = SCAN_CHUNK_BYTES) -> str:
        digest = hashlib.sha256()
        for chunk in self.iter_bytes(chunk_bytes=chunk_bytes):
            digest.update(chunk)
        return digest.hexdigest()


class ScanStats:
    """Exact allocation maxima observed while scanning one artifact.

    ``max_value_chars`` is the largest single JSON value handed to one
    ``raw_decode`` call (a lazy-array record or an eager member);
    ``window_high_water_chars`` is the largest decoded text window held at
    any moment (at most one read chunk beyond the largest value);
    ``oversized_values`` counts values above
    :data:`RECORD_DECODE_SOFT_LIMIT_BYTES`.
    """

    __slots__ = (
        "checkpoint_entries",
        "index_in_memory",
        "max_value_chars",
        "oversized_values",
        "values_decoded",
        "window_high_water_chars",
    )

    def __init__(self) -> None:
        self.max_value_chars = 0
        self.oversized_values = 0
        self.values_decoded = 0
        self.window_high_water_chars = 0
        self.checkpoint_entries = 0
        self.index_in_memory = False

    def note_value(self, chars: int) -> None:
        """Record one value's size: decoded text characters, or bytes when
        the value was skipped structurally (equal for ASCII artifacts)."""
        self.values_decoded += 1
        if chars > self.max_value_chars:
            self.max_value_chars = chars
        if chars > RECORD_DECODE_SOFT_LIMIT_BYTES:
            self.oversized_values += 1

    def note_window(self, chars: int) -> None:
        if chars > self.window_high_water_chars:
            self.window_high_water_chars = chars

    def as_dict(self) -> dict[str, int | bool]:
        return {
            "max_value_chars": self.max_value_chars,
            "oversized_values": self.oversized_values,
            "values_decoded": self.values_decoded,
            "window_high_water_chars": self.window_high_water_chars,
            "checkpoint_entries": self.checkpoint_entries,
            "index_in_memory": self.index_in_memory,
        }


class _Cursor:
    """A decoded text window over an artifact, advanced record by record.

    ``text`` holds a bounded decoded window starting at ``window_start``
    bytes into the file; ``pos`` is the character index of the next token.
    Reads happen only when the parser needs more text, feed the optional
    hasher, and invoke the optional checkpoint callable so a cancellation
    check runs at least once per chunk.
    """

    __slots__ = (
        "_ascii",
        "_checkpoint",
        "_chunk_bytes",
        "_decoder",
        "_hasher",
        "_next_read",
        "_source",
        "_window_start",
        "eof",
        "pos",
        "stats",
        "text",
    )

    def __init__(
        self,
        source: ArtifactSource,
        offset: int,
        *,
        chunk_bytes: int,
        hasher: Any | None = None,
        checkpoint: Callable[[], None] | None = None,
        stats: ScanStats | None = None,
    ) -> None:
        self._source = source
        self._chunk_bytes = max(1, int(chunk_bytes))
        self._hasher = hasher
        self._checkpoint = checkpoint
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._window_start = int(offset)
        self._next_read = int(offset)
        self._ascii = True
        self.text = ""
        self.pos = 0
        self.eof = False
        self.stats = stats if stats is not None else ScanStats()

    @property
    def byte_pos(self) -> int:
        """Byte offset in the file of ``text[pos]``."""
        if self._ascii:
            return self._window_start + self.pos
        return self._window_start + len(self.text[: self.pos].encode("utf-8"))

    def _append(self, piece: str) -> None:
        if not piece:
            return
        if self._ascii and not piece.isascii():
            self._ascii = False
        self.text = self.text + piece if self.text else piece
        self.stats.note_window(len(self.text))

    def fill(self, min_ahead: int) -> None:
        """Ensure ``min_ahead`` characters follow ``pos`` unless at EOF."""
        while not self.eof and len(self.text) - self.pos < min_ahead:
            chunk = self._source.pread(self._next_read, self._chunk_bytes)
            if self._checkpoint is not None:
                self._checkpoint()
            if not chunk:
                self.eof = True
                try:
                    tail = self._decoder.decode(b"", final=True)
                except UnicodeDecodeError as exc:
                    raise CanonicalArtifactSyntaxError(
                        f"canonical audit artifact is not valid UTF-8: {exc}"
                    ) from exc
                self._append(tail)
                break
            self._next_read += len(chunk)
            if self._hasher is not None:
                self._hasher.update(chunk)
            try:
                self._append(self._decoder.decode(chunk))
            except UnicodeDecodeError as exc:
                raise CanonicalArtifactSyntaxError(
                    f"canonical audit artifact is not valid UTF-8: {exc}"
                ) from exc

    def trim(self, *, force: bool = False) -> None:
        """Drop consumed text once it exceeds one chunk; keeps windows bounded."""
        if self.pos < self._chunk_bytes and not force:
            return
        if self.pos == 0:
            return
        self._window_start = self.byte_pos
        self.text = self.text[self.pos :]
        self.pos = 0
        self._ascii = self.text.isascii()

    def skip_ws(self) -> None:
        while True:
            self.pos = _WHITESPACE.match(self.text, self.pos).end()
            if self.pos < len(self.text) or self.eof:
                return
            self.fill(1)

    def peek(self) -> str:
        self.fill(1)
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def expect(self, char: str) -> None:
        if self.peek() != char:
            raise CanonicalArtifactSyntaxError(
                f"canonical audit artifact is malformed at byte {self.byte_pos}: "
                f"expected {char!r}"
            )
        self.pos += 1

    def at_eof(self) -> bool:
        self.fill(1)
        return self.eof and self.pos >= len(self.text)

    def decode_value(self) -> Any:
        """Decode one JSON value at ``pos`` (whitespace already skipped).

        A value cut by the window boundary extends the window until it fits;
        the growth is geometric so an oversized field costs a handful of
        reads, and nothing beyond that one value is retained.
        """
        growth = self._chunk_bytes
        while True:
            self.fill(1)
            try:
                value, end = _DECODER.raw_decode(self.text, self.pos)
            except json.JSONDecodeError as exc:
                if self.eof:
                    raise CanonicalArtifactSyntaxError(
                        f"canonical audit artifact is malformed: {exc}"
                    ) from exc
                self.fill(len(self.text) - self.pos + growth)
                growth *= 2
                continue
            if end >= len(self.text) and not self.eof:
                # A number or literal may continue in the next chunk.
                self.fill(len(self.text) - self.pos + growth)
                growth *= 2
                continue
            self.stats.note_value(end - self.pos)
            self.pos = end
            return value

    def skip_value(self) -> None:
        """Advance past one JSON value without retaining or decoding it.

        A flat record that fits the window is skipped by one regex match.
        Anything else -- a record straddling the window, a nested value, a
        string or scalar of any size -- is walked structurally window by
        window, so the text held never exceeds about two read chunks even
        for a value far larger than the chunk. Structure (containers,
        strings, escapes, separators, end of file) is validated here; token
        syntax inside a skipped value is validated whenever the value is
        decoded by a consumer, and the artifact as a whole by the verifier.
        """
        char = self.peek()
        if char == "{":
            match = _FLAT_OBJECT.match(self.text, self.pos)
            if match is not None:
                self.stats.note_value(match.end() - self.pos)
                self.pos = match.end()
                return
        start = self.byte_pos
        if char in "{[":
            self._skip_container()
        elif char == '"':
            self.pos += 1
            self._skip_string_body()
        else:
            self._skip_scalar()
        self.stats.note_value(self.byte_pos - start)

    def _advance_window(self) -> None:
        """Consume the whole window and read the next chunk."""
        self.pos = len(self.text)
        self.trim(force=True)
        self.fill(1)
        if self.eof and self.pos >= len(self.text):
            raise CanonicalArtifactSyntaxError(
                "canonical audit artifact is malformed: unterminated value"
            )

    def _skip_escaped_char(self) -> None:
        """Skip the character following a backslash inside a string."""
        if self.pos >= len(self.text):
            self._advance_window()
        self.pos += 1

    def _skip_string_body(self) -> None:
        """Skip to just past the closing quote; ``pos`` is inside the string."""
        while True:
            match = _STRING_STRUCTURAL.search(self.text, self.pos)
            if match is None:
                self._advance_window()
                continue
            self.pos = match.end()
            if match.group() == "\\":
                self._skip_escaped_char()
            else:
                return
            if self.pos >= self._chunk_bytes:
                self.trim()

    def _skip_container(self) -> None:
        depth = 0
        while True:
            match = _STRUCTURAL.search(self.text, self.pos)
            if match is None:
                self._advance_window()
                continue
            char = match.group()
            self.pos = match.end()
            if char == '"':
                self._skip_string_body()
            elif char == "\\":
                raise CanonicalArtifactSyntaxError(
                    f"canonical audit artifact is malformed at byte {self.byte_pos}: "
                    "unexpected escape"
                )
            elif char in "{[":
                depth += 1
            else:
                depth -= 1
                if depth == 0:
                    return
                if depth < 0:
                    raise CanonicalArtifactSyntaxError(
                        f"canonical audit artifact is malformed at byte {self.byte_pos}: "
                        "unbalanced container"
                    )
            if self.pos >= self._chunk_bytes:
                self.trim()

    def _skip_scalar(self) -> None:
        start = self.byte_pos
        while True:
            match = _SCALAR_END.search(self.text, self.pos)
            if match is None:
                if self.eof:
                    self.pos = len(self.text)
                    break
                self._advance_window()
                continue
            self.pos = match.start()
            break
        if self.byte_pos == start:
            raise CanonicalArtifactSyntaxError(
                f"canonical audit artifact is malformed at byte {start}: expected a value"
            )


def _is_plain(value: Any, depth: int) -> bool:
    """True when ``json.dumps`` can encode ``value`` without a lazy member."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if depth <= 0:
        return isinstance(value, (dict, list, tuple))
    if isinstance(value, dict):
        return all(_is_plain(item, depth - 1) for item in value.values())
    if isinstance(value, (list, tuple)):
        return all(_is_plain(item, depth - 1) for item in value)
    return False


class _Coalescer:
    __slots__ = ("_chunk_chars", "_pending", "_pending_chars")

    def __init__(self, chunk_chars: int) -> None:
        self._chunk_chars = int(chunk_chars)
        self._pending: list[str] = []
        self._pending_chars = 0

    def push(self, text: str) -> str | None:
        if not text:
            return None
        self._pending.append(text)
        self._pending_chars += len(text)
        if self._pending_chars < self._chunk_chars:
            return None
        return self.flush()

    def flush(self) -> str | None:
        if not self._pending:
            return None
        chunk = self._pending[0] if len(self._pending) == 1 else "".join(self._pending)
        self._pending = []
        self._pending_chars = 0
        return chunk


def _encode_pieces(
    value: Any,
    *,
    batch_records: int,
    chunk_chars: int,
) -> Iterator[str]:
    if isinstance(value, Mapping):
        keys = list(value)
        if any(not isinstance(key, str) for key in keys):
            yield json.dumps(
                value if isinstance(value, dict) else dict(value),
                separators=_COMPACT_SEPARATORS,
            )
            return
        yield "{"
        first = True
        for key in keys:
            yield ("" if first else ",") + json.dumps(key) + ":"
            first = False
            yield from _encode_pieces(
                value[key],
                batch_records=batch_records,
                chunk_chars=chunk_chars,
            )
        yield "}"
        return
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        yield json.dumps(value, separators=_COMPACT_SEPARATORS)
        return
    yield "["
    if isinstance(value, (LazyRecordSequence, LazyRecordSlice)) or (
        isinstance(value, (list, tuple)) and _is_plain(value, 2)
    ):
        yield from iter_json_array_text_chunks(
            value,
            batch_records=batch_records,
            chunk_chars=chunk_chars,
        )
    else:
        first = True
        for item in value:
            if not first:
                yield ","
            first = False
            yield from _encode_pieces(
                item,
                batch_records=batch_records,
                chunk_chars=chunk_chars,
            )
    yield "]"


def iter_json_chunks(
    value: Any,
    *,
    batch_records: int = JSON_BATCH_RECORDS,
    chunk_chars: int = JSON_CHUNK_CHARS,
) -> Iterator[str]:
    """``json.dumps(value, separators=(",", ":"))`` in bounded text chunks.

    Mappings are encoded member by member, lazy record sequences and plain
    lists batch by batch (each ``json.dumps`` covers at most
    ``batch_records`` items), and every other value through one
    ``json.dumps`` bounded by that value's own size. Concatenating the
    chunks reproduces the compact encoding byte for byte; every chunk is
    ASCII, so its character count is its byte count.
    """
    coalescer = _Coalescer(chunk_chars)
    for piece in _encode_pieces(
        value,
        batch_records=batch_records,
        chunk_chars=chunk_chars,
    ):
        chunk = coalescer.push(piece)
        if chunk is not None:
            yield chunk
    chunk = coalescer.flush()
    if chunk is not None:
        yield chunk


def iter_json_byte_chunks(
    value: Any,
    *,
    batch_records: int = JSON_BATCH_RECORDS,
    chunk_chars: int = JSON_CHUNK_CHARS,
) -> Iterator[bytes]:
    """:func:`iter_json_chunks`, UTF-8 encoded chunk by chunk."""
    for chunk in iter_json_chunks(
        value,
        batch_records=batch_records,
        chunk_chars=chunk_chars,
    ):
        yield chunk.encode("utf-8")


def json_chunks_sha256_and_size(chunks: Iterable[bytes]) -> tuple[str, int]:
    """Digest and byte count of a chunk stream without joining it."""
    digest = hashlib.sha256()
    size = 0
    for chunk in chunks:
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _sequences_equal(left: Sequence[Any], right: Any) -> bool:
    if isinstance(right, (str, bytes, bytearray)) or not isinstance(right, Sequence):
        return NotImplemented  # type: ignore[return-value]
    if len(left) != len(right):
        return False
    right_iter = iter(right)
    for item in left:
        try:
            other = next(right_iter)
        except StopIteration:
            return False
        if item != other:
            return False
    try:
        next(right_iter)
    except StopIteration:
        return True
    return False


class LazyRecordSequence(Sequence):
    """A replayable, read-only sequence of JSON records inside an artifact.

    Records live between ``start`` (the byte after the opening bracket) and
    ``end`` (the byte after the closing bracket) of one JSON array in the
    source. Iteration streams the array through bounded windows; indexing
    seeks to the checkpoint preceding the index and skips forward. Slices
    with unit step return a :class:`LazyRecordSlice` over the same file;
    other slices return a list bounded by the slice itself.
    """

    __slots__ = (
        "__weakref__",
        "_chunk_bytes",
        "_count",
        "_end",
        "_index",
        "_index_base",
        "_source",
        "_start",
        "_stride",
    )

    def __init__(
        self,
        source: ArtifactSource,
        *,
        start: int,
        end: int,
        count: int,
        index: _CheckpointIndex,
        index_base: int,
        stride: int = RECORD_INDEX_STRIDE,
        chunk_bytes: int = SCAN_CHUNK_BYTES,
    ) -> None:
        self._source = source
        self._start = int(start)
        self._end = int(end)
        self._count = int(count)
        self._index = index
        self._index_base = int(index_base)
        self._stride = max(1, int(stride))
        self._chunk_bytes = int(chunk_bytes)

    @property
    def source(self) -> ArtifactSource:
        return self._source

    @property
    def byte_range(self) -> tuple[int, int]:
        return self._start, self._end

    def __len__(self) -> int:
        return self._count

    def __repr__(self) -> str:
        return f"LazyRecordSequence(count={self._count}, bytes={self._end - self._start})"

    def _cursor(self, offset: int) -> _Cursor:
        return _Cursor(self._source, offset, chunk_bytes=self._chunk_bytes)

    def iter_range(self, start: int, stop: int) -> Iterator[Any]:
        """Records ``start`` (inclusive) to ``stop`` (exclusive), streamed."""
        start = max(0, int(start))
        stop = min(self._count, int(stop))
        if start >= stop:
            return
        self._source.verify_identity()
        block = start // self._stride
        cursor = self._cursor(self._index.get(self._index_base + block))
        index = block * self._stride
        while index < start:
            cursor.skip_ws()
            cursor.skip_value()
            cursor.skip_ws()
            cursor.expect(",")
            cursor.trim()
            index += 1
        while index < stop:
            cursor.skip_ws()
            yield cursor.decode_value()
            index += 1
            cursor.trim()
            if index < self._count:
                cursor.skip_ws()
                cursor.expect(",")
        if stop == self._count:
            cursor.skip_ws()
            cursor.expect("]")
        self._source.verify_identity()

    def __iter__(self) -> Iterator[Any]:
        return self.iter_range(0, self._count)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            start, stop, step = index.indices(self._count)
            if step == 1:
                return LazyRecordSlice(self, start, max(start, stop))
            return [self[position] for position in range(start, stop, step)]
        if isinstance(index, bool) or not isinstance(index, int):
            raise TypeError("lazy record indices must be integers or slices")
        if index < 0:
            index += self._count
        if not 0 <= index < self._count:
            raise IndexError("lazy record index out of range")
        for record in self.iter_range(index, index + 1):
            return record
        raise IndexError("lazy record index out of range")

    def batches(self, size: int) -> Iterator[LazyRecordSlice]:
        """Consecutive slices of at most ``size`` records."""
        size = max(1, int(size))
        for start in range(0, self._count, size):
            yield LazyRecordSlice(self, start, min(self._count, start + size))

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        return _sequences_equal(self, other)

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    __hash__ = None  # type: ignore[assignment]


class LazyRecordSlice(Sequence):
    """A bounded window of a :class:`LazyRecordSequence`, streamed on demand."""

    __slots__ = ("_parent", "_start", "_stop")

    def __init__(self, parent: LazyRecordSequence, start: int, stop: int) -> None:
        self._parent = parent
        self._start = int(start)
        self._stop = int(stop)

    def __len__(self) -> int:
        return self._stop - self._start

    def __repr__(self) -> str:
        return f"LazyRecordSlice({self._start}:{self._stop})"

    def __iter__(self) -> Iterator[Any]:
        return self._parent.iter_range(self._start, self._stop)

    def __getitem__(self, index: int | slice) -> Any:
        length = len(self)
        if isinstance(index, slice):
            start, stop, step = index.indices(length)
            if step == 1:
                return LazyRecordSlice(
                    self._parent,
                    self._start + start,
                    self._start + max(start, stop),
                )
            return [self[position] for position in range(start, stop, step)]
        if isinstance(index, bool) or not isinstance(index, int):
            raise TypeError("lazy record indices must be integers or slices")
        if index < 0:
            index += length
        if not 0 <= index < length:
            raise IndexError("lazy record index out of range")
        return self._parent[self._start + index]

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        return _sequences_equal(self, other)

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    __hash__ = None  # type: ignore[assignment]


def is_lazy_sequence(value: object) -> bool:
    return isinstance(value, (LazyRecordSequence, LazyRecordSlice))


def _scan_array(
    cursor: _Cursor,
    *,
    source: ArtifactSource,
    index: _CheckpointIndex,
    stride: int,
    chunk_bytes: int,
) -> Any:
    cursor.skip_ws()
    if cursor.peek() != "[":
        return cursor.decode_value()
    cursor.pos += 1
    start = cursor.byte_pos
    index_base = index.count
    count = 0
    cursor.skip_ws()
    if cursor.peek() == "]":
        cursor.pos += 1
        return LazyRecordSequence(
            source,
            start=start,
            end=cursor.byte_pos,
            count=0,
            index=index,
            index_base=index_base,
            stride=stride,
            chunk_bytes=chunk_bytes,
        )
    while True:
        cursor.skip_ws()
        if count % stride == 0:
            index.append(cursor.byte_pos)
        cursor.skip_value()
        count += 1
        cursor.trim()
        cursor.skip_ws()
        char = cursor.peek()
        if char == ",":
            cursor.pos += 1
            continue
        if char == "]":
            cursor.pos += 1
            break
        raise CanonicalArtifactSyntaxError(
            f"canonical audit artifact is malformed at byte {cursor.byte_pos}: "
            "expected ',' or ']'"
        )
    return LazyRecordSequence(
        source,
        start=start,
        end=cursor.byte_pos,
        count=count,
        index=index,
        index_base=index_base,
        stride=stride,
        chunk_bytes=chunk_bytes,
    )


def _parse_object(
    cursor: _Cursor,
    path: tuple[str, ...],
    *,
    lazy_paths: Sequence[tuple[str, ...]],
    source: ArtifactSource,
    index: _CheckpointIndex,
    stride: int,
    chunk_bytes: int,
) -> dict[str, Any]:
    cursor.skip_ws()
    cursor.expect("{")
    members: dict[str, Any] = {}
    cursor.skip_ws()
    if cursor.peek() == "}":
        cursor.pos += 1
        return members
    while True:
        cursor.skip_ws()
        if cursor.peek() != '"':
            raise CanonicalArtifactSyntaxError(
                f"canonical audit artifact is malformed at byte {cursor.byte_pos}: "
                "expected a member name"
            )
        key = cursor.decode_value()
        cursor.skip_ws()
        cursor.expect(":")
        member_path = path + (str(key),)
        if member_path in lazy_paths:
            value = _scan_array(
                cursor,
                source=source,
                index=index,
                stride=stride,
                chunk_bytes=chunk_bytes,
            )
        elif any(
            len(lazy) > len(member_path) and lazy[: len(member_path)] == member_path
            for lazy in lazy_paths
        ):
            cursor.skip_ws()
            if cursor.peek() == "{":
                value = _parse_object(
                    cursor,
                    member_path,
                    lazy_paths=lazy_paths,
                    source=source,
                    index=index,
                    stride=stride,
                    chunk_bytes=chunk_bytes,
                )
            else:
                value = cursor.decode_value()
        else:
            cursor.skip_ws()
            value = cursor.decode_value()
        members[str(key)] = value
        cursor.trim()
        cursor.skip_ws()
        char = cursor.peek()
        if char == ",":
            cursor.pos += 1
            continue
        if char == "}":
            cursor.pos += 1
            return members
        raise CanonicalArtifactSyntaxError(
            f"canonical audit artifact is malformed at byte {cursor.byte_pos}: "
            "expected ',' or '}'"
        )


class CanonicalAuditBundleView(Mapping):
    """Bounded, immutable mapping view of one canonical audit artifact.

    Construct with :meth:`scan`, which reads the file exactly once through
    bounded windows, digests the raw bytes as it goes, parses every member
    outside ``lazy_paths`` eagerly and indexes the lazy arrays. The view
    binds the file identity it scanned; every later lazy read re-checks it.

    Equality with a dictionary (either operand order) compares member by
    member, streaming the lazy arrays record by record, so callers keep the
    full logical comparison the dictionary path performed without building
    a window-sized object graph.
    """

    def __init__(
        self,
        source: ArtifactSource,
        members: dict[str, Any],
        *,
        sha256_hex: str,
        lazy_paths: tuple[tuple[str, ...], ...],
        scan_stats: ScanStats | None = None,
        index: _CheckpointIndex | None = None,
    ) -> None:
        self._source = source
        self._members = members
        self._sha256_hex = sha256_hex
        self._lazy_paths = lazy_paths
        self._scan_stats = scan_stats if scan_stats is not None else ScanStats()
        self._index = index

    @classmethod
    def scan(
        cls,
        fd: int,
        *,
        path: Path | None = None,
        cancellation: Callable[[], None] | None = None,
        chunk_bytes: int = SCAN_CHUNK_BYTES,
        stride: int = RECORD_INDEX_STRIDE,
        lazy_paths: Sequence[tuple[str, ...]] = CANONICAL_BUNDLE_LAZY_PATHS,
    ) -> CanonicalAuditBundleView:
        """Scan the artifact behind ``fd``; the view takes ownership of ``fd``.

        ``cancellation`` runs once per read chunk so a cancelled build stops
        within one bounded read. The whole file is consumed so the returned
        digest covers every byte, and trailing garbage is rejected.
        """
        source = ArtifactSource(fd, path=path)
        index = _CheckpointIndex()
        try:
            hasher = hashlib.sha256()
            stats = ScanStats()
            stats.index_in_memory = index.in_memory
            cursor = _Cursor(
                source,
                0,
                chunk_bytes=chunk_bytes,
                hasher=hasher,
                checkpoint=cancellation,
                stats=stats,
            )
            lazy = tuple(tuple(str(part) for part in item) for item in lazy_paths)
            members = _parse_object(
                cursor,
                (),
                lazy_paths=lazy,
                source=source,
                index=index,
                stride=stride,
                chunk_bytes=chunk_bytes,
            )
            cursor.skip_ws()
            if not cursor.at_eof():
                raise CanonicalArtifactSyntaxError(
                    "canonical audit artifact has trailing data after the bundle"
                )
            source.verify_identity()
            if cursor.byte_pos != source.identity.size:
                raise CanonicalArtifactError(
                    "canonical audit artifact size does not match its scan"
                )
            index.flush()
            stats.checkpoint_entries = index.count
        except BaseException:
            index.close()
            source.close()
            raise
        return cls(
            source,
            members,
            sha256_hex=hasher.hexdigest(),
            lazy_paths=lazy,
            scan_stats=stats,
            index=index,
        )

    @classmethod
    def scan_path(
        cls,
        path: Path,
        *,
        cancellation: Callable[[], None] | None = None,
        chunk_bytes: int = SCAN_CHUNK_BYTES,
        stride: int = RECORD_INDEX_STRIDE,
        lazy_paths: Sequence[tuple[str, ...]] = CANONICAL_BUNDLE_LAZY_PATHS,
    ) -> CanonicalAuditBundleView:
        """Open ``path`` without following symlinks and scan it."""
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        return cls.scan(
            fd,
            path=Path(path),
            cancellation=cancellation,
            chunk_bytes=chunk_bytes,
            stride=stride,
            lazy_paths=lazy_paths,
        )

    # Mapping protocol.

    def __getitem__(self, key: str) -> Any:
        return self._members[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._members)

    def __len__(self) -> int:
        return len(self._members)

    def __contains__(self, key: object) -> bool:
        return key in self._members

    def __repr__(self) -> str:
        return (
            f"CanonicalAuditBundleView(sha256={self._sha256_hex}, "
            f"bytes={self.byte_length}, members={list(self._members)})"
        )

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        if not isinstance(other, Mapping):
            return NotImplemented
        if len(self._members) != len(other):
            return False
        for key, value in self._members.items():
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

    __hash__ = None  # type: ignore[assignment]

    # Artifact binding.

    @property
    def source(self) -> ArtifactSource:
        return self._source

    @property
    def path(self) -> Path | None:
        return self._source.path

    @property
    def identity(self) -> ArtifactIdentity:
        return self._source.identity

    @property
    def sha256_hex(self) -> str:
        return self._sha256_hex

    @property
    def byte_length(self) -> int:
        return self._source.identity.size

    @property
    def lazy_paths(self) -> tuple[tuple[str, ...], ...]:
        return self._lazy_paths

    @property
    def scan_stats(self) -> ScanStats:
        """Allocation maxima observed by the initial scan of this artifact."""
        return self._scan_stats

    def verify_identity(self) -> None:
        self._source.verify_identity()

    def iter_bytes(self, *, chunk_bytes: int = SCAN_CHUNK_BYTES) -> Iterator[bytes]:
        return self._source.iter_bytes(chunk_bytes=chunk_bytes)

    def key_index(self, key: str) -> int:
        return list(self._members).index(key)

    def without(self, *keys: str) -> dict[str, Any]:
        """Ordered copy of the members minus ``keys``; values are shared."""
        return {
            key: value for key, value in self._members.items() if key not in keys
        }

    def close(self) -> None:
        """Retire the artifact and index descriptors this view owns.

        Lazy sequences handed out earlier keep their own references to the
        source and index and fail closed once these are gone.
        """
        if self._index is not None:
            self._index.close()
        self._source.close()

    @property
    def closed(self) -> bool:
        return self._source.closed


def compare_streaming(left: Any, right: Any) -> bool:
    """``left == right`` where either side may embed lazy sequences or views."""
    return bool(left == right)


__all__ = [
    "ArtifactIdentity",
    "ArtifactSource",
    "CANONICAL_BUNDLE_LAZY_PATHS",
    "CanonicalArtifactError",
    "CanonicalArtifactSyntaxError",
    "CanonicalAuditBundleView",
    "JSON_BATCH_RECORDS",
    "JSON_CHUNK_CHARS",
    "LazyRecordSequence",
    "LazyRecordSlice",
    "RECORD_DECODE_SOFT_LIMIT_BYTES",
    "RECORD_INDEX_STRIDE",
    "SCAN_CHUNK_BYTES",
    "ScanStats",
    "compare_streaming",
    "is_lazy_sequence",
    "iter_json_byte_chunks",
    "iter_json_chunks",
    "json_chunks_sha256_and_size",
]
