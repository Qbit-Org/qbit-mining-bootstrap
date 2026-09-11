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

from contextlib import contextmanager

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
import subprocess
import sys
import tempfile
import threading
import time
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
# Member paths of a canonical audit bundle whose length scales with the
# window (one entry per accepted or counted share), with the block's
# transactions (one leaf per transaction) or with recipient cardinality
# (balances, accounts, entitlements, settlement recipients and fanout
# manifests). Recipient cardinality is itself bounded only by the window,
# so none of these is a byte bound; every one stays on disk. What remains
# eager is fixed-size: the found block, policy, attestation, digests and
# the signed coinbase manifest, whose outputs are capped by configuration.
CANONICAL_BUNDLE_LAZY_PATHS: tuple[tuple[str, ...], ...] = (
    ("shares",),
    ("reward_manifest", "shares"),
    ("reward_manifest", "entitlements"),
    ("witness_merkle_leaves_hex",),
    ("audit_commitment_leaves_hex",),
    ("prior_balances",),
    ("payout_policy_manifest", "accounts"),
    ("payout_policy_manifest", "onchain_entitlements"),
    ("settlement_mode_decision", "direct_recipients"),
    ("settlement_mode_decision", "fanout_chunks"),
    ("ctv_fanout_manifest_set", "manifests"),
)
# Largest single JSON value the coordinator decodes in-process with one
# ``raw_decode`` call. A record above this limit is normalized by an
# isolated helper process instead (see RawJsonRecord); it is never
# rejected. Values above the limit are also counted in the scan statistics.
RECORD_DECODE_SOFT_LIMIT_BYTES = 1024 * 1024
# Members of an isolated record that are kept in the coordinator as plain
# values: any member whose compact encoding is at most this many bytes.
RAW_RECORD_MEMBER_LIMIT_BYTES = 4096
# Wall-clock ceiling for one isolated record normalization, admission wait
# included.
RAW_RECORD_HELPER_TIMEOUT_SECONDS = 300.0
# Isolated helper processes admitted at once: each one may hold a whole
# record (and two encodings of it), so the slots bound helper memory.
RAW_RECORD_HELPER_SLOTS = 1
RAW_RECORD_HELPER_MEMORY_BYTES = 4 * 1024 * 1024 * 1024
RAW_RECORD_HELPER_HEADER_BYTES = 1024 * 1024
# Supervisor poll interval while a helper runs or a slot is awaited.
RAW_RECORD_HELPER_POLL_SECONDS = 0.05
# Bytes of a helper's diagnostics retained (the tail) for the error report.
RAW_RECORD_HELPER_DIAGNOSTIC_BYTES = 64 * 1024

_WHITESPACE = re.compile(r"[ \t\n\r]*")
# Strict JSON token grammar (RFC 8259 plus the standard decoder's NaN and
# Infinity constants). The quantifiers are possessive: when a window ends
# inside a long token the match must fail in one pass instead of
# backtracking through every prefix, which would allocate a regex state
# stack proportional to the window.
_WS = r"[ \t\n\r]*+"
_STRING_TOKEN = r'"(?:[^"\\\x00-\x1f]++|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*+"'
_NUMBER_TOKEN = r"-?(?:0|[1-9][0-9]*+)(?:\.[0-9]++)?+(?:[eE][+-]?[0-9]++)?+"
_SCALAR_TOKEN = rf"(?:{_STRING_TOKEN}|{_NUMBER_TOKEN}|true|false|null|NaN|Infinity|-Infinity)"
# One complete JSON object with no nested containers: every share record
# in a canonical bundle. Matching it validates and skips the record at C
# speed without building a dictionary; anything it does not match (a
# record cut by the window, a nested value, or a malformed one) goes to
# the streaming validator below, which reports malformed input.
_FLAT_OBJECT = re.compile(
    r"\{"
    + _WS
    + rf"(?:{_STRING_TOKEN}{_WS}:{_WS}{_SCALAR_TOKEN}"
    + rf"(?:{_WS},{_WS}{_STRING_TOKEN}{_WS}:{_WS}{_SCALAR_TOKEN})*+)?+"
    + _WS
    + r"\}"
)
_NON_STRING_SCALAR = re.compile(
    rf"(?:{_NUMBER_TOKEN}|true|false|null|NaN|Infinity|-Infinity)\Z"
)
# Characters that end a string's ordinary run: the closing quote, an
# escape, or a control character the grammar forbids.
_STRING_STRUCTURAL = re.compile(r'["\\\x00-\x1f]')
_SCALAR_END = re.compile(r"[,\]}: \t\n\r\"{\[]")
_ESCAPE_TOKEN = re.compile(r'["\\/bfnrt]|u[0-9a-fA-F]{4}')
# Characters that can extend a JSON number token past a decoded prefix.
_NUMBER_CONTINUATION = frozenset("0123456789.eE+-")
_DECODER = json.JSONDecoder()
_COMPACT_SEPARATORS = (",", ":")
# Checkpoint entries buffered before they are written to the index file.
_INDEX_FLUSH_BYTES = 4096


def _close_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _scratch_fd(purpose: str) -> int:
    """An unlinked temporary file's descriptor, or resource pressure."""
    try:
        handle = tempfile.TemporaryFile()
    except OSError as exc:
        raise ArtifactResourcePressure(
            f"cannot create the {purpose} scratch file: {exc}"
        ) from exc
    try:
        return os.dup(handle.fileno())
    except OSError as exc:
        raise ArtifactResourcePressure(
            f"cannot duplicate the {purpose} scratch descriptor: {exc}"
        ) from exc
    finally:
        handle.close()


def _pwrite_all(fd: int, data: bytes, offset: int, *, purpose: str) -> None:
    view = memoryview(data)
    while view:
        try:
            written = os.pwrite(fd, view, offset)
        except InterruptedError:
            continue
        except OSError as exc:
            raise ArtifactResourcePressure(
                f"cannot write the {purpose} scratch file: {exc}"
            ) from exc
        view = view[written:]
        offset += written


class _CheckpointIndex:
    """Fixed-width record-offset checkpoints for the lazy arrays of one scan.

    Entries are 8-byte little-endian byte offsets appended while scanning
    and read back with positional reads, so the index scales with the
    window on disk, never in memory: an unlinked temporary file holds it
    (the share-window spool's approach) and its descriptor closes with the
    last owner. There is no in-memory fallback: when the scratch file
    cannot be created or written, :class:`ArtifactResourcePressure` is
    raised so the caller retries later rather than growing the process.
    """

    __slots__ = (
        "__weakref__",
        "_count",
        "_fd",
        "_finalizer",
        "_pending",
        "_written",
    )

    ENTRY = struct.Struct("<Q")

    def __init__(self) -> None:
        self._count = 0
        self._written = 0
        self._pending = bytearray()
        self._fd = _scratch_fd("checkpoint index")
        self._finalizer = weakref.finalize(self, _close_fd, self._fd)

    @property
    def count(self) -> int:
        return self._count

    def append(self, offset: int) -> None:
        self._pending += self.ENTRY.pack(int(offset))
        if len(self._pending) >= _INDEX_FLUSH_BYTES:
            self.flush()
        self._count += 1

    def flush(self) -> None:
        if not self._pending:
            return
        if not self._finalizer.alive:
            raise CanonicalArtifactError("canonical audit artifact index is closed")
        _pwrite_all(
            self._fd,
            bytes(self._pending),
            self._written * self.ENTRY.size,
            purpose="checkpoint index",
        )
        self._written += len(self._pending) // self.ENTRY.size
        self._pending = bytearray()

    def get(self, index: int) -> int:
        if not 0 <= index < self._count:
            raise IndexError("checkpoint index out of range")
        self.flush()
        if not self._finalizer.alive:
            raise CanonicalArtifactError("canonical audit artifact index is closed")
        entry = os.pread(self._fd, self.ENTRY.size, index * self.ENTRY.size)
        if len(entry) != self.ENTRY.size:
            raise CanonicalArtifactError("canonical audit artifact index is truncated")
        return int(self.ENTRY.unpack(entry)[0])

    def close(self) -> None:
        self._finalizer()


class ArtifactResourcePressure(Exception):
    """A scratch resource (checkpoint index, record spool, helper) is unavailable.

    Raised instead of degrading to an unbounded in-memory structure. The
    artifact itself is intact, so this is deliberately neither a
    :class:`CanonicalArtifactError` nor a :class:`json.JSONDecodeError`:
    callers must not classify it as corruption or mismatch, and the
    operation is retry-safe once the pressure passes.
    """


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
        "isolated_records",
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
        # Records normalized by the isolated helper instead of in-process.
        self.isolated_records = 0

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

    def as_dict(self) -> dict[str, int]:
        return {
            "max_value_chars": self.max_value_chars,
            "oversized_values": self.oversized_values,
            "values_decoded": self.values_decoded,
            "window_high_water_chars": self.window_high_water_chars,
            "checkpoint_entries": self.checkpoint_entries,
            "isolated_records": self.isolated_records,
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
        "generation",
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
        # Bumped whenever consumed text is dropped, so a caller can tell
        # whether a slice of ``text`` taken before a skip is still valid.
        self.generation = 0
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
        self.generation += 1

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

        Token completion is checked explicitly for numbers: ``raw_decode``
        happily returns the prefix ``1`` of a window ending in ``1e+`` or
        ``1.`` without touching the window end, so a number is complete
        only when the character after it cannot continue a number (or the
        file has ended). A continuation character with the whole file in
        view is a malformed number, exactly as the standard decoder reports
        for the complete document.
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
            if end >= len(self.text):
                if not self.eof:
                    # A number or literal may continue in the next chunk.
                    self.fill(len(self.text) - self.pos + growth)
                    growth *= 2
                    continue
            elif (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and self.text[end] in _NUMBER_CONTINUATION
            ):
                if not self.eof:
                    self.fill(len(self.text) - self.pos + growth)
                    growth *= 2
                    continue
                raise CanonicalArtifactSyntaxError(
                    f"canonical audit artifact is malformed at byte {self.byte_pos}: "
                    "invalid number"
                )
            self.stats.note_value(end - self.pos)
            self.pos = end
            return value

    def skip_value(self) -> None:
        """Validate and advance past one JSON value without retaining it.

        A flat record that fits the window is validated and skipped by one
        strict-grammar regex match. Anything else -- a record straddling
        the window, a nested value, a string or scalar of any size -- is
        walked by a streaming token validator window by window, so the text
        held never exceeds about two read chunks even for a value far larger
        than the chunk. The validation is the standard decoder's: token
        syntax, escapes, control characters, separators, container balance
        and termination all fail closed here, at scan time.
        """
        char = self.peek()
        if char == "{":
            match = _FLAT_OBJECT.match(self.text, self.pos)
            if match is not None:
                self.stats.note_value(match.end() - self.pos)
                self.pos = match.end()
                return
        start = self.byte_pos
        self._walk_value()
        self.stats.note_value(self.byte_pos - start)

    def _malformed(self, detail: str) -> CanonicalArtifactSyntaxError:
        return CanonicalArtifactSyntaxError(
            f"canonical audit artifact is malformed at byte {self.byte_pos}: {detail}"
        )

    def _advance_window(self) -> None:
        """Consume the whole window and read the next chunk."""
        self.pos = len(self.text)
        self.trim(force=True)
        self.fill(1)
        if self.eof and self.pos >= len(self.text):
            raise CanonicalArtifactSyntaxError(
                "canonical audit artifact is malformed: unterminated value"
            )

    def _walk_value(self) -> None:
        """Streaming validator for one JSON value starting at ``pos``."""
        stack: list[str] = []
        state = "value"
        while True:
            self.skip_ws()
            char = self.peek()
            if state == "value":
                if char == '"':
                    self.pos += 1
                    self._skip_string_body()
                    state = "after"
                elif char == "{":
                    self.pos += 1
                    stack.append("O")
                    state = "key_or_end"
                elif char == "[":
                    self.pos += 1
                    stack.append("A")
                    state = "value_or_end"
                else:
                    self._skip_scalar()
                    state = "after"
            elif state == "value_or_end":
                if char == "]":
                    self.pos += 1
                    stack.pop()
                    state = "after"
                else:
                    state = "value"
                    continue
            elif state == "key_or_end":
                if char == "}":
                    self.pos += 1
                    stack.pop()
                    state = "after"
                elif char == '"':
                    self.pos += 1
                    self._skip_string_body()
                    state = "colon"
                else:
                    raise self._malformed("expected a member name or '}'")
            elif state == "key":
                if char != '"':
                    raise self._malformed("expected a member name")
                self.pos += 1
                self._skip_string_body()
                state = "colon"
            elif state == "colon":
                if char != ":":
                    raise self._malformed("expected ':'")
                self.pos += 1
                state = "value"
            else:  # after a complete value
                if not stack:
                    return
                top = stack[-1]
                if char == ",":
                    self.pos += 1
                    state = "key" if top == "O" else "value"
                elif char == "]" and top == "A":
                    self.pos += 1
                    stack.pop()
                elif char == "}" and top == "O":
                    self.pos += 1
                    stack.pop()
                elif char == "":
                    raise CanonicalArtifactSyntaxError(
                        "canonical audit artifact is malformed: unterminated value"
                    )
                else:
                    raise self._malformed("expected ',' or a closing bracket")
            if self.pos >= self._chunk_bytes:
                self.trim()

    def _skip_string_body(self) -> None:
        """Validate to just past the closing quote; ``pos`` is inside the string."""
        while True:
            match = _STRING_STRUCTURAL.search(self.text, self.pos)
            if match is None:
                self._advance_window()
                continue
            char = match.group()
            self.pos = match.end()
            if char == '"':
                return
            if char != "\\":
                self.pos -= 1
                raise self._malformed("control character in string")
            # An escape needs up to five more characters; make them visible
            # before validating so a window boundary cannot split it.
            self.fill(5)
            escape = _ESCAPE_TOKEN.match(self.text, self.pos)
            if escape is None:
                raise self._malformed("invalid escape sequence")
            self.pos = escape.end()
            if self.pos >= self._chunk_bytes:
                self.trim()

    def _skip_scalar(self) -> None:
        """Validate one number or literal token; it may span windows."""
        start = self.byte_pos
        pieces: list[str] = []
        while True:
            match = _SCALAR_END.search(self.text, self.pos)
            if match is None:
                if self.eof:
                    pieces.append(self.text[self.pos :])
                    self.pos = len(self.text)
                    break
                pieces.append(self.text[self.pos :])
                self._advance_window()
                continue
            pieces.append(self.text[self.pos : match.start()])
            self.pos = match.start()
            break
        token = "".join(pieces)
        if not token:
            raise CanonicalArtifactSyntaxError(
                f"canonical audit artifact is malformed at byte {start}: expected a value"
            )
        if _NON_STRING_SCALAR.match(token) is None:
            raise CanonicalArtifactSyntaxError(
                f"canonical audit artifact is malformed at byte {start}: invalid token {token[:32]!r}"
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


class RawJsonDocument:
    """A pre-encoded JSON value: a byte span of a source emitted verbatim.

    The streaming encoder copies its bytes chunk by chunk, so a whole
    canonical artifact can be embedded in a larger JSON document (the
    legacy inline persistence lane) without decoding or re-encoding it.
    """

    __slots__ = ("_end", "_source", "_start")

    def __init__(self, source: ArtifactSource, *, start: int = 0, end: int | None = None) -> None:
        self._source = source
        self._start = int(start)
        self._end = int(source.identity.size if end is None else end)

    @property
    def byte_length(self) -> int:
        return self._end - self._start

    def iter_byte_chunks(self, *, chunk_bytes: int = SCAN_CHUNK_BYTES) -> Iterator[bytes]:
        self._source.verify_identity()
        offset = self._start
        while offset < self._end:
            chunk = self._source.pread(offset, min(chunk_bytes, self._end - offset))
            if not chunk:
                raise CanonicalArtifactError("canonical audit artifact span is truncated")
            offset += len(chunk)
            yield chunk
        self._source.verify_identity()

    def iter_text_chunks(self, *, chunk_bytes: int = SCAN_CHUNK_BYTES) -> Iterator[str]:
        decoder = codecs.getincrementaldecoder("utf-8")()
        try:
            for chunk in self.iter_byte_chunks(chunk_bytes=chunk_bytes):
                text = decoder.decode(chunk)
                if text:
                    yield text
            tail = decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            raise CanonicalArtifactSyntaxError(
                f"canonical audit artifact is not valid UTF-8: {exc}"
            ) from exc
        if tail:
            yield tail


class StreamedJsonString:
    """A JSON string value whose text is produced chunk by chunk.

    The streaming encoder escapes each chunk as it goes, so a canonical
    encoding of a recipient-scaled member can be embedded as a *string*
    member of a larger document (the CTV recovery payload's
    ``manifest_set_json``) without ever holding the text whole. Chunks are
    Python text, so a boundary never splits a code point and chunk-wise
    escaping concatenates exactly.
    """

    __slots__ = ("_chunks_factory",)

    def __init__(self, chunks_factory: Callable[[], Iterable[str]]) -> None:
        self._chunks_factory = chunks_factory

    def iter_text_chunks(self) -> Iterator[str]:
        return iter(self._chunks_factory())

    def iter_encoded_chunks(self) -> Iterator[str]:
        yield '"'
        for chunk in self._chunks_factory():
            if chunk:
                yield json.dumps(chunk)[1:-1]
        yield '"'


class MappedSequence(Sequence):
    """A replayable sequence applying ``transform`` to another sequence's items.

    Iteration and indexing apply the transform on demand, so a derived
    per-item structure (the CTV recovery artifacts derived from lazy
    manifests) never exists as a whole list.
    """

    __slots__ = ("_base", "_transform")

    def __init__(self, base: Sequence[Any], transform: Callable[[Any], Any]) -> None:
        self._base = base
        self._transform = transform

    def __len__(self) -> int:
        return len(self._base)

    def __iter__(self) -> Iterator[Any]:
        for item in self._base:
            yield self._transform(item)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return MappedSequence(self._base[index], self._transform)
        return self._transform(self._base[index])

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


class RawJsonRecord(Mapping):
    """One JSON value normalized outside the coordinator's decoder.

    Built for records above :data:`RECORD_DECODE_SOFT_LIMIT_BYTES`: an
    isolated helper process decodes the record and returns its compact
    encodings (original key order, and sorted keys for equality) plus the
    members small enough to keep, which are spooled to an unlinked
    temporary file. The coordinator never calls ``json.loads`` on the
    record. Small members read like a mapping; a member above
    :data:`RAW_RECORD_MEMBER_LIMIT_BYTES` is reachable only through the
    streamed encodings and raises :class:`CanonicalArtifactError` when
    indexed. Equality with a mapping or another record compares the
    sorted-key encodings record by record; the streaming encoder emits the
    original-order encoding verbatim.
    """

    __slots__ = (
        "__weakref__",
        "_is_object",
        "_keys",
        "_members",
        "_omitted",
        "_ordered",
        "_sorted",
        "_spool",
    )

    def __init__(
        self,
        spool: ArtifactSource,
        *,
        ordered: tuple[int, int],
        sorted_span: tuple[int, int],
        keys: Sequence[str],
        members: Mapping[str, Any],
        omitted: Iterable[str],
        is_object: bool,
    ) -> None:
        self._spool = spool
        self._ordered = ordered
        self._sorted = sorted_span
        self._keys = tuple(keys)
        self._members = dict(members)
        self._omitted = frozenset(omitted)
        self._is_object = bool(is_object)

    @property
    def is_object(self) -> bool:
        return self._is_object

    @property
    def omitted_members(self) -> frozenset[str]:
        return self._omitted

    @property
    def encoded_length(self) -> int:
        return self._ordered[1] - self._ordered[0]

    def __getitem__(self, key: str) -> Any:
        if key in self._members:
            return self._members[key]
        if key in self._omitted:
            raise CanonicalArtifactError(
                f"record member {key!r} exceeds the in-process decode limit; "
                "it is available only through the streamed encodings"
            )
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __contains__(self, key: object) -> bool:
        return key in self._members or key in self._omitted

    def __repr__(self) -> str:
        return f"RawJsonRecord(members={list(self._keys)}, bytes={self.encoded_length})"

    def ordered_text_chunks(self) -> Iterator[str]:
        return RawJsonDocument(
            self._spool,
            start=self._ordered[0],
            end=self._ordered[1],
        ).iter_text_chunks()

    def sorted_text_chunks(self) -> Iterator[str]:
        return RawJsonDocument(
            self._spool,
            start=self._sorted[0],
            end=self._sorted[1],
        ).iter_text_chunks()

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        if isinstance(other, RawJsonRecord):
            return _text_streams_equal(self.sorted_text_chunks(), other.sorted_text_chunks())
        if isinstance(other, Mapping) or (
            isinstance(other, Sequence) and not isinstance(other, (str, bytes, bytearray))
        ) or other is None or isinstance(other, (str, int, float, bool)):
            try:
                other_chunks = iter_json_chunks(other, sort_keys=True)
            except (TypeError, ValueError):
                return False
            return _text_streams_equal(self.sorted_text_chunks(), other_chunks)
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result  # type: ignore[return-value]
        return not result

    __hash__ = None  # type: ignore[assignment]

    def close(self) -> None:
        self._spool.close()


def _text_streams_equal(left: Iterable[str], right: Iterable[str]) -> bool:
    """Compare two text streams without joining either."""
    left_iter = iter(left)
    right_iter = iter(right)
    left_buffer = ""
    right_buffer = ""
    left_done = right_done = False
    while True:
        while not left_buffer and not left_done:
            try:
                left_buffer = next(left_iter)
            except StopIteration:
                left_done = True
        while not right_buffer and not right_done:
            try:
                right_buffer = next(right_iter)
            except StopIteration:
                right_done = True
        if left_done or right_done:
            return left_done and right_done and not left_buffer and not right_buffer
        size = min(len(left_buffer), len(right_buffer))
        if left_buffer[:size] != right_buffer[:size]:
            return False
        left_buffer = left_buffer[size:]
        right_buffer = right_buffer[size:]


def _helper_main(argv: Sequence[str]) -> int:
    """Isolated record normalization: stdin record -> header + encodings.

    Runs in a child process so a record of any size is decoded outside the
    lease-bearing coordinator. Exit status 2 marks a malformed record.
    """
    import resource

    resource.setrlimit(resource.RLIMIT_AS, (RAW_RECORD_HELPER_MEMORY_BYTES, RAW_RECORD_HELPER_MEMORY_BYTES))
    member_limit = RAW_RECORD_MEMBER_LIMIT_BYTES
    if len(argv) >= 2 and argv[0] == "--normalize-record":
        member_limit = int(argv[1])
    elif argv != ["--normalize-record"]:
        sys.stderr.write("usage: --normalize-record [member-limit-bytes]\n")
        return 64
    raw = sys.stdin.buffer.read()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        sys.stderr.write(f"malformed record: {exc}\n")
        return 2
    ordered = json.dumps(value, separators=_COMPACT_SEPARATORS).encode("utf-8")
    sorted_text = json.dumps(value, sort_keys=True, separators=_COMPACT_SEPARATORS).encode("utf-8")
    keys: list[str] = []
    members: dict[str, Any] = {}
    omitted: list[str] = []
    is_object = isinstance(value, dict)
    if is_object:
        for key, item in value.items():
            keys.append(str(key))
            if len(json.dumps(item, separators=_COMPACT_SEPARATORS)) <= member_limit:
                members[str(key)] = item
            else:
                omitted.append(str(key))
    header = json.dumps(
        {
            "is_object": is_object,
            "keys": keys,
            "members": members,
            "omitted": omitted,
            "ordered_size": len(ordered),
            "sorted_size": len(sorted_text),
        },
        separators=_COMPACT_SEPARATORS,
    ).encode("utf-8")
    out = sys.stdout.buffer
    out.write(header + b"\n")
    out.write(ordered)
    out.write(sorted_text)
    out.flush()
    return 0


class HelperAdmission:
    """Bounded admission slots shared by isolated helper processes.

    Every helper kind that decodes a whole document outside the coordinator
    (the record normalizer here, the legacy candidate helper) can share one
    instance so their combined memory stays bounded by the slot count.
    Waiting is supervised by the caller's deadline/cancellation check.
    """

    __slots__ = ("_semaphore", "slots")

    def __init__(self, slots: int = RAW_RECORD_HELPER_SLOTS) -> None:
        self.slots = max(1, int(slots))
        self._semaphore = threading.BoundedSemaphore(self.slots)

    def acquire(
        self,
        check: Callable[[], None],
        *,
        poll_seconds: float = RAW_RECORD_HELPER_POLL_SECONDS,
    ) -> None:
        """Take a slot; ``check`` runs between polls and raises to give up."""
        while not self._semaphore.acquire(timeout=max(0.001, float(poll_seconds))):
            check()

    def release(self) -> None:
        self._semaphore.release()

    @contextmanager
    def hold(self, check: Callable[[], None]) -> Iterator[None]:
        self.acquire(check)
        try:
            check()
            yield
        finally:
            self.release()


RECORD_HELPER_ADMISSION = HelperAdmission()


def _feed_helper_stdin(process: subprocess.Popen[bytes], source: ArtifactSource, start: int, end: int) -> list[BaseException]:
    errors: list[BaseException] = []
    assert process.stdin is not None
    try:
        offset = start
        while offset < end:
            chunk = source.pread(offset, min(SCAN_CHUNK_BYTES, end - offset))
            if not chunk:
                raise CanonicalArtifactError("canonical audit artifact span is truncated")
            process.stdin.write(chunk)
            offset += len(chunk)
        process.stdin.flush()
    except BrokenPipeError:
        pass
    except BaseException as exc:  # surfaced by the supervisor
        errors.append(exc)
    finally:
        try:
            process.stdin.close()
        except OSError:
            pass
    return errors


def _read_exact(stream: Any, size: int) -> Iterator[bytes]:
    remaining = size
    while remaining > 0:
        chunk = stream.read(min(SCAN_CHUNK_BYTES, remaining))
        if not chunk:
            raise ArtifactResourcePressure("record helper output ended early")
        remaining -= len(chunk)
        yield chunk


class _HelperOutput:
    """What the stdout reader thread collected: header facts and the spool."""

    __slots__ = ("error", "header", "no_header", "spool_fd")

    def __init__(self) -> None:
        self.header: dict[str, Any] | None = None
        self.no_header = False
        self.spool_fd: int | None = None
        self.error: BaseException | None = None


def _read_helper_output(stream: Any, output: _HelperOutput) -> None:
    """Read the header line, then stream both encodings into a scratch file."""
    try:
        header_line = stream.readline(RAW_RECORD_HELPER_HEADER_BYTES + 1)
        if len(header_line) > RAW_RECORD_HELPER_HEADER_BYTES:
            raise ArtifactResourcePressure("record helper metadata exceeds its byte limit")
        if not header_line:
            output.no_header = True
            return
        try:
            header = json.loads(header_line)
            parsed = {
                "ordered_size": int(header["ordered_size"]),
                "sorted_size": int(header["sorted_size"]),
                "keys": [str(key) for key in header["keys"]],
                "members": dict(header["members"]),
                "omitted": [str(key) for key in header["omitted"]],
                "is_object": bool(header["is_object"]),
            }
        except (ValueError, KeyError, TypeError) as exc:
            raise ArtifactResourcePressure(f"record helper header is malformed: {exc}") from exc
        output.spool_fd = _scratch_fd("record spool")
        offset = 0
        for chunk in _read_exact(stream, parsed["ordered_size"] + parsed["sorted_size"]):
            _pwrite_all(output.spool_fd, chunk, offset, purpose="record spool")
            offset += len(chunk)
        output.header = parsed
    except BaseException as exc:  # classified by the supervisor
        output.error = exc


def _drain_helper_stderr(stream: Any, tail: bytearray, limit: int) -> None:
    """Drain diagnostics so a noisy child never blocks; keep only the tail."""
    try:
        while True:
            chunk = stream.read(8192)
            if not chunk:
                return
            tail.extend(chunk)
            if len(tail) > limit:
                del tail[:-limit]
    except OSError:
        return


def normalize_record_isolated(
    source: ArtifactSource,
    start: int,
    end: int,
    *,
    member_limit: int = RAW_RECORD_MEMBER_LIMIT_BYTES,
    timeout_seconds: float = RAW_RECORD_HELPER_TIMEOUT_SECONDS,
    cancellation: Callable[[], None] | None = None,
    admission: HelperAdmission | None = None,
) -> RawJsonRecord:
    """Normalize the record at ``source[start:end)`` in a helper process.

    The record bytes stream to the helper through a pipe and its encodings
    stream back into an unlinked scratch file, so the coordinator holds at
    most one read chunk of the record at any time. Every protocol step --
    the wait for an admission slot, feeding stdin, reading the header and
    the encodings, draining diagnostics -- runs under one deadline
    (``timeout_seconds`` from the call) and the optional ``cancellation``
    check, which raises to abandon the helper. On every exit the child is
    killed if still alive, reaped, and its pipes closed; a scratch
    descriptor not handed to the record is closed. No ``preexec_fn`` is
    used: the coordinator is multithreaded.

    A malformed record is a :class:`CanonicalArtifactSyntaxError`. A helper
    that cannot be admitted or spawned in time, exits abnormally, times out,
    or a scratch file that cannot be written is
    :class:`ArtifactResourcePressure`: retryable, never corruption.
    """
    deadline = time.monotonic() + float(timeout_seconds)

    def check() -> None:
        if cancellation is not None:
            cancellation()
        if time.monotonic() >= deadline:
            raise ArtifactResourcePressure("record helper exceeded its deadline")

    slots = admission if admission is not None else RECORD_HELPER_ADMISSION
    slots.acquire(check)
    try:
        return _run_record_helper(source, start, end, member_limit=member_limit, check=check)
    finally:
        slots.release()


def _run_record_helper(
    source: ArtifactSource,
    start: int,
    end: int,
    *,
    member_limit: int,
    check: Callable[[], None],
) -> RawJsonRecord:
    check()
    command = [sys.executable, "-m", __name__, "--normalize-record", str(int(member_limit))]
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
    except OSError as exc:
        raise ArtifactResourcePressure(f"cannot start the record helper: {exc}") from exc
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    output = _HelperOutput()
    feeder_errors: list[BaseException] = []
    diagnostics = bytearray()
    threads = [
        threading.Thread(
            target=lambda: feeder_errors.extend(_feed_helper_stdin(process, source, start, end)),
            name="prism-audit-record-helper-feed",
            daemon=True,
        ),
        threading.Thread(
            target=_read_helper_output,
            args=(process.stdout, output),
            name="prism-audit-record-helper-output",
            daemon=True,
        ),
        threading.Thread(
            target=_drain_helper_stderr,
            args=(process.stderr, diagnostics, RAW_RECORD_HELPER_DIAGNOSTIC_BYTES),
            name="prism-audit-record-helper-stderr",
            daemon=True,
        ),
    ]
    try:
        for thread in threads:
            thread.start()
        while process.poll() is None:
            check()
            if output.error is not None or feeder_errors:
                break
            time.sleep(RAW_RECORD_HELPER_POLL_SECONDS)
        if process.poll() is None:
            # A transport error while the child still runs: stop it now so
            # the pipe threads unblock, then report the error below.
            process.kill()
            process.wait()
        for thread in threads:
            while thread.is_alive():
                check()
                thread.join(RAW_RECORD_HELPER_POLL_SECONDS)
        tail = diagnostics.decode("utf-8", "replace").strip()
        if process.returncode == 2:
            raise CanonicalArtifactSyntaxError(
                "canonical audit artifact record is malformed: " + tail
            )
        if process.returncode != 0:
            raise ArtifactResourcePressure(
                f"record helper exited with status {process.returncode}: {tail}"
            )
        if feeder_errors:
            raise feeder_errors[0]
        if output.error is not None:
            raise output.error
        if output.no_header or output.header is None or output.spool_fd is None:
            raise ArtifactResourcePressure("record helper produced no header: " + tail)
        header = output.header
        spool_fd = output.spool_fd
        output.spool_fd = None
        try:
            spool = ArtifactSource(spool_fd, path=None)
        except BaseException:
            _close_fd(spool_fd)
            raise
        return RawJsonRecord(
            spool,
            ordered=(0, header["ordered_size"]),
            sorted_span=(header["ordered_size"], header["ordered_size"] + header["sorted_size"]),
            keys=header["keys"],
            members=header["members"],
            omitted=header["omitted"],
            is_object=header["is_object"],
        )
    finally:
        if process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
        try:
            process.wait()
        except OSError:
            pass
        for stream in (process.stdin, process.stdout, process.stderr):
            try:
                stream.close()
            except OSError:
                pass
        for thread in threads:
            thread.join(timeout=5.0)
        if output.spool_fd is not None:
            _close_fd(output.spool_fd)


def streamed_sha256_json_hex(value: Any) -> str:
    """``sha256(json.dumps(value, sort_keys=True, separators=(",", ":")))``, streamed.

    The ledger's ``sha256_json_hex`` over a value that may embed lazy
    sequences or isolated records, without building the encoding whole.
    """
    digest = hashlib.sha256()
    for chunk in iter_json_byte_chunks(value, sort_keys=True):
        digest.update(chunk)
    return digest.hexdigest()


def materialize_json(value: Any) -> Any:
    """A plain JSON object graph for consumers that need one.

    Lazy sequences become lists, views become dictionaries, and an isolated
    record is decoded in-process from its ordered encoding. This is the
    explicit boundary for statement builders that must embed a whole
    recipient-scaled member; it is never applied to a window-sized member
    on the finalization path.
    """
    if isinstance(value, RawJsonRecord):
        return json.loads("".join(value.ordered_text_chunks()))
    if isinstance(value, RawJsonDocument):
        return json.loads("".join(value.iter_text_chunks()))
    if isinstance(value, StreamedJsonString):
        return "".join(value.iter_text_chunks())
    if not isinstance(value, (Mapping, Sequence)):
        # A streamed scalar from another bounded view family (a candidate
        # spool string or raw token): text when it has some, else its JSON.
        if callable(getattr(value, "iter_text_chunks", None)):
            return "".join(value.iter_text_chunks())
        if callable(getattr(value, "iter_encoded_chunks", None)):
            return json.loads("".join(value.iter_encoded_chunks()))
    if isinstance(value, Mapping):
        return {str(key): materialize_json(item) for key, item in value.items()}
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        return value
    return [materialize_json(item) for item in value]


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


def _encode_record_batches(
    records: Iterable[Any],
    *,
    sort_keys: bool,
    batch_records: int,
    chunk_chars: int,
) -> Iterator[str]:
    """Array body (no brackets) of a record stream, isolated records verbatim.

    Plain records are encoded ``batch_records`` at a time through one
    ``json.dumps``; an isolated :class:`RawJsonRecord` streams its own
    encoding between batches.
    """
    batch: list[Any] = []
    first = True

    def flush() -> Iterator[str]:
        nonlocal batch, first
        if not batch:
            return
        text = json.dumps(batch, sort_keys=sort_keys, separators=_COMPACT_SEPARATORS)[1:-1]
        batch = []
        if not first:
            text = "," + text
        first = False
        yield text

    for record in records:
        if isinstance(record, RawJsonRecord):
            yield from flush()
            if not first:
                yield ","
            first = False
            yield from (record.sorted_text_chunks() if sort_keys else record.ordered_text_chunks())
            continue
        if not _is_plain(record, 2):
            yield from flush()
            if not first:
                yield ","
            first = False
            yield from _encode_pieces(
                record,
                sort_keys=sort_keys,
                batch_records=batch_records,
                chunk_chars=chunk_chars,
            )
            continue
        batch.append(record)
        if len(batch) >= batch_records:
            yield from flush()
    yield from flush()


def _encode_pieces(
    value: Any,
    *,
    sort_keys: bool,
    batch_records: int,
    chunk_chars: int,
) -> Iterator[str]:
    if isinstance(value, RawJsonRecord):
        yield from (value.sorted_text_chunks() if sort_keys else value.ordered_text_chunks())
        return
    if isinstance(value, RawJsonDocument):
        if sort_keys:
            raise TypeError("a pre-encoded document cannot be re-sorted")
        yield from value.iter_text_chunks()
        return
    if isinstance(value, StreamedJsonString):
        yield from value.iter_encoded_chunks()
        return
    if not isinstance(value, (Mapping, Sequence)) and callable(
        getattr(value, "iter_encoded_chunks", None)
    ):
        # A streamed scalar from another bounded view family (a candidate
        # spool string or raw token) is emitted verbatim; its encoding does
        # not depend on key order. Lazy containers take the generic Mapping
        # and Sequence routes below, member by member.
        yield from value.iter_encoded_chunks()
        return
    if isinstance(value, Mapping):
        keys = list(value)
        if any(not isinstance(key, str) for key in keys):
            yield json.dumps(
                value if isinstance(value, dict) else dict(value),
                sort_keys=sort_keys,
                separators=_COMPACT_SEPARATORS,
            )
            return
        if sort_keys:
            keys.sort()
        yield "{"
        first = True
        for key in keys:
            yield ("" if first else ",") + json.dumps(key) + ":"
            first = False
            yield from _encode_pieces(
                value[key],
                sort_keys=sort_keys,
                batch_records=batch_records,
                chunk_chars=chunk_chars,
            )
        yield "}"
        return
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        yield json.dumps(value, sort_keys=sort_keys, separators=_COMPACT_SEPARATORS)
        return
    yield "["
    if isinstance(value, (LazyRecordSequence, LazyRecordSlice, MappedSequence)):
        yield from _encode_record_batches(
            value,
            sort_keys=sort_keys,
            batch_records=batch_records,
            chunk_chars=chunk_chars,
        )
    elif isinstance(value, (list, tuple)) and _is_plain(value, 2):
        yield from iter_json_array_text_chunks(
            value,
            sort_keys=sort_keys,
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
                sort_keys=sort_keys,
                batch_records=batch_records,
                chunk_chars=chunk_chars,
            )
    yield "]"


def iter_json_chunks(
    value: Any,
    *,
    sort_keys: bool = False,
    batch_records: int = JSON_BATCH_RECORDS,
    chunk_chars: int = JSON_CHUNK_CHARS,
) -> Iterator[str]:
    """``json.dumps(value, separators=(",", ":"), sort_keys=...)`` in bounded text chunks.

    Mappings are encoded member by member, lazy record sequences and plain
    lists batch by batch (each ``json.dumps`` covers at most
    ``batch_records`` items), isolated records and pre-encoded documents
    verbatim from their scratch files, and every other value through one
    ``json.dumps`` bounded by that value's own size. Concatenating the
    chunks reproduces the compact encoding byte for byte; every chunk is
    ASCII unless a pre-encoded document carries raw UTF-8.
    """
    coalescer = _Coalescer(chunk_chars)
    for piece in _encode_pieces(
        value,
        sort_keys=sort_keys,
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
    sort_keys: bool = False,
    batch_records: int = JSON_BATCH_RECORDS,
    chunk_chars: int = JSON_CHUNK_CHARS,
) -> Iterator[bytes]:
    """:func:`iter_json_chunks`, UTF-8 encoded chunk by chunk."""
    for chunk in iter_json_chunks(
        value,
        sort_keys=sort_keys,
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
        "_cancellation",
        "_chunk_bytes",
        "_count",
        "_end",
        "_index",
        "_index_base",
        "_source",
        "_start",
        "_stats",
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
        stats: ScanStats | None = None,
        cancellation: Callable[[], None] | None = None,
    ) -> None:
        self._source = source
        self._start = int(start)
        self._end = int(end)
        self._count = int(count)
        self._index = index
        self._index_base = int(index_base)
        self._stride = max(1, int(stride))
        self._chunk_bytes = int(chunk_bytes)
        self._stats = stats if stats is not None else ScanStats()
        # Runs once per read chunk of every lazy read and before each
        # isolated-helper step; raises to abandon the work.
        self._cancellation = cancellation

    def _decode_record(self, cursor: _Cursor) -> Any:
        """Decode the record at the cursor, isolating an oversized one.

        The record is first skipped structurally (bounded), which yields its
        byte span. A record within the in-process limit is decoded from the
        window text when it is still held, otherwise from one positional
        read of exactly that span; a larger record goes to the isolated
        helper and comes back as a :class:`RawJsonRecord`.
        """
        cursor.skip_ws()
        text_start = cursor.pos
        generation = cursor.generation
        start = cursor.byte_pos
        cursor.skip_value()
        end = cursor.byte_pos
        size = end - start
        if size <= RECORD_DECODE_SOFT_LIMIT_BYTES:
            if cursor.generation == generation:
                text = cursor.text[text_start : cursor.pos]
            else:
                text = self._source.pread(start, size).decode("utf-8")
            try:
                return json.loads(text)
            except ValueError as exc:
                raise CanonicalArtifactSyntaxError(
                    f"canonical audit artifact record is malformed: {exc}"
                ) from exc
        self._stats.isolated_records += 1
        return normalize_record_isolated(
            self._source, start, end, cancellation=self._cancellation
        )

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
        return _Cursor(
            self._source,
            offset,
            chunk_bytes=self._chunk_bytes,
            checkpoint=self._cancellation,
        )

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
            yield self._decode_record(cursor)
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
    cancellation: Callable[[], None] | None = None,
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
            stats=cursor.stats,
            cancellation=cancellation,
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
        stats=cursor.stats,
        cancellation=cancellation,
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
    cancellation: Callable[[], None] | None = None,
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
                cancellation=cancellation,
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
                    cancellation=cancellation,
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
        within one bounded read; the lazy sequences the view hands out keep
        it, so later lazy reads and isolated-helper runs honour it as well.
        The whole file is consumed so the returned digest covers every
        byte, and trailing garbage is rejected.
        """
        source = ArtifactSource(fd, path=path)
        try:
            index = _CheckpointIndex()
        except BaseException:
            source.close()
            raise
        try:
            hasher = hashlib.sha256()
            stats = ScanStats()
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
                cancellation=cancellation,
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


def spool_json_document(
    chunks: Iterable[bytes],
    *,
    lazy_paths: Sequence[tuple[str, ...]],
    stride: int = RECORD_INDEX_STRIDE,
) -> CanonicalAuditBundleView:
    """Write a JSON document to an unlinked scratch file and scan it lazily.

    Used for derived metadata that would otherwise become a window-scaled
    list -- the compact body's share-part index -- so it stays replayable
    on disk with the same bounded readers as the artifact itself.
    """
    fd = _scratch_fd("json spool")
    try:
        offset = 0
        for chunk in chunks:
            _pwrite_all(fd, chunk, offset, purpose="json spool")
            offset += len(chunk)
    except BaseException:
        _close_fd(fd)
        raise
    return CanonicalAuditBundleView.scan(fd, lazy_paths=lazy_paths, stride=stride)


__all__ = [
    "ArtifactIdentity",
    "ArtifactResourcePressure",
    "ArtifactSource",
    "CANONICAL_BUNDLE_LAZY_PATHS",
    "CanonicalArtifactError",
    "CanonicalArtifactSyntaxError",
    "CanonicalAuditBundleView",
    "HelperAdmission",
    "JSON_BATCH_RECORDS",
    "JSON_CHUNK_CHARS",
    "LazyRecordSequence",
    "LazyRecordSlice",
    "MappedSequence",
    "RAW_RECORD_HELPER_DIAGNOSTIC_BYTES",
    "RAW_RECORD_HELPER_POLL_SECONDS",
    "RAW_RECORD_HELPER_SLOTS",
    "RAW_RECORD_HELPER_TIMEOUT_SECONDS",
    "RAW_RECORD_MEMBER_LIMIT_BYTES",
    "RECORD_DECODE_SOFT_LIMIT_BYTES",
    "RECORD_HELPER_ADMISSION",
    "RECORD_INDEX_STRIDE",
    "RawJsonDocument",
    "RawJsonRecord",
    "SCAN_CHUNK_BYTES",
    "ScanStats",
    "StreamedJsonString",
    "compare_streaming",
    "is_lazy_sequence",
    "iter_json_byte_chunks",
    "iter_json_chunks",
    "json_chunks_sha256_and_size",
    "materialize_json",
    "normalize_record_isolated",
    "spool_json_document",
    "streamed_sha256_json_hex",
]


if __name__ == "__main__":
    raise SystemExit(_helper_main(sys.argv[1:]))
