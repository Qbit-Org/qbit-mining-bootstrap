#!/usr/bin/env python3
"""Bounded JSON encoding and digesting for PRISM share arrays.

Why this exists: a payout window at union-mainnet size (roughly 210k
accepted shares; 400k at the stress size) used to pass through single
``json.dumps``/``json.loads`` calls -- the daemon prepare request, the
audit-builder compact payload, the digest of a freshly read snapshot, the
lazy parse of a daemon-prepared window -- each one a C call that holds the
GIL for hundreds of milliseconds while the writer-lease monitor thread waits
to run (#236). Every helper here produces or consumes exactly the bytes the
historical whole-array call did, while bounding any one C call to a batch
of records, so the interpreter can switch threads between batches.

Byte identity is the contract, not a nicety: canonical share bytes are
hashed into ``share_snapshot_sha256`` and mirrored by the Rust builder, so
the streamed encoding must reproduce CPython's ``json.dumps`` output exactly
(``ensure_ascii`` escapes, non-BMP surrogate pairs, sorted keys where
canonical, arbitrary-precision integers). The batch form relies on one fact
about CPython's encoder that the tests pin: ``json.dumps(items)[1:-1]`` is
the items' individual encodings joined by ``,``, so a batch of any size
encodes to the same bytes as the whole array minus its brackets.

This module is a leaf: standard library only, imported by the ledger, the
bundle compiler and the payout-state service alike.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from typing import Any, Callable

# Records encoded per ``json.dumps`` call. 512 matches the payout window's
# page size, so a page encodes in exactly one bounded call.
SHARE_JSON_BATCH_RECORDS = 512
# Target size of one yielded chunk. Batches are coalesced up to this many
# characters (bytes: every chunk is ASCII) before being handed to a
# transport, so a pipe write or spool write moves tens of KiB at a time
# instead of one record.
SHARE_JSON_CHUNK_BYTES = 64 * 1024

_COMPACT_SEPARATORS = (",", ":")
# Entries dropped per ``del`` slice by release_share_list_incrementally.
RELEASE_BATCH_RECORDS = 2048


def _batch_encoder(
    *,
    sort_keys: bool,
    default: Callable[[Any], Any] | None,
) -> Callable[[list[Any]], str]:
    """One ``json.dumps`` over a batch, minus the array brackets."""

    def encode(batch: list[Any]) -> str:
        return json.dumps(
            batch,
            sort_keys=sort_keys,
            separators=_COMPACT_SEPARATORS,
            default=default,
        )[1:-1]

    return encode


class _ChunkCoalescer:
    """Accumulates text pieces and releases them in bounded chunks."""

    __slots__ = ("_chunk_chars", "_pending", "_pending_chars")

    def __init__(self, chunk_chars: int) -> None:
        self._chunk_chars = int(chunk_chars)
        self._pending: list[str] = []
        self._pending_chars = 0

    def push(self, text: str) -> str | None:
        """Take one piece; return a chunk once the target size is reached."""
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
        chunk = (
            self._pending[0]
            if len(self._pending) == 1
            else "".join(self._pending)
        )
        self._pending = []
        self._pending_chars = 0
        return chunk


def iter_json_array_text_chunks(
    items: Iterable[Any],
    *,
    sort_keys: bool = False,
    default: Callable[[Any], Any] | None = None,
    batch_records: int = SHARE_JSON_BATCH_RECORDS,
    chunk_chars: int = SHARE_JSON_CHUNK_BYTES,
) -> Iterator[str]:
    """The items' encodings joined by ``,`` -- no brackets -- in bounded chunks.

    Concatenating every yielded chunk gives exactly
    ``json.dumps(list(items), separators=(",", ":"), sort_keys=sort_keys,
    default=default)[1:-1]``. No chunk ever splits an item, so a consumer
    may frame chunk boundaries however it likes (a spool file, a pipe, a
    digest). Each ``json.dumps`` call covers at most ``batch_records`` items.
    """
    if batch_records <= 0:
        raise ValueError("batch_records must be positive")
    encode = _batch_encoder(sort_keys=sort_keys, default=default)
    coalescer = _ChunkCoalescer(chunk_chars)
    batch: list[Any] = []
    first_batch = True
    for item in items:
        batch.append(item)
        if len(batch) < batch_records:
            continue
        text = encode(batch)
        batch = []
        if not first_batch:
            text = "," + text
        first_batch = False
        chunk = coalescer.push(text)
        if chunk is not None:
            yield chunk
    if batch:
        text = encode(batch)
        if not first_batch:
            text = "," + text
        chunk = coalescer.push(text)
        if chunk is not None:
            yield chunk
    chunk = coalescer.flush()
    if chunk is not None:
        yield chunk


def iter_json_array_byte_chunks(
    items: Iterable[Any],
    *,
    sort_keys: bool = False,
    default: Callable[[Any], Any] | None = None,
    batch_records: int = SHARE_JSON_BATCH_RECORDS,
    chunk_chars: int = SHARE_JSON_CHUNK_BYTES,
) -> Iterator[bytes]:
    """:func:`iter_json_array_text_chunks`, UTF-8 encoded chunk by chunk."""
    for chunk in iter_json_array_text_chunks(
        items,
        sort_keys=sort_keys,
        default=default,
        batch_records=batch_records,
        chunk_chars=chunk_chars,
    ):
        yield chunk.encode("utf-8")


def iter_json_object_text_chunks(
    fields: Mapping[str, Any],
    *,
    array_keys: Collection[str],
    sort_keys: bool = False,
    default: Callable[[Any], Any] | None = None,
    batch_records: int = SHARE_JSON_BATCH_RECORDS,
    chunk_chars: int = SHARE_JSON_CHUNK_BYTES,
) -> Iterator[str]:
    """One JSON object in bounded chunks, streaming its large array members.

    Concatenating every yielded chunk gives exactly ``json.dumps(fields,
    separators=(",", ":"), sort_keys=sort_keys, default=default)``. Members
    named in ``array_keys`` whose values are lists or tuples are encoded
    item-batch by item-batch through :func:`iter_json_array_text_chunks`;
    every other member is one ``json.dumps`` call, which is bounded by that
    member's own size (found-block facts, balances, policy). A mapping with a
    non-string key is encoded whole so CPython's own key coercion rules keep
    applying; no share payload has such keys.
    """
    keys = list(fields)
    if any(not isinstance(key, str) for key in keys):
        yield json.dumps(
            fields,
            sort_keys=sort_keys,
            separators=_COMPACT_SEPARATORS,
            default=default,
        )
        return
    if sort_keys:
        keys.sort()
    coalescer = _ChunkCoalescer(chunk_chars)
    chunk = coalescer.push("{")
    if chunk is not None:
        yield chunk
    first = True
    for key in keys:
        value = fields[key]
        prefix = ("" if first else ",") + json.dumps(key) + ":"
        first = False
        if key in array_keys and isinstance(value, (list, tuple)):
            chunk = coalescer.push(prefix + "[")
            if chunk is not None:
                yield chunk
            for piece in iter_json_array_text_chunks(
                value,
                sort_keys=sort_keys,
                default=default,
                batch_records=batch_records,
                chunk_chars=chunk_chars,
            ):
                chunk = coalescer.push(piece)
                if chunk is not None:
                    yield chunk
            chunk = coalescer.push("]")
        else:
            chunk = coalescer.push(
                prefix
                + json.dumps(
                    value,
                    sort_keys=sort_keys,
                    separators=_COMPACT_SEPARATORS,
                    default=default,
                )
            )
        if chunk is not None:
            yield chunk
    chunk = coalescer.push("}")
    if chunk is not None:
        yield chunk
    chunk = coalescer.flush()
    if chunk is not None:
        yield chunk


def iter_canonical_share_item_chunks(
    shares: Iterable[Any],
    *,
    batch_records: int = SHARE_JSON_BATCH_RECORDS,
    chunk_chars: int = SHARE_JSON_CHUNK_BYTES,
) -> Iterator[bytes]:
    """Canonical share items (sorted keys, ``default=str``) in bounded chunks.

    Joined, the chunks are byte-identical to the historical
    ``b",".join(json.dumps(share, sort_keys=True, separators=(",", ":"),
    default=str).encode() for share in shares)`` -- the payout window page
    encoding, the digest input framed by ``[``/``]``, and the daemon's
    ``canonical_items`` stream.
    """
    return iter_json_array_byte_chunks(
        shares,
        sort_keys=True,
        default=str,
        batch_records=batch_records,
        chunk_chars=chunk_chars,
    )


def canonical_share_items_bytes(shares: Iterable[Any]) -> bytes:
    """The complete canonical items stream for one bounded batch of shares.

    For a window page (at most the batch size) this is one ``json.dumps``
    call; callers holding a whole window must stream the chunks instead.
    """
    return b"".join(iter_canonical_share_item_chunks(shares))


def canonical_share_array_sha256(shares: Iterable[Any]) -> str:
    """``sha256(json.dumps(list(shares), sort_keys=True, ...))`` streamed.

    Prefers a ``canonical_json_sha256`` method when the sequence carries one
    (the paged window view, the daemon mirror), so an existing digest is
    reused rather than recomputed; otherwise the array framing and every
    record batch feed one running SHA-256 without allocating a whole
    serialized array.
    """
    existing = getattr(shares, "canonical_json_sha256", None)
    if callable(existing):
        return str(existing())
    digest = hashlib.sha256()
    digest.update(b"[")
    for chunk in iter_canonical_share_item_chunks(shares):
        digest.update(chunk)
    digest.update(b"]")
    return digest.hexdigest()


class ShareArrayJsonSequence(Sequence):
    """A plain share list with the paged view's ``canonical_json_sha256``.

    The coordinator's digest hooks (``canonical_json_sha256`` in the job
    bundle, payout-state and coordinator modules, and the test-patchable
    overrides wired through them) already prefer a sequence's own digest
    method over serializing the value whole. Wrapping a freshly converted
    list in this view routes it through that same preference, so the one
    O(window) ``json.dumps`` those hooks would otherwise make becomes the
    streamed digest -- without changing the hooks or what they are called
    with elsewhere.
    """

    __slots__ = ("_shares",)

    def __init__(self, shares: Sequence[Any]) -> None:
        self._shares = shares

    def __len__(self) -> int:
        return len(self._shares)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._shares)

    def __getitem__(self, index: int | slice) -> Any:
        return self._shares[index]

    def canonical_json_sha256(self) -> str:
        return canonical_share_array_sha256(self._shares)


def share_array_json_view(shares: Sequence[Any]) -> Any:
    """``shares`` itself when it can digest itself, else a streaming view."""
    if callable(getattr(shares, "canonical_json_sha256", None)):
        return shares
    return ShareArrayJsonSequence(shares)


def release_share_list_incrementally(
    values: Any,
    *,
    batch_records: int = RELEASE_BATCH_RECORDS,
) -> None:
    """Empty a finished per-share list in bounded slices.

    Dropping a list of a few hundred thousand records or dicts frees every
    element in one refcount cascade, which CPython runs as a single C call
    under the GIL -- measured at ~90-215 ms for 210k-400k parsed shares in
    the #236 probe, on the same order as the JSON calls this module bounds.
    Deleting from the tail in slices lets the interpreter switch threads
    between batches. Only a list can be emptied in place; anything else is
    left to ordinary garbage collection.
    """
    if not isinstance(values, list):
        return
    batch_records = max(1, int(batch_records))
    while values:
        del values[-batch_records:]


__all__ = [
    "SHARE_JSON_BATCH_RECORDS",
    "SHARE_JSON_CHUNK_BYTES",
    "ShareArrayJsonSequence",
    "canonical_share_array_sha256",
    "canonical_share_items_bytes",
    "iter_canonical_share_item_chunks",
    "iter_json_array_byte_chunks",
    "iter_json_array_text_chunks",
    "iter_json_object_text_chunks",
    "release_share_list_incrementally",
    "share_array_json_view",
]
