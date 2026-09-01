#!/usr/bin/env python3
"""Single-writer accepted-share ledger helpers for the direct PRISM coordinator."""

from __future__ import annotations

import json
import copy
import hashlib
import logging
import os
import math
import shlex
import subprocess
import time
import traceback
import uuid
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import BoundedSemaphore, Lock, Thread, local
from typing import Any, Callable, ClassVar, Iterator, Protocol, runtime_checkable

from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    CanonicalAuditBundleCorrupt,
    canonical_audit_bundle_bytes,
)
from lab.prism.writer_lease_timing import (  # noqa: F401 - compatibility re-export
    DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
    WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS,
    WRITER_LEASE_VERIFICATION_MAX_STATEMENTS,
)


LOGGER = logging.getLogger(__name__)


def _default_bundle_canonicalizer() -> Callable[[dict[str, Any]], bytes]:
    # Resolved at call time: bundle_compiler pulls in coordinator_config,
    # whose CTV imports reach back into this module during initialization.
    from lab.prism.bundle_compiler import canonical_bundle_bytes

    return canonical_bundle_bytes

AUDIT_BODY_REF_SCHEMA = "qbit.prism.audit-body-ref.v1"
AUDIT_BUNDLE_V2_SCHEMA = "qbit.prism.audit-bundle.v2"
AUDIT_SHARE_SEGMENT_SCHEMA = "qbit.prism.audit-share-segment.v1"
AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA = "qbit.prism.window-completeness-proof.v1"
DEFAULT_AUDIT_SHARE_SEGMENT_SIZE = 10_000
DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT = 20
DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS = 300
DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE = 512
# Startup writer-lease acquisition runs each statement under this per-attempt
# lock/statement deadline and gives up after this many timed-out attempts (see
# _run_lease_acquisition_json). 5 attempts x 5s per attempt (~25s) is sized to
# outlast a typical orphan reap rather than to guarantee one: the 15s
# idle_in_transaction_session_timeout below runs on the blocking backend's own
# clock, started when its transaction went idle and not when the successor
# began retrying, so the two are not directly comparable. In the common case
# the reap lands inside the budget and a later attempt acquires the lease with
# no operator action; a budget that runs out reaches the fatal error, which is
# still the point — a bounded visible failure instead of an unbounded wait.
DEFAULT_LEASE_ACQUIRE_ATTEMPTS = 5
DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS = 5.0
# Session guards every coordinator Postgres session carries (see
# PostgresSessionGuards). idle_in_transaction_session_timeout makes the server
# abort a transaction whose client vanished between statements — the failure
# that otherwise leaves the qbit_ledger_writer_lease row lock held until TCP
# keepalive teardown (hours at OS defaults). The server-side tcp_keepalives_*
# GUCs are the backstop for backends idle *outside* a transaction, where the
# idle-in-transaction timer does not apply: on a TCP connection whose platform
# implements them, the defaults bound the server's teardown of a socket toward
# a vanished client at 30 + 3x10 = 60 seconds. Unix-socket connections ignore
# them, leaving the idle-in-transaction timeout as the guard there.
DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS = 15.0
# read_replica_status() runs on the public read service's background probe
# thread on a 5s cadence by default; a probe that outlives its own interval
# tells the freshness gate nothing it does not already know from the previous
# answer's age, so bound it well inside one interval.
DEFAULT_READ_REPLICA_PROBE_TIMEOUT_SECONDS = 3.0
DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT = 3
DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS = 30
DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS = 10
# Only the coordinator assigns this prefix, and PsqlShareLedger preserves it
# only after acquiring the writer/epoch advisory guard. Other ledger users and
# psql-only deployments retain ordinary TTL fencing and are never treated as
# fast-adoptable merely because they share an identity with a replacement.
WRITER_LEASE_HEARTBEAT_SESSION_PREFIX = "heartbeat-v1:"
# The third arm of _try_acquire_writer_lease's COALESCE. Both of the first two
# arms are empty at once in exactly one reachable case: the first-ever
# concurrent acquisition against an empty lease table, where the loser's
# ON CONFLICT DO UPDATE affects no rows and the holder SELECT still reads the
# statement snapshot it took before the winner committed. That is a retry
# signal local to this one statement -- a fresh statement snapshot sees the
# committed row -- so the statement names it instead of evaluating to SQL NULL
# and reaching the generic no-JSON parser error.
WRITER_LEASE_ACQUIRE_RETRY_KEY = "lease_snapshot_retry"
WRITER_LEASE_ACQUIRE_RETRY_SUBJECT = "qbit ledger writer lease"
# Attempts spent taking a fresh statement snapshot before the race is treated
# as something other than the transient it is. Two attempts suffice under
# READ COMMITTED (the second statement's snapshot postdates the winner's
# commit); the third covers a re-raced row and bounds a caller that pinned an
# older snapshot for the whole transaction, which no retry can advance.
WRITER_LEASE_ACQUIRE_RETRY_ATTEMPTS = 3


class WriterLeaseRenewalDeferred(RuntimeError):
    """An external side effect was withheld while the lease renewal is deferred.

    The guarded session is live, but its lease TTL renewal is blocked behind
    this coordinator's own fenced write that has outlasted the TTL. Liveness
    there assumes the write commits; a rollback would instead hand the
    expired row to a queued different-identity claimant, so the fence
    refuses the RPC without fencing the process. Callers retry on their own
    cadence (broadcast pass interval, block-candidate outbox replay) and
    succeed once a verification lands a renewal.

    Defined here, with the lease machinery, so side-effect executors like
    the CTV broadcaster can pass the refusal through to their retrying
    caller instead of misclassifying it as a failed attempt.
    """


# WRITER_LEASE_VERIFICATION_MAX_STATEMENTS and
# DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS now live in
# lab.prism.writer_lease_timing, next to the rest of the heartbeat timing
# policy they are terms of, and are re-exported above so every existing
# import site keeps working.
VALID_CREDIT_POLICIES = frozenset({"stale-grace"})


class LedgerOperationTimeout(TimeoutError):
    """A caller-scoped PostgreSQL deadline expired before work completed."""


class ReadOnlyLedgerError(RuntimeError):
    """A read-only ledger was asked for the writer lock."""


class _RefusingWriterGate:
    """Stands in for the writer lock on a read-only ledger.

    Every fenced statement and every writer-lock read in PsqlShareLedger --
    appends, block landing, settlement, and the O(recipients) reads like
    current_owed_balances() and audit_bundle() -- reaches the lock through
    ``_operation_gate(self._lock, "writer lock")``. Substituting a gate that
    refuses to be acquired makes "this process never fences" a property of the
    object rather than of which methods its callers happen to use, so a future
    public route that reached a fenced read would fail loudly here instead of
    quietly serializing dashboard traffic against the block-landing path.
    """

    __slots__ = ()

    _MESSAGE = "ledger is read-only: the writer lock is not available"

    def acquire(self, timeout: float = -1.0) -> bool:
        raise ReadOnlyLedgerError(self._MESSAGE)

    def release(self) -> None:
        raise ReadOnlyLedgerError(self._MESSAGE)

    def __enter__(self) -> None:
        raise ReadOnlyLedgerError(self._MESSAGE)

    def __exit__(self, *exc_info: object) -> None:
        raise ReadOnlyLedgerError(self._MESSAGE)


def _is_postgres_deadline_error(error: BaseException | str) -> bool:
    """Recognize backend cancellations caused by an armed caller deadline."""
    message = str(error).casefold()
    sqlstate = getattr(error, "sqlstate", None)
    if sqlstate is None:
        sqlstate = getattr(getattr(error, "diag", None), "sqlstate", None)
    normalized_sqlstate = str(sqlstate or "").upper()
    if normalized_sqlstate in {"57014", "55P03"}:
        return True
    # psql's verbose mode exposes SQLSTATE even when lc_messages localizes the
    # text. This helper is called only while our statement/lock deadlines are
    # armed, and ledger SQL does not use NOWAIT, so scoped 55P03 is a timeout.
    if "57014:" in message or "55p03:" in message:
        return True
    return any(
        marker in message
        for marker in (
            "canceling statement due to statement timeout",
            "canceling statement due to lock timeout",
            "connection timeout expired",
            "timeout expired",
            "connection timed out",
            "operation timed out",
        )
    )


def validate_credit_policy(credit_policy: str | None) -> str | None:
    if credit_policy is None:
        return None
    if credit_policy not in VALID_CREDIT_POLICIES:
        raise ValueError(f"unsupported credit_policy: {credit_policy!r}")
    return credit_policy


@dataclass(frozen=True)
class AcceptedShareRecord:
    share_seq: int
    share_id: str
    miner_id: str
    order_key: str
    p2mr_program_hex: str
    share_difficulty: int
    network_difficulty: int
    template_height: int
    job_id: str
    job_issued_at_ms: int
    accepted_at_ms: int
    ntime: int
    credit_policy: str | None = None
    # Append-result metadata is deliberately excluded from the durable/public
    # share identity. It lets the coordinator make process-local accounting
    # idempotent and observe the candidate state from the same transaction
    # that established the pre-submit outbox boundary.
    newly_inserted: bool = field(default=True, compare=False, repr=False)
    candidate_outbox_state: str | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    def to_prism_json(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "share_seq": self.share_seq,
            "share_id": self.share_id,
            "miner_id": self.miner_id,
            "order_key": self.order_key,
            "p2mr_program_hex": self.p2mr_program_hex,
            "share_difficulty": self.share_difficulty,
            "network_difficulty": self.network_difficulty,
            "template_height": self.template_height,
            "job_id": self.job_id,
            "job_issued_at_ms": self.job_issued_at_ms,
            "accepted_at_ms": self.accepted_at_ms,
            "ntime": self.ntime,
        }
        if self.credit_policy is not None:
            payload["credit_policy"] = self.credit_policy
        return payload


@dataclass(frozen=True)
class PrismWindowShare:
    share: AcceptedShareRecord
    counted_difficulty: Decimal


@dataclass(frozen=True)
class PendingShare:
    share_id: str
    miner_id: str
    order_key: str
    p2mr_program_hex: str
    share_difficulty: int
    network_difficulty: int
    template_height: int
    job_id: str
    job_issued_at_ms: int
    accepted_at_ms: int
    ntime: int
    credit_policy: str | None = None


class IncrementalWindowFallback(RuntimeError):
    """The cached payout window cannot be advanced from an append-only delta."""


@dataclass(frozen=True)
class IncrementalWindowAdvanceStats:
    """Bounded work performed while advancing one cached payout window."""

    added_rows: int
    expired_rows: int
    touched_pages: int


@dataclass(frozen=True)
class _IncrementalShareWindowPage:
    records: tuple[AcceptedShareRecord, ...]
    total_difficulty: int
    prism_json_records: tuple[dict[str, object], ...]
    canonical_json_items: bytes

    @classmethod
    def from_records(
        cls,
        records: tuple[AcceptedShareRecord, ...],
    ) -> _IncrementalShareWindowPage:
        prism_json_records = tuple(record.to_prism_json() for record in records)
        return cls(
            records=records,
            total_difficulty=sum(int(record.share_difficulty) for record in records),
            prism_json_records=prism_json_records,
            canonical_json_items=b",".join(
                json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
                for record in prism_json_records
            ),
        )


@dataclass(frozen=True)
class IncrementalShareJsonSequence(Sequence[dict[str, object]]):
    """Immutable JSON view backed by the payout window's persistent pages.

    Advancing a window allocates JSON only for append/head boundary pages.
    Iteration remains wire-identical to the historical flat tuple, while the
    canonical digest streams already-encoded page fragments without calling
    ``to_prism_json`` on retained shares.
    """

    pages: tuple[_IncrementalShareWindowPage, ...]
    record_count: int

    def __len__(self) -> int:
        return self.record_count

    def __iter__(self) -> Iterator[dict[str, object]]:
        for page in self.pages:
            yield from page.prism_json_records

    def __getitem__(
        self,
        index: int | slice,
    ) -> dict[str, object] | tuple[dict[str, object], ...]:
        if isinstance(index, slice):
            return tuple(self)[index]
        resolved = index
        if resolved < 0:
            resolved += self.record_count
        if resolved < 0 or resolved >= self.record_count:
            raise IndexError(index)
        for page in self.pages:
            if resolved < len(page.prism_json_records):
                return page.prism_json_records[resolved]
            resolved -= len(page.prism_json_records)
        raise IndexError(index)

    def canonical_json_sha256(self) -> str:
        digest = hashlib.sha256()
        digest.update(b"[")
        needs_separator = False
        for page in self.pages:
            if not page.canonical_json_items:
                continue
            if needs_separator:
                digest.update(b",")
            digest.update(page.canonical_json_items)
            needs_separator = True
        digest.update(b"]")
        return digest.hexdigest()


@dataclass(frozen=True)
class IncrementalShareWindow:
    """Immutable paged cache of the exact whole-share payout-window superset.

    Pages are stable in ascending ``share_seq`` order. Normal advancement only
    extends the newest page and expires weight from the oldest pages; retained
    interior pages are never revisited. The final whole share crossing
    ``window_weight`` is deliberately retained, matching the Postgres oracle.
    """

    anchor_job_issued_at_ms: int
    window_weight: int
    page_size: int
    pages: tuple[_IncrementalShareWindowPage, ...]
    total_difficulty: int

    @classmethod
    def from_full_snapshot(
        cls,
        records: list[AcceptedShareRecord] | tuple[AcceptedShareRecord, ...],
        *,
        anchor_job_issued_at_ms: int,
        window_weight: int,
        page_size: int = DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
    ) -> IncrementalShareWindow:
        """Build cache state from the full-rescan oracle.

        Applying the exact crossing-row cutoff here normalizes full-oracle
        implementations and also provides the authoritative reset after an
        incremental invariant failure.
        """

        anchor_job_issued_at_ms = int(anchor_job_issued_at_ms)
        window_weight = int(window_weight)
        page_size = int(page_size)
        if window_weight <= 0:
            raise ValueError("window_weight must be positive")
        if page_size <= 0:
            raise ValueError("page_size must be positive")

        eligible = sorted(
            (
                record
                for record in records
                if int(record.job_issued_at_ms) <= anchor_job_issued_at_ms
                and int(record.accepted_at_ms) <= anchor_job_issued_at_ms
            ),
            key=lambda record: int(record.share_seq),
        )
        prior_seq: int | None = None
        share_ids: set[str] = set()
        for record in eligible:
            share_seq = int(record.share_seq)
            if prior_seq is not None and share_seq <= prior_seq:
                raise ValueError("full payout window contains duplicate share_seq")
            if record.share_id in share_ids:
                raise ValueError("full payout window contains duplicate share_id")
            if int(record.share_difficulty) <= 0:
                raise ValueError("full payout window contains non-positive difficulty")
            prior_seq = share_seq
            share_ids.add(record.share_id)

        start = len(eligible)
        retained_weight = 0
        for index in range(len(eligible) - 1, -1, -1):
            if retained_weight >= window_weight:
                break
            retained_weight += int(eligible[index].share_difficulty)
            start = index
        retained = tuple(eligible[start:])
        pages = tuple(
            _IncrementalShareWindowPage.from_records(
                retained[offset : offset + page_size]
            )
            for offset in range(0, len(retained), page_size)
        )
        return cls(
            anchor_job_issued_at_ms=anchor_job_issued_at_ms,
            window_weight=window_weight,
            page_size=page_size,
            pages=pages,
            total_difficulty=retained_weight,
        )

    def records(self) -> tuple[AcceptedShareRecord, ...]:
        return tuple(record for page in self.pages for record in page.records)

    @property
    def record_count(self) -> int:
        """Retained record total; shared surface with DaemonShareWindowMirror."""
        return sum(len(page.records) for page in self.pages)

    def json_records(self) -> IncrementalShareJsonSequence:
        return IncrementalShareJsonSequence(
            pages=self.pages,
            record_count=sum(len(page.records) for page in self.pages),
        )

    def advance(
        self,
        delta_records: list[AcceptedShareRecord]
        | tuple[AcceptedShareRecord, ...],
        *,
        anchor_job_issued_at_ms: int,
    ) -> tuple[IncrementalShareWindow, IncrementalWindowAdvanceStats]:
        """Fold one newly eligible append delta and expire only the old edge."""

        anchor_job_issued_at_ms = int(anchor_job_issued_at_ms)
        if anchor_job_issued_at_ms < self.anchor_job_issued_at_ms:
            raise IncrementalWindowFallback("snapshot anchor moved backwards")

        delta = tuple(delta_records)
        prior_seq = (
            int(self.pages[-1].records[-1].share_seq) if self.pages else None
        )
        prior_delta_seq: int | None = None
        for record in delta:
            share_seq = int(record.share_seq)
            if int(record.share_difficulty) <= 0:
                raise IncrementalWindowFallback(
                    "delta contains non-positive share difficulty"
                )
            if (
                int(record.job_issued_at_ms) > anchor_job_issued_at_ms
                or int(record.accepted_at_ms) > anchor_job_issued_at_ms
            ):
                raise IncrementalWindowFallback(
                    "delta contains a share ineligible at the new anchor"
                )
            if (
                int(record.job_issued_at_ms) <= self.anchor_job_issued_at_ms
                and int(record.accepted_at_ms) <= self.anchor_job_issued_at_ms
            ):
                raise IncrementalWindowFallback(
                    "delta repeats a share eligible at the previous anchor"
                )
            if prior_seq is not None and share_seq <= prior_seq:
                raise IncrementalWindowFallback(
                    "newly eligible share is not an append"
                )
            if prior_delta_seq is not None and share_seq <= prior_delta_seq:
                raise IncrementalWindowFallback("delta share_seq order is not increasing")
            prior_delta_seq = share_seq

        # Copy page references, never their retained contents. ``touched`` is
        # intentionally defined as pre-existing retained boundary pages whose
        # records must be inspected or rewritten; newly allocated append pages
        # and pages expired wholesale are not part of the retained interior.
        pages = list(self.pages)
        touched_existing_pages: set[int] = set()
        delta_offset = 0
        if delta and pages and len(pages[-1].records) < self.page_size:
            available = self.page_size - len(pages[-1].records)
            appended = delta[:available]
            if appended:
                pages[-1] = _IncrementalShareWindowPage.from_records(
                    pages[-1].records + appended
                )
                delta_offset = len(appended)
                touched_existing_pages.add(len(self.pages) - 1)
        while delta_offset < len(delta):
            page_records = delta[delta_offset : delta_offset + self.page_size]
            pages.append(_IncrementalShareWindowPage.from_records(page_records))
            delta_offset += len(page_records)

        total_difficulty = self.total_difficulty + sum(
            int(record.share_difficulty) for record in delta
        )
        expired_rows = 0
        first_retained_page = 0
        while (
            first_retained_page < len(pages)
            and total_difficulty
            - pages[first_retained_page].total_difficulty
            >= self.window_weight
        ):
            page = pages[first_retained_page]
            total_difficulty -= page.total_difficulty
            expired_rows += len(page.records)
            first_retained_page += 1
        original_page_offset = first_retained_page
        if first_retained_page:
            pages = pages[first_retained_page:]

        if pages:
            first_page = pages[0]
            partial_expired = 0
            while (
                partial_expired < len(first_page.records)
                and total_difficulty
                - int(first_page.records[partial_expired].share_difficulty)
                >= self.window_weight
            ):
                total_difficulty -= int(
                    first_page.records[partial_expired].share_difficulty
                )
                partial_expired += 1
            if partial_expired:
                pages[0] = _IncrementalShareWindowPage.from_records(
                    first_page.records[partial_expired:]
                )
                expired_rows += partial_expired
                if original_page_offset < len(self.pages):
                    touched_existing_pages.add(original_page_offset)

        advanced = IncrementalShareWindow(
            anchor_job_issued_at_ms=anchor_job_issued_at_ms,
            window_weight=self.window_weight,
            page_size=self.page_size,
            pages=tuple(pages),
            total_difficulty=total_difficulty,
        )
        return advanced, IncrementalWindowAdvanceStats(
            added_rows=len(delta),
            expired_rows=expired_rows,
            touched_pages=len(touched_existing_pages),
        )


class DaemonWindowMirrorDivergence(RuntimeError):
    """The coordinator's byte mirror disagrees with what the daemon reported.

    Every mirror construction re-hashes the canonical items it holds against
    the digest the daemon reported AND counts the records those bytes
    actually contain against the count it reported, so a divergence between
    the two implementations -- or a bug in the byte surgery itself -- becomes
    a detected full-rescan instead of a silently wrong payout artifact.
    Both checks are eager: nothing downstream may hold a mirror whose bytes
    and metadata have not already been reconciled.
    """


def _canonical_items_layout(fragment: bytes) -> tuple[int, bool]:
    """Record count and trailing-separator flag for one canonical items span.

    Walks the span with ``raw_decode``, which reports where each record's
    JSON ends without re-encoding anything, so the count comes from the
    bytes themselves rather than from a number the daemon declared. A span
    that is not a run of complete records separated by ``,`` -- a truncated
    record, a doubled or leading separator, junk between records -- is a
    divergence, not a parse the caller may retry.

    The trailing flag distinguishes ``a,b`` from ``a,b,``: a dropped prefix
    legitimately ends on a separator when records remain behind it, and a
    complete stream never does.
    """
    if not fragment:
        return 0, False
    try:
        text = fragment.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DaemonWindowMirrorDivergence(
            "daemon window mirror items are not valid UTF-8"
        ) from exc
    decoder = json.JSONDecoder()
    count = 0
    position = 0
    length = len(text)
    while True:
        if text[position] != "{":
            raise DaemonWindowMirrorDivergence(
                "daemon window mirror items hold "
                f"{text[position]!r} where a record was expected"
            )
        try:
            _, end = decoder.raw_decode(text, position)
        except ValueError as exc:
            raise DaemonWindowMirrorDivergence(
                f"daemon window mirror items hold a malformed record: {exc}"
            ) from exc
        count += 1
        if end >= length:
            return count, False
        if text[end] != ",":
            raise DaemonWindowMirrorDivergence(
                "daemon window mirror items hold "
                f"{text[end]!r} where a record separator was expected"
            )
        position = end + 1
        if position >= length:
            return count, True


def _canonical_items_record_count(fragment: bytes) -> int:
    """Record count of one complete canonical items stream."""
    count, trailing = _canonical_items_layout(fragment)
    if trailing:
        raise DaemonWindowMirrorDivergence(
            "daemon window mirror items end on a record separator"
        )
    return count


class DaemonShareJsonSequence(Sequence):
    """Lazy, byte-backed twin of :class:`IncrementalShareJsonSequence`.

    When the daemon owns the payout-window fold, the coordinator holds only
    the canonical items stream (every record's canonical JSON encoding joined
    with ``,``) rather than materialized dicts. The routine build path needs
    only the length, the digest, and this object's identity; the rare
    consumers that genuinely need dicts (the found-block audit build, the
    durable candidate intent, the one-shot builder fallback) force one
    ``json.loads`` here, paying the parse exactly where the bytes are used.
    Iteration order and the canonical digest are byte-identical to the paged
    sequence by construction: both stream the same fragments in the same
    order, and the digest framing is invariant to page layout.

    The count check below is a floor, not the contract: a sequence handed
    out by :class:`DaemonShareWindowMirror` had its count reconciled with
    its bytes at construction, so no consumer of a mirror can be the first
    to learn of a divergence.
    """

    __slots__ = ("canonical_items", "record_count", "_parse_lock", "_parsed")

    def __init__(self, canonical_items: bytes, record_count: int) -> None:
        self.canonical_items = bytes(canonical_items)
        self.record_count = int(record_count)
        self._parse_lock = Lock()
        self._parsed: tuple[dict[str, object], ...] | None = None

    def __len__(self) -> int:
        return self.record_count

    def _records(self) -> tuple[dict[str, object], ...]:
        with self._parse_lock:
            if self._parsed is None:
                parsed = json.loads(b"[" + self.canonical_items + b"]")
                if len(parsed) != self.record_count:
                    raise DaemonWindowMirrorDivergence(
                        "daemon window mirror parsed "
                        f"{len(parsed)} records where {self.record_count}"
                        " were declared"
                    )
                self._parsed = tuple(parsed)
            return self._parsed

    def __iter__(self) -> Iterator[dict[str, object]]:
        return iter(self._records())

    def __getitem__(
        self,
        index: int | slice,
    ) -> dict[str, object] | tuple[dict[str, object], ...]:
        return self._records()[index]

    def canonical_json_sha256(self) -> str:
        digest = hashlib.sha256()
        digest.update(b"[")
        digest.update(self.canonical_items)
        digest.update(b"]")
        return digest.hexdigest()


@dataclass(frozen=True)
class DaemonShareWindowMirror:
    """Coordinator-held opaque mirror of one daemon-prepared payout window.

    Exposes the same identity surface as :class:`IncrementalShareWindow`
    (anchor, weight, page size, ``record_count``) so the payout-state cache
    policy code reads either interchangeably, while the window contents stay
    pre-encoded bytes the daemon produced. Advancing applies the daemon's
    reported byte surgery -- drop a prefix, append a suffix -- and every
    construction verifies the resulting stream hashes to the daemon's digest
    before anything downstream may consume it.
    """

    anchor_job_issued_at_ms: int
    window_weight: int
    page_size: int
    record_count: int
    canonical_items: bytes = field(repr=False)
    share_snapshot_sha256: str

    @staticmethod
    def _verified_items_digest(canonical_items: bytes, declared_digest: str) -> None:
        digest = hashlib.sha256()
        digest.update(b"[")
        digest.update(canonical_items)
        digest.update(b"]")
        if digest.hexdigest() != declared_digest:
            raise DaemonWindowMirrorDivergence(
                "daemon window mirror bytes do not hash to the daemon's digest"
            )

    @staticmethod
    def _verified_record_count(counted: int, declared_count: int) -> None:
        """Pin the declared count to one counted from the mirror's own bytes.

        The digest above pins the bytes but says nothing about the count the
        daemon reported alongside them, and the count is the one divergence
        that would otherwise surface only when some consumer first parsed the
        stream -- arbitrarily far from the materialization that produced it,
        and after a partial request may already be on the daemon's stdin.
        Counting eagerly here puts it beside the digest check, where every
        construction pays it once and no consumer can be surprised.
        """
        if counted != int(declared_count):
            raise DaemonWindowMirrorDivergence(
                f"daemon window mirror holds {counted} records where "
                f"{int(declared_count)} were declared"
            )

    @classmethod
    def from_full_items(
        cls,
        *,
        anchor_job_issued_at_ms: int,
        window_weight: int,
        page_size: int,
        record_count: int,
        canonical_items: bytes,
        share_snapshot_sha256: str,
    ) -> DaemonShareWindowMirror:
        cls._verified_items_digest(canonical_items, share_snapshot_sha256)
        cls._verified_record_count(
            _canonical_items_record_count(canonical_items),
            record_count,
        )
        return cls(
            anchor_job_issued_at_ms=int(anchor_job_issued_at_ms),
            window_weight=int(window_weight),
            page_size=int(page_size),
            record_count=int(record_count),
            canonical_items=bytes(canonical_items),
            share_snapshot_sha256=share_snapshot_sha256,
        )

    def _advanced_record_count(
        self,
        retained_drop_bytes: int,
        appended_items: bytes,
    ) -> int:
        """Count an advance's records from the surgery, not the whole stream.

        Only the dropped prefix and the appended suffix are walked, so this
        costs what the byte surgery itself costs rather than what a rescan of
        the retained window would. The already-verified count of this mirror
        supplies the retained middle, and the separator placement at both
        seams is checked: a drop or an append landing inside a record would
        otherwise let a miscount masquerade as an aligned edit.
        """
        dropped, dropped_trailing = _canonical_items_layout(
            self.canonical_items[:retained_drop_bytes]
        )
        retained_items = self.canonical_items[retained_drop_bytes:]
        if retained_items:
            if dropped and not dropped_trailing:
                raise DaemonWindowMirrorDivergence(
                    "daemon window advance dropped a partial record"
                )
        elif dropped_trailing:
            raise DaemonWindowMirrorDivergence(
                "daemon window advance dropped every record but kept a"
                " separator"
            )
        retained = int(self.record_count) - dropped
        if retained < 0:
            raise DaemonWindowMirrorDivergence(
                "daemon window advance dropped more records than the mirror"
                " holds"
            )
        if not appended_items:
            return retained
        if retained_items:
            # The retained span already ends on a record, so the suffix must
            # carry the separator that rejoins the two.
            if appended_items[:1] != b",":
                raise DaemonWindowMirrorDivergence(
                    "daemon window advance appended items without a record"
                    " separator"
                )
            appended = _canonical_items_record_count(appended_items[1:])
        else:
            appended = _canonical_items_record_count(appended_items)
        if not appended:
            # A suffix that carried only a separator would leave the stream
            # ending on one, which no complete window ever does.
            raise DaemonWindowMirrorDivergence(
                "daemon window advance appended a separator with no record"
            )
        return retained + appended

    def advanced(
        self,
        *,
        anchor_job_issued_at_ms: int,
        record_count: int,
        retained_drop_bytes: int,
        appended_items: bytes,
        share_snapshot_sha256: str,
    ) -> DaemonShareWindowMirror:
        retained_drop_bytes = int(retained_drop_bytes)
        if retained_drop_bytes < 0 or retained_drop_bytes > len(self.canonical_items):
            raise DaemonWindowMirrorDivergence(
                "daemon window advance dropped more bytes than the mirror holds"
            )
        if retained_drop_bytes == 0 and not appended_items:
            # Anchor-only advance: the stream is byte-identical, so a digest
            # string comparison replaces the copy and the re-hash, and the
            # count must be unchanged for the same reason.
            canonical_items = self.canonical_items
            if share_snapshot_sha256 != self.share_snapshot_sha256:
                raise DaemonWindowMirrorDivergence(
                    "daemon window advance changed the digest without bytes"
                )
            self._verified_record_count(self.record_count, record_count)
        else:
            canonical_items = (
                self.canonical_items[retained_drop_bytes:] + bytes(appended_items)
            )
            self._verified_items_digest(canonical_items, share_snapshot_sha256)
            self._verified_record_count(
                self._advanced_record_count(
                    retained_drop_bytes,
                    bytes(appended_items),
                ),
                record_count,
            )
        return DaemonShareWindowMirror(
            anchor_job_issued_at_ms=int(anchor_job_issued_at_ms),
            window_weight=self.window_weight,
            page_size=self.page_size,
            record_count=int(record_count),
            canonical_items=canonical_items,
            share_snapshot_sha256=share_snapshot_sha256,
        )

    def json_records(self) -> DaemonShareJsonSequence:
        return DaemonShareJsonSequence(
            canonical_items=self.canonical_items,
            record_count=self.record_count,
        )


@dataclass(frozen=True)
class BlockCandidateIntentPersistResult:
    inserted: bool
    state: str

    def __bool__(self) -> bool:
        return self.inserted


def _block_candidate_cursor_parts(cursor: object) -> tuple[object, str]:
    """Split one opaque pending-candidate cursor into its ordering parts.

    The cursor is whatever ``pending_block_candidate_rows`` handed back on a
    prior row and travels through the caller (and, for the Postgres backend,
    through JSON) untouched, so it is validated on the way back in rather
    than trusted. The shape is deliberately the two ordering columns and
    nothing else: ordering by the creation stamp alone cannot resume, because
    equal stamps would either re-emit or skip their peers.
    """
    if isinstance(cursor, str) or not isinstance(cursor, Sequence):
        raise ValueError("pending block candidate cursor is not a two-element list")
    parts = list(cursor)
    if len(parts) != 2:
        raise ValueError("pending block candidate cursor is not a two-element list")
    created_at, block_hash = parts
    if not isinstance(block_hash, str) or not block_hash:
        raise ValueError("pending block candidate cursor has no block hash")
    return created_at, block_hash


def _normalized_block_candidate_hash_set(
    block_hashes: Sequence[str],
) -> tuple[str, ...]:
    """Normalize a caller-supplied block-hash set for a batch outbox write.

    Both backends key the outbox on the lowercase hash, so the same
    lowercasing the single-hash terminal updates apply per call is applied
    once here to the whole set. Duplicates collapse and the result is sorted
    so a given input set always produces one identical target set --
    including the generated SQL text, which a caller must be able to reason
    about without knowing the caller's iteration order.
    """
    return tuple(sorted({str(block_hash).lower() for block_hash in block_hashes}))


def _memory_block_candidate_row_key(row: dict[str, Any]) -> tuple[float, str]:
    """Total-order key for one in-memory pending outbox row."""
    return (float(row.get("created_monotonic", 0.0)), str(row["block_hash"]))


def _memory_block_candidate_cursor_key(cursor: object) -> tuple[float, str]:
    """Rebuild the in-memory ordering key from a returned cursor."""
    created_monotonic, block_hash = _block_candidate_cursor_parts(cursor)
    if isinstance(created_monotonic, bool) or not isinstance(
        created_monotonic, (int, float)
    ):
        raise ValueError("pending block candidate cursor has no creation stamp")
    return (float(created_monotonic), block_hash)


class ShareReplayConflict(RuntimeError):
    """A recovery row reused a share ID with a different durable payload."""


@dataclass(frozen=True)
class ShareReplayResult:
    """Typed result for one legacy recovery-journal append."""

    disposition: str
    record: AcceptedShareRecord


class SingleWriterShareLedger:
    """Assigns canonical share_seq values and returns immutable snapshots.

    The direct Stratum coordinator should append accepted shares through one
    instance of this class. Later Postgres integration can keep this API shape
    while moving storage to `qbit_share_ledger`.
    """

    def __init__(
        self,
        *,
        first_share_seq: int = 1,
        ctv_broadcast_attempt_detail_limit: int = DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
        ctv_broadcast_retry_backoff_seconds: int = DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
    ):
        if first_share_seq < 1:
            raise ValueError("first_share_seq must be >= 1")
        ctv_broadcast_attempt_detail_limit = int(ctv_broadcast_attempt_detail_limit)
        if ctv_broadcast_attempt_detail_limit < 0:
            raise ValueError("ctv_broadcast_attempt_detail_limit must be non-negative")
        ctv_broadcast_retry_backoff_seconds = int(ctv_broadcast_retry_backoff_seconds)
        if ctv_broadcast_retry_backoff_seconds < 0:
            raise ValueError("ctv_broadcast_retry_backoff_seconds must be non-negative")
        self._ctv_broadcast_attempt_detail_limit = ctv_broadcast_attempt_detail_limit
        self._ctv_broadcast_retry_backoff_seconds = ctv_broadcast_retry_backoff_seconds
        self._next_share_seq = first_share_seq
        self._shares: list[AcceptedShareRecord] = []
        self._share_ids: set[str] = set()
        self._shares_by_id: dict[str, AcceptedShareRecord] = {}
        self._block_candidate_outbox: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_sets: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_statuses: dict[str, dict[str, Any]] = {}
        self._ctv_fanout_attempts: dict[str, list[dict[str, Any]]] = {}
        self._audit_publication_sequences: dict[str, int | None] = {}
        self._next_audit_publication_sequence = 1
        self._inactive_audit_publications: set[str] = set()
        self._memory_pool_blocks: dict[str, tuple[int, str, str]] = {}
        self._lock = Lock()

    def append(self, pending: PendingShare) -> AcceptedShareRecord:
        if pending.share_difficulty <= 0:
            raise ValueError("share_difficulty must be positive")
        if pending.network_difficulty <= 0:
            raise ValueError("network_difficulty must be positive")
        credit_policy = validate_credit_policy(pending.credit_policy)
        with self._lock:
            if pending.share_id in self._share_ids:
                existing = self._shares_by_id[pending.share_id]
                if self._pending_matches_record(pending, existing, credit_policy=credit_policy):
                    return replace(existing, newly_inserted=False)
                raise ValueError("duplicate share_id payload mismatch")
            record = AcceptedShareRecord(
                share_seq=self._next_share_seq,
                share_id=pending.share_id,
                miner_id=pending.miner_id,
                order_key=pending.order_key,
                p2mr_program_hex=pending.p2mr_program_hex,
                share_difficulty=pending.share_difficulty,
                network_difficulty=pending.network_difficulty,
                template_height=pending.template_height,
                job_id=pending.job_id,
                job_issued_at_ms=pending.job_issued_at_ms,
                accepted_at_ms=pending.accepted_at_ms,
                ntime=pending.ntime,
                credit_policy=credit_policy,
            )
            self._shares.append(record)
            self._share_ids.add(pending.share_id)
            self._shares_by_id[pending.share_id] = record
            self._next_share_seq += 1
            return record

    def append_recovered_share(self, pending: PendingShare) -> ShareReplayResult:
        """Append one recovery row with an explicit exact/conflict outcome."""
        if pending.share_difficulty <= 0:
            raise ValueError("share_difficulty must be positive")
        if pending.network_difficulty <= 0:
            raise ValueError("network_difficulty must be positive")
        credit_policy = validate_credit_policy(pending.credit_policy)
        with self._lock:
            existing = self._shares_by_id.get(pending.share_id)
            if existing is not None:
                if not self._pending_matches_record(
                    pending,
                    existing,
                    credit_policy=credit_policy,
                ):
                    raise ShareReplayConflict(
                        f"recovered share payload conflicts with {pending.share_id}"
                    )
                return ShareReplayResult(
                    "exact_existing",
                    replace(existing, newly_inserted=False),
                )
            record = AcceptedShareRecord(
                share_seq=self._next_share_seq,
                share_id=pending.share_id,
                miner_id=pending.miner_id,
                order_key=pending.order_key,
                p2mr_program_hex=pending.p2mr_program_hex,
                share_difficulty=pending.share_difficulty,
                network_difficulty=pending.network_difficulty,
                template_height=pending.template_height,
                job_id=pending.job_id,
                job_issued_at_ms=pending.job_issued_at_ms,
                accepted_at_ms=pending.accepted_at_ms,
                ntime=pending.ntime,
                credit_policy=credit_policy,
            )
            self._shares.append(record)
            self._share_ids.add(pending.share_id)
            self._shares_by_id[pending.share_id] = record
            self._next_share_seq += 1
            return ShareReplayResult("inserted", replace(record))

    @staticmethod
    def _pending_matches_record(
        pending: PendingShare,
        record: AcceptedShareRecord,
        *,
        credit_policy: str | None,
    ) -> bool:
        return (
            pending.share_id == record.share_id
            and pending.miner_id == record.miner_id
            and pending.order_key == record.order_key
            and pending.p2mr_program_hex.lower() == record.p2mr_program_hex.lower()
            and int(pending.share_difficulty) == int(record.share_difficulty)
            and int(pending.network_difficulty) == int(record.network_difficulty)
            and int(pending.template_height) == int(record.template_height)
            and pending.job_id == record.job_id
            and int(pending.job_issued_at_ms) == int(record.job_issued_at_ms)
            # accepted_at_ms is assigned when the coordinator receives an
            # attempt. An exact header replay after restart gets a fresh stamp;
            # the original durable row remains authoritative.
            and int(pending.ntime) == int(record.ntime)
            and credit_policy == record.credit_policy
        )

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, Any] | None]],
    ) -> list[AcceptedShareRecord]:
        """Atomically append a small coordinator group-commit batch.

        The in-memory backend is used by tests and local demonstrations.  Its
        lock provides the same all-at-once visibility expected from the
        Postgres implementation.
        """
        records: list[AcceptedShareRecord] = []
        with self._lock:
            # Validate the complete batch before mutating either collection.
            seen_ids: set[str] = set()
            seen_blocks: set[str] = set()
            for pending, candidate in entries:
                if pending.share_id in seen_ids:
                    raise ValueError("duplicate share_id in append batch")
                seen_ids.add(pending.share_id)
                credit_policy = validate_credit_policy(pending.credit_policy)
                existing = self._shares_by_id.get(pending.share_id)
                if existing is not None and not self._pending_matches_record(
                    pending, existing, credit_policy=credit_policy
                ):
                    raise ValueError("duplicate share_id payload mismatch")
                if candidate is not None:
                    block_hash = str(candidate.get("block_hash_hex", "")).lower()
                    if not block_hash:
                        raise ValueError("block candidate is missing block_hash_hex")
                    if block_hash in seen_blocks:
                        raise ValueError("duplicate block candidate in append batch")
                    seen_blocks.add(block_hash)
                    outbox = self._block_candidate_outbox.get(block_hash)
                    candidate_sha256 = block_candidate_identity_sha256(candidate)
                    if outbox is not None and (
                        outbox["share_id"] not in {None, pending.share_id}
                        or
                        outbox["candidate_sha256"] != candidate_sha256
                        or (
                            outbox["candidate"] is not None
                            and block_candidate_identity(outbox["candidate"])
                            != block_candidate_identity(candidate)
                        )
                    ):
                        raise ValueError("block candidate payload mismatch")

            for pending, candidate in entries:
                existing = self._shares_by_id.get(pending.share_id)
                newly_inserted = existing is None
                if existing is None:
                    credit_policy = validate_credit_policy(pending.credit_policy)
                    existing = AcceptedShareRecord(
                        share_seq=self._next_share_seq,
                        share_id=pending.share_id,
                        miner_id=pending.miner_id,
                        order_key=pending.order_key,
                        p2mr_program_hex=pending.p2mr_program_hex,
                        share_difficulty=pending.share_difficulty,
                        network_difficulty=pending.network_difficulty,
                        template_height=pending.template_height,
                        job_id=pending.job_id,
                        job_issued_at_ms=pending.job_issued_at_ms,
                        accepted_at_ms=pending.accepted_at_ms,
                        ntime=pending.ntime,
                        credit_policy=credit_policy,
                    )
                    self._shares.append(existing)
                    self._share_ids.add(pending.share_id)
                    self._shares_by_id[pending.share_id] = existing
                    self._next_share_seq += 1
                candidate_state: str | None = None
                if candidate is not None:
                    block_hash = str(candidate["block_hash_hex"]).lower()
                    self._block_candidate_outbox.setdefault(
                        block_hash,
                        {
                            "block_hash": block_hash,
                            "share_id": pending.share_id,
                            "candidate": candidate,
                            "candidate_sha256": block_candidate_identity_sha256(candidate),
                            "state": "pending",
                            "attempt_count": 0,
                            "last_error": None,
                            "created_monotonic": time.monotonic(),
                        },
                    )
                    self._block_candidate_outbox[block_hash]["share_id"] = pending.share_id
                    candidate_state = str(
                        self._block_candidate_outbox[block_hash]["state"]
                    )
                records.append(
                    replace(
                        existing,
                        newly_inserted=newly_inserted,
                        candidate_outbox_state=candidate_state,
                    )
                )
        return records

    def persist_block_candidate_intent(
        self,
        candidate: dict[str, Any],
    ) -> BlockCandidateIntentPersistResult:
        """Persist candidate work before a below-share-target synchronous submit."""
        block_hash = str(candidate.get("block_hash_hex", "")).lower()
        if not block_hash:
            raise ValueError("block candidate is missing block_hash_hex")
        candidate_sha256 = block_candidate_identity_sha256(candidate)
        with self._lock:
            existing = self._block_candidate_outbox.get(block_hash)
            if existing is not None:
                if existing["candidate_sha256"] != candidate_sha256:
                    raise ValueError("block candidate payload mismatch")
                return BlockCandidateIntentPersistResult(
                    inserted=False,
                    state=str(existing["state"]),
                )
            self._block_candidate_outbox[block_hash] = {
                "block_hash": block_hash,
                "share_id": None,
                "candidate": candidate,
                "candidate_sha256": candidate_sha256,
                "state": "pending",
                "attempt_count": 0,
                "last_error": None,
                "created_monotonic": time.monotonic(),
            }
            return BlockCandidateIntentPersistResult(
                inserted=True,
                state="pending",
            )

    def pending_block_candidates(self, *, limit: int = 32) -> list[dict[str, Any]]:
        return [
            row["candidate"]
            for row in self.pending_block_candidate_rows(limit=limit)
        ]

    def pending_block_candidate_rows(
        self,
        *,
        limit: int = 32,
        after_cursor: object | None = None,
    ) -> list[dict[str, Any]]:
        """Return pending payloads together with their authoritative row keys.

        Rows carry an opaque ``cursor`` the caller passes back verbatim as
        ``after_cursor`` to resume strictly after that row. The order is the
        total order ``(created, block_hash)``, so a page shorter than
        ``limit`` proves no further pending row existed at query time and a
        backlog of any size enumerates completely in bounded pages. The
        block-hash tiebreak is what makes it a *total* order: creation
        stamps collide (``time.monotonic`` here, one transaction's
        ``clock_timestamp`` in Postgres), and a cursor on a colliding stamp
        alone would either replay or skip its peers.

        Each row also carries ``pool_block_exists``: whether a durable
        ``qbit_pool_blocks`` row exists for that hash, i.e. whether the
        candidate ever reached ``persist_accepted_block``. It is answered
        inside this page read -- one bounded existence probe per returned row
        -- because the alternative is one round trip per row, and a page is
        read precisely when the backlog is large. The fact is advisory by the
        time the caller holds it; the terminal batch update re-checks it under
        the writer fence.
        """
        after = (
            None
            if after_cursor is None
            else _memory_block_candidate_cursor_key(after_cursor)
        )
        with self._lock:
            ordered = sorted(
                (
                    (_memory_block_candidate_row_key(row), row)
                    for row in self._block_candidate_outbox.values()
                    if row["state"] == "pending"
                ),
                key=lambda entry: entry[0],
            )
            return [
                {
                    "block_hash": str(row["block_hash"]),
                    "candidate": (
                        dict(row["candidate"])
                        if isinstance(row["candidate"], dict)
                        else row["candidate"]
                    ),
                    "pool_block_exists": (
                        str(row["block_hash"]) in self._memory_pool_blocks
                    ),
                    "cursor": list(key),
                }
                for key, row in ordered
                if after is None or key > after
            ][:limit]

    def block_candidate_pending_metrics(self) -> dict[str, int | float]:
        """Return bounded pending-candidate age gauges without exposing hashes."""
        now = time.monotonic()
        with self._lock:
            pending = [
                row
                for row in self._block_candidate_outbox.values()
                if row["state"] == "pending"
            ]
            unattempted = [
                row for row in pending if int(row["attempt_count"]) == 0
            ]

            def oldest_age(rows: list[dict[str, Any]]) -> float:
                return max(
                    (
                        max(0.0, now - float(row.get("created_monotonic", now)))
                        for row in rows
                    ),
                    default=0.0,
                )

            return {
                "pending_count": len(pending),
                "oldest_pending_age_seconds": oldest_age(pending),
                "oldest_unattempted_age_seconds": oldest_age(unattempted),
            }

    def mark_block_candidate_attempted(self, *, block_hash: str) -> bool:
        """Record admission to a real processing phase for one durable row."""
        with self._lock:
            row = self._block_candidate_outbox.get(block_hash.lower())
            if row is None or row["state"] != "pending":
                return False
            row["attempt_count"] = int(row["attempt_count"]) + 1
            return True

    def mark_block_candidate_submitted(self, *, block_hash: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="submitted", error=None)

    def mark_block_candidate_abandoned(self, *, block_hash: str, error: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="abandoned", error=error)

    def _finish_block_candidate(self, *, block_hash: str, state: str, error: str | None) -> bool:
        with self._lock:
            row = self._block_candidate_outbox.get(block_hash.lower())
            if row is None or row["state"] != "pending":
                return False
            row["state"] = state
            row["last_error"] = error
            row["candidate"] = None
            return True

    def mark_block_candidates_abandoned(
        self,
        *,
        block_hashes: Sequence[str],
        error: str,
    ) -> tuple[str, ...]:
        """Terminally abandon a caller-supplied page of pending rows at once.

        Mirrors ``mark_block_candidate_abandoned`` for a set: only rows whose
        current state is exactly ``pending`` transition, and the return value
        is the normalized hashes this call actually transitioned rather than a
        count, so the caller can restrict any follow-up cleanup to the rows it
        won. Already-terminal and missing hashes are neither returned nor
        mutated, and an empty set is a no-op.

        A hash that owns a durable pool-block row is also left alone: that row
        only exists for a candidate that was offered and landed, so it is not
        a superseded sibling regardless of what the caller observed earlier.

        The whole page runs under one lock acquisition so the returned set is
        the outcome of a single atomic decision, matching the Postgres backend
        where the same page is one fenced statement.
        """
        targets = _normalized_block_candidate_hash_set(block_hashes)
        if not targets:
            return ()
        abandoned: list[str] = []
        with self._lock:
            for block_hash in targets:
                row = self._block_candidate_outbox.get(block_hash)
                if row is None or row["state"] != "pending":
                    continue
                if block_hash in self._memory_pool_blocks:
                    continue
                row["state"] = "abandoned"
                row["last_error"] = error
                row["candidate"] = None
                abandoned.append(block_hash)
        return tuple(abandoned)

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> list[AcceptedShareRecord]:
        with self._lock:
            eligible = [
                replace(share)
                for share in self._shares
                if share.job_issued_at_ms <= anchor_job_issued_at_ms
                and share.accepted_at_ms <= anchor_job_issued_at_ms
            ]
        if window_weight is None:
            return eligible
        # Match the Postgres oracle's exact whole-share crossing rule. This
        # keeps synchronous and incremental artifacts byte-identical in local
        # and embedded deployments instead of treating the bound as a hint.
        return list(
            IncrementalShareWindow.from_full_snapshot(
                eligible,
                anchor_job_issued_at_ms=anchor_job_issued_at_ms,
                window_weight=int(window_weight),
            ).records()
        )

    def snapshot_between_job_issues(
        self,
        previous_anchor_job_issued_at_ms: int,
        anchor_job_issued_at_ms: int,
    ) -> list[AcceptedShareRecord]:
        """Return shares becoming eligible between two inclusive anchors."""

        previous_anchor = int(previous_anchor_job_issued_at_ms)
        anchor = int(anchor_job_issued_at_ms)
        if anchor < previous_anchor:
            raise ValueError("snapshot anchor moved backwards")
        if anchor == previous_anchor:
            return []
        with self._lock:
            return [
                replace(share)
                for share in self._shares
                if share.job_issued_at_ms <= anchor
                and share.accepted_at_ms <= anchor
                and (
                    share.job_issued_at_ms > previous_anchor
                    or share.accepted_at_ms > previous_anchor
                )
            ]

    def all_shares(self) -> list[AcceptedShareRecord]:
        with self._lock:
            return [replace(share) for share in self._shares]

    def accepted_share_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "accepted_share_count": len(self._shares),
                "distinct_miner_count": len({share.miner_id for share in self._shares}),
            }

    def current_owed_balances(self) -> list[dict[str, object]]:
        return []

    def current_prior_balances(self) -> list[dict[str, object]]:
        return []

    def carry_forward_integrity_report(self) -> dict[str, object]:
        return {
            "schema": "qbit.prism.carry-forward-integrity.v1",
            "backend": "memory",
            "checked_active_rows": 0,
            "audit_chain_version": "qbit.prism.carry-forward-active-delta-chain.v1",
            "audit_row_count": 0,
            "audit_head_sha256": "00" * 32,
            "mismatch_count": 0,
            "current_drift_count": 0,
            "current_drift": [],
            "mismatches": [],
        }

    def audit_share_window(
        self,
        *,
        anchor_job_issued_at_ms: int,
        network_difficulty: int,
    ) -> list[dict[str, object]]:
        return []

    def audit_block_payouts(self, *, block_hash: str) -> list[dict[str, object]]:
        return []

    def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
        return []

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        return None

    def audit_bundle_by_commitment(self, *, commitment_leaf_hex: str) -> dict[str, object] | None:
        return None

    def persist_ctv_fanout_manifest_set(
        self,
        *,
        block_hash: str,
        manifest_set: dict[str, Any],
        manifest_set_sha256: str,
    ) -> dict[str, int | str]:
        payload = ctv_fanout_recovery_payload(
            block_hash=block_hash,
            manifest_set=manifest_set,
            manifest_set_sha256=manifest_set_sha256,
        )
        with self._lock:
            existing = self._ctv_fanout_sets.get(block_hash)
            if existing is not None and existing != payload:
                raise RuntimeError("existing CTV fanout manifest set does not match payload")
            self._ctv_fanout_sets[block_hash] = copy.deepcopy(payload)
            for artifact in payload["artifacts"]:
                fanout_txid = str(artifact["fanout_txid"])
                existing_status = self._ctv_fanout_statuses.get(fanout_txid)
                status_payload = {
                    **copy.deepcopy(artifact),
                    "schema": "qbit.prism.ctv-fanout-status.v1",
                    "block_hash": block_hash,
                    "manifest_set_sha256": manifest_set_sha256,
                    "settlement_status": existing_status.get("settlement_status", "awaiting_maturity")
                    if existing_status
                    else "awaiting_maturity",
                    "broadcast_attempts": self._ctv_fanout_attempts.get(fanout_txid, []),
                }
                if existing_status:
                    for key in _ctv_broadcast_summary_fields():
                        if key in existing_status:
                            status_payload[key] = copy.deepcopy(existing_status[key])
                else:
                    status_payload.update(_empty_ctv_broadcast_summary())
                audit_bundle_sha256 = payload.get("audit_bundle_sha256")
                if audit_bundle_sha256 is not None:
                    status_payload["audit_bundle_sha256"] = audit_bundle_sha256
                status_payload["broadcast_attempt_summary"] = _ctv_broadcast_attempt_summary(status_payload)
                self._ctv_fanout_statuses[fanout_txid] = status_payload
        return {
            "backend": "memory",
            "fanout_set_count": 1,
            "fanout_artifact_count": len(payload["artifacts"]),
        }

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        with self._lock:
            payload = self._ctv_fanout_sets.get(block_hash)
            return copy.deepcopy(payload) if payload is not None else None

    def audit_ctv_fanouts(self, *, block_hash: str) -> list[dict[str, object]]:
        with self._lock:
            payload = self._ctv_fanout_sets.get(block_hash)
            if payload is None:
                return []
            return copy.deepcopy(payload["artifacts"])

    def ctv_fanout_status(self, *, fanout_txid: str) -> dict[str, object] | None:
        with self._lock:
            payload = self._ctv_fanout_statuses.get(fanout_txid)
            return copy.deepcopy(payload) if payload is not None else None

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        limit = max(1, min(int(limit), 1_000))
        now = datetime.now(timezone.utc)
        with self._lock:
            rows = [
                copy.deepcopy(payload)
                for payload in self._ctv_fanout_statuses.values()
                if payload.get("settlement_status") not in {"confirmed", "reorged", "failed"}
                and _ctv_broadcast_attempt_due(payload.get("next_broadcast_attempt_at"), now)
            ]
        rows.sort(key=lambda row: (str(row.get("block_hash", "")), int(row.get("chunk_index", 0))))
        return rows[:limit]

    def dashboard_pending_fanout_rows(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        now = datetime.now(timezone.utc)
        with self._lock:
            rows = [
                copy.deepcopy(payload)
                for payload in self._ctv_fanout_statuses.values()
                if payload.get("settlement_status") not in {"confirmed", "reorged", "failed"}
                and _ctv_broadcast_attempt_due(payload.get("next_broadcast_attempt_at"), now)
            ]
        rows.sort(key=lambda row: (str(row.get("block_hash", "")), int(row.get("chunk_index", 0))))
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
        }

    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        document = self.dashboard_public_artifact_document(sha256=sha256)
        if document is None:
            return None
        return document.get("payload")

    def dashboard_public_artifact_document(self, *, sha256: str) -> dict[str, object] | None:
        """Artifact payload plus, for manifest kinds, its canonical text.

        canonical_json is the exact serialized text the artifact's sha256 was
        computed over, persisted at record time. Audit bundles are hashed
        over Rust struct-order bytes that are not stored, so their
        canonical_json is None and they keep the re-serialized response.
        """
        with self._lock:
            for payload in self._ctv_fanout_sets.values():
                if payload.get("audit_bundle_sha256") == sha256:
                    audit_bundle = payload.get("audit_bundle")
                    if not isinstance(audit_bundle, dict):
                        return None
                    return {"payload": copy.deepcopy(audit_bundle), "canonical_json": None}
                if payload.get("manifest_set_sha256") == sha256:
                    manifest_set = payload.get("manifest_set")
                    if not isinstance(manifest_set, dict):
                        return None
                    manifest_set_json = payload.get("manifest_set_json")
                    return {
                        "payload": copy.deepcopy(manifest_set),
                        "canonical_json": manifest_set_json if isinstance(manifest_set_json, str) else None,
                    }
                for artifact in payload.get("artifacts", []):
                    if not isinstance(artifact, dict):
                        continue
                    if artifact.get("manifest_sha256") == sha256:
                        manifest = artifact.get("manifest")
                        if not isinstance(manifest, dict):
                            return None
                        manifest_json = artifact.get("manifest_json")
                        return {
                            "payload": copy.deepcopy(manifest),
                            "canonical_json": manifest_json if isinstance(manifest_json, str) else None,
                        }
        return None

    def update_ctv_fanout_status(self, *, fanout_txid: str, settlement_status: str) -> dict[str, int | str]:
        validate_ctv_fanout_status(settlement_status)
        with self._lock:
            if fanout_txid not in self._ctv_fanout_statuses:
                raise RuntimeError("unknown CTV fanout txid")
            self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = settlement_status
        return {"backend": "memory", "updated_count": 1}

    def record_ctv_fanout_broadcast_attempt(
        self,
        *,
        fanout_txid: str,
        attempt_status: str,
        package_tx_hexes: list[str] | None = None,
        package_txids: list[str] | None = None,
        submit_result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, int | str]:
        validate_ctv_fanout_attempt_status(attempt_status)
        with self._lock:
            if fanout_txid not in self._ctv_fanout_statuses:
                raise RuntimeError("unknown CTV fanout txid")
            attempts = self._ctv_fanout_attempts.setdefault(fanout_txid, [])
            attempted_at = datetime.now(timezone.utc)
            status_payload = self._ctv_fanout_statuses[fanout_txid]
            total_attempts = int(status_payload.get("broadcast_attempt_count") or 0) + 1
            attempt = {
                "attempt_seq": total_attempts,
                "attempted_at": attempted_at,
                "attempt_status": attempt_status,
                "package_tx_hexes": package_tx_hexes or [],
                "package_txids": package_txids or [],
                "submit_result": submit_result,
                "error": error,
            }
            if self._ctv_broadcast_attempt_detail_limit > 0:
                if len(attempts) >= self._ctv_broadcast_attempt_detail_limit:
                    del attempts[0 : len(attempts) - self._ctv_broadcast_attempt_detail_limit + 1]
                attempts.append(attempt)
            counts = copy.deepcopy(status_payload.get("broadcast_attempt_status_counts") or {})
            if not isinstance(counts, dict):
                counts = {}
            counts[attempt_status] = int(counts.get(attempt_status) or 0) + 1
            next_attempt_at = None
            retry_backoff_seconds = 0
            if attempt_status == "planned" and self._ctv_broadcast_retry_backoff_seconds > 0:
                retry_backoff_seconds = self._ctv_broadcast_retry_backoff_seconds
                next_attempt_at = attempted_at + timedelta(seconds=retry_backoff_seconds)
            status_payload.update(
                {
                    "broadcast_attempt_count": total_attempts,
                    "broadcast_attempt_detail_count": len(attempts),
                    "first_broadcast_attempt_at": status_payload.get("first_broadcast_attempt_at") or attempted_at,
                    "last_broadcast_attempt_at": attempted_at,
                    "last_broadcast_attempt_status": attempt_status,
                    "last_broadcast_package_tx_hexes": package_tx_hexes or [],
                    "last_broadcast_package_txids": package_txids or [],
                    "last_broadcast_submit_result": submit_result,
                    "last_broadcast_error": error,
                    "broadcast_attempt_status_counts": counts,
                    "next_broadcast_attempt_at": next_attempt_at,
                    "broadcast_retry_backoff_seconds": retry_backoff_seconds,
                }
            )
            self._ctv_fanout_statuses[fanout_txid]["broadcast_attempts"] = copy.deepcopy(attempts)
            if attempt_status in {"submitted", "accepted"}:
                self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = "broadcast_submitted"
            elif attempt_status in {"rejected", "failed"}:
                self._ctv_fanout_statuses[fanout_txid]["settlement_status"] = "failed"
            self._ctv_fanout_statuses[fanout_txid]["broadcast_attempt_summary"] = _ctv_broadcast_attempt_summary(
                self._ctv_fanout_statuses[fanout_txid]
            )
        return {"backend": "memory", "attempt_count": 1, "updated_count": 1 if attempt_status in {"submitted", "accepted", "rejected", "failed"} else 0}

    def metrics(self) -> dict[str, int]:
        return {
            "shares": len(self),
            "blocks": 0,
            "confirmed_blocks": 0,
            "inactive_blocks": 0,
            "rejected_blocks": 0,
            "reversed_blocks": 0,
            "payout_entries": 0,
            "owed_accounts": 0,
            "ctv_fanouts_failed": len(
                [
                    payload
                    for payload in self._ctv_fanout_statuses.values()
                    if payload.get("settlement_status") == "failed"
                ]
            ),
        }

    def dashboard_pool_snapshot(
        self,
        *,
        current_network_difficulty: int | str | Decimal,
        generated_at: str,
    ) -> dict[str, object]:
        from lab.prism import public_api

        shares = self.all_shares()
        now = datetime.now(timezone.utc)
        window_weight = _reward_window_weight(current_network_difficulty)
        window_shares = _prism_window_shares(
            shares,
            anchor_job_issued_at_ms=int(now.timestamp() * 1000),
            requested_window_weight=window_weight,
        )
        newest = max((row.share.accepted_at_ms for row in window_shares), default=None)
        oldest = min((row.share.accepted_at_ms for row in window_shares), default=None)
        return {
            "hashrate_ths": {
                "h1": public_api.hashrate_ths_from_difficulty(
                    _share_difficulty_between(shares, now - timedelta(hours=1), now),
                    60 * 60,
                ),
                "h3": public_api.hashrate_ths_from_difficulty(
                    _share_difficulty_between(shares, now - timedelta(hours=3), now),
                    3 * 60 * 60,
                ),
                "h24": public_api.hashrate_ths_from_difficulty(
                    _share_difficulty_between(shares, now - timedelta(hours=24), now),
                    24 * 60 * 60,
                ),
            },
            "participants_3h": len(
                {
                    share.miner_id
                    for share in shares
                    if now - timedelta(hours=3) <= _datetime_from_ms(share.accepted_at_ms) <= now
                }
            ),
            "blocks_found_total": 0,
            "prism_blocks_total": 0,
            "total_mined_bits": 0,
            "latest_block": None,
            "reward_window": {
                "window_multiplier": 8,
                "requested_window_weight": public_api.decimal_string(window_weight),
                "oldest_share_accepted_at": _iso_from_ms(oldest),
                "newest_share_accepted_at": _iso_from_ms(newest),
                "included_share_count": len(window_shares),
            },
        }

    def dashboard_miner_reward_window(
        self,
        *,
        recipient_id: str,
        current_network_difficulty: int | str | Decimal,
    ) -> dict[str, object]:
        from lab.prism import public_api

        window_weight = _reward_window_weight(current_network_difficulty)
        window_shares = _prism_window_shares(
            self.all_shares(),
            anchor_job_issued_at_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
            requested_window_weight=window_weight,
        )
        miner_difficulty = sum(
            row.counted_difficulty
            for row in window_shares
            if row.share.miner_id == recipient_id
        )
        pool_difficulty = sum(row.counted_difficulty for row in window_shares)
        share_percent = None
        if pool_difficulty > 0:
            share_percent = public_api.decimal_string(miner_difficulty * Decimal(100) / pool_difficulty)
        return {
            "accepted_difficulty": public_api.decimal_string(miner_difficulty),
            "pool_accepted_difficulty": public_api.decimal_string(pool_difficulty),
            "share_percent": share_percent,
        }

    def dashboard_blocks(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        return {"pagination": public_api.pagination(page, limit, 0), "rows": []}

    def dashboard_miner_lifetime_earnings_bits(self, *, recipient_id: str) -> int:
        return 0

    def dashboard_miner_pending_maturity_bits(self, *, recipient_id: str) -> int:
        return 0

    def dashboard_miner_payout_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        return {"pagination": public_api.pagination(page, limit, 0), "rows": []}

    def dashboard_miner_earning_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        return {"pagination": public_api.pagination(page, limit, 0), "rows": []}

    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        from lab.prism import public_api

        now = datetime.now(timezone.utc)
        started = now - timedelta(hours=3)
        shares = [
            share
            for share in self.all_shares()
            if started <= _datetime_from_ms(share.accepted_at_ms) <= now
            and (not search or search.lower() in share.miner_id.lower())
        ]
        by_miner: dict[str, dict[str, object]] = {}
        for share in shares:
            row = by_miner.setdefault(
                share.miner_id,
                {
                    "recipient_id": share.miner_id,
                    "difficulty": 0,
                    "last_share_at_ms": share.accepted_at_ms,
                },
            )
            row["difficulty"] = int(row["difficulty"]) + int(share.share_difficulty)
            row["last_share_at_ms"] = max(int(row["last_share_at_ms"]), share.accepted_at_ms)
        total_difficulty = sum(int(row["difficulty"]) for row in by_miner.values())
        pool_hashrate_ths = public_api.hashrate_ths_from_difficulty(total_difficulty, 3 * 60 * 60)
        ranked = sorted(
            by_miner.values(),
            key=lambda row: (-int(row["difficulty"]), str(row["recipient_id"])),
        )
        rows: list[dict[str, object]] = []
        for index, row in enumerate(ranked, start=1):
            percent = None
            if total_difficulty:
                percent = public_api.decimal_string(Decimal(int(row["difficulty"])) * Decimal(100) / Decimal(total_difficulty))
            hashrate_ths = public_api.hashrate_ths_from_difficulty(int(row["difficulty"]), 3 * 60 * 60)
            rows.append(
                {
                    "rank": index,
                    "recipient_id": row["recipient_id"],
                    "display_name": None,
                    "hashrate_ths_3h": hashrate_ths,
                    "share_percent": percent,
                    "hash_percent": _hash_percent(hashrate_ths, pool_hashrate_ths),
                    "blocks_found": 0,
                    "last_share_at": _iso_from_ms(int(row["last_share_at_ms"])),
                }
            )
        offset = (page - 1) * limit
        return {
            "started_at": public_api.iso_datetime(started),
            "ended_at": public_api.iso_datetime(now),
            "totals": {
                "pool_hashrate_ths": pool_hashrate_ths,
                "pool_accepted_share_difficulty": str(total_difficulty),
                "participant_count": len(ranked),
            },
            "pagination": public_api.pagination(page, limit, len(ranked)),
            "rows": rows[offset : offset + limit],
        }

    def dashboard_reward_leaderboard(
        self,
        *,
        page: int,
        limit: int,
        current_network_difficulty: int | str | Decimal,
        search: str | None = None,
        recipient_id: str | None = None,
    ) -> dict[str, object]:
        from lab.prism import public_api

        if search is not None and recipient_id is not None:
            raise ValueError("search and recipient_id are mutually exclusive")
        now = datetime.now(timezone.utc)
        anchor_ms = int(now.timestamp() * 1000)
        requested_window_weight = _reward_window_weight(current_network_difficulty)
        window_shares = _prism_window_shares(
            self.all_shares(),
            anchor_job_issued_at_ms=anchor_ms,
            requested_window_weight=requested_window_weight,
        )
        counted_window_weight = sum(
            (row.counted_difficulty for row in window_shares),
            Decimal(0),
        )
        oldest_ms = min((row.share.accepted_at_ms for row in window_shares), default=None)
        observed_span_seconds = None
        if oldest_ms is not None:
            observed_span_seconds = max(0, (anchor_ms - oldest_ms) // 1000)

        by_miner: dict[str, dict[str, object]] = {}
        for window_share in window_shares:
            share = window_share.share
            row = by_miner.setdefault(
                share.miner_id,
                {
                    "recipient_id": share.miner_id,
                    "included_share_count": 0,
                    "counted_share_difficulty": Decimal(0),
                    "last_share_at_ms": share.accepted_at_ms,
                },
            )
            row["included_share_count"] = int(row["included_share_count"]) + 1
            row["counted_share_difficulty"] = Decimal(row["counted_share_difficulty"]) + window_share.counted_difficulty
            row["last_share_at_ms"] = max(int(row["last_share_at_ms"]), share.accepted_at_ms)

        ranked = sorted(
            by_miner.values(),
            key=lambda row: (-Decimal(row["counted_share_difficulty"]), str(row["recipient_id"])),
        )
        rows: list[dict[str, object]] = []
        for index, row in enumerate(ranked, start=1):
            counted_share_difficulty = Decimal(row["counted_share_difficulty"])
            share_percent = None
            if counted_window_weight > 0:
                share_percent = public_api.decimal_string(
                    counted_share_difficulty * Decimal(100) / counted_window_weight
                )
            hashrate_ths = None
            if observed_span_seconds is not None and observed_span_seconds > 0:
                hashrate_ths = public_api.hashrate_ths_from_difficulty(
                    counted_share_difficulty,
                    observed_span_seconds,
                )
            rows.append(
                {
                    "rank": index,
                    "recipient_id": row["recipient_id"],
                    "display_name": None,
                    "hashrate_ths": hashrate_ths,
                    "included_share_count": int(row["included_share_count"]),
                    "counted_share_difficulty": public_api.decimal_string(counted_share_difficulty),
                    "share_percent": share_percent,
                    "blocks_found_total": 0,
                    "last_share_at": _iso_from_ms(int(row["last_share_at_ms"])),
                }
            )

        filtered_rows = rows
        if recipient_id is not None:
            filtered_rows = [
                row
                for row in rows
                if row["recipient_id"] == recipient_id
            ]
        elif search:
            normalized_search = search.lower()
            filtered_rows = [
                row
                for row in rows
                if normalized_search in str(row["recipient_id"]).lower()
            ]
        offset = (page - 1) * limit

        pool_hashrate_ths = None
        expected_time_to_block = None
        if observed_span_seconds is not None and observed_span_seconds > 0 and counted_window_weight > 0:
            pool_hashrate_ths = public_api.hashrate_ths_from_difficulty(
                counted_window_weight,
                observed_span_seconds,
            )
            expected_time_to_block = public_api.expected_time_to_block_seconds(
                hashrate_ths=pool_hashrate_ths,
                network_difficulty=public_api.decimal_string(current_network_difficulty),
            )

        return {
            "window": {
                "id": "reward",
                "started_at": _iso_from_ms(oldest_ms),
                "ended_at": public_api.iso_datetime(now),
                "observed_span_seconds": observed_span_seconds,
                "network_difficulty": public_api.decimal_string(current_network_difficulty),
                "window_multiplier": 8,
                "requested_window_weight": public_api.decimal_string(requested_window_weight),
                "counted_window_weight": public_api.decimal_string(counted_window_weight),
                "included_share_count": len(window_shares),
                "is_complete": requested_window_weight > 0 and counted_window_weight >= requested_window_weight,
            },
            "totals": {
                "pool_hashrate_ths": pool_hashrate_ths,
                "pool_counted_share_difficulty": public_api.decimal_string(counted_window_weight),
                "participant_count": len(ranked),
                "expected_time_to_block_seconds": expected_time_to_block,
            },
            "pagination": public_api.pagination(page, limit, len(filtered_rows)),
            "rows": filtered_rows[offset : offset + limit],
        }

    def dashboard_hashrate_series(
        self,
        *,
        subject_type: str,
        subject_id: str | None,
        range_id: str,
        bucket: str,
        lookback_seconds: int = 0,
        range_anchor_epoch: int | None = None,
    ) -> list[dict[str, object]]:
        from lab.prism import public_api

        now = datetime.now(timezone.utc)
        # Anchor the range lower bound on the caller's clock when provided so
        # it agrees with the caller's min_epoch trim regardless of clock skew.
        anchor = (
            datetime.fromtimestamp(range_anchor_epoch, timezone.utc)
            if range_anchor_epoch is not None
            else now
        )
        started = _series_start(anchor, range_id)
        if lookback_seconds > 0 and range_id != "all":
            # Pre-range context requested by the smoother; the caller trims
            # these buckets from the response after windowing.
            started -= timedelta(seconds=int(lookback_seconds))
        bucket_seconds = {"5m": 300, "1h": 3600, "1d": 86400}[bucket]
        buckets: dict[int, dict[str, int]] = {}
        for share in self.all_shares():
            accepted_at = _datetime_from_ms(share.accepted_at_ms)
            if accepted_at < started or accepted_at > now:
                continue
            if subject_type == "miner" and share.miner_id != subject_id:
                continue
            bucket_epoch = int(accepted_at.timestamp()) // bucket_seconds * bucket_seconds
            entry = buckets.setdefault(bucket_epoch, {"count": 0, "difficulty": 0})
            entry["count"] += 1
            entry["difficulty"] += int(share.share_difficulty)
        return [
            {
                "timestamp": public_api.iso_datetime(datetime.fromtimestamp(bucket_epoch, timezone.utc)),
                "hashrate_ths": public_api.hashrate_ths_from_difficulty(entry["difficulty"], bucket_seconds),
                "accepted_share_count": entry["count"],
                "accepted_share_difficulty": str(entry["difficulty"]),
            }
            for bucket_epoch, entry in sorted(buckets.items())
        ]

    def persist_accepted_block(
        self,
        *,
        block_hash: str,
        block_height: int,
        parent_hash: str,
        final_bundle: dict[str, Any],
        audit_report: dict[str, Any],
        canonical_bundle_path: Path | None = None,
    ) -> dict[str, int | str]:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            previous = self._memory_pool_blocks.get(block_hash)
            if previous is not None and previous[0] != int(block_height):
                raise RuntimeError("memory pool block height conflicts")
            if previous is None:
                self._memory_pool_blocks[block_hash] = (
                    int(block_height),
                    "prepared",
                    str(parent_hash),
                )
            self._audit_publication_sequences.setdefault(block_hash, None)
            share_count = len(self._shares)
        return {
            "backend": "memory",
            "share_count": share_count,
            "block_count": 0,
            "payout_entry_count": 0,
            "carry_forward_count": 0,
        }

    def reverse_immature_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            block = self._memory_pool_blocks.get(block_hash)
            if (
                block is not None
                and block[1] in {"confirmed", "inactive"}
                and int(active_tip_height) >= block[0] + 1000
            ):
                raise RuntimeError(
                    f"refusing to reverse mature pool block {block_hash}"
                )
            if block is None or block[1] not in {
                "prepared",
                "confirmed",
                "inactive",
            }:
                count = 0
            else:
                self._memory_pool_blocks[block_hash] = (
                    block[0],
                    "reversed",
                    block[2],
                )
                self._inactive_audit_publications.discard(block_hash)
                count = 1
        return {
            "backend": "memory",
            "reversed_count": count,
        }

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            block = self._memory_pool_blocks.get(block_hash)
            if block is None or block[1] != "prepared":
                count = 0
            else:
                self._memory_pool_blocks[block_hash] = (
                    block[0],
                    "rejected",
                    block[2],
                )
                count = 1
        return {
            "backend": "memory",
            "rejected_count": count,
        }

    def confirm_accepted_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            block = self._memory_pool_blocks.get(block_hash)
            if (
                block is None
                or block[0] != int(active_tip_height)
                or block[1] not in {"prepared", "confirmed"}
            ):
                # Mirror the Postgres disposition split: a row already
                # terminally disposed (reorg quarantine, rejection, or
                # reversal) reports superseded (-1); 0 keeps meaning no row
                # or a live row this confirmation does not match.
                if block is not None and block[1] in {
                    "inactive",
                    "rejected",
                    "reversed",
                }:
                    return {"backend": "memory", "confirmed_count": -1}
                return {"backend": "memory", "confirmed_count": 0}
            # Mirror the Postgres disposition split again: only a flip out of
            # 'prepared' is a fresh confirmation (1). A row already confirmed
            # at this height is an idempotent replay (2) that keeps the
            # ordinal its flip allocated and burns none.
            already_confirmed = block[1] == "confirmed"
            publication_sequence = self._audit_publication_sequences.get(block_hash)
            if publication_sequence is None:
                publication_sequence = self._next_audit_publication_sequence
                self._next_audit_publication_sequence += 1
                self._audit_publication_sequences[block_hash] = publication_sequence
            self._memory_pool_blocks[block_hash] = (
                block[0],
                "confirmed",
                block[2],
            )
        return {
            "backend": "memory",
            "confirmed_count": 2 if already_confirmed else 1,
            "audit_publication_sequence": publication_sequence,
        }

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        return []

    def stranded_prepared_blocks(
        self,
        *,
        active_tip_height: int,
        min_depth: int,
        limit: int = 64,
    ) -> list[dict[str, object]]:
        """Return deeply buried rows still parked in the prepared state.

        ``reorg_watch_blocks`` deliberately watches only confirmed/inactive
        rows, and a prepared row is normally resolved by the live
        submit/replay path that owns it. A row whose outbox entry is gone
        (quarantined, or completed by a process that died before confirming)
        therefore has nothing left to re-examine it, and stays prepared
        forever. This read finds those rows; the caller decides, against the
        active chain, which ones are provably orphaned.
        """
        with self._lock:
            rows = [
                {
                    "block_hash": block_hash,
                    "block_height": int(block[0]),
                    "parent_hash": str(block[2]),
                }
                for block_hash, block in self._memory_pool_blocks.items()
                # The memory backend derives maturity_state from chain_state
                # (see pool_block_state): 'prepared' is always 'immature'.
                if block[1] == "prepared"
                and int(block[0]) <= int(active_tip_height) - int(min_depth)
            ]
        rows.sort(key=lambda row: (int(row["block_height"]), str(row["block_hash"])))
        return rows[: max(0, int(limit))]

    def mark_pool_block_inactive(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            block = self._memory_pool_blocks.get(block_hash)
            if block is None or block[1] != "confirmed":
                count = 0
            else:
                self._inactive_audit_publications.add(block_hash)
                self._memory_pool_blocks[block_hash] = (
                    block[0],
                    "inactive",
                    block[2],
                )
                count = 1
        return {
            "backend": "memory",
            "inactive_count": count,
        }

    def reactivate_pool_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            block = self._memory_pool_blocks.get(block_hash)
            if (
                block is None
                or block[0] > int(active_tip_height)
                or block[1] != "inactive"
                or block_hash not in self._inactive_audit_publications
            ):
                count = 0
                sequence = None
            else:
                sequence = self._audit_publication_sequences.get(block_hash)
                if sequence is None:
                    raise RuntimeError(
                        "inactive pool block has no audit publication sequence"
                    )
                self._inactive_audit_publications.remove(block_hash)
                self._memory_pool_blocks[block_hash] = (
                    block[0],
                    "confirmed",
                    block[2],
                )
                count = 1
        return {
            "backend": "memory",
            "reactivated_count": count,
            **(
                {"audit_publication_sequence": int(sequence)}
                if sequence is not None
                else {}
            ),
        }

    def pool_block_state(self, *, block_hash: str) -> dict[str, object] | None:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        with self._lock:
            block = self._memory_pool_blocks.get(block_hash)
            if block is None:
                return None
            publication_sequence = self._audit_publication_sequences.get(block_hash)
            return {
                "block_hash": block_hash,
                "block_height": block[0],
                "parent_hash": block[2],
                "chain_state": block[1],
                "maturity_state": (
                    "reversed"
                    if block[1] in {"rejected", "reversed"}
                    else "immature"
                ),
                "audit_publication_sequence": publication_sequence,
            }

    def audit_publication_sequence_floor(self) -> int:
        """Return the newest ordinal attached to any durable pool-block row."""

        with self._lock:
            return max(
                (
                    sequence
                    for sequence in self._audit_publication_sequences.values()
                    if sequence is not None
                ),
                default=0,
            )

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int | str]:
        return {
            "backend": "memory",
            "matured_count": 0,
        }

    @property
    def backend_name(self) -> str:
        return "memory"

    def __len__(self) -> int:
        with self._lock:
            return len(self._shares)


def _datetime_from_ms(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc)


def _iso_from_ms(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    from lab.prism import public_api

    return public_api.iso_datetime(_datetime_from_ms(timestamp_ms))


def _share_difficulty_between(shares: list[AcceptedShareRecord], started_at: datetime, ended_at: datetime) -> int:
    return sum(
        int(share.share_difficulty)
        for share in shares
        if started_at <= _datetime_from_ms(share.accepted_at_ms) <= ended_at
    )


def _reward_window_weight(current_network_difficulty: int | str | Decimal) -> Decimal:
    difficulty = Decimal(str(current_network_difficulty))
    if difficulty < 0:
        difficulty = Decimal(0)
    return difficulty * Decimal(8)


def _hash_percent(hashrate_ths: str, pool_hashrate_ths: str) -> str | None:
    from lab.prism import public_api

    pool_hashrate = Decimal(str(pool_hashrate_ths))
    if pool_hashrate <= 0:
        return None
    return public_api.decimal_string(Decimal(str(hashrate_ths)) * Decimal(100) / pool_hashrate)


def _solver_worker_name_sql(share_id_column: str) -> str:
    """SQL expression deriving a worker name from a solving share's share_id.

    A solving share's share_id is "<stratum_username>:<block_hash>" (see
    pending_share_from_submission), and the stratum username is
    "<payout_address>[.<worker_name>]". Strip the trailing ":<block_hash>" segment
    to recover the username, then take the substring after the first '.'. Returns
    NULL when the share is absent (unattributed block) or carried no worker suffix,
    matching the nullable-string schema and mirroring the worker-name derivation in
    dashboard_miner_worker_rows.
    """
    username = f"regexp_replace({share_id_column}, ':[^:]*$', '')"
    dot = f"position('.' IN {username})"
    return (
        f"CASE WHEN {share_id_column} IS NULL THEN null "
        f"WHEN {dot} > 0 THEN NULLIF(substring({username} FROM {dot} + 1), '') "
        f"ELSE null END"
    )


def _prism_window_shares(
    shares: list[AcceptedShareRecord],
    *,
    anchor_job_issued_at_ms: int,
    requested_window_weight: int | Decimal,
) -> list[PrismWindowShare]:
    if requested_window_weight <= 0:
        return []
    requested = Decimal(str(requested_window_weight))
    total = Decimal(0)
    window_shares: list[PrismWindowShare] = []
    eligible = [
        share
        for share in shares
        if share.job_issued_at_ms <= anchor_job_issued_at_ms and share.accepted_at_ms <= anchor_job_issued_at_ms
    ]
    for share in sorted(eligible, key=lambda item: item.share_seq, reverse=True):
        if total >= requested:
            break
        share_difficulty = Decimal(int(share.share_difficulty))
        counted_difficulty = min(share_difficulty, requested - total)
        total += counted_difficulty
        window_shares.append(PrismWindowShare(share=share, counted_difficulty=counted_difficulty))
    return window_shares


def _series_start(now: datetime, range_id: str) -> datetime:
    if range_id == "1w":
        return now - timedelta(days=7)
    if range_id == "1m":
        return now - timedelta(days=30)
    if range_id == "6m":
        return now - timedelta(days=180)
    return datetime.fromtimestamp(0, timezone.utc)


def database_url_from_psql_command(command: list[str]) -> str | None:
    """Best-effort DSN extraction from a psql invocation.

    Handles the common shapes the coordinator and deploy tooling produce:
    ``psql postgres://...``, ``psql -d postgres://...`` and
    ``psql --dbname=postgres://...``. Anything else (host/user flags, service
    files) stays on the subprocess backend rather than risking a mistranslated
    connection.
    """
    expect_dbname = False
    for arg in command[1:]:
        if expect_dbname:
            candidate = arg
            expect_dbname = False
        elif arg in {"-d", "--dbname"}:
            expect_dbname = True
            continue
        elif arg.startswith("--dbname="):
            candidate = arg.split("=", 1)[1]
        else:
            candidate = arg
        if candidate.startswith("postgres://") or candidate.startswith("postgresql://"):
            return candidate
    return None


def _writer_lease_advisory_lock_key(writer_id: str, writer_epoch: int) -> int:
    """Return a stable signed bigint key namespaced to the PRISM writer lease."""
    digest = hashlib.sha256(
        b"qbit-prism-writer-lease\0"
        + writer_id.encode("utf-8")
        + b"\0"
        + str(writer_epoch).encode("ascii")
    ).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


@runtime_checkable
class LedgerSqlPort(Protocol):
    """The pooled statement-execution seam `PsqlShareLedger` writes through.

    `_NativePostgresClient` is the production implementation. Naming the
    contract lets a test substitute a whole PostgreSQL model at construction
    time (see `sql_backend_factory`) instead of reassigning bound methods on
    a live ledger, which is what the lease and landing concurrency tests
    need: statement timing, tuple-lock waits and transaction lifetime are
    properties of this seam, not of any single method.
    """

    def run_json(
        self,
        sql: str,
        *,
        retry_safe: bool = False,
        timeout_seconds: float | None = None,
        on_statement_start: Callable[[], None] | None = None,
    ) -> Any: ...

    def run_script(self, sql: str) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class LeaseGuardPort(Protocol):
    """The dedicated writer-lease guard session seam.

    `_NativePostgresLeaseGuard` is the production implementation. Unlike
    `LedgerSqlPort` this session is never transparently replaced: losing it
    loses the advisory lock and must fence the owning coordinator, so the
    heartbeat's liveness proof depends on the session's identity surviving.
    A substitute must honour the same contract, including the serialized
    query slot that `on_query_start` marks, the per-statement round trip
    `on_statement_end` marks, and the in-slot `followup` statement the
    attribution recheck relies on.
    """

    def try_acquire(self) -> bool: ...

    @property
    def held(self) -> bool: ...

    def run_json(
        self,
        sql: str,
        *,
        on_query_start: Callable[[], None] | None = None,
        on_statement_end: Callable[[], None] | None = None,
        followup: Callable[[Any], str | None] | None = None,
    ) -> Any: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class PostgresSessionGuards:
    """Session-level GUCs this ledger sets on every connection it opens.

    Two guarantees ride the one libpq ``options`` carrier, because a setting
    delivered at connect time covers every statement the session can run —
    including the autocommit ones — rather than being re-asserted per
    transaction by code that a future caller might bypass.

    The first is orphan reaping. A coordinator client that disappears without
    an RST — network partition, SIGSTOP, VM pause — can leave its backend idle
    in transaction, still holding every row lock the transaction took. The
    writer-lease landing CTE row-locks the qbit_ledger_writer_lease singleton,
    so one such orphan blocks a successor's startup lease upsert until the
    kernel's TCP keepalive teardown (hours at OS defaults). These GUCs make the
    *server* resolve that on its own: idle_in_transaction_session_timeout
    aborts the orphaned transaction and releases its locks, and the
    server-side tcp_keepalives_* settings (all PGC_USERSET, settable per
    session) bound how long the server keeps a dead socket alive for backends
    the idle-in-transaction timer cannot cover.

    The second is read-only enforcement. ``read_only`` adds
    default_transaction_read_only=on, so PostgreSQL refuses the write itself
    for a ledger built with ``read_only=True``. The refusing writer gate is
    the in-process half of that promise and remains defense in depth, but it
    only covers paths that take the gate; the GUC covers every statement the
    session can issue, wherever the ledger points. Without it the guarantee
    came from the deployment topology — a hot standby refusing writes — which
    is absent whenever the read tier is aimed at a writable primary.

    Constructed once in PsqlShareLedger.__init__ and shared by all three
    connection paths — the pooled native client, the dedicated lease-guard
    session, and the psql subprocess backend. The orphan coverage is real but
    bounded: the tcp_keepalives_* settings are silently ignored on
    Unix-socket connections and on platforms that do not implement them, so
    idle_in_transaction_session_timeout is the guard that always applies;
    and these are session settings, so they reach only the sessions *this*
    coordinator opens. A foreign session already holding the lease row —
    another deployment's coordinator, an operator's psql — is bounded only
    by the caller-side acquisition deadline in _run_lease_acquisition_json.
    """

    idle_in_transaction_timeout_seconds: float
    tcp_keepalives_idle_seconds: int
    tcp_keepalives_interval_seconds: int
    tcp_keepalives_count: int
    read_only: bool = False

    def options_fragment(self) -> str:
        """Render the guards as libpq ``options`` / PGOPTIONS ``-c`` flags.

        The read-only setting goes last so that it wins over any earlier
        duplicate, the same way the whole fragment is placed after an
        operator's DSN-level options by _merged_session_options.
        """
        idle_ms = max(1, int(self.idle_in_transaction_timeout_seconds * 1000))
        fragment = (
            f"-c idle_in_transaction_session_timeout={idle_ms}ms "
            f"-c tcp_keepalives_idle={self.tcp_keepalives_idle_seconds} "
            f"-c tcp_keepalives_interval={self.tcp_keepalives_interval_seconds} "
            f"-c tcp_keepalives_count={self.tcp_keepalives_count}"
        )
        if self.read_only:
            fragment += " -c default_transaction_read_only=on"
        return fragment


def _merged_session_options(
    psycopg_module: Any,
    conninfo: str,
    *coordinator_fragments: str,
) -> str:
    """Merge the coordinator's session options with the conninfo's own.

    Passing ``options`` as a connect kwarg replaces any ``options`` value the
    operator embedded in the DSN outright, so that value must be read back
    out of the conninfo and kept. The coordinator's fragments go last: libpq
    applies ``-c`` settings left to right with the last duplicate winning, so
    a DSN-level default can never silently disable the session guards.
    """
    existing = psycopg_module.conninfo.conninfo_to_dict(conninfo).get("options")
    fragments = [str(existing).strip()] if existing else []
    fragments.extend(coordinator_fragments)
    return " ".join(fragment for fragment in fragments if fragment)


class _NativePostgresClient:
    """Persistent pooled psycopg client for the share ledger.

    Executes the exact same self-contained SQL text the psql subprocess
    backend runs (values are inlined by the callers), but over long-lived
    connections instead of one fork+connect per statement. Every statement
    returns a single JSON value, mirroring the psql `--tuples-only` contract.
    Connections are created lazily, run in autocommit (each statement is its
    own synchronous commit, exactly like a psql invocation — including the
    group-commit ``append_batch`` statement, whose durability comes from its
    own ``set_config('synchronous_commit', 'on', true)``), and a connection
    that raises is discarded so the next acquisition reconnects.

    ``application_name`` (overriding any value in the conninfo) marks every
    pooled backend in ``pg_stat_activity`` as belonging to this process, so
    the lease guard can attribute an in-flight lease-tuple lock to the
    writer's own fenced transaction rather than a competing expiry claim
    (see ``verify_writer_lease_guard_session``).
    """

    def __init__(
        self,
        conninfo: str,
        *,
        pool_size: int,
        application_name: str | None = None,
        session_guards: PostgresSessionGuards | None = None,
    ):
        import psycopg  # deferred: the subprocess backend must work without it

        self._psycopg = psycopg
        self._conninfo = conninfo
        self._application_name = application_name
        self._session_guards = session_guards
        self._pool_size = max(1, int(pool_size))
        self._slots = BoundedSemaphore(self._pool_size)
        self._idle: list[Any] = []
        self._idle_lock = Lock()
        self._closed = False

    @property
    def pool_size(self) -> int:
        return self._pool_size

    def _connect(self, timeout_seconds: float | None = None) -> Any:
        kwargs: dict[str, Any] = {"autocommit": True}
        if timeout_seconds is not None:
            # libpq accepts integral connect_timeout seconds. Rounding up keeps
            # sub-second statement budgets valid without silently disabling
            # the connection deadline.
            kwargs["connect_timeout"] = max(1, math.ceil(timeout_seconds))
        if self._application_name is not None:
            kwargs["application_name"] = self._application_name
        session_guards = getattr(self, "_session_guards", None)
        if session_guards is not None:
            # Every pooled session carries the orphan-reaping guards, merged
            # so an operator's DSN-level options value survives with the
            # guard fragment last (last -c duplicate wins).
            kwargs["options"] = _merged_session_options(
                self._psycopg,
                self._conninfo,
                session_guards.options_fragment(),
            )
        return self._psycopg.connect(self._conninfo, **kwargs)

    @contextmanager
    def connection(self, *, timeout_seconds: float | None = None) -> Iterator[Any]:
        """Borrow a pooled connection; discard it if the caller raises."""
        started = time.monotonic()
        if timeout_seconds is None:
            acquired = self._slots.acquire()
        else:
            acquired = self._slots.acquire(timeout=max(0.0, timeout_seconds))
        if not acquired:
            raise LedgerOperationTimeout("timed out waiting for a postgres pool slot")
        conn = None
        try:
            with self._idle_lock:
                if self._closed:
                    raise RuntimeError("postgres client is closed")
                if self._idle:
                    conn = self._idle.pop()
            if conn is None or conn.closed:
                connect_timeout = timeout_seconds
                if connect_timeout is not None:
                    connect_timeout = max(
                        0.001,
                        connect_timeout - (time.monotonic() - started),
                    )
                conn = self._connect(connect_timeout)
            yield conn
        except BaseException:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            raise
        else:
            with self._idle_lock:
                if self._closed:
                    keep = False
                else:
                    self._idle.append(conn)
                    keep = True
            if not keep:
                try:
                    conn.close()
                except Exception:
                    pass
        finally:
            self._slots.release()

    def run_json(
        self,
        sql: str,
        *,
        retry_safe: bool = False,
        timeout_seconds: float | None = None,
        on_statement_start: Callable[[], None] | None = None,
    ) -> Any:
        """Run one JSON-returning statement.

        An ``OperationalError`` does not reveal whether PostgreSQL committed
        before the response was lost. Retry once only when the caller has
        explicitly classified the statement as safe to execute again; every
        mutation fails after the first ambiguous execution.
        """
        attempts = 2 if retry_safe else 1
        deadline = (
            None
            if timeout_seconds is None
            else time.monotonic() + max(0.0, timeout_seconds)
        )
        for attempt in range(attempts):
            try:
                remaining = (
                    None
                    if deadline is None
                    else max(0.0, deadline - time.monotonic())
                )
                if remaining is not None and remaining <= 0:
                    raise LedgerOperationTimeout("postgres statement deadline expired")
                connection = (
                    self.connection()
                    if remaining is None
                    else self.connection(timeout_seconds=remaining)
                )
                with connection as conn:
                    if deadline is None:
                        if on_statement_start is not None:
                            on_statement_start()
                        row = conn.execute(sql).fetchone()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise LedgerOperationTimeout(
                                "postgres statement deadline expired"
                            )
                        if on_statement_start is not None:
                            on_statement_start()
                        timeout_ms = max(1, int(remaining * 1000))
                        # SET LOCAL confines both guards to this explicit
                        # transaction, so pooled connections cannot leak a
                        # submitter-specific deadline into unrelated work.
                        with conn.transaction():
                            conn.execute(
                                f"SET LOCAL statement_timeout = '{timeout_ms}ms'"
                            )
                            conn.execute(
                                f"SET LOCAL lock_timeout = '{timeout_ms}ms'"
                            )
                            row = conn.execute(sql).fetchone()
                return parse_single_json_value(row[0] if row else None)
            except self._psycopg.OperationalError as exc:
                if timeout_seconds is not None and _is_postgres_deadline_error(exc):
                    raise LedgerOperationTimeout(
                        f"postgres operation exceeded {timeout_seconds:g}s"
                    ) from exc
                if attempt + 1 >= attempts:
                    raise RuntimeError(f"postgres query failed: {exc}") from exc
        raise AssertionError("unreachable")

    def run_script(self, sql: str) -> None:
        """Run a multi-statement script (schema initialization)."""
        with self.connection() as conn:
            conn.execute(sql)

    def close(self) -> None:
        with self._idle_lock:
            self._closed = True
            idle, self._idle = self._idle, []
        for conn in idle:
            try:
                conn.close()
            except Exception:
                pass


class _NativePostgresLeaseGuard:
    """Hold one writer identity's PostgreSQL session advisory lock.

    Unlike the ordinary native pool, this connection is never transparently
    replaced: losing it also loses the advisory lock and must fence the owning
    coordinator. The lease heartbeat runs on this same isolated session.
    """

    def __init__(
        self,
        conninfo: str,
        advisory_lock_key: int,
        *,
        session_guards: PostgresSessionGuards | None = None,
    ):
        import psycopg  # deferred: psql-only users retain TTL fencing

        # The short statement_timeout stays: the guard session must never
        # queue behind a fenced write (see verify_writer_lease_guard_session).
        # It is also the dominant term in the heartbeat's staleness envelope
        # (WriterLeaseHeartbeatPolicy.max_healthy_server_gap_seconds), so the
        # value is taken from the timing policy rather than written twice.
        # The session guards ride the same options string so this dedicated
        # connection carries the same orphan bounds as every pooled one.
        # Merged with any operator DSN-level options, coordinator fragments
        # last so they win.
        statement_timeout_ms = int(
            round(WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS * 1000)
        )
        options = f"-c statement_timeout={statement_timeout_ms}"
        if session_guards is not None:
            options = _merged_session_options(
                psycopg,
                conninfo,
                options,
                session_guards.options_fragment(),
            )
        self._connection = psycopg.connect(
            conninfo,
            autocommit=True,
            connect_timeout=2,
            options=options,
        )
        self._advisory_lock_key = advisory_lock_key
        self._query_lock = Lock()
        self._closed = False
        self._held = False

    def try_acquire(self) -> bool:
        row = self._connection.execute(
            f"SELECT pg_try_advisory_lock({self._advisory_lock_key})"
        ).fetchone()
        self._held = bool(row and row[0])
        return self._held

    @property
    def held(self) -> bool:
        return self._held and not self._closed and not self._connection.closed

    def run_json(
        self,
        sql: str,
        *,
        on_query_start: Callable[[], None] | None = None,
        on_statement_end: Callable[[], None] | None = None,
        followup: Callable[[Any], str | None] | None = None,
    ) -> Any:
        with self._query_lock:
            # Queries on this session serialize behind the periodic heartbeat
            # and any concurrent caller. The callback marks the moment the
            # serialized slot is acquired, so callers can budget queue wait
            # and statement execution separately.
            if on_query_start is not None:
                on_query_start()
            if not self.held:
                raise RuntimeError("postgres writer lease guard is not held")
            row = self._connection.execute(sql).fetchone()
            result = parse_single_json_value(row[0] if row else None)
            # Every completed round trip is server-proven liveness, and the
            # boundary the caller's phase attribution charges guard SQL
            # against. It fires after the result is parsed so a malformed
            # response is not counted as a healthy round trip.
            if on_statement_end is not None:
                on_statement_end()
            # A followup runs inside this same serialized slot: no second
            # queue wait behind other guard callers can be charged to the
            # caller's execution budget. Each execute on this autocommit
            # session is still its own statement with a fresh snapshot,
            # which is exactly what verification rechecks need. The
            # callback owns termination by returning None.
            while followup is not None:
                next_sql = followup(result)
                if next_sql is None:
                    break
                row = self._connection.execute(next_sql).fetchone()
                result = parse_single_json_value(row[0] if row else None)
                if on_statement_end is not None:
                    on_statement_end()
            return result

    def close(self) -> None:
        """Close the session, releasing its advisory lock server-side."""
        if self._closed:
            return
        self._closed = True
        self._held = False
        try:
            self._connection.close()
        except Exception:
            pass


def parse_single_json_value(value: object) -> Any:
    """Normalize a one-row/one-column JSON query result.

    psycopg already decodes json/jsonb columns to Python objects; the psql
    subprocess path yields text. A NULL result matches the subprocess
    behavior of raising on empty output.
    """
    if value is None:
        raise RuntimeError("postgres query returned no JSON")
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


class PsqlShareLedger:
    """Postgres-backed implementation of the coordinator share-ledger API.

    The process that owns this object is the single logical writer. It delegates
    sequence assignment to `qbit_share_ledger.share_seq` and uses the canonical
    SQL schema under `crates/qbit-prism/sql`.
    """

    durable_payout_state = True

    # The lease lifecycle's clock, as a class attribute so an instance built
    # without __init__ (several tests exercise one statement that way) still
    # reads a working clock. __init__ overrides it per instance with whatever
    # was injected. Declaring it here rather than resolving it through getattr
    # at each call site means a rename fails loudly at the assignment instead
    # of silently reverting every scenario to wall-clock time.
    _monotonic: Callable[[], float] = staticmethod(time.monotonic)

    # Guards the one-time creation of the per-instance read-timing state for
    # ledgers built through __new__ (several focused tests exercise a single
    # statement that way). A class attribute, so the check-then-set inside
    # _ensure_ledger_read_timings cannot let two threads each publish their
    # own dict and lose one of them; __init__ builds the state directly and
    # never reaches it.
    _ledger_read_timings_bootstrap: ClassVar[Lock] = Lock()

    @staticmethod
    def _resolve_lease_authority_margin_seconds(
        lease_ttl_seconds: float,
        lease_authority_margin_seconds: float | None,
    ) -> float:
        """Resolve the own-write deferral margin for external side effects.

        Never below half the lease TTL: that floor keeps the deferral
        engaged through the eroded tail of a long own fenced write even
        when the configured guarded-RPC deadlines are short. A caller
        supplies a larger margin when its longest fence-guarded RPC could
        outlast the floor — the margin must cover the longest effect the
        fence can authorize, or the effect outlives its runway and
        degenerates into rollback-dependent authority.

        A margin that reaches the TTL is rejected outright rather than
        accepted as defer-every-skip: the deferral only gates renewal
        *skips*, while a verification over an uncontended row renews and
        authorizes unconditionally — and a landed renewal's runway is
        exactly one TTL. When the guarded effect's deadline can reach the
        TTL, even a freshly renewed lease cannot outlast the effect, so
        no deferral policy closes the authorize-then-expire window and
        the configuration itself is unsafe. Failing construction turns a
        silent split-brain hazard into a startup error: raise the lease
        TTL or lower the guarded RPC deadlines.
        """
        floor = lease_ttl_seconds / 2.0
        if lease_authority_margin_seconds is None:
            return floor
        margin = float(lease_authority_margin_seconds)
        if not math.isfinite(margin) or margin < 0:
            raise ValueError(
                "lease_authority_margin_seconds must be finite and non-negative"
            )
        if margin >= lease_ttl_seconds:
            raise ValueError(
                "lease_authority_margin_seconds "
                f"({margin}) must stay below lease_ttl_seconds "
                f"({lease_ttl_seconds}): a fence-guarded RPC whose deadline "
                "can reach the lease TTL can outlive even a freshly renewed "
                "lease, and renewals bypass the own-write deferral entirely; "
                "raise the lease TTL or lower the guarded RPC deadlines"
            )
        return max(floor, margin)

    def __init__(
        self,
        *,
        psql_command: str,
        database_url: str | None = None,
        native_client_mode: str = "auto",
        writer_id: str = "prism-coordinator",
        writer_epoch: int = 1,
        writer_session_token: str | None = None,
        initialize_schema: bool = False,
        schema_path: Path | None = None,
        lease_retry_sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        pool_application_name: str | None = None,
        sql_backend_factory: Callable[..., LedgerSqlPort | None] | None = None,
        lease_guard_factory: Callable[..., LeaseGuardPort | None] | None = None,
        lease_retry_max_sleep_seconds: float = 15.0,
        lease_ttl_seconds: float = 60.0,
        lease_authority_margin_seconds: float | None = None,
        lease_adoption_silence_seconds: float = DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
        lease_acquire_lock_timeout_seconds: float = DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
        lease_acquire_attempts: int = DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
        postgres_idle_in_transaction_timeout_seconds: float = DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
        postgres_tcp_keepalives_idle_seconds: int = DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
        postgres_tcp_keepalives_interval_seconds: int = DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
        postgres_tcp_keepalives_count: int = DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
        read_only: bool = False,
        read_concurrency: int = 4,
        accepted_stats_cache_seconds: float = 60.0,
        reward_window_cache_seconds: float = 30.0,
        audit_body_dir: str | Path | None = None,
        audit_bundle_canonicalizer: Callable[[dict[str, Any]], bytes] | None = None,
        audit_share_segment_size: int = 0,
        audit_artifact_store: AuditArtifactStore | None = None,
        ctv_broadcast_attempt_detail_limit: int = DEFAULT_CTV_BROADCAST_ATTEMPT_DETAIL_LIMIT,
        ctv_broadcast_retry_backoff_seconds: int = DEFAULT_CTV_BROADCAST_RETRY_BACKOFF_SECONDS,
    ):
        if writer_epoch < 0:
            raise ValueError("writer_epoch must be >= 0")
        read_only = bool(read_only)
        if read_only and initialize_schema:
            raise ValueError("a read-only ledger cannot initialize the schema")
        accepted_stats_cache_seconds = float(accepted_stats_cache_seconds)
        if not math.isfinite(accepted_stats_cache_seconds) or accepted_stats_cache_seconds < 0:
            raise ValueError("accepted_stats_cache_seconds must be finite and non-negative")
        reward_window_cache_seconds = float(reward_window_cache_seconds)
        if not math.isfinite(reward_window_cache_seconds) or reward_window_cache_seconds < 0:
            raise ValueError("reward_window_cache_seconds must be finite and non-negative")
        lease_retry_max_sleep_seconds = float(lease_retry_max_sleep_seconds)
        if lease_retry_max_sleep_seconds <= 0:
            raise ValueError("lease_retry_max_sleep_seconds must be positive")
        lease_ttl_seconds = float(lease_ttl_seconds)
        if not math.isfinite(lease_ttl_seconds) or lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be finite and positive")
        lease_authority_margin_seconds = (
            self._resolve_lease_authority_margin_seconds(
                lease_ttl_seconds,
                lease_authority_margin_seconds,
            )
        )
        lease_adoption_silence_seconds = float(lease_adoption_silence_seconds)
        if (
            not math.isfinite(lease_adoption_silence_seconds)
            or lease_adoption_silence_seconds <= 0
        ):
            raise ValueError("lease_adoption_silence_seconds must be finite and positive")
        # Validated here, not only in load_config: run_ctv_broadcaster_daemon
        # and backfill_ctv_fanouts construct this class directly and must not
        # be able to disarm the lease-acquisition bound or the session guards
        # with a zero or negative value.
        lease_acquire_lock_timeout_seconds = float(lease_acquire_lock_timeout_seconds)
        if (
            not math.isfinite(lease_acquire_lock_timeout_seconds)
            or lease_acquire_lock_timeout_seconds <= 0
        ):
            raise ValueError("lease_acquire_lock_timeout_seconds must be finite and positive")
        lease_acquire_attempts = int(lease_acquire_attempts)
        if lease_acquire_attempts <= 0:
            raise ValueError("lease_acquire_attempts must be positive")
        postgres_idle_in_transaction_timeout_seconds = float(
            postgres_idle_in_transaction_timeout_seconds
        )
        if (
            not math.isfinite(postgres_idle_in_transaction_timeout_seconds)
            or postgres_idle_in_transaction_timeout_seconds <= 0
        ):
            raise ValueError(
                "postgres_idle_in_transaction_timeout_seconds must be finite and positive"
            )
        postgres_tcp_keepalives_idle_seconds = int(postgres_tcp_keepalives_idle_seconds)
        if postgres_tcp_keepalives_idle_seconds <= 0:
            raise ValueError("postgres_tcp_keepalives_idle_seconds must be positive")
        postgres_tcp_keepalives_interval_seconds = int(
            postgres_tcp_keepalives_interval_seconds
        )
        if postgres_tcp_keepalives_interval_seconds <= 0:
            raise ValueError("postgres_tcp_keepalives_interval_seconds must be positive")
        postgres_tcp_keepalives_count = int(postgres_tcp_keepalives_count)
        if postgres_tcp_keepalives_count <= 0:
            raise ValueError("postgres_tcp_keepalives_count must be positive")
        read_concurrency = int(read_concurrency)
        if read_concurrency <= 0:
            raise ValueError("read_concurrency must be positive")
        self._command = shlex.split(psql_command)
        if not self._command:
            raise ValueError("psql_command must not be empty")
        self._writer_id = writer_id
        self._writer_epoch = writer_epoch
        self._writer_session_token = writer_session_token or uuid.uuid4().hex
        # Advertised as application_name by every pooled connection so the
        # lease guard can recognize the lease tuple's locker as one of this
        # process's own backends (a fenced write outlasting the TTL) rather
        # than a competing expiry claim. Unique per ledger instance: a
        # predecessor or replacement process never shares it, so their
        # in-flight writes still fence this session out.
        self._pool_application_name = (
            pool_application_name or f"qbit-prism-writer-{uuid.uuid4().hex}"
        )
        self._lease_ttl_seconds = lease_ttl_seconds
        # SQL fragment for the writer-lease expiry. The lease is refreshed on
        # every append (the dominant liveness signal during active mining), so a
        # short TTL bounds how long a same-identity replacement writer waits
        # after an *ungraceful* crash. Graceful shutdown releases the lease
        # outright (see release_writer_lease), making restarts near-instant.
        self._lease_interval_sql = f"make_interval(secs => {lease_ttl_seconds})"
        self._lease_authority_margin_seconds = lease_authority_margin_seconds
        # SQL fragment for the own-write deferral margin (see
        # verify_writer_lease_guard_session): an own-write renewal skip over
        # a committed row with less than this much TTL remaining defers
        # external side effects instead of authorizing them.
        self._lease_authority_margin_sql = (
            f"make_interval(secs => {lease_authority_margin_seconds})"
        )
        self._lease_retry_sleep = lease_retry_sleep or time.sleep
        # The lease lifecycle's only clock. Every interval this process
        # measures itself — adoption silence from guard acquisition, caller
        # deadlines, lease refresh age — reads through here, so a test can
        # supply a virtual clock and drive an interleaving by advancing it
        # rather than by sleeping and hoping. Production passes None and gets
        # time.monotonic.
        # Resolved at construction, not at each call: an instance built the
        # ordinary way binds whichever clock is in force now, while the class
        # attribute above still covers instances built without __init__.
        self._monotonic = monotonic or time.monotonic
        self._sql_backend_factory = sql_backend_factory
        self._lease_guard_factory = lease_guard_factory
        self._lease_retry_max_sleep_seconds = lease_retry_max_sleep_seconds
        self._lease_retry_min_sleep_seconds = min(0.25, self._lease_retry_max_sleep_seconds)
        self._lease_adoption_silence_seconds = lease_adoption_silence_seconds
        self._lease_acquire_lock_timeout_seconds = lease_acquire_lock_timeout_seconds
        self._lease_acquire_attempts = lease_acquire_attempts
        # One immutable guard set shared by all three connection paths (the
        # pooled native client, the dedicated lease-guard session, and the
        # psql subprocess backend), so a session this coordinator opens is
        # disowned by the server instead of holding the lease row on after
        # its client vanishes. See PostgresSessionGuards for what the guards
        # do and do not reach.
        self._session_guards = PostgresSessionGuards(
            idle_in_transaction_timeout_seconds=postgres_idle_in_transaction_timeout_seconds,
            tcp_keepalives_idle_seconds=postgres_tcp_keepalives_idle_seconds,
            tcp_keepalives_interval_seconds=postgres_tcp_keepalives_interval_seconds,
            tcp_keepalives_count=postgres_tcp_keepalives_count,
            # Fail closed at connect time rather than transaction by
            # transaction: a read-only ledger tells the server it is read-only
            # once, and every connection it can create -- pool slots, read
            # slots, the psql subprocess, autocommit statements included --
            # inherits the refusal.
            read_only=read_only,
        )
        self._operation_timeout_local = local()
        self._statement_timeout_local = local()
        # Created here, not lazily in operation_progress: that scope's
        # check-then-set is not atomic, so two block-work threads entering
        # their first-ever scope on a fresh ledger can each build a local()
        # and have one assignment win. The loser's hook would then live on an
        # orphaned object and its admission wait would silently fall back to
        # the heartbeat-silent path -- the exact failure the hook exists to
        # remove. The scope keeps its lazy fallback for ledgers built through
        # __new__ in focused tests; the production path must not race.
        self._operation_progress_local = local()
        self._read_only = read_only
        # A read-only ledger never holds the writer lock, so it is not given
        # one: see _RefusingWriterGate. Read-slot traffic is unaffected.
        self._lock = _RefusingWriterGate() if read_only else Lock()
        self._read_semaphore = BoundedSemaphore(read_concurrency)
        audit_share_segment_size = int(audit_share_segment_size)
        if audit_share_segment_size < 0:
            raise ValueError("audit_share_segment_size must be non-negative")
        if audit_artifact_store is None and audit_body_dir is not None:
            body_root = Path(audit_body_dir)
            audit_artifact_store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=body_root,
                    evidence_path=body_root / "prism-live-stratum-evidence.json",
                    share_segment_size=audit_share_segment_size,
                ),
                # Legacy direct-ledger construction is an explicit adapter.
                # Coordinator production wiring injects the shared A1 store.
                canonicalizer=(
                    audit_bundle_canonicalizer or _default_bundle_canonicalizer()
                ),
            )
        self._audit_artifact_store = audit_artifact_store
        self._audit_bundle_canonicalizer = (
            audit_bundle_canonicalizer or _default_bundle_canonicalizer()
        )
        ctv_broadcast_attempt_detail_limit = int(ctv_broadcast_attempt_detail_limit)
        if ctv_broadcast_attempt_detail_limit < 0:
            raise ValueError("ctv_broadcast_attempt_detail_limit must be non-negative")
        self._ctv_broadcast_attempt_detail_limit = ctv_broadcast_attempt_detail_limit
        ctv_broadcast_retry_backoff_seconds = int(ctv_broadcast_retry_backoff_seconds)
        if ctv_broadcast_retry_backoff_seconds < 0:
            raise ValueError("ctv_broadcast_retry_backoff_seconds must be non-negative")
        self._ctv_broadcast_retry_backoff_seconds = ctv_broadcast_retry_backoff_seconds
        self._accepted_stats_cache_seconds = accepted_stats_cache_seconds
        self._reward_window_cache_seconds = reward_window_cache_seconds
        self._reward_window_cache_lock = Lock()
        self._reward_window_cache: tuple[str, float, dict[str, Any]] | None = None
        self._stats_lock = Lock()
        self._stats_refresh_lock = Lock()
        self._stats_counts: dict[str, int] | None = None
        self._stats_max_share_seq = 0
        self._stats_note_buffer: dict[int, bool] | None = None
        self._stats_refreshed_monotonic: float | None = None
        self._stats_background_refresh_thread: Thread | None = None
        self._stats_reconcile_failures = 0
        self._prior_balances_read_stats_lock = Lock()
        self._prior_balances_reads_total = 0
        self._prior_balances_read_last_seconds = 0.0
        self._prior_balances_read_max_seconds = 0.0
        # Attribution for read-slot operations: local admission and server
        # execution are recorded separately (see _run_attributed_read_json).
        # Built here so the production path never reaches the __new__
        # bootstrap in _ensure_ledger_read_timings; the dict is published
        # before the lock that guards it, so a reader that observes the lock
        # observes the dict too.
        self._ledger_read_timings: dict[str, dict[str, float | int]] = {}
        self._ledger_read_timings_lock = Lock()
        self._native = self._make_native_client(
            native_client_mode,
            database_url,
            read_concurrency=read_concurrency,
        )
        self._writer_lease_guard: LeaseGuardPort | None = None
        try:
            if not read_only:
                # Constructing an ordinary ledger claims the single-writer
                # lease. A read-only ledger must not, or a second process
                # opening one would contend with -- and could adopt -- the
                # lease the coordinator lands blocks under.
                self._initialize_writer_lease_guard(database_url)
                if initialize_schema:
                    path = schema_path or Path(__file__).resolve().parents[2] / "crates/qbit-prism/sql/001_share_ledger.sql"
                    self._run_script(path.read_text(encoding="utf-8"))
                self._ensure_writer_lease()
        except BaseException:
            self.close()
            raise

    def _make_native_client(
        self,
        native_client_mode: str,
        database_url: str | None,
        *,
        read_concurrency: int,
    ) -> LedgerSqlPort | None:
        mode = (native_client_mode or "auto").strip().lower()
        if mode in {"0", "false", "no", "off", "psql"}:
            return None
        if mode not in {"auto", "1", "true", "yes", "on", "native"}:
            raise ValueError(f"unsupported native client mode: {native_client_mode!r}")
        required = mode != "auto"
        conninfo = database_url or database_url_from_psql_command(self._command)
        if conninfo is None:
            if required:
                raise ValueError(
                    "PRISM_POSTGRES_NATIVE_CLIENT=1 requires PRISM_DATABASE_URL or a "
                    "postgres:// DSN inside PRISM_POSTGRES_PSQL_COMMAND"
                )
            return None
        # One pooled connection per concurrent reader plus one for the
        # serialized write path (the coordinator's share writer thread).
        pool_size = read_concurrency + 1
        if self._sql_backend_factory is not None:
            # An injected backend is authoritative: it stands in for the
            # whole server, so psycopg's availability is irrelevant and a
            # None return means the same thing it does below (fall back to
            # the psql subprocess path).
            return self._sql_backend_factory(
                conninfo,
                pool_size=pool_size,
                application_name=self._pool_application_name,
            )
        try:
            return _NativePostgresClient(
                conninfo,
                pool_size=pool_size,
                application_name=self._pool_application_name,
                session_guards=getattr(self, "_session_guards", None),
            )
        except ImportError:
            if required:
                raise ValueError(
                    "PRISM_POSTGRES_NATIVE_CLIENT=1 requires the psycopg package"
                ) from None
            # The daemon's startup line reports the active execution backend,
            # but this silent capability loss is worth its own line too: the
            # psql fallback applies the schema through a subprocess whose
            # atomicity contract every operator relies on.
            print(
                "prism ledger native PostgreSQL client unavailable: "
                "psycopg import failed; falling back to the psql subprocess "
                "backend",
                flush=True,
            )
            return None

    def _make_writer_lease_guard(
        self,
        database_url: str | None,
    ) -> LeaseGuardPort | None:
        if self._native is None:
            return None
        conninfo = database_url or database_url_from_psql_command(self._command)
        if conninfo is None:
            return None
        advisory_lock_key = _writer_lease_advisory_lock_key(
            self._writer_id,
            self._writer_epoch,
        )
        if self._lease_guard_factory is not None:
            return self._lease_guard_factory(
                conninfo,
                advisory_lock_key=advisory_lock_key,
            )
        return _NativePostgresLeaseGuard(
            conninfo,
            advisory_lock_key,
            session_guards=getattr(self, "_session_guards", None),
        )

    def _initialize_writer_lease_guard(self, database_url: str | None) -> None:
        if not self._writer_session_token.startswith(
            WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
        ):
            return
        warned = False
        while True:
            guard = self._make_writer_lease_guard(database_url)
            if guard is None:
                # A psql subprocess cannot retain a session advisory lock.
                # Downgrade before publishing the token so other processes
                # conservatively retain TTL fencing for this owner.
                self._writer_session_token = uuid.uuid4().hex
                print(
                    "prism ledger writer fast adoption disabled: persistent "
                    "native PostgreSQL connection unavailable; using TTL fencing",
                    flush=True,
                )
                return
            try:
                acquired = guard.try_acquire()
            except Exception:
                guard.close()
                raise
            if acquired:
                self._writer_lease_guard = guard
                # Adoption silence is measured from this moment as well as
                # from the lease row's updated_at. The predecessor that just
                # lost this advisory lock self-fences within its heartbeat
                # failure budget; counting from acquisition guarantees it
                # that time even when its lease row is already stale because
                # a long fenced transaction withheld updated_at refreshes.
                self._writer_lease_guard_acquired_monotonic = self._monotonic()
                return
            guard.close()
            if not warned:
                print(
                    "prism ledger writer guard held by a live same-identity "
                    "coordinator; waiting before lease acquisition",
                    flush=True,
                )
                warned = True
            self._lease_retry_sleep(self._lease_retry_min_sleep_seconds)

    @property
    def execution_backend(self) -> str:
        if getattr(self, "_native", None) is not None:
            return "psycopg-pool"
        return "psql-subprocess"

    def close(self) -> None:
        """Release pooled native connections. Safe to call multiple times."""
        native = getattr(self, "_native", None)
        if native is not None:
            native.close()
        self._close_writer_lease_guard()

    def _close_writer_lease_guard(self) -> None:
        guard = getattr(self, "_writer_lease_guard", None)
        if guard is None:
            return
        self._writer_lease_guard = None
        guard.close()

    @property
    def writer_lease_fast_adoption_capable(self) -> bool:
        guard = getattr(self, "_writer_lease_guard", None)
        return bool(
            self._writer_session_token.startswith(
                WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
            )
            and guard is not None
            and guard.held
        )

    @property
    def writer_lease_guard_required(self) -> bool:
        return self._writer_session_token.startswith(
            WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
        )

    @property
    def writer_lease_last_refresh_monotonic(self) -> float | None:
        return getattr(self, "_writer_lease_last_refresh_monotonic", None)

    @property
    def backend_name(self) -> str:
        return "postgres-psql"

    def read_replica_status(
        self,
        *,
        timeout_seconds: float = DEFAULT_READ_REPLICA_PROBE_TIMEOUT_SECONDS,
    ) -> dict[str, object]:
        """Replication-state probe backing the public read staleness contract.

        Returns:

        - ``in_recovery`` -- true only on a hot standby;
        - ``replay_lag_seconds`` -- wall clock minus the newest replayed
          transaction's commit time (None before any replay). This is an
          informational staleness *indicator*, not a freshness proof: on an
          idle primary it grows with wall time even though the replica is
          fully caught up;
        - ``receiver_heartbeat_age_seconds`` -- wall clock minus the newest
          message from the primary's WAL sender (None when the walreceiver
          is not connected). Heartbeats flow every
          ``wal_receiver_status_interval`` (default 10s) even when the
          primary is idle, so this is the replica-side liveness proof the
          public read service enforces;
        - ``apply_backlog_bytes`` -- WAL bytes received but not yet replayed.

        The extracted public read service (issue #145) polls this to enforce
        its bounded-staleness contract and to refuse serving from a writable
        primary. Reads run through the ordinary read pool, so the probe never
        touches the writer lease.

        Bounded by default: this runs on the service's background probe
        thread, and an unbounded probe against a wedged standby would leave
        the freshness gate holding its last answer indefinitely rather than
        ageing out into a refusal.
        """
        sql = """
SELECT json_build_object(
    'in_recovery', pg_is_in_recovery(),
    'replay_lag_seconds', CASE
        WHEN pg_last_xact_replay_timestamp() IS NULL THEN NULL
        ELSE extract(epoch FROM (clock_timestamp() - pg_last_xact_replay_timestamp()))
    END,
    'receiver_heartbeat_age_seconds', CASE
        WHEN (SELECT last_msg_receipt_time FROM pg_stat_wal_receiver) IS NULL THEN NULL
        ELSE extract(epoch FROM (clock_timestamp() - (SELECT last_msg_receipt_time FROM pg_stat_wal_receiver)))
    END,
    'apply_backlog_bytes', CASE
        WHEN pg_last_wal_receive_lsn() IS NULL OR pg_last_wal_replay_lsn() IS NULL THEN NULL
        ELSE pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
    END
);
"""
        with self.operation_timeout(timeout_seconds):
            row = self._run_read_json(sql)
        if not isinstance(row, dict):
            raise RuntimeError("read replica status probe did not return an object")
        return row

    @contextmanager
    def operation_timeout(self, timeout_seconds: float) -> Iterator[None]:
        """Bound PostgreSQL and local admission for the current thread.

        The block submitter uses this scope for direct outbox operations.
        Nested scopes keep the earliest deadline, so helper calls cannot
        accidentally widen the caller's liveness budget.
        """
        timeout_seconds = float(timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("operation timeout must be finite and positive")
        timeout_local = getattr(self, "_operation_timeout_local", None)
        if timeout_local is None:
            timeout_local = local()
            self._operation_timeout_local = timeout_local
        previous = getattr(timeout_local, "deadline", None)
        deadline = self._monotonic() + timeout_seconds
        timeout_local.deadline = (
            deadline if previous is None else min(float(previous), deadline)
        )
        try:
            yield
        finally:
            if previous is None:
                try:
                    del timeout_local.deadline
                except AttributeError:
                    pass
            else:
                timeout_local.deadline = previous

    @contextmanager
    def statement_timeout(self, timeout_seconds: float) -> Iterator[None]:
        """Apply a fresh bound to each lock admission and SQL statement.

        Unlike ``operation_timeout``, this budget does not start counting down
        across non-database work between calls. The block accounting tail can
        therefore build and verify an audit bundle before giving each later
        Postgres step its own short deadline.
        """
        timeout_seconds = float(timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("statement timeout must be finite and positive")
        timeout_local = getattr(self, "_statement_timeout_local", None)
        if timeout_local is None:
            timeout_local = local()
            self._statement_timeout_local = timeout_local
        previous = getattr(timeout_local, "timeout_seconds", None)
        timeout_local.timeout_seconds = (
            timeout_seconds
            if previous is None
            else min(float(previous), timeout_seconds)
        )
        try:
            yield
        finally:
            if previous is None:
                try:
                    del timeout_local.timeout_seconds
                except AttributeError:
                    pass
            else:
                timeout_local.timeout_seconds = previous

    @contextmanager
    def operation_progress(
        self,
        on_progress: Callable[[], None],
        *,
        slice_seconds: float,
    ) -> Iterator[None]:
        """Stamp caller liveness while a ledger admission wait is blocked.

        A landing-class caller runs its ledger step directly on a
        watchdog-monitored block-work thread. Waiting for the writer lock or
        the read semaphore is not database work: no statement has been sent,
        so neither ``statement_timeout`` nor any server-side cancellation
        bounds it, and the ledger has nothing to report until admission
        succeeds. Without this hook the whole admission budget is
        heartbeat-silent, and a coordinator that is merely queued behind
        another writer is hard-exited by its own watchdog mid-landing --
        which loses the escalation state the retry depended on and restarts
        the same doomed cycle.

        The hook fires between acquire slices, so it never runs while this
        thread holds the gate, and it never replaces the caller's deadline:
        ``_operation_gate`` still raises at the deadline
        ``_remaining_operation_timeout`` reports. An exception from
        ``on_progress`` propagates to the caller; a liveness stamp that
        cannot be taken is a real failure, not something to swallow inside a
        lock wait.

        Nesting merges the way ``operation_timeout``/``statement_timeout``
        merge: the effective slice is the minimum of the enclosing slices, so
        an inner scope can only tighten the stamp cadence, never widen it. A
        nested scope carrying a larger slice would otherwise lengthen the
        heartbeat gaps its enclosing caller had already sized against its own
        monitor. The callback itself does replace -- the innermost caller is
        the one whose liveness is at stake -- and the outer pair is restored
        on exit.
        """
        slice_seconds = float(slice_seconds)
        if not math.isfinite(slice_seconds) or slice_seconds <= 0:
            raise ValueError("operation progress slice must be finite and positive")
        progress_local = getattr(self, "_operation_progress_local", None)
        if progress_local is None:
            progress_local = local()
            self._operation_progress_local = progress_local
        previous = getattr(progress_local, "hook", None)
        progress_local.hook = (
            (on_progress, slice_seconds)
            if previous is None
            else (on_progress, min(float(previous[1]), slice_seconds))
        )
        try:
            yield
        finally:
            if previous is None:
                try:
                    del progress_local.hook
                except AttributeError:
                    pass
            else:
                progress_local.hook = previous

    def _operation_progress_hook(self) -> tuple[Callable[[], None], float] | None:
        progress_local = getattr(self, "_operation_progress_local", None)
        if progress_local is None:
            return None
        return getattr(progress_local, "hook", None)

    def _note_operation_progress(self) -> None:
        """Report liveness between the statements of one gate-holding step.

        ``_acquire_operation_gate`` stamps only while a caller is *waiting*
        for a gate, which is all a single-statement operation needs: once the
        gate opens the caller is inside one server-side statement, and its
        liveness monitor is sized for exactly that. An operation that issues
        a second statement without releasing the gate breaks that sizing.
        No admission slice runs between the two -- the gate is already held --
        so the monitor sees one unbroken silence of two statement budgets
        where it was promised one. That is indistinguishable from a wedged
        operation, and a watchdog with any tolerance below twice the budget
        hard-exits a coordinator that is in fact making normal progress
        (issue #125). Reporting here restores the contract: the first
        statement's full round trip has completed, which is precisely the
        evidence a liveness monitor watches for, while a genuinely stuck
        statement still produces no report at all.

        With no hook installed this does nothing; an ordinary ledger caller
        has no monitor to satisfy. An exception from the hook propagates,
        matching ``_acquire_operation_gate``: a liveness stamp that cannot be
        taken is a real failure, not something to swallow mid-operation.
        """
        hook = self._operation_progress_hook()
        if hook is None:
            return
        on_progress, _slice_seconds = hook
        on_progress()

    def _remaining_operation_timeout(self) -> float | None:
        timeout_local = getattr(self, "_operation_timeout_local", None)
        deadline = (
            getattr(timeout_local, "deadline", None)
            if timeout_local is not None
            else None
        )
        statement_timeout_local = getattr(
            self,
            "_statement_timeout_local",
            None,
        )
        statement_timeout_seconds = (
            getattr(statement_timeout_local, "timeout_seconds", None)
            if statement_timeout_local is not None
            else None
        )
        if deadline is None:
            return (
                None
                if statement_timeout_seconds is None
                else float(statement_timeout_seconds)
            )
        remaining = float(deadline) - self._monotonic()
        if remaining <= 0:
            raise LedgerOperationTimeout("postgres operation deadline expired")
        if statement_timeout_seconds is not None:
            remaining = min(remaining, float(statement_timeout_seconds))
        return remaining

    def _acquire_operation_gate(self, gate: Any, name: str) -> None:
        """Wait for one ledger gate inside the caller's remaining deadline.

        With no progress hook installed this is a single blocking acquire, as
        it has always been: an ordinary caller has no liveness monitor to
        satisfy and gains nothing from waking up. With a hook installed the
        same total wait is served in slices so the caller can stamp its
        heartbeat between them (see ``operation_progress`` for why an
        admission wait would otherwise be silent). Slicing never widens the
        wait: the deadline derived from ``_remaining_operation_timeout`` stays
        authoritative and still produces the same timeout error.
        """
        remaining = self._remaining_operation_timeout()
        hook = self._operation_progress_hook()
        if hook is None:
            acquired = (
                gate.acquire()
                if remaining is None
                else gate.acquire(timeout=max(0.0, remaining))
            )
            if not acquired:
                raise LedgerOperationTimeout(f"timed out waiting for postgres {name}")
            return
        on_progress, slice_seconds = hook
        deadline = (
            None if remaining is None else self._monotonic() + max(0.0, remaining)
        )
        while True:
            wait_seconds = slice_seconds
            if deadline is not None:
                wait_seconds = min(wait_seconds, deadline - self._monotonic())
            # An expired budget still gets one non-blocking attempt, so a
            # zero or negative remaining fails exactly where it always did.
            if gate.acquire(timeout=max(0.0, wait_seconds)):
                return
            if deadline is not None and self._monotonic() >= deadline:
                raise LedgerOperationTimeout(f"timed out waiting for postgres {name}")
            on_progress()

    @contextmanager
    def _operation_gate(self, gate: Any, name: str) -> Iterator[None]:
        """Acquire a ledger lock/semaphore within the caller's deadline."""
        self._acquire_operation_gate(gate, name)
        try:
            yield
        finally:
            gate.release()

    def append(self, pending: PendingShare) -> AcceptedShareRecord:
        if pending.share_difficulty <= 0:
            raise ValueError("share_difficulty must be positive")
        if pending.network_difficulty <= 0:
            raise ValueError("network_difficulty must be positive")
        credit_policy = validate_credit_policy(pending.credit_policy)
        payload = {
            **pending.__dict__,
            "credit_policy": credit_policy,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
existing_share AS (
    SELECT share_seq
    FROM qbit_share_ledger
    WHERE share_id = (SELECT data->>'share_id' FROM payload)
),
existing_miner AS (
    SELECT 1
    FROM qbit_share_ledger
    WHERE accepted
      AND miner_id = (SELECT data->>'miner_id' FROM payload)
    LIMIT 1
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
inserted AS (
    INSERT INTO qbit_share_ledger (
        share_id,
        miner_id,
        payout_order_key,
        p2mr_program,
        share_difficulty,
        network_difficulty,
        template_height,
        job_id,
        job_issued_at,
        ntime,
        accepted_at,
        credit_policy,
        accepted,
        writer_id,
        writer_epoch
    )
    SELECT
        data->>'share_id',
        data->>'miner_id',
        data->>'order_key',
        decode(data->>'p2mr_program_hex', 'hex'),
        (data->>'share_difficulty')::numeric,
        (data->>'network_difficulty')::numeric,
        (data->>'template_height')::bigint,
        data->>'job_id',
        to_timestamp(((data->>'job_issued_at_ms')::double precision / 1000.0)),
        (data->>'ntime')::bigint,
        to_timestamp(((data->>'accepted_at_ms')::double precision / 1000.0)),
        data->>'credit_policy',
        true,
        data->>'writer_id',
        (data->>'writer_epoch')::bigint
    FROM payload, lease
    WHERE NOT EXISTS (SELECT 1 FROM existing_share)
    RETURNING *
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN EXISTS (SELECT 1 FROM existing_share) THEN
        json_build_object('error', 'duplicate share_id')
    ELSE
        (SELECT json_build_object(
            'share_seq', share_seq,
            'share_id', share_id,
            'miner_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'share_difficulty', share_difficulty::text,
            'network_difficulty', network_difficulty::text,
            'template_height', template_height,
            'job_id', job_id,
            'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
            'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
            'ntime', ntime,
            'credit_policy', credit_policy,
            'new_miner', NOT EXISTS (SELECT 1 FROM existing_miner)
        ) FROM inserted)
END;
"""
        # Serialize the single writer through its durable commit and cache note.
        # Stats reconciliation uses a separate read connection plus a share-seq
        # watermark, so it never acquires this writer lock.
        with self._operation_gate(self._lock, "writer lock"):
            result = self._run_json(sql)
            if "error" in result:
                raise RuntimeError(str(result["error"]))
            record = self._record_from_json(result)
            self._note_appended_share(
                record,
                new_miner=bool(result.get("new_miner", False)),
            )
            return record

    def _append_batch_with_replay_outcomes(
        self,
        entries: list[tuple[PendingShare, dict[str, Any] | None]],
    ) -> list[ShareReplayResult]:
        """Commit accepted shares and optional block intents in one transaction.

        Replaying the exact same payload is idempotent.  Reusing a share ID or
        block hash with different content fails the whole batch — a share-ID
        payload mismatch raises the typed :class:`ShareReplayConflict`.
        Postgres assigns the share sequence and makes every row visible before
        this method returns, which is the coordinator's Stratum ACK boundary.
        Each entry's outcome reports whether its row was ``inserted`` by this
        statement or was an ``exact_existing`` durable duplicate.
        """
        if not entries:
            return []
        payloads: list[dict[str, Any]] = []
        share_ids: set[str] = set()
        block_hashes: set[str] = set()
        for pending, candidate in entries:
            if pending.share_difficulty <= 0:
                raise ValueError("share_difficulty must be positive")
            if pending.network_difficulty <= 0:
                raise ValueError("network_difficulty must be positive")
            if pending.share_id in share_ids:
                raise ValueError("duplicate share_id in append batch")
            share_ids.add(pending.share_id)
            candidate_payload = candidate
            if candidate_payload is not None:
                block_hash = str(candidate_payload.get("block_hash_hex", "")).lower()
                if not block_hash:
                    raise ValueError("block candidate is missing block_hash_hex")
                if block_hash in block_hashes:
                    raise ValueError("duplicate block candidate in append batch")
                block_hashes.add(block_hash)
                candidate_payload = {**candidate_payload, "block_hash_hex": block_hash}
            payloads.append(
                {
                    "share": {
                        **pending.__dict__,
                        "credit_policy": validate_credit_policy(pending.credit_policy),
                    },
                    "candidate": candidate_payload,
                    "candidate_sha256": (
                        block_candidate_identity_sha256(candidate_payload)
                        if candidate_payload is not None
                        else None
                    ),
                }
            )
        payload = {
            "entries": payloads,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH input AS (
    SELECT
        {self._jsonb_literal(payload)} AS root,
        set_config('synchronous_commit', 'on', true) AS durability
),
payload AS (
    SELECT
        item->'share' AS data,
        NULLIF(item->'candidate', 'null'::jsonb) AS candidate,
        item->>'candidate_sha256' AS candidate_sha256,
        ordinality
    FROM input,
         jsonb_array_elements(root->'entries') WITH ORDINALITY AS rows(item, ordinality)
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM input
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = root->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (root->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = root->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
share_mismatch AS (
    SELECT data->>'share_id' AS share_id
    FROM payload
    JOIN qbit_share_ledger ledger ON ledger.share_id = data->>'share_id'
    WHERE ledger.miner_id IS DISTINCT FROM data->>'miner_id'
       OR ledger.payout_order_key IS DISTINCT FROM data->>'order_key'
       OR ledger.p2mr_program IS DISTINCT FROM decode(data->>'p2mr_program_hex', 'hex')
       OR ledger.share_difficulty IS DISTINCT FROM (data->>'share_difficulty')::numeric
       OR ledger.network_difficulty IS DISTINCT FROM (data->>'network_difficulty')::numeric
       OR ledger.template_height IS DISTINCT FROM (data->>'template_height')::bigint
       OR ledger.job_id IS DISTINCT FROM data->>'job_id'
       OR ledger.job_issued_at IS DISTINCT FROM to_timestamp((data->>'job_issued_at_ms')::double precision / 1000.0)
       OR ledger.ntime IS DISTINCT FROM (data->>'ntime')::bigint
       OR ledger.credit_policy IS DISTINCT FROM data->>'credit_policy'
),
candidate_mismatch AS (
    SELECT payload.candidate->>'block_hash_hex' AS block_hash
    FROM payload
    JOIN qbit_block_candidate_outbox outbox
      ON outbox.block_hash = payload.candidate->>'block_hash_hex'
    WHERE payload.candidate IS NOT NULL
      AND ((outbox.share_id IS NOT NULL AND outbox.share_id IS DISTINCT FROM payload.data->>'share_id')
           OR outbox.candidate_sha256 IS DISTINCT FROM payload.candidate_sha256
           OR (outbox.candidate IS NOT NULL
               AND (outbox.candidate #- '{{pending_share,accepted_at_ms}}')
                   IS DISTINCT FROM (payload.candidate #- '{{pending_share,accepted_at_ms}}')))
),
candidate_states AS (
    SELECT
        payload.ordinality,
        CASE
            WHEN payload.candidate IS NULL THEN NULL
            ELSE COALESCE(outbox.state, 'pending')
        END AS candidate_outbox_state
    FROM payload
    LEFT JOIN qbit_block_candidate_outbox outbox
      ON outbox.block_hash = payload.candidate->>'block_hash_hex'
),
batch_ok AS (
    SELECT 1 AS ok
    WHERE EXISTS (SELECT 1 FROM lease)
      AND NOT EXISTS (SELECT 1 FROM share_mismatch)
      AND NOT EXISTS (SELECT 1 FROM candidate_mismatch)
),
inserted_shares AS (
    INSERT INTO qbit_share_ledger (
        share_id, miner_id, payout_order_key, p2mr_program,
        share_difficulty, network_difficulty, template_height, job_id,
        job_issued_at, ntime, accepted_at, credit_policy, accepted,
        writer_id, writer_epoch
    )
    SELECT
        data->>'share_id', data->>'miner_id', data->>'order_key',
        decode(data->>'p2mr_program_hex', 'hex'),
        (data->>'share_difficulty')::numeric,
        (data->>'network_difficulty')::numeric,
        (data->>'template_height')::bigint, data->>'job_id',
        to_timestamp((data->>'job_issued_at_ms')::double precision / 1000.0),
        (data->>'ntime')::bigint,
        to_timestamp((data->>'accepted_at_ms')::double precision / 1000.0),
        data->>'credit_policy', true, root->>'writer_id',
        (root->>'writer_epoch')::bigint
    FROM payload, input, batch_ok
    WHERE NOT EXISTS (
        SELECT 1 FROM qbit_share_ledger existing
        WHERE existing.share_id = payload.data->>'share_id'
    )
    ORDER BY payload.ordinality
    ON CONFLICT (share_id) DO NOTHING
    RETURNING qbit_share_ledger.*
),
inserted_candidates AS (
    INSERT INTO qbit_block_candidate_outbox (
        block_hash, share_id, candidate, candidate_sha256
    )
    SELECT
        payload.candidate->>'block_hash_hex', payload.data->>'share_id',
        payload.candidate, payload.candidate_sha256
    FROM payload, batch_ok
    WHERE payload.candidate IS NOT NULL
    ON CONFLICT (block_hash) DO UPDATE
    SET share_id = EXCLUDED.share_id,
        updated_at = clock_timestamp()
    WHERE qbit_block_candidate_outbox.share_id IS NULL
      AND qbit_block_candidate_outbox.candidate_sha256 = EXCLUDED.candidate_sha256
    RETURNING block_hash
),
records AS (
    SELECT
        ledger.*, payload.ordinality, false AS newly_inserted,
        false AS new_miner, candidate_states.candidate_outbox_state
    FROM payload
    JOIN qbit_share_ledger ledger ON ledger.share_id = payload.data->>'share_id'
    JOIN candidate_states ON candidate_states.ordinality = payload.ordinality
    UNION ALL
    SELECT
        inserted_shares.*, payload.ordinality, true AS newly_inserted,
        -- Data-modifying CTEs and this main query share one pre-statement
        -- MVCC snapshot. This base-table probe cannot see inserted_shares;
        -- only the RETURNING CTE exposes those new rows to this statement.
        NOT EXISTS (
            SELECT 1
            FROM qbit_share_ledger existing_miner
            WHERE existing_miner.accepted
              AND existing_miner.miner_id = inserted_shares.miner_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM inserted_shares earlier_insert
            WHERE earlier_insert.miner_id = inserted_shares.miner_id
              AND earlier_insert.share_seq < inserted_shares.share_seq
        ) AS new_miner,
        candidate_states.candidate_outbox_state
    FROM inserted_shares
    JOIN payload ON payload.data->>'share_id' = inserted_shares.share_id
    JOIN candidate_states ON candidate_states.ordinality = payload.ordinality
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    WHEN EXISTS (SELECT 1 FROM share_mismatch) THEN
        json_build_object(
            'error', 'duplicate share_id payload mismatch',
            'error_kind', 'share_replay_conflict',
            'share_ids', (SELECT json_agg(share_id ORDER BY share_id) FROM share_mismatch)
        )
    WHEN EXISTS (SELECT 1 FROM candidate_mismatch) THEN
        json_build_object(
            'error', 'block candidate payload mismatch',
            'block_hashes', (SELECT json_agg(block_hash ORDER BY block_hash) FROM candidate_mismatch)
        )
    ELSE json_build_object(
        'records', (
            SELECT json_agg(json_build_object(
                'share_seq', records.share_seq,
                'share_id', records.share_id,
                'miner_id', records.miner_id,
                'order_key', records.payout_order_key,
                'p2mr_program_hex', encode(records.p2mr_program, 'hex'),
                'share_difficulty', records.share_difficulty::text,
                'network_difficulty', records.network_difficulty::text,
                'template_height', records.template_height,
                'job_id', records.job_id,
                'job_issued_at_ms', round(extract(epoch FROM records.job_issued_at) * 1000)::bigint,
                'accepted_at_ms', round(extract(epoch FROM records.accepted_at) * 1000)::bigint,
                'ntime', records.ntime,
                'credit_policy', records.credit_policy,
                'newly_inserted', records.newly_inserted,
                'new_miner', records.new_miner,
                'candidate_outbox_state', records.candidate_outbox_state
            ) ORDER BY records.ordinality)
            FROM records
        )
    )
END;
"""
        with self._operation_gate(self._lock, "writer lock"):
            result = self._run_json(sql)
            if "error" in result:
                if result.get("error_kind") == "share_replay_conflict":
                    raise ShareReplayConflict(str(result["error"]))
                raise RuntimeError(str(result["error"]))
            records = result.get("records")
            if not isinstance(records, list) or len(records) != len(entries):
                raise RuntimeError("Postgres share batch returned an incomplete result")
            parsed = [self._record_from_json(record) for record in records]
            committed = sorted(
                zip(records, parsed, strict=True),
                key=lambda item: item[1].share_seq,
            )
            for payload, record in committed:
                if bool(payload.get("newly_inserted", True)):
                    self._note_appended_share(
                        record,
                        new_miner=bool(payload.get("new_miner", False)),
                    )
            return [
                ShareReplayResult(
                    (
                        "inserted"
                        if bool(payload.get("newly_inserted", True))
                        else "exact_existing"
                    ),
                    record,
                )
                for payload, record in zip(records, parsed, strict=True)
            ]

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, Any] | None]],
    ) -> list[AcceptedShareRecord]:
        return [
            outcome.record
            for outcome in self._append_batch_with_replay_outcomes(entries)
        ]

    def append_recovered_share(self, pending: PendingShare) -> ShareReplayResult:
        """Use the exact batch comparator for one typed recovery outcome."""
        outcomes = self._append_batch_with_replay_outcomes([(pending, None)])
        if len(outcomes) != 1:
            raise RuntimeError("Postgres recovery append returned an incomplete result")
        return outcomes[0]

    def persist_block_candidate_intent(
        self,
        candidate: dict[str, Any],
    ) -> BlockCandidateIntentPersistResult:
        """Persist candidate work that is not yet eligible for share credit."""
        block_hash = str(candidate.get("block_hash_hex", "")).lower()
        if not block_hash:
            raise ValueError("block candidate is missing block_hash_hex")
        candidate = {**candidate, "block_hash_hex": block_hash}
        candidate_sha256 = block_candidate_identity_sha256(candidate)
        sql = f"""
WITH durability AS (
    SELECT set_config('synchronous_commit', 'on', true)
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM durability
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
),
existing AS (
    SELECT candidate_sha256, state
    FROM qbit_block_candidate_outbox
    WHERE block_hash = {self._text_literal(block_hash)}
),
inserted AS (
    INSERT INTO qbit_block_candidate_outbox (
        block_hash, share_id, candidate, candidate_sha256
    )
    SELECT
        {self._text_literal(block_hash)}, NULL,
        {self._jsonb_literal(candidate)}, {self._text_literal(candidate_sha256)}
    FROM lease
    WHERE NOT EXISTS (SELECT 1 FROM existing)
    ON CONFLICT (block_hash) DO NOTHING
    RETURNING block_hash
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    WHEN EXISTS (
        SELECT 1 FROM existing
        WHERE candidate_sha256 <> {self._text_literal(candidate_sha256)}
    ) THEN
        json_build_object('error', 'block candidate payload mismatch')
    ELSE
        json_build_object(
            'inserted', (SELECT count(*) FROM inserted),
            'state', COALESCE((SELECT state FROM existing), 'pending')
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return BlockCandidateIntentPersistResult(
            inserted=int(result.get("inserted", 0)) > 0,
            state=str(result.get("state", "pending")),
        )

    def pending_block_candidates(self, *, limit: int = 32) -> list[dict[str, Any]]:
        return [
            row["candidate"]
            for row in self.pending_block_candidate_rows(limit=limit)
        ]

    def pending_block_candidate_rows(
        self,
        *,
        limit: int = 32,
        after_cursor: object | None = None,
    ) -> list[dict[str, Any]]:
        """Return pending payloads together with their authoritative row keys.

        Rows carry an opaque ``cursor`` the caller passes back verbatim as
        ``after_cursor`` to resume strictly after that row, so a backlog
        larger than one window enumerates completely in bounded pages
        instead of forcing an ever-wider single query. The keyset predicate
        and the ordering both stay on ``(created_at, block_hash)``, which is
        exactly the partial index
        ``qbit_block_candidate_outbox_pending_idx``: every page is one
        bounded index range scan regardless of how far in the backlog it
        starts.

        The cursor stamp is rendered at microsecond precision with an
        explicit UTC marker because ``created_at`` is a ``timestamptz`` whose
        stored resolution is microseconds: a second-precision stamp (the
        format the public API endpoints use) would truncate, and the
        resulting predicate would re-emit or skip whole sub-second groups.

        Each row also carries ``pool_block_exists``: whether a durable
        ``qbit_pool_blocks`` row exists for that hash, i.e. whether the
        candidate ever reached ``persist_accepted_block``. It is answered
        inside this page read -- one bounded existence probe per returned row
        -- because the alternative is one round trip per row, and a page is
        read precisely when the backlog is large. The fact is advisory by the
        time the caller holds it; the terminal batch update re-checks it under
        the writer fence.

        **Gate.** This is one read-only statement, and it takes the bounded
        read slot rather than the global writer lock (issue #211). Waiting for
        writer admission bought this page nothing: the query already crosses
        no in-process state, and every fact it returns is advisory the instant
        the statement commits -- a candidate can land, or be terminalized by
        another path, between the snapshot and anything the caller does with
        it. What holding the lock did buy was a convoy. During accepted-block
        accounting the enumeration spent most or all of its bounded budget
        queued behind an unrelated long write, and on ``union-mainnet`` that
        cost the fast-call budget outright while PostgreSQL was idle: the
        outer call reported ``exceeded 5s`` with only ~0.4-1.3s of the inner
        statement deadline consumed, and accepted candidates converged in
        79-186s with ``qbit_prism_accepted_parent_unresolved_oldest_seconds``
        reaching ~101s.

        Nothing downstream weakens as a result, because nothing downstream
        ever trusted this snapshot:

        * ``mark_block_candidates_abandoned`` re-asks ``qbit_pool_blocks``
          *inside* the fenced ``UPDATE``, under the writer-id/epoch/
          session-token lease predicate, and returns the exact hash set it
          transitioned -- so a row that acquired a pool block after this read
          is silently absent instead of abandoned.
        * ``mark_block_candidate_attempted`` and ``_finish_block_candidate``
          carry the same lease fence and additionally require
          ``state = 'pending'``, so a row another path terminalized between
          the snapshot and the write transitions nobody.
        * The node-offer path re-reads ``pool_block_state`` and the chain at
          dequeue rather than reusing a page fact
          (``_skip_superseded_block_candidate_at_dequeue``).

        Capacity is unchanged too: the read semaphore admits
        ``read_concurrency`` callers and the pooled client holds
        ``read_concurrency + 1`` connections, so moving this statement from
        the writer-lock class to the read-slot class still cannot demand more
        connections than the pool has, and it opens no connection and starts
        no thread of its own.
        """
        if limit <= 0:
            return []
        after_predicate = ""
        if after_cursor is not None:
            created_at_text, cursor_block_hash = _block_candidate_cursor_parts(
                after_cursor
            )
            if not isinstance(created_at_text, str):
                raise ValueError(
                    "pending block candidate cursor has no creation stamp"
                )
            after_predicate = (
                "\n      AND (created_at, block_hash) > "
                f"({self._text_literal(created_at_text)}::timestamptz, "
                f"{self._text_literal(cursor_block_hash)})"
            )
        sql = f"""
SELECT COALESCE(
    json_agg(
        json_build_object(
            'block_hash', pending.block_hash,
            'candidate', pending.candidate,
            'pool_block_exists', EXISTS (
                SELECT 1
                FROM qbit_pool_blocks pool
                WHERE pool.block_hash = pending.block_hash
            ),
            'cursor', json_build_array(
                pending.cursor_created_at,
                pending.block_hash
            )
        )
        ORDER BY pending.created_at, pending.block_hash
    ),
    '[]'::json
)
FROM (
    SELECT
        candidate,
        created_at,
        to_char(
            created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ) AS cursor_created_at,
        block_hash
    FROM qbit_block_candidate_outbox
    WHERE state = 'pending'{after_predicate}
    ORDER BY created_at, block_hash
    LIMIT {int(limit)}
) pending;
"""
        rows = list(
            self._run_attributed_read_json(
                sql,
                operation="pending_block_candidate_rows",
            )
        )
        # A row whose existence fact did not arrive is not a row with no pool
        # block: reading a missing key as false would hand a caller a false
        # negative on the one fact that keeps an offered, landed candidate out
        # of a terminal set. Fail the page instead.
        for row in rows:
            if not isinstance(row, dict) or "pool_block_exists" not in row:
                raise RuntimeError(
                    "pending block candidate row is missing pool block existence"
                )
            row["pool_block_exists"] = bool(row["pool_block_exists"])
        return rows

    def block_candidate_pending_metrics(self) -> dict[str, int | float]:
        """Return aggregate pending ages using the existing outbox index."""
        sql = """
SELECT json_build_object(
    'pending_count', count(*),
    'oldest_pending_age_seconds', COALESCE(
        EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)),
        0
    ),
    'oldest_unattempted_age_seconds', COALESCE(
        EXTRACT(
            EPOCH FROM clock_timestamp()
            - min(created_at) FILTER (WHERE attempt_count = 0)
        ),
        0
    )
)
FROM qbit_block_candidate_outbox
WHERE state = 'pending';
"""
        with self._operation_gate(self._lock, "writer lock"):
            metrics = self._run_retry_safe_read_json(sql)
        return {
            "pending_count": int(metrics.get("pending_count", 0)),
            "oldest_pending_age_seconds": float(
                metrics.get("oldest_pending_age_seconds", 0.0)
            ),
            "oldest_unattempted_age_seconds": float(
                metrics.get("oldest_unattempted_age_seconds", 0.0)
            ),
        }

    def mark_block_candidate_attempted(self, *, block_hash: str) -> bool:
        """Fence and count entry into a candidate processing attempt."""
        sql = f"""
WITH lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
),
updated AS (
    UPDATE qbit_block_candidate_outbox
    SET attempt_count = attempt_count + 1,
        updated_at = clock_timestamp()
    FROM lease
    WHERE block_hash = {self._text_literal(block_hash.lower())}
      AND state = 'pending'
    RETURNING block_hash
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object('updated', (SELECT count(*) FROM updated))
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return int(result.get("updated", 0)) > 0

    def mark_block_candidate_submitted(self, *, block_hash: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="submitted", error=None)

    def mark_block_candidate_abandoned(self, *, block_hash: str, error: str) -> bool:
        return self._finish_block_candidate(block_hash=block_hash, state="abandoned", error=error)

    def mark_block_candidates_abandoned(
        self,
        *,
        block_hashes: Sequence[str],
        error: str,
    ) -> tuple[str, ...]:
        """Abandon a caller-supplied page of pending rows in one fenced write.

        Same writer-id/epoch/session-token fence as the single-hash terminal
        updates, and the same terminal column set, but the row predicate is
        set-oriented: one ``block_hash = ANY(...)`` statement for the whole
        page rather than one statement per hash. At the storm cardinalities
        this exists for, the per-row form is the cost.

        ``RETURNING`` feeds the result, so the value is the exact set of
        hashes this fenced statement transitioned -- not a count, and not the
        requested set. Rows that were already terminal, that another writer
        won, or that do not exist are silently absent from it, which is what
        lets the caller confine follow-up cleanup to rows it actually won.
        An empty request performs no query at all, so no degenerate
        ``ANY(ARRAY[])`` statement is ever generated.

        The predicate re-checks ``qbit_pool_blocks`` rather than trusting the
        ``pool_block_exists`` the caller read on a prior page: a candidate can
        land between that read and this write, and a pool-block row is the
        durable evidence that it did. Re-asking under the writer fence closes
        that window inside the statement that does the transition, so a row
        that acquired one is silently absent from the returned set instead of
        being abandoned after it was won.
        """
        targets = _normalized_block_candidate_hash_set(block_hashes)
        if not targets:
            return ()
        sql = f"""
WITH lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
),
updated AS (
    UPDATE qbit_block_candidate_outbox
    SET state = 'abandoned',
        last_error = {self._text_literal(error)},
        updated_at = clock_timestamp(),
        completed_at = clock_timestamp(),
        candidate = NULL
    FROM lease
    WHERE block_hash = ANY({self._text_array_literal(targets)})
      AND state = 'pending'
      AND NOT EXISTS (
          SELECT 1
          FROM qbit_pool_blocks pool
          WHERE pool.block_hash = qbit_block_candidate_outbox.block_hash
      )
    RETURNING block_hash
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'abandoned',
            COALESCE(
                (SELECT json_agg(block_hash ORDER BY block_hash) FROM updated),
                '[]'::json
            )
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return tuple(str(value) for value in result.get("abandoned", ()))

    def _finish_block_candidate(self, *, block_hash: str, state: str, error: str | None) -> bool:
        if state not in {"submitted", "abandoned"}:
            raise ValueError("invalid block candidate terminal state")
        sql = f"""
WITH lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
),
updated AS (
    UPDATE qbit_block_candidate_outbox
    SET state = {self._text_literal(state)},
        last_error = {self._text_literal(error) if error is not None else 'NULL'},
        updated_at = clock_timestamp(),
        completed_at = clock_timestamp(),
        candidate = NULL
    FROM lease
    WHERE block_hash = {self._text_literal(block_hash.lower())}
      AND state = 'pending'
    RETURNING block_hash
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object('updated', (SELECT count(*) FROM updated))
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return int(result.get("updated", 0)) > 0

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> list[AcceptedShareRecord]:
        anchor = (
            f"to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0))"
        )
        if window_weight is None:
            # Whole accepted history up to the anchor. Kept for callers that
            # want the full ledger (tools/tests); the coordinator passes a
            # window_weight so the hot job-build path stays bounded.
            rows_cte = f"""
WITH rows AS (
    SELECT *
    FROM qbit_share_ledger
    WHERE accepted
      AND job_issued_at <= {anchor}
      AND accepted_at <= {anchor}
)"""
        else:
            # Find the bounded reward window in indexed pages, then apply one
            # exact cumulative cutoff over only those pages. The previous
            # recursive query fetched one row per recursive step; at production
            # window sizes that meant 100k+ lateral index probes. Paging keeps
            # the scan O(window), not O(history), while returning the identical
            # final crossing row and therefore byte-identical audit input.
            #
            # The final pass must consume the cutoff as a scalar subquery, not
            # a joined relation: a join makes the planner apply the cutoff as
            # a filter after walking the whole pkey backwards, while a scalar
            # subquery becomes an InitPlan the index scan can use as its start
            # bound, keeping this pass O(window) too.
            #
            # The page-granular stop relies on share_difficulty being NOT NULL
            # and strictly positive (schema CHECK), which keeps the cumulative
            # weight strictly increasing so the crossing row always lies in
            # the last fetched page. The same holds for qbit_prism_window()
            # in the schema file; relax the constraint and both walks need
            # revisiting.
            rows_cte = f"""
WITH RECURSIVE pages AS (
    SELECT page.min_share_seq,
           page.page_weight,
           page.page_weight AS cumulative_weight
    FROM LATERAL (
        SELECT min(page_rows.share_seq) AS min_share_seq,
               COALESCE(sum(page_rows.share_difficulty), 0)::numeric AS page_weight
        FROM (
            SELECT ledger.share_seq, ledger.share_difficulty
            FROM qbit_share_ledger ledger
            WHERE ledger.accepted
              AND ledger.job_issued_at <= {anchor}
              AND ledger.accepted_at <= {anchor}
            ORDER BY ledger.share_seq DESC
            LIMIT 4096
        ) page_rows
    ) page
    UNION ALL
    SELECT page.min_share_seq,
           page.page_weight,
           pages.cumulative_weight + page.page_weight
    FROM pages
    CROSS JOIN LATERAL (
        SELECT min(page_rows.share_seq) AS min_share_seq,
               COALESCE(sum(page_rows.share_difficulty), 0)::numeric AS page_weight
        FROM (
            SELECT ledger.share_seq, ledger.share_difficulty
            FROM qbit_share_ledger ledger
            WHERE ledger.accepted
              AND ledger.job_issued_at <= {anchor}
              AND ledger.accepted_at <= {anchor}
              AND ledger.share_seq < pages.min_share_seq
            ORDER BY ledger.share_seq DESC
            LIMIT 4096
        ) page_rows
    ) page
    WHERE pages.cumulative_weight < {int(window_weight)}::numeric
      AND pages.min_share_seq IS NOT NULL
),
page_cutoff AS (
    SELECT min(min_share_seq) AS min_share_seq
    FROM pages
    WHERE min_share_seq IS NOT NULL
),
ranked AS (
    SELECT ledger.*,
           sum(ledger.share_difficulty) OVER (
               ORDER BY ledger.share_seq DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )::numeric AS cumulative_difficulty
    FROM qbit_share_ledger ledger
    WHERE ledger.accepted
      AND ledger.job_issued_at <= {anchor}
      AND ledger.accepted_at <= {anchor}
      AND ledger.share_seq >= (SELECT min_share_seq FROM page_cutoff)
),
rows AS (
    SELECT *
    FROM ranked
    WHERE cumulative_difficulty - share_difficulty < {int(window_weight)}::numeric
)"""
        sql = rows_cte + """
SELECT COALESCE(json_agg(json_build_object(
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'network_difficulty', network_difficulty::text,
    'template_height', template_height,
    'job_id', job_id,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'ntime', ntime,
    'credit_policy', credit_policy
) ORDER BY share_seq ASC), '[]'::json)
FROM rows;
"""
        # Job construction is a retry-safe MVCC read. Use the independent read
        # pool so an accepted block's fenced bulk write cannot stall replacement
        # work behind the single-writer connection lock.
        return [
            self._record_from_json(item)
            for item in self._run_read_json(sql)
        ]

    def snapshot_between_job_issues(
        self,
        previous_anchor_job_issued_at_ms: int,
        anchor_job_issued_at_ms: int,
    ) -> list[AcceptedShareRecord]:
        """Return only shares that became anchor-eligible since a snapshot.

        The disjoint UNION branches let Postgres use the accepted-at and
        job-issued-at indexes independently. A share belongs to exactly one
        branch according to which timestamp crossed the previous anchor;
        rows already eligible at that anchor cannot reappear.
        """

        previous_anchor_ms = int(previous_anchor_job_issued_at_ms)
        anchor_ms = int(anchor_job_issued_at_ms)
        if anchor_ms < previous_anchor_ms:
            raise ValueError("snapshot anchor moved backwards")
        if anchor_ms == previous_anchor_ms:
            return []
        previous_anchor = (
            f"to_timestamp(({previous_anchor_ms}::double precision / 1000.0))"
        )
        anchor = f"to_timestamp(({anchor_ms}::double precision / 1000.0))"
        sql = f"""
WITH rows AS (
    SELECT ledger.*
    FROM qbit_share_ledger ledger
    WHERE ledger.accepted
      AND ledger.accepted_at > {previous_anchor}
      AND ledger.accepted_at <= {anchor}
      AND ledger.job_issued_at <= {anchor}
    UNION ALL
    SELECT ledger.*
    FROM qbit_share_ledger ledger
    WHERE ledger.accepted
      AND ledger.job_issued_at > {previous_anchor}
      AND ledger.job_issued_at <= {anchor}
      AND ledger.accepted_at <= {previous_anchor}
)
SELECT COALESCE(json_agg(json_build_object(
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'network_difficulty', network_difficulty::text,
    'template_height', template_height,
    'job_id', job_id,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'ntime', ntime,
    'credit_policy', credit_policy
) ORDER BY share_seq ASC), '[]'::json)
FROM rows;
"""
        return [
            self._record_from_json(item)
            for item in self._run_read_json(sql)
        ]

    def all_shares(self) -> list[AcceptedShareRecord]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'network_difficulty', network_difficulty::text,
    'template_height', template_height,
    'job_id', job_id,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'ntime', ntime,
    'credit_policy', credit_policy
) ORDER BY share_seq ASC), '[]'::json)
FROM qbit_share_ledger
WHERE accepted;
"""
        with self._operation_gate(self._lock, "writer lock"):
            return [
                self._record_from_json(item)
                for item in self._run_retry_safe_read_json(sql)
            ]

    def accepted_share_stats(self) -> dict[str, int]:
        """Aggregate counts without materializing the full share history.

        Health checks and readiness gates only need these two numbers, but
        they ask for them every few seconds (bundle readiness re-checks, the
        health refresher, metrics scrapes). Running the aggregate per call
        parallel-seq-scans the whole ledger each time, so the counts are kept
        incrementally instead: this process is the single lease-holding
        writer, every accepted share passes through ``append``/``append_batch``,
        and the counters are reconciled against the database once per
        ``accepted_stats_cache_seconds`` in case anything mutated rows out of
        band (e.g. reorg reversals flipping ``accepted``).

        Reconciliation never blocks a reader that already has counters: an
        expired read returns the maintained counters immediately and arms a
        single background refresh. The full-history aggregate takes minutes
        on a grown ledger, and parking the first expired caller handed that
        wait to whichever subsystem lost the race -- metrics scrapes,
        readiness gates, and block-candidate evidence all read these
        counters from latency-sensitive paths. Only the cold seed (no
        counters yet) still waits for the exact aggregate.
        """
        ttl = getattr(self, "_accepted_stats_cache_seconds", 0.0)
        stats_lock = getattr(self, "_stats_lock", None)
        if stats_lock is not None and ttl > 0:
            now = time.monotonic()
            with stats_lock:
                if (
                    self._stats_counts is not None
                    and self._stats_refreshed_monotonic is not None
                ):
                    if now - self._stats_refreshed_monotonic <= ttl:
                        return dict(self._stats_counts)
                    counters = dict(self._stats_counts)
                    self._start_background_stats_reconcile_locked()
                    return counters
        return self._refresh_accepted_share_stats()

    def _refresh_accepted_share_stats(self) -> dict[str, int]:
        # Single-flight: concurrent TTL expiries (health refresher plus a
        # metrics scrape) must not both run the aggregate and then race their
        # cache writes, or an older snapshot could overwrite a newer one along
        # with any increments noted in between. The second caller waits, sees
        # the fresh cache, and returns it without another query.
        refresh_lock = getattr(self, "_stats_refresh_lock", None)
        stats_lock = getattr(self, "_stats_lock", None)
        if refresh_lock is None or stats_lock is None:
            return self._query_accepted_share_stats()[0]
        with refresh_lock:
            ttl = getattr(self, "_accepted_stats_cache_seconds", 0.0)
            if ttl > 0:
                now = time.monotonic()
                with stats_lock:
                    if (
                        self._stats_counts is not None
                        and self._stats_refreshed_monotonic is not None
                        and now - self._stats_refreshed_monotonic <= ttl
                    ):
                        return dict(self._stats_counts)
            # Open the note buffer before taking the database snapshot. The
            # aggregate runs on the concurrent read pool, never under the
            # writer lock, and returns the highest share sequence visible in
            # the same MVCC snapshot as its scalar counts.
            note_buffer: dict[int, bool] = {}
            with stats_lock:
                self._stats_note_buffer = note_buffer
            try:
                counts, snapshot_max_share_seq = self._query_accepted_share_stats()
            except BaseException:
                with stats_lock:
                    if self._stats_note_buffer is note_buffer:
                        self._stats_note_buffer = None
                raise
            with stats_lock:
                for share_seq, new_miner in note_buffer.items():
                    if share_seq <= snapshot_max_share_seq:
                        continue
                    counts["accepted_share_count"] += 1
                    counts["distinct_miner_count"] += int(new_miner)
                self._stats_counts = dict(counts)
                self._stats_max_share_seq = max(
                    [snapshot_max_share_seq, *note_buffer.keys()]
                )
                self._stats_note_buffer = None
                self._stats_refreshed_monotonic = time.monotonic()
                return dict(self._stats_counts)

    def _start_background_stats_reconcile_locked(self) -> None:
        """Arm the reconcile aggregate unless one is already running.

        Callers hold ``_stats_lock``. The thread slot, not
        ``_stats_refresh_lock``, is the single-flight guard here: waiting on
        the refresh lock would queue the caller behind the running
        aggregate, which is exactly the wait the stale-serving read removes.
        """
        thread = getattr(self, "_stats_background_refresh_thread", None)
        if thread is not None and thread.is_alive():
            return
        thread = Thread(
            target=self._run_background_stats_reconcile,
            name="prism-share-ledger-stats-reconcile",
            daemon=True,
        )
        self._stats_background_refresh_thread = thread
        thread.start()

    def _run_background_stats_reconcile(self) -> None:
        try:
            self._refresh_accepted_share_stats()
        except BaseException:
            # Readers keep the last reconciled counters, which every append
            # still advances; the next expired read arms another attempt.
            # Failures no longer surface as caller errors, so they are
            # counted for accepted_stats_reconcile_status instead.
            stats_lock = getattr(self, "_stats_lock", None)
            if stats_lock is not None:
                with stats_lock:
                    self._stats_reconcile_failures = (
                        getattr(self, "_stats_reconcile_failures", 0) + 1
                    )
            traceback.print_exc()

    def accepted_stats_reconcile_status(self) -> dict[str, float | int | None]:
        """Expose reconcile liveness for metrics and alerting.

        Serving maintained counters means a failing aggregate is no longer
        visible as caller errors, and a wedged reconcile thread occupies the
        single-flight slot until the process restarts. Both conditions
        surface here: ``failures`` counts failed background passes, and
        ``age_seconds`` grows while no reconcile publishes -- including
        while one is wedged -- so alerting can page on either.
        """
        stats_lock = getattr(self, "_stats_lock", None)
        if stats_lock is None:
            return {"age_seconds": None, "failures": 0}
        with stats_lock:
            refreshed = self._stats_refreshed_monotonic
            failures = int(getattr(self, "_stats_reconcile_failures", 0))
        age = None if refreshed is None else max(0.0, time.monotonic() - refreshed)
        return {"age_seconds": age, "failures": failures}

    def _query_accepted_share_stats(self) -> tuple[dict[str, int], int]:
        sql = """
SELECT json_build_object(
    'accepted_share_count', count(*),
    'distinct_miner_count', count(DISTINCT miner_id),
    'max_share_seq', COALESCE(max(share_seq), 0)
)
FROM qbit_share_ledger
WHERE accepted;
"""
        payload = self._run_read_json(sql)
        counts = {
            "accepted_share_count": int(payload["accepted_share_count"]),
            "distinct_miner_count": int(payload["distinct_miner_count"]),
        }
        return counts, int(payload["max_share_seq"])

    def _note_appended_share(
        self,
        record: AcceptedShareRecord,
        *,
        new_miner: bool,
    ) -> None:
        """Advance the cached stats for a share this writer just committed.

        A refresh buffers notes while its aggregate runs, then replays only
        records newer than the aggregate's share-sequence watermark. The
        published watermark also suppresses a delayed note for a commit that
        was already visible in the snapshot. Idempotent ``append_batch``
        replays are not noted because the batch result identifies rows it
        actually inserted.
        """
        stats_lock = getattr(self, "_stats_lock", None)
        if stats_lock is None:
            return
        with stats_lock:
            note_buffer = getattr(self, "_stats_note_buffer", None)
            if note_buffer is not None:
                note_buffer[record.share_seq] = new_miner
            if self._stats_counts is None:
                return
            if record.share_seq <= self._stats_max_share_seq:
                return
            self._stats_counts["accepted_share_count"] += 1
            self._stats_counts["distinct_miner_count"] += int(new_miner)
            self._stats_max_share_seq = record.share_seq

    def current_owed_balances(self) -> list[dict[str, object]]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'balance_sats', owed_balance_sats::text
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_current_owed_balances()
WHERE owed_balance_sats > 0;
"""
        with self._operation_gate(self._lock, "writer lock"):
            balances = self._run_retry_safe_read_json(sql)
        for balance in balances:
            balance["balance_sats"] = int(balance["balance_sats"])
        return balances

    def dashboard_readiness_probe(self) -> bool:
        """Cheapest possible proof that Postgres is reachable through a read slot.

        The extracted public read tier's /healthz needs to answer "can I still
        reach the database" without running any aggregate: a health check that
        scans the share ledger turns a liveness probe into load, and the
        compose healthcheck runs it on an interval forever. This is a constant
        select -- it touches no PRISM table -- taken through the same bounded
        read slot as every other public read, so a saturated read pool shows up
        as an unhealthy service rather than as a probe that jumps the queue.
        """
        payload = self._run_read_json("SELECT json_build_object('ok', true);")
        return bool(isinstance(payload, dict) and payload.get("ok"))

    def dashboard_miner_owed_balance_bits(self, *, recipient_id: str) -> int:
        """One recipient's owed balance, read without the writer lock.

        The same total as summing current_owed_balances() for this recipient,
        but filtered in SQL and taken through the bounded read slot instead of
        the writer gate. current_owed_balances() returns every recipient's
        balance under the writer lock, so serving the most-polled public route
        (/public/v1/miners/{recipient_id}) from it made a dashboard poll
        serialize against the lease-holding writer. The predicates match that
        method exactly -- rows from qbit_current_owed_balances() with
        owed_balance_sats > 0, restricted to this miner_id -- so the integer
        returned here is the integer the Python sum produced.
        """
        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
SELECT json_build_object(
    'owed_balance_bits',
    COALESCE((
        SELECT sum(owed_balance_sats)
        FROM qbit_current_owed_balances()
        WHERE owed_balance_sats > 0
          AND miner_id = {self._text_literal(recipient_id)}
    ), 0)::text
);
"""
        payload = self._run_read_json(sql)
        return int(payload["owed_balance_bits"])

    def prior_balances_after_pool_block(
        self,
        *,
        block_hash: str,
    ) -> list[dict[str, object]]:
        """Return active carry balances as of one confirmed pool block."""
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        sql = f"""
WITH target AS (
    SELECT block_height
    FROM qbit_pool_blocks
    WHERE block_hash = {self._text_literal(block_hash)}
      AND chain_state = 'confirmed'
      AND maturity_state <> 'reversed'
),
balances AS (
    SELECT
        (array_agg(carry.miner_id ORDER BY carry.payout_order_key, carry.miner_id))[1] AS miner_id,
        (array_agg(carry.payout_order_key ORDER BY carry.payout_order_key, carry.miner_id))[1] AS payout_order_key,
        carry.p2mr_program,
        SUM(carry.gross_amount_sats::numeric - carry.onchain_amount_sats::numeric) AS balance_sats
    FROM qbit_payout_carry_forward carry
    JOIN qbit_pool_blocks block
      ON block.block_hash = carry.block_hash
    CROSS JOIN target
    WHERE carry.maturity_state <> 'reversed'
      AND block.chain_state = 'confirmed'
      AND block.maturity_state <> 'reversed'
      AND block.block_height <= target.block_height
    GROUP BY carry.p2mr_program
    HAVING SUM(carry.gross_amount_sats::numeric - carry.onchain_amount_sats::numeric) <> 0
)
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'balance_sats', balance_sats::text
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM balances;
"""
        balances = list(self._run_read_json(sql))
        for balance in balances:
            balance["balance_sats"] = int(balance["balance_sats"])
        return balances

    def current_prior_balances(self) -> list[dict[str, object]]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'balance_sats', balance_sats::text
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_current_carry_forward_balances();
"""
        started = time.monotonic()
        with self._operation_gate(self._lock, "writer lock"):
            balances = self._run_retry_safe_read_json(sql)
        self._note_prior_balances_read(max(0.0, time.monotonic() - started))
        for balance in balances:
            balance["balance_sats"] = int(balance["balance_sats"])
        return balances

    def _note_prior_balances_read(self, duration_seconds: float) -> None:
        with self._prior_balances_read_stats_lock:
            self._prior_balances_reads_total += 1
            self._prior_balances_read_last_seconds = duration_seconds
            self._prior_balances_read_max_seconds = max(
                self._prior_balances_read_max_seconds, duration_seconds
            )

    def prior_balances_read_stats(self) -> dict[str, float | int]:
        """Latency of the prior-balances read, the query whose silent growth
        past the submitter deadline caused the #188 landing livelock."""
        with self._prior_balances_read_stats_lock:
            return {
                "reads_total": self._prior_balances_reads_total,
                "last_seconds": self._prior_balances_read_last_seconds,
                "max_seconds": self._prior_balances_read_max_seconds,
            }

    def carry_forward_integrity_report(self) -> dict[str, object]:
        sql = "SELECT qbit_carry_forward_integrity_report();"
        with self._operation_gate(self._lock, "writer lock"):
            report = self._run_retry_safe_read_json(sql)
            # The audit head is a second statement under the same held lock:
            # the two reads must observe one another's rows, so the gate
            # cannot be released between them. Report the first statement's
            # completed round trip so the pair costs a monitor one budget of
            # silence at a time (see _note_operation_progress).
            self._note_operation_progress()
            audit_head = self._carry_forward_audit_head_locked()
        report["backend"] = "postgres-psql"
        report.update(audit_head)
        report["checked_active_rows"] = int(report["checked_active_rows"])
        report["mismatch_count"] = int(report["mismatch_count"])
        report["current_drift_count"] = int(report.get("current_drift_count", 0))
        for row in report.get("mismatches", []):
            for key in (
                "prior_balance_sats",
                "expected_prior_balance_sats",
                "candidate_balance_sats",
                "expected_candidate_balance_sats",
                "carry_forward_balance_sats",
                "expected_carry_forward_balance_sats",
            ):
                row[key] = int(row[key])
        return report

    def _carry_forward_audit_head_locked(self) -> dict[str, object]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'carry_forward_seq', carry_forward_seq,
    'block_hash', block_hash,
    'block_height', block_height,
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'gross_amount_sats', gross_amount_sats,
    'prior_balance_sats', prior_balance_sats::text,
    'candidate_balance_sats', candidate_balance_sats::text,
    'onchain_amount_sats', onchain_amount_sats,
    'settlement_fee_sats', settlement_fee_sats,
    'carry_forward_balance_sats', carry_forward_balance_sats::text,
    'action', action,
    'maturity_state', maturity_state
) ORDER BY block_height ASC, carry_forward_seq ASC), '[]'::json)
FROM (
    SELECT ledger.*
    FROM qbit_payout_carry_forward ledger
    JOIN qbit_pool_blocks block
      ON block.block_hash = ledger.block_hash
    WHERE ledger.maturity_state <> 'reversed'
      AND block.chain_state = 'confirmed'
      AND block.maturity_state <> 'reversed'
) active;
"""
        rows = self._run_retry_safe_read_json(sql)
        previous = bytes.fromhex("00" * 32)
        version = "qbit.prism.carry-forward-active-delta-chain.v1"
        for row in rows:
            row_json = json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
            previous = hashlib.sha256(previous + row_json).digest()
        return {
            "audit_chain_version": version,
            "audit_row_count": len(rows),
            "audit_head_sha256": previous.hex() if rows else "00" * 32,
        }

    def audit_share_window(
        self,
        *,
        anchor_job_issued_at_ms: int,
        network_difficulty: int,
    ) -> list[dict[str, object]]:
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'window_multiplier', window_multiplier::text,
    'requested_window_weight', requested_window_weight::text,
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'counted_difficulty', counted_difficulty::text,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'credit_policy', credit_policy
) ORDER BY share_seq DESC), '[]'::json)
FROM qbit_audit_share_window(
    to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0)),
    {int(network_difficulty)}::numeric
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            rows = self._run_retry_safe_read_json(sql)
        for row in rows:
            for key in (
                "window_multiplier",
                "requested_window_weight",
                "share_difficulty",
                "counted_difficulty",
            ):
                row[key] = int(row[key])
        return rows

    def audit_block_payouts(self, *, block_hash: str) -> list[dict[str, object]]:
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'block_height', block_height,
    'coinbase_txid', coinbase_txid,
    'payout_manifest_sha256', payout_manifest_sha256,
    'chain_state', chain_state,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'onchain_amount_sats', onchain_amount_sats,
    'carry_forward_balance_sats', carry_forward_balance_sats::text,
    'action', action,
    'maturity_state', maturity_state
) ORDER BY payout_order_key, miner_id, encode(p2mr_program, 'hex')), '[]'::json)
FROM qbit_audit_block_payouts({self._text_literal(block_hash)});
"""
        with self._operation_gate(self._lock, "writer lock"):
            rows = self._run_retry_safe_read_json(sql)
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return rows

    def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
        if not recipient_id:
            raise ValueError("recipient_id is required")
        limit = max(1, min(int(limit), 250))
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block.block_hash,
    'block_height', block.block_height,
    'coinbase_txid', block.coinbase_txid,
    'payout_manifest_sha256', block.payout_manifest_sha256,
    'recipient_id', payout.miner_id,
    'order_key', payout.payout_order_key,
    'p2mr_program_hex', encode(payout.p2mr_program, 'hex'),
    'onchain_amount_sats', payout.onchain_amount_sats,
    'carry_forward_balance_sats', payout.carry_forward_balance_sats::text,
    'action', payout.action,
    'maturity_state', payout.maturity_state,
    'created_at', payout.created_at::text
) ORDER BY payout.block_height DESC, payout.payout_entry_seq DESC), '[]'::json)
FROM (
    SELECT *
    FROM qbit_pool_payout_entries
    WHERE miner_id = {self._text_literal(recipient_id)}
    ORDER BY block_height DESC, payout_entry_seq DESC
    LIMIT {limit}
) payout
JOIN qbit_pool_blocks block
  ON block.block_hash = payout.block_hash;
"""
        with self._operation_gate(self._lock, "writer lock"):
            rows = self._run_retry_safe_read_json(sql)
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return rows

    def dashboard_miner_lifetime_earnings_bits(self, *, recipient_id: str) -> int:
        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
SELECT json_build_object(
    'lifetime_earnings_bits',
    COALESCE((
        SELECT sum(carry.gross_amount_sats)
        FROM qbit_payout_carry_forward carry
        JOIN qbit_pool_blocks block
          ON block.block_hash = carry.block_hash
        WHERE carry.miner_id = {self._text_literal(recipient_id)}
          AND carry.maturity_state <> 'reversed'
          AND block.chain_state <> 'reversed'
          AND block.maturity_state <> 'reversed'
    ), 0)
);
"""
        payload = self._run_read_json(sql)
        return int(payload["lifetime_earnings_bits"])

    def dashboard_miner_pending_maturity_bits(self, *, recipient_id: str) -> int:
        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
SELECT json_build_object(
    'pending_maturity_bits',
    COALESCE(sum(GREATEST(carry.onchain_amount_sats - carry.settlement_fee_sats, 0)), 0)
)
FROM qbit_payout_carry_forward carry
JOIN qbit_pool_blocks block
  ON block.block_hash = carry.block_hash
WHERE carry.miner_id = {self._text_literal(recipient_id)}
  AND carry.action = 'onchain'
  AND carry.maturity_state = 'immature'
  AND block.chain_state = 'confirmed'
  AND block.maturity_state = 'immature';
"""
        payload = self._run_read_json(sql)
        return int(payload["pending_maturity_bits"])

    def dashboard_miner_share_summary(self, *, recipient_id: str) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS now_at
),
pool AS (
    SELECT COALESCE(sum(share_difficulty), 0)::text AS h3_difficulty
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND accepted_at >= bounds.now_at - interval '3 hours'
      AND accepted_at <= bounds.now_at
),
miner_rollups AS (
    SELECT
        count(*) FILTER (WHERE accepted_at >= bounds.now_at - interval '3 hours') AS accepted_3h,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '1 minute'), 0)::text AS m1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '5 minutes'), 0)::text AS m5_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '10 minutes'), 0)::text AS m10_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.now_at - interval '3 hours'), 0)::text AS h3_difficulty,
        COALESCE(sum(share_difficulty), 0)::text AS h24_difficulty
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND miner_id = {self._text_literal(recipient_id)}
      AND accepted_at >= bounds.now_at - interval '24 hours'
      AND accepted_at <= bounds.now_at
),
miner_last AS (
    SELECT
        to_char(max(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_share_at
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND miner_id = {self._text_literal(recipient_id)}
      AND accepted_at <= bounds.now_at
)
SELECT json_build_object(
    'accepted_3h', (SELECT accepted_3h FROM miner_rollups),
    'm1_difficulty', (SELECT m1_difficulty FROM miner_rollups),
    'm5_difficulty', (SELECT m5_difficulty FROM miner_rollups),
    'm10_difficulty', (SELECT m10_difficulty FROM miner_rollups),
    'h3_difficulty', (SELECT h3_difficulty FROM miner_rollups),
    'h24_difficulty', (SELECT h24_difficulty FROM miner_rollups),
    'pool_h3_difficulty', (SELECT h3_difficulty FROM pool),
    'last_share_at', (SELECT last_share_at FROM miner_last)
);
"""
        payload = self._run_read_json(sql)
        miner_3h_difficulty = str(payload.get("h3_difficulty") or "0")
        pool_3h_difficulty = str(payload.get("pool_h3_difficulty") or "0")
        m1_difficulty = str(payload.get("m1_difficulty") or "0")
        m5_difficulty = str(payload.get("m5_difficulty") or "0")
        m10_difficulty = str(payload.get("m10_difficulty") or "0")
        h24_difficulty = str(payload.get("h24_difficulty") or "0")
        return {
            "hashrate_ths": {
                "m1": public_api.hashrate_ths_from_difficulty(m1_difficulty, 60),
                "m5": public_api.hashrate_ths_from_difficulty(m5_difficulty, 5 * 60),
                "m10": public_api.hashrate_ths_from_difficulty(m10_difficulty, 10 * 60),
                "h3": public_api.hashrate_ths_from_difficulty(miner_3h_difficulty, 3 * 60 * 60),
                "h24": public_api.hashrate_ths_from_difficulty(h24_difficulty, 24 * 60 * 60),
            },
            "accepted_3h": int(payload.get("accepted_3h") or 0),
            "accepted_difficulty_3h": miner_3h_difficulty,
            "last_share_at": payload.get("last_share_at"),
            "share_percent": public_api.percent_string(miner_3h_difficulty, pool_3h_difficulty),
        }

    def _pool_reward_window_aggregate(self, window_weight: Decimal) -> dict[str, Any]:
        """Pool-wide PRISM reward-window totals grouped by miner, briefly cached.

        Every public miner page needs the same pool-wide recursive
        qbit_prism_window scan and only differs in which miner's slice it
        reads, so the aggregate is computed once and every miner-page request
        within the cache window is served from the shared copy. Concurrent
        misses wait for the in-flight computation instead of re-running it.
        The cache is keyed by the requested window weight, so a
        network-difficulty change recomputes immediately; a TTL of 0 disables
        caching.
        """
        from lab.prism import public_api

        window_weight_sql = public_api.decimal_string(window_weight)
        cache_seconds = self._reward_window_cache_seconds
        with self._reward_window_cache_lock:
            cached = self._reward_window_cache
            if (
                cache_seconds > 0
                and cached is not None
                and cached[0] == window_weight_sql
                and time.monotonic() - cached[1] < cache_seconds
            ):
                return cached[2]
            sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
),
window_rows AS (
    SELECT window_row.*
    FROM bounds
    CROSS JOIN LATERAL qbit_prism_window(bounds.ended_at, {window_weight_sql}::numeric) AS window_row
)
SELECT json_build_object(
    'pool_counted_difficulty', COALESCE((SELECT sum(counted_difficulty) FROM window_rows), 0)::text,
    'miner_counted_difficulty', COALESCE((
        SELECT json_object_agg(miner_id, counted_difficulty)
        FROM (
            SELECT miner_id, sum(counted_difficulty)::text AS counted_difficulty
            FROM window_rows
            GROUP BY miner_id
        ) grouped
    ), '{{}}'::json)
);
"""
            payload = self._run_read_json(sql)
            self._reward_window_cache = (window_weight_sql, time.monotonic(), payload)
            return payload

    def dashboard_miner_reward_window(
        self,
        *,
        recipient_id: str,
        current_network_difficulty: int | str | Decimal,
    ) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        window_weight = _reward_window_weight(current_network_difficulty)
        aggregate = self._pool_reward_window_aggregate(window_weight)
        pool_difficulty = Decimal(str(aggregate["pool_counted_difficulty"]))
        by_miner = aggregate["miner_counted_difficulty"]
        miner_difficulty = Decimal(str(by_miner.get(recipient_id, "0")))
        share_percent = None
        if pool_difficulty > 0:
            share_percent = public_api.decimal_string(miner_difficulty * Decimal(100) / pool_difficulty)
        return {
            "accepted_difficulty": public_api.decimal_string(miner_difficulty),
            "pool_accepted_difficulty": public_api.decimal_string(pool_difficulty),
            "share_percent": share_percent,
        }

    def dashboard_miner_payout_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        offset = (page - 1) * limit
        sql = f"""
WITH filtered AS (
    SELECT
        payout.payout_entry_seq,
        payout.block_hash,
        payout.block_height,
        payout.miner_id,
        payout.payout_order_key,
        payout.p2mr_program,
        payout.onchain_amount_sats,
        payout.carry_forward_balance_sats,
        payout.action,
        payout.maturity_state,
        payout.created_at,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_pool_payout_entries payout
    JOIN qbit_pool_blocks block
      ON block.block_hash = payout.block_hash
    WHERE payout.miner_id = {self._text_literal(recipient_id)}
      AND payout.maturity_state <> 'reversed'
      AND block.chain_state <> 'reversed'
      AND block.maturity_state <> 'reversed'
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY block_height DESC, payout_entry_seq DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
),
resolved AS (
    SELECT
        page_rows.*,
        fanout.fanout_txid,
        fanout.fanout_vout,
        fanout.fanout_amount_sats,
        fanout.fanout_fee_sats,
        fanout.fanout_gross_amount_sats,
        fanout.fanout_status
    FROM page_rows
    LEFT JOIN LATERAL (
        SELECT
            artifact.fanout_txid,
            artifact.settlement_status AS fanout_status,
            (output.value->>'vout')::integer AS fanout_vout,
            (output.value->>'amount_sats')::bigint AS fanout_amount_sats,
            COALESCE((output.value->>'fee_sats')::bigint, 0) AS fanout_fee_sats,
            COALESCE(
                (output.value->>'gross_amount_sats')::bigint,
                (output.value->>'amount_sats')::bigint
            ) AS fanout_gross_amount_sats
        FROM qbit_ctv_fanout_artifacts artifact
        CROSS JOIN LATERAL jsonb_array_elements(artifact.manifest->'precommitment'->'outputs') AS output(value)
        WHERE artifact.block_hash = page_rows.block_hash
          AND page_rows.action = 'onchain'
          AND output.value->>'recipient_id' = page_rows.miner_id
          AND output.value->>'order_key' = page_rows.payout_order_key
          AND output.value->>'p2mr_program_hex' = encode(page_rows.p2mr_program, 'hex')
        ORDER BY artifact.chunk_index ASC, (output.value->>'vout')::integer ASC
        LIMIT 1
    ) fanout ON TRUE
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'recipient_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'onchain_amount_sats', onchain_amount_sats,
            'carry_forward_balance_sats', carry_forward_balance_sats::text,
            'action', action,
            'maturity_state', maturity_state,
            'created_at', created_at::text,
            'fanout_txid', fanout_txid,
            'fanout_vout', fanout_vout,
            'fanout_amount_sats', fanout_amount_sats,
            'fanout_fee_sats', fanout_fee_sats,
            'fanout_gross_amount_sats', fanout_gross_amount_sats,
            'fanout_status', fanout_status
        ) ORDER BY block_height DESC, payout_entry_seq DESC)
        FROM resolved
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = payload["rows"]
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": [public_api.miner_payout_row(row) for row in rows],
        }

    def dashboard_miner_earning_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        offset = (page - 1) * limit
        sql = f"""
WITH filtered AS (
    SELECT
        carry.carry_forward_seq,
        carry.block_hash,
        carry.block_height,
        carry.miner_id,
        carry.payout_order_key,
        carry.p2mr_program,
        carry.gross_amount_sats,
        carry.onchain_amount_sats,
        carry.settlement_fee_sats,
        carry.carry_forward_balance_sats,
        carry.action,
        carry.maturity_state,
        carry.created_at,
        block.found_at,
        block.coinbase_txid,
        block.payout_manifest_sha256
    FROM qbit_payout_carry_forward carry
    JOIN qbit_pool_blocks block
      ON block.block_hash = carry.block_hash
    WHERE carry.miner_id = {self._text_literal(recipient_id)}
      AND carry.maturity_state <> 'reversed'
      AND block.chain_state <> 'reversed'
      AND block.maturity_state <> 'reversed'
),
page_base AS (
    SELECT *
    FROM filtered
    ORDER BY block_height DESC, carry_forward_seq DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
),
block_totals AS (
    SELECT block_hash, sum(gross_amount_sats) AS block_gross_amount_sats
    FROM qbit_payout_carry_forward
    WHERE block_hash IN (SELECT block_hash FROM page_base)
    GROUP BY block_hash
),
page_rows AS (
    SELECT
        page_base.*,
        block_totals.block_gross_amount_sats,
        CASE
            WHEN block_totals.block_gross_amount_sats > 0 THEN
                (page_base.gross_amount_sats::numeric * 100::numeric / block_totals.block_gross_amount_sats::numeric)::text
            ELSE '0'
        END AS reward_share_percent
    FROM page_base
    JOIN block_totals
      ON block_totals.block_hash = page_base.block_hash
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'recipient_id', miner_id,
            'order_key', payout_order_key,
            'p2mr_program_hex', encode(p2mr_program, 'hex'),
            'gross_amount_sats', gross_amount_sats,
            'onchain_amount_sats', onchain_amount_sats,
            'settlement_fee_sats', settlement_fee_sats,
            'carry_forward_balance_sats', carry_forward_balance_sats::text,
            'action', action,
            'maturity_state', maturity_state,
            'created_at', created_at::text,
            'found_at', found_at::text,
            'block_gross_amount_sats', block_gross_amount_sats,
            'reward_share_percent', reward_share_percent
        ) ORDER BY block_height DESC, carry_forward_seq DESC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = payload["rows"]
        for row in rows:
            row["carry_forward_balance_sats"] = int(row["carry_forward_balance_sats"])
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": [public_api.miner_earning_row(row) for row in rows],
        }

    def dashboard_miner_worker_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
        search: str | None,
        hide_inactive: bool,
    ) -> dict[str, object]:
        from lab.prism import public_api

        if not recipient_id:
            raise ValueError("recipient_id is required")
        offset = (page - 1) * limit
        filters = ["true"]
        if search:
            filters.append(f"strpos(lower(worker_name), {self._text_literal(search.lower())}) > 0")
        if hide_inactive:
            filters.append("active")
        where_filter = " AND ".join(filters)
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS now_at
),
named AS (
    SELECT
        CASE
            WHEN username = {self._text_literal(recipient_id)} THEN 'default'
            WHEN left(username, {len(recipient_id) + 1}) = {self._text_literal(recipient_id + ".")} THEN COALESCE(NULLIF(substr(username, {len(recipient_id) + 2}), ''), 'default')
            WHEN position('.' IN username) > 0 THEN COALESCE(NULLIF(substring(username FROM position('.' IN username) + 1), ''), 'default')
            ELSE 'default'
        END AS worker_name,
        share_difficulty,
        accepted_at
    FROM (
        SELECT
            regexp_replace(share_id, ':[^:]*$', '') AS username,
            share_difficulty,
            accepted_at
        FROM qbit_share_ledger
        WHERE accepted
          AND miner_id = {self._text_literal(recipient_id)}
          -- 3 hours is the largest rollup window this endpoint reports
          -- (h3_difficulty), so the scan never needs the miner's full history.
          AND accepted_at >= (SELECT now_at FROM bounds) - interval '3 hours'
    ) shares
),
grouped AS (
    SELECT
        worker_name,
        max(accepted_at) AS last_share_at,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= (SELECT now_at FROM bounds) - interval '1 minute'), 0)::text AS m1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= (SELECT now_at FROM bounds) - interval '3 hours'), 0)::text AS h3_difficulty,
        max(accepted_at) >= (SELECT now_at FROM bounds) - interval '10 minutes' AS active
    FROM named
    GROUP BY worker_name
),
filtered AS (
    SELECT *
    FROM grouped
    WHERE {where_filter}
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY active DESC, worker_name ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'active_count', (SELECT count(*) FROM grouped WHERE active),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'worker_name', worker_name,
            'status', CASE WHEN active THEN 'active' ELSE 'inactive' END,
            'last_share_at', to_char(last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'm1_difficulty', m1_difficulty,
            'h3_difficulty', h3_difficulty
        ) ORDER BY active DESC, worker_name ASC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = [
            {
                "worker_name": row["worker_name"],
                "status": row["status"],
                "last_share_at": row["last_share_at"],
                "hashrate_ths_60s": public_api.hashrate_ths_from_difficulty(row["m1_difficulty"], 60),
                "hashrate_ths_3h": public_api.hashrate_ths_from_difficulty(row["h3_difficulty"], 3 * 60 * 60),
            }
            for row in payload["rows"]
        ]
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": rows,
            "active_count": int(payload["active_count"]),
        }

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        sql = f"""
SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', bundle.block_hash,
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'coinbase_tx_hex', bundle.coinbase_tx_hex,
            'audit_bundle', bundle.audit_bundle,
            'body_uri', bundle.body_uri
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block
          ON block.block_hash = bundle.block_hash
        WHERE bundle.block_hash = {self._text_literal(block_hash)}
    ),
    'null'::json
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            row = self._run_retry_safe_read_json(sql)
        return self._resolve_audit_bundle_row(row)

    def dashboard_direct_coinbase_settlement(
        self,
        *,
        block_hash: str,
    ) -> dict[str, object] | None:
        """Public settlement facts for a direct-coinbase block, without fencing.

        /public/v1/blocks/{block_hash}/settlement-artifacts serves CTV blocks
        from audit_ctv_fanout_manifest_set() (a read slot), but a
        direct-coinbase block fell through to audit_bundle(), which reads the
        same row under the writer lock. This is that read taken through the
        bounded read slot instead, returning exactly the dict
        public_api.direct_coinbase_settlement_payload() builds today.

        The row is resolved through _resolve_audit_bundle_row, so an
        externalized body is read from body_uri on disk and sha256-verified
        against audit_bundle_sha256 by the same helper the fenced path uses.
        A body that is unretrievable, hash-mismatched, or not valid JSON is
        reported as None rather than raised, matching what
        public_api.audit_bundle_body_read_failed() already tolerates.

        Returns None when the block has no audit bundle, or when the bundle
        settled in any mode other than direct_coinbase.
        """
        from lab.prism import public_api

        sql = f"""
SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', bundle.block_hash,
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'audit_bundle', bundle.audit_bundle,
            'body_uri', bundle.body_uri
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block
          ON block.block_hash = bundle.block_hash
        WHERE bundle.block_hash = {self._text_literal(block_hash)}
    ),
    'null'::json
);
"""
        row = self._run_read_json(sql)
        try:
            resolved = self._resolve_audit_bundle_row(row)
        except RuntimeError as exc:
            if public_api.audit_bundle_body_read_failed(exc):
                return None
            raise
        if not isinstance(resolved, dict):
            return None
        bundle = resolved.get("audit_bundle")
        if not isinstance(bundle, dict):
            return None
        if public_api.audit_bundle_settlement_mode(bundle) != "direct_coinbase":
            return None
        return {
            "block_hash": public_api.optional_hex_hash(resolved.get("block_hash"))
            or block_hash,
            "block_height": public_api.first_int(
                resolved.get("block_height"),
                public_api.audit_bundle_section_value(
                    bundle, "found_block", "block_height"
                ),
                public_api.audit_bundle_section_value(
                    bundle, "reward_manifest", "block_height"
                ),
                public_api.audit_bundle_section_value(
                    bundle, "ledger_window_attestation", "block_height"
                ),
                default=0,
            ),
            "settlement_mode": "direct_coinbase",
            "audit_bundle_sha256": public_api.nullable_str(
                resolved.get("audit_bundle_sha256")
            ),
            "payout_manifest_sha256": public_api.nullable_str(
                resolved.get("payout_manifest_sha256")
            ),
            "artifacts": [],
        }

    def audit_bundle_by_commitment(self, *, commitment_leaf_hex: str) -> dict[str, object] | None:
        leaf = self._text_literal(commitment_leaf_hex)
        sql = f"""
SELECT COALESCE(
    (
        SELECT json_build_object(
            'block_hash', bundle.block_hash,
            'block_height', block.block_height,
            'payout_manifest_sha256', block.payout_manifest_sha256,
            'audit_commitment_leaf_hex', {leaf},
            'audit_bundle_sha256', bundle.audit_bundle_sha256,
            'coinbase_tx_hex', bundle.coinbase_tx_hex,
            'audit_bundle', bundle.audit_bundle,
            'body_uri', bundle.body_uri
        )
        FROM qbit_pool_audit_bundles bundle
        JOIN qbit_pool_blocks block ON block.block_hash = bundle.block_hash
        WHERE bundle.audit_commitment_leaves_hex ? {leaf}
           OR bundle.witness_merkle_leaves_hex ? {leaf}
           OR bundle.audit_bundle->'audit_commitment_leaves_hex' ? {leaf}
           OR bundle.audit_bundle->'witness_merkle_leaves_hex' ? {leaf}
        ORDER BY block.block_height DESC, bundle.created_at DESC, bundle.block_hash
        LIMIT 1
    ),
    'null'::json
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            row = self._run_retry_safe_read_json(sql)
        return self._resolve_audit_bundle_row(row)

    def persist_ctv_fanout_manifest_set(
        self,
        *,
        block_hash: str,
        manifest_set: dict[str, Any],
        manifest_set_sha256: str,
    ) -> dict[str, int | str]:
        payload = {
            **ctv_fanout_recovery_payload(
                block_hash=block_hash,
                manifest_set=manifest_set,
                manifest_set_sha256=manifest_set_sha256,
            ),
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
block_row AS (
    SELECT block_hash
    FROM qbit_pool_blocks
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
existing_set AS (
    SELECT *
    FROM qbit_ctv_fanout_sets
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
inserted_set AS (
    INSERT INTO qbit_ctv_fanout_sets (
        block_hash,
        manifest_set_json,
        manifest_set,
        manifest_set_sha256,
        settlement_mode,
        parent_coinbase_txid,
        parent_coinbase_tx_hex,
        fanout_count,
        fanout_output_sum_sats,
        covenant_output_value_sats
    )
    SELECT
        data->>'block_hash',
        data->>'manifest_set_json',
        data->'manifest_set',
        data->>'manifest_set_sha256',
        data->>'settlement_mode',
        data->>'parent_coinbase_txid',
        data->>'parent_coinbase_tx_hex',
        (data->>'fanout_count')::integer,
        (data->>'fanout_output_sum_sats')::bigint,
        (data->>'covenant_output_value_sats')::bigint
    FROM payload, lease, block_row
    WHERE NOT EXISTS (SELECT 1 FROM existing_set)
    RETURNING block_hash
),
artifacts AS (
    SELECT data, artifact
    FROM payload,
         jsonb_array_elements(data->'artifacts') AS artifact
),
matching_existing_set AS (
    SELECT existing_set.block_hash
    FROM existing_set, payload
    WHERE existing_set.manifest_set = data->'manifest_set'
      AND existing_set.manifest_set_json = data->>'manifest_set_json'
      AND existing_set.manifest_set_sha256 = data->>'manifest_set_sha256'
      AND existing_set.settlement_mode = data->>'settlement_mode'
      AND existing_set.parent_coinbase_txid = data->>'parent_coinbase_txid'
      AND existing_set.parent_coinbase_tx_hex = data->>'parent_coinbase_tx_hex'
      AND existing_set.fanout_count = (data->>'fanout_count')::integer
      AND existing_set.fanout_output_sum_sats = (data->>'fanout_output_sum_sats')::bigint
      AND existing_set.covenant_output_value_sats = (data->>'covenant_output_value_sats')::bigint
),
expected_artifact_rows AS (
    SELECT
        artifact->>'fanout_txid' AS fanout_txid,
        data->>'block_hash' AS block_hash,
        data->>'manifest_set_sha256' AS manifest_set_sha256,
        artifact->>'manifest_json' AS manifest_json,
        artifact->'manifest' AS manifest,
        artifact->>'manifest_sha256' AS manifest_sha256,
        artifact->>'precommitment_sha256' AS precommitment_sha256,
        artifact->>'ctv_hash' AS ctv_hash,
        artifact->>'commitment_witness_leaf_hex' AS commitment_witness_leaf_hex,
        (artifact->>'chunk_index')::integer AS chunk_index,
        (artifact->>'chunk_count')::integer AS chunk_count,
        artifact->>'parent_coinbase_txid' AS parent_coinbase_txid,
        (artifact->>'parent_coinbase_vout')::integer AS parent_coinbase_vout,
        artifact->>'fanout_tx_template_hex' AS fanout_tx_template_hex,
        artifact->>'fanout_tx_hex' AS fanout_tx_hex,
        (artifact->>'anchor_vout')::integer AS anchor_vout,
        (artifact->>'covenant_output_value_sats')::bigint AS covenant_output_value_sats,
        (artifact->>'fanout_output_sum_sats')::bigint AS fanout_output_sum_sats
    FROM artifacts
),
existing_artifact_rows AS (
    SELECT
        fanout_txid,
        block_hash,
        manifest_set_sha256,
        manifest_json,
        manifest,
        manifest_sha256,
        precommitment_sha256,
        ctv_hash,
        commitment_witness_leaf_hex,
        chunk_index,
        chunk_count,
        parent_coinbase_txid,
        parent_coinbase_vout,
        fanout_tx_template_hex,
        fanout_tx_hex,
        anchor_vout,
        covenant_output_value_sats,
        fanout_output_sum_sats
    FROM qbit_ctv_fanout_artifacts
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
artifact_extra AS (
    SELECT * FROM existing_artifact_rows
    EXCEPT ALL
    SELECT * FROM expected_artifact_rows
),
inserted_artifacts AS (
    INSERT INTO qbit_ctv_fanout_artifacts (
        fanout_txid,
        block_hash,
        manifest_set_sha256,
        manifest_json,
        manifest,
        manifest_sha256,
        precommitment_sha256,
        ctv_hash,
        commitment_witness_leaf_hex,
        chunk_index,
        chunk_count,
        parent_coinbase_txid,
        parent_coinbase_vout,
        fanout_tx_template_hex,
        fanout_tx_hex,
        anchor_vout,
        covenant_output_value_sats,
        fanout_output_sum_sats
    )
    SELECT
        expected.fanout_txid,
        expected.block_hash,
        expected.manifest_set_sha256,
        expected.manifest_json,
        expected.manifest,
        expected.manifest_sha256,
        expected.precommitment_sha256,
        expected.ctv_hash,
        expected.commitment_witness_leaf_hex,
        expected.chunk_index,
        expected.chunk_count,
        expected.parent_coinbase_txid,
        expected.parent_coinbase_vout,
        expected.fanout_tx_template_hex,
        expected.fanout_tx_hex,
        expected.anchor_vout,
        expected.covenant_output_value_sats,
        expected.fanout_output_sum_sats
    FROM expected_artifact_rows expected
    WHERE EXISTS (SELECT 1 FROM lease)
      AND (
          EXISTS (SELECT 1 FROM inserted_set)
          OR (
              EXISTS (SELECT 1 FROM matching_existing_set)
              AND NOT EXISTS (SELECT 1 FROM artifact_extra)
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM existing_artifact_rows existing
          WHERE existing.fanout_txid = expected.fanout_txid
      )
    RETURNING fanout_txid
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM block_row) = 0 THEN
        json_build_object('error', 'unknown PRISM block')
    WHEN (SELECT count(*) FROM existing_set) > 0
      AND (SELECT count(*) FROM matching_existing_set) = 0 THEN
        json_build_object('error', 'existing CTV fanout manifest set does not match payload')
    WHEN (SELECT count(*) FROM existing_set) > 0
      AND EXISTS (SELECT 1 FROM artifact_extra) THEN
        json_build_object('error', 'existing CTV fanout artifacts do not match payload')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'fanout_set_count', CASE
                WHEN (SELECT count(*) FROM inserted_set) > 0 THEN (SELECT count(*) FROM inserted_set)
                ELSE (SELECT count(*) FROM existing_set)
            END,
            'fanout_artifact_count',
                (SELECT count(*) FROM existing_artifact_rows)
                + (SELECT count(*) FROM inserted_artifacts)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "fanout_set_count": int(result["fanout_set_count"]),
            "fanout_artifact_count": int(result["fanout_artifact_count"]),
        }

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        sql = f"SELECT qbit_audit_block_fanouts({self._text_literal(block_hash)});"
        return self._run_read_json(sql)

    def audit_ctv_fanouts(self, *, block_hash: str) -> list[dict[str, object]]:
        payload = self.audit_ctv_fanout_manifest_set(block_hash=block_hash)
        if payload is None:
            return []
        artifacts = payload.get("artifacts", [])
        if not isinstance(artifacts, list):
            return []
        return artifacts

    def ctv_fanout_status(self, *, fanout_txid: str) -> dict[str, object] | None:
        sql = f"SELECT qbit_fanout_status({self._text_literal(fanout_txid)});"
        return self._run_read_json(sql)

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        limit = max(1, min(int(limit), 1_000))
        sql = f"""
SELECT COALESCE(json_agg(row_payload ORDER BY block_height ASC, chunk_index ASC), '[]'::json)
FROM (
    SELECT json_build_object(
        'schema', 'qbit.prism.ctv-fanout-status.v1',
        'fanout_txid', artifact.fanout_txid,
        'block_hash', artifact.block_hash,
        'block_height', block.block_height,
        'parent_hash', block.parent_hash,
        'chain_state', block.chain_state,
        'maturity_state', block.maturity_state,
        'coinbase_txid', block.coinbase_txid,
        'payout_manifest_sha256', block.payout_manifest_sha256,
        'audit_bundle_sha256', bundle.audit_bundle_sha256,
        'manifest_set_sha256', artifact.manifest_set_sha256,
        'manifest_sha256', artifact.manifest_sha256,
        'precommitment_sha256', artifact.precommitment_sha256,
        'ctv_hash', artifact.ctv_hash,
        'commitment_witness_leaf_hex', artifact.commitment_witness_leaf_hex,
        'chunk_index', artifact.chunk_index,
        'chunk_count', artifact.chunk_count,
        'parent_coinbase_txid', artifact.parent_coinbase_txid,
        'parent_coinbase_vout', artifact.parent_coinbase_vout,
        'fanout_tx_hex', artifact.fanout_tx_hex,
        'anchor_vout', artifact.anchor_vout,
        'covenant_output_value_sats', artifact.covenant_output_value_sats,
        'fanout_output_sum_sats', artifact.fanout_output_sum_sats,
        'settlement_status', artifact.settlement_status,
        'updated_at', artifact.updated_at::text,
        'broadcast_attempt_count', artifact.broadcast_attempt_count,
        'broadcast_attempt_detail_count', artifact.broadcast_attempt_detail_count,
        'first_broadcast_attempt_at', artifact.first_broadcast_attempt_at::text,
        'last_broadcast_attempt_at', artifact.last_broadcast_attempt_at::text,
        'last_broadcast_attempt_status', artifact.last_broadcast_attempt_status,
        'last_broadcast_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
        'last_broadcast_package_txids', artifact.last_broadcast_package_txids,
        'last_broadcast_submit_result', artifact.last_broadcast_submit_result,
        'last_broadcast_error', artifact.last_broadcast_error,
        'broadcast_attempt_status_counts', artifact.broadcast_attempt_status_counts,
        'next_broadcast_attempt_at', artifact.next_broadcast_attempt_at::text,
        'broadcast_retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds,
        'broadcast_attempt_summary', json_build_object(
            'attempt_count', artifact.broadcast_attempt_count,
            'detail_count', artifact.broadcast_attempt_detail_count,
            'first_attempt_at', artifact.first_broadcast_attempt_at::text,
            'last_attempt_at', artifact.last_broadcast_attempt_at::text,
            'last_attempt_status', artifact.last_broadcast_attempt_status,
            'last_package_tx_hexes', artifact.last_broadcast_package_tx_hexes,
            'last_package_txids', artifact.last_broadcast_package_txids,
            'last_submit_result', artifact.last_broadcast_submit_result,
            'last_error', artifact.last_broadcast_error,
            'status_counts', artifact.broadcast_attempt_status_counts,
            'next_attempt_at', artifact.next_broadcast_attempt_at::text,
            'retry_backoff_seconds', artifact.broadcast_retry_backoff_seconds
        ),
        'broadcast_attempts', COALESCE(
            (
                SELECT json_agg(json_build_object(
                    'attempt_seq', attempt.attempt_seq,
                    'attempted_at', attempt.attempted_at::text,
                    'attempt_status', attempt.attempt_status,
                    'package_tx_hexes', attempt.package_tx_hexes,
                    'package_txids', attempt.package_txids,
                    'submit_result', attempt.submit_result,
                    'error', attempt.error
                ) ORDER BY attempt.attempt_seq ASC)
                FROM qbit_ctv_fanout_broadcast_attempts attempt
                WHERE attempt.fanout_txid = artifact.fanout_txid
            ),
            '[]'::json
        )
    ) AS row_payload,
    block.block_height,
    artifact.chunk_index
    FROM qbit_ctv_fanout_artifacts artifact
    JOIN qbit_pool_blocks block
      ON block.block_hash = artifact.block_hash
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = artifact.block_hash
    WHERE artifact.settlement_status NOT IN ('confirmed', 'reorged', 'failed')
      AND (
          artifact.next_broadcast_attempt_at IS NULL
          OR artifact.next_broadcast_attempt_at <= clock_timestamp()
      )
    ORDER BY block.block_height ASC, artifact.chunk_index ASC
    LIMIT {limit}
) pending;
"""
        return self._run_read_json(sql)

    def dashboard_pending_fanout_rows(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        offset = (page - 1) * limit
        sql = f"""
WITH filtered AS (
    SELECT
        artifact.fanout_txid,
        artifact.block_hash,
        block.block_height,
        block.parent_hash,
        block.chain_state,
        block.maturity_state,
        block.coinbase_txid,
        block.payout_manifest_sha256,
        bundle.audit_bundle_sha256,
        artifact.manifest_set_sha256,
        artifact.manifest_sha256,
        artifact.precommitment_sha256,
        artifact.ctv_hash,
        artifact.commitment_witness_leaf_hex,
        artifact.chunk_index,
        artifact.chunk_count,
        artifact.parent_coinbase_txid,
        artifact.parent_coinbase_vout,
        artifact.fanout_tx_hex,
        artifact.anchor_vout,
        artifact.covenant_output_value_sats,
        artifact.fanout_output_sum_sats,
        artifact.settlement_status,
        artifact.updated_at,
        artifact.broadcast_attempt_count,
        artifact.broadcast_attempt_detail_count,
        artifact.first_broadcast_attempt_at,
        artifact.last_broadcast_attempt_at,
        artifact.last_broadcast_attempt_status,
        artifact.last_broadcast_package_tx_hexes,
        artifact.last_broadcast_package_txids,
        artifact.last_broadcast_submit_result,
        artifact.last_broadcast_error,
        artifact.broadcast_attempt_status_counts,
        artifact.next_broadcast_attempt_at,
        artifact.broadcast_retry_backoff_seconds
    FROM qbit_ctv_fanout_artifacts artifact
    JOIN qbit_pool_blocks block
      ON block.block_hash = artifact.block_hash
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = artifact.block_hash
    WHERE artifact.settlement_status NOT IN ('confirmed', 'reorged', 'failed')
      AND (
          artifact.next_broadcast_attempt_at IS NULL
          OR artifact.next_broadcast_attempt_at <= clock_timestamp()
      )
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY block_height ASC, chunk_index ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'schema', 'qbit.prism.ctv-fanout-status.v1',
            'fanout_txid', fanout_txid,
            'block_hash', block_hash,
            'block_height', block_height,
            'parent_hash', parent_hash,
            'chain_state', chain_state,
            'maturity_state', maturity_state,
            'coinbase_txid', coinbase_txid,
            'payout_manifest_sha256', payout_manifest_sha256,
            'audit_bundle_sha256', audit_bundle_sha256,
            'manifest_set_sha256', manifest_set_sha256,
            'manifest_sha256', manifest_sha256,
            'precommitment_sha256', precommitment_sha256,
            'ctv_hash', ctv_hash,
            'commitment_witness_leaf_hex', commitment_witness_leaf_hex,
            'chunk_index', chunk_index,
            'chunk_count', chunk_count,
            'parent_coinbase_txid', parent_coinbase_txid,
            'parent_coinbase_vout', parent_coinbase_vout,
            'fanout_tx_hex', fanout_tx_hex,
            'anchor_vout', anchor_vout,
            'covenant_output_value_sats', covenant_output_value_sats,
            'fanout_output_sum_sats', fanout_output_sum_sats,
            'settlement_status', settlement_status,
            'updated_at', updated_at::text,
            'broadcast_attempt_count', broadcast_attempt_count,
            'broadcast_attempt_detail_count', broadcast_attempt_detail_count,
            'first_broadcast_attempt_at', first_broadcast_attempt_at::text,
            'last_broadcast_attempt_at', last_broadcast_attempt_at::text,
            'last_broadcast_attempt_status', last_broadcast_attempt_status,
            'last_broadcast_package_tx_hexes', last_broadcast_package_tx_hexes,
            'last_broadcast_package_txids', last_broadcast_package_txids,
            'last_broadcast_submit_result', last_broadcast_submit_result,
            'last_broadcast_error', last_broadcast_error,
            'broadcast_attempt_status_counts', broadcast_attempt_status_counts,
            'next_broadcast_attempt_at', next_broadcast_attempt_at::text,
            'broadcast_retry_backoff_seconds', broadcast_retry_backoff_seconds,
            'broadcast_attempt_summary', json_build_object(
                'attempt_count', broadcast_attempt_count,
                'detail_count', broadcast_attempt_detail_count,
                'first_attempt_at', first_broadcast_attempt_at::text,
                'last_attempt_at', last_broadcast_attempt_at::text,
                'last_attempt_status', last_broadcast_attempt_status,
                'last_package_tx_hexes', last_broadcast_package_tx_hexes,
                'last_package_txids', last_broadcast_package_txids,
                'last_submit_result', last_broadcast_submit_result,
                'last_error', last_broadcast_error,
                'status_counts', broadcast_attempt_status_counts,
                'next_attempt_at', next_broadcast_attempt_at::text,
                'retry_backoff_seconds', broadcast_retry_backoff_seconds
            ),
            'broadcast_attempts', COALESCE(
                (
                    SELECT json_agg(json_build_object(
                        'attempt_seq', attempt.attempt_seq,
                        'attempted_at', attempt.attempted_at::text,
                        'attempt_status', attempt.attempt_status,
                        'package_tx_hexes', attempt.package_tx_hexes,
                        'package_txids', attempt.package_txids,
                        'submit_result', attempt.submit_result,
                        'error', attempt.error
                    ) ORDER BY attempt.attempt_seq ASC)
                    FROM qbit_ctv_fanout_broadcast_attempts attempt
                    WHERE attempt.fanout_txid = fanout_txid
                ),
                '[]'::json
            )
        ) ORDER BY block_height ASC, chunk_index ASC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": payload["rows"],
        }

    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        document = self.dashboard_public_artifact_document(sha256=sha256)
        if document is None:
            return None
        return document.get("payload")

    def dashboard_public_artifact_document(self, *, sha256: str) -> dict[str, object] | None:
        """Artifact payload plus its stored canonical text when available.

        canonical_json is the manifest_set_json/manifest_json text persisted
        at record time next to the JSONB copies — the exact byte sequence the
        artifact's sha256 was computed over. Audit bundles use the immutable
        compressed canonical artifact beside the external body. One statement
        serves both database reads so a request touches each artifact table at
        most once.

        Replica ordering is intentionally simple. A visible row plus a missing
        canonical file is legacy history and falls back to the JSONB/external
        body. A file that reached shared storage before this replica replays its
        row remains invisible and returns no match until replay catches up.
        """
        sha256 = str(sha256).lower()
        lit = self._text_literal(sha256)
        sql = f"""
WITH audit AS (
    SELECT block_hash, audit_bundle, audit_bundle_sha256, body_uri
    FROM qbit_pool_audit_bundles
    WHERE audit_bundle_sha256 = {lit}
    ORDER BY created_at DESC
    LIMIT 1
),
set_row AS (
    SELECT manifest_set, manifest_set_json
    FROM qbit_ctv_fanout_sets
    WHERE manifest_set_sha256 = {lit}
    ORDER BY created_at DESC
    LIMIT 1
),
artifact_row AS (
    SELECT manifest, manifest_json
    FROM qbit_ctv_fanout_artifacts
    WHERE manifest_sha256 = {lit}
    ORDER BY updated_at DESC
    LIMIT 1
)
SELECT json_build_object(
    'block_hash', (SELECT block_hash FROM audit),
    'audit_bundle', (SELECT audit_bundle FROM audit),
    'audit_bundle_sha256', (SELECT audit_bundle_sha256 FROM audit),
    'body_uri', (SELECT body_uri FROM audit),
    'has_audit_row', (SELECT count(*) FROM audit) > 0,
    'fallback', COALESCE(
        (SELECT manifest_set FROM set_row),
        (SELECT manifest FROM artifact_row)
    ),
    'fallback_canonical', COALESCE(
        (SELECT manifest_set_json FROM set_row),
        (SELECT manifest_json FROM artifact_row)
    )
);
"""
        row = self._run_read_json(sql)
        if not isinstance(row, dict):
            return None
        if row.get("has_audit_row"):
            block_hash = row.get("block_hash")
            if isinstance(block_hash, str):
                canonical_bytes = self._stored_canonical_audit_bundle_bytes(
                    block_hash,
                    sha256,
                )
                if canonical_bytes is not None:
                    try:
                        canonical_payload = json.loads(canonical_bytes)
                        canonical_json = canonical_bytes.decode("utf-8")
                    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
                        LOGGER.error(
                            "canonical audit bundle unavailable reason=corrupt "
                            "block_hash=%s audit_bundle_sha256=%s",
                            block_hash,
                            sha256,
                        )
                        raise CanonicalAuditBundleCorrupt(
                            "canonical audit bundle is not valid UTF-8 JSON"
                        ) from exc
                    if not isinstance(canonical_payload, dict):
                        LOGGER.error(
                            "canonical audit bundle unavailable reason=corrupt "
                            "block_hash=%s audit_bundle_sha256=%s",
                            block_hash,
                            sha256,
                        )
                        raise CanonicalAuditBundleCorrupt(
                            "canonical audit bundle JSON is not an object"
                        )
                    return {
                        "payload": canonical_payload,
                        "canonical_json": canonical_json,
                    }
            body = row.get("audit_bundle")
            if body is None:
                body = self._read_external_body(row.get("body_uri"), expected_sha256=sha256)
            if body is not None:
                return {
                    "payload": body,
                    "canonical_json": None,
                    "canonical_fallback_reason": "missing",
                }
        fallback = row.get("fallback")
        if not isinstance(fallback, dict):
            return None
        fallback_canonical = row.get("fallback_canonical")
        return {
            "payload": fallback,
            "canonical_json": fallback_canonical if isinstance(fallback_canonical, str) else None,
        }

    def dashboard_public_artifact_exists(self, *, sha256: str) -> bool:
        sha256 = str(sha256).lower()
        lit = self._text_literal(sha256)
        sql = f"""
WITH audit AS (
    SELECT block_hash, audit_bundle, audit_bundle_sha256, body_uri
    FROM qbit_pool_audit_bundles
    WHERE audit_bundle_sha256 = {lit}
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT json_build_object(
    'has_audit_row', (SELECT count(*) FROM audit) > 0,
    'block_hash', (SELECT block_hash FROM audit),
    'audit_bundle_sha256', (SELECT audit_bundle_sha256 FROM audit),
    'audit_bundle_inline', (SELECT audit_bundle IS NOT NULL FROM audit),
    'body_uri', (SELECT body_uri FROM audit),
    'fallback_exists',
        EXISTS (
            SELECT 1
            FROM qbit_ctv_fanout_sets
            WHERE manifest_set_sha256 = {lit}
        )
        OR EXISTS (
            SELECT 1
            FROM qbit_ctv_fanout_artifacts
            WHERE manifest_sha256 = {lit}
        )
);
"""
        row = self._run_read_json(sql)
        if not isinstance(row, dict):
            return False
        if row.get("has_audit_row"):
            block_hash = row.get("block_hash")
            if (
                isinstance(block_hash, str)
                and self._stored_canonical_audit_bundle_bytes(block_hash, sha256)
                is not None
            ):
                return True
            if row.get("audit_bundle_inline"):
                return True
            body_uri = row.get("body_uri")
            if not body_uri:
                return False
            return self._external_body_available_for_sha(body_uri, sha256)
        return bool(row.get("fallback_exists"))

    def update_ctv_fanout_status(self, *, fanout_txid: str, settlement_status: str) -> dict[str, int | str]:
        validate_ctv_fanout_status(settlement_status)
        payload = {
            "fanout_txid": fanout_txid,
            "settlement_status": settlement_status,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
updated AS (
    UPDATE qbit_ctv_fanout_artifacts
    SET settlement_status = (SELECT data->>'settlement_status' FROM payload),
        updated_at = clock_timestamp()
    FROM lease
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
    RETURNING fanout_txid
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'updated_count', (SELECT count(*) FROM updated)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {"backend": str(result["backend"]), "updated_count": int(result["updated_count"])}

    def record_ctv_fanout_broadcast_attempt(
        self,
        *,
        fanout_txid: str,
        attempt_status: str,
        package_tx_hexes: list[str] | None = None,
        package_txids: list[str] | None = None,
        submit_result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, int | str]:
        validate_ctv_fanout_attempt_status(attempt_status)
        next_status = None
        if attempt_status in {"submitted", "accepted"}:
            next_status = "broadcast_submitted"
        elif attempt_status in {"rejected", "failed"}:
            next_status = "failed"
        payload = {
            "fanout_txid": fanout_txid,
            "attempt_status": attempt_status,
            "package_tx_hexes": package_tx_hexes or [],
            "package_txids": package_txids or [],
            "submit_result": submit_result,
            "error": error,
            "next_status": next_status,
            "attempt_detail_limit": self._ctv_broadcast_attempt_detail_limit,
            "retry_backoff_seconds": self._ctv_broadcast_retry_backoff_seconds,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
artifact_row AS (
    SELECT fanout_txid
    FROM qbit_ctv_fanout_artifacts
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
),
existing_detail_count AS (
    SELECT count(*)::bigint AS detail_count
    FROM qbit_ctv_fanout_broadcast_attempts
    WHERE fanout_txid = (SELECT data->>'fanout_txid' FROM payload)
),
pruned AS (
    DELETE FROM qbit_ctv_fanout_broadcast_attempts old_attempt
    USING payload, artifact_row
    WHERE old_attempt.fanout_txid = artifact_row.fanout_txid
      AND old_attempt.attempt_seq IN (
          SELECT retained.attempt_seq
          FROM qbit_ctv_fanout_broadcast_attempts retained
          WHERE retained.fanout_txid = artifact_row.fanout_txid
          ORDER BY retained.attempt_seq DESC
          OFFSET GREATEST((data->>'attempt_detail_limit')::integer - 1, 0)
      )
    RETURNING old_attempt.attempt_seq
),
pruned_count AS (
    SELECT count(*)::bigint AS pruned_count FROM pruned
),
inserted AS (
    INSERT INTO qbit_ctv_fanout_broadcast_attempts (
        fanout_txid,
        attempt_status,
        package_tx_hexes,
        package_txids,
        submit_result,
        error
    )
    SELECT
        data->>'fanout_txid',
        data->>'attempt_status',
        data->'package_tx_hexes',
        data->'package_txids',
        data->'submit_result',
        data->>'error'
    FROM payload, lease, artifact_row, pruned_count
    WHERE (data->>'attempt_detail_limit')::integer > 0
    RETURNING attempt_seq
),
inserted_count AS (
    SELECT count(*)::bigint AS inserted_count FROM inserted
),
updated AS (
    UPDATE qbit_ctv_fanout_artifacts artifact
    SET settlement_status = COALESCE(data->>'next_status', artifact.settlement_status),
        updated_at = clock_timestamp(),
        broadcast_attempt_count = artifact.broadcast_attempt_count + 1,
        broadcast_attempt_detail_count = CASE
            WHEN (data->>'attempt_detail_limit')::integer <= 0 THEN 0
            ELSE LEAST(
                (data->>'attempt_detail_limit')::bigint,
                GREATEST(
                    0,
                    existing_detail_count.detail_count
                    - pruned_count.pruned_count
                    + inserted_count.inserted_count
                )
            )
        END,
        first_broadcast_attempt_at = COALESCE(artifact.first_broadcast_attempt_at, clock_timestamp()),
        last_broadcast_attempt_at = clock_timestamp(),
        last_broadcast_attempt_status = data->>'attempt_status',
        last_broadcast_package_tx_hexes = data->'package_tx_hexes',
        last_broadcast_package_txids = data->'package_txids',
        last_broadcast_submit_result = data->'submit_result',
        last_broadcast_error = data->>'error',
        broadcast_attempt_status_counts = jsonb_set(
            COALESCE(artifact.broadcast_attempt_status_counts, '{{}}'::jsonb),
            ARRAY[data->>'attempt_status'],
            to_jsonb(
                COALESCE((artifact.broadcast_attempt_status_counts->>(data->>'attempt_status'))::bigint, 0)
                + 1
            ),
            true
        ),
        next_broadcast_attempt_at = CASE
            WHEN data->>'attempt_status' = 'planned'
              AND (data->>'retry_backoff_seconds')::bigint > 0 THEN
                clock_timestamp() + make_interval(secs => (data->>'retry_backoff_seconds')::double precision)
            ELSE NULL
        END,
        broadcast_retry_backoff_seconds = CASE
            WHEN data->>'attempt_status' = 'planned' THEN (data->>'retry_backoff_seconds')::bigint
            ELSE 0
        END
    FROM payload, lease, artifact_row, existing_detail_count, pruned_count, inserted_count
    WHERE artifact.fanout_txid = artifact_row.fanout_txid
    RETURNING artifact.fanout_txid, artifact.broadcast_attempt_count, artifact.broadcast_attempt_detail_count
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM artifact_row) = 0 THEN
        json_build_object('error', 'unknown CTV fanout txid')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'attempt_count', (SELECT count(*) FROM inserted),
            'updated_count', (SELECT count(*) FROM updated),
            'broadcast_attempt_count', (SELECT broadcast_attempt_count FROM updated LIMIT 1),
            'broadcast_attempt_detail_count', (SELECT broadcast_attempt_detail_count FROM updated LIMIT 1)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "attempt_count": int(result["attempt_count"]),
            "updated_count": int(result["updated_count"]),
        }

    def metrics(self) -> dict[str, int]:
        # The accepted-share count comes from the incrementally maintained
        # stats (see accepted_share_stats) so metrics scrapes never seq-scan
        # the share ledger; the block/payout tables stay small.
        accepted_share_count = int(self.accepted_share_stats()["accepted_share_count"])
        sql = """
SELECT json_build_object(
    'blocks', (SELECT count(*) FROM qbit_pool_blocks),
    'confirmed_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'confirmed'),
    'inactive_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'inactive'),
    'rejected_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'rejected'),
    'reversed_blocks', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'reversed'),
    'payout_entries', (SELECT count(*) FROM qbit_pool_payout_entries),
    'owed_accounts', (SELECT count(*) FROM qbit_current_owed_balances() WHERE owed_balance_sats > 0),
    'ctv_fanouts_failed', (SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE settlement_status = 'failed')
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            metrics = self._run_retry_safe_read_json(sql)
        report = {str(key): int(value) for key, value in metrics.items()}
        report["shares"] = accepted_share_count
        return report

    def dashboard_pool_snapshot(
        self,
        *,
        current_network_difficulty: int | str | Decimal,
        generated_at: str,
    ) -> dict[str, object]:
        from lab.prism import public_api

        window_weight = _reward_window_weight(current_network_difficulty)
        window_weight_sql = public_api.decimal_string(window_weight)
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
),
latest_block_row AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256
    FROM qbit_pool_blocks block
    WHERE block.chain_state <> 'reversed'
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT 1
),
latest_block AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256,
        bundle.audit_bundle_sha256,
        solver.miner_id AS solver_recipient_id,
        solver.share_id AS solver_share_id
    FROM latest_block_row block
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = block.block_hash
    LEFT JOIN LATERAL (
        SELECT share.miner_id, share.share_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
),
window_rows AS (
    SELECT window_row.*
    FROM bounds
    CROSS JOIN LATERAL qbit_prism_window(bounds.ended_at, {window_weight_sql}::numeric) AS window_row
),
window_summary AS (
    SELECT
        count(*) AS included_share_count,
        to_char(min(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS oldest_share_accepted_at,
        to_char(max(accepted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS newest_share_accepted_at
    FROM window_rows
),
rollups AS (
    SELECT
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '1 hour'), 0)::text AS h1_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours'), 0)::text AS h3_difficulty,
        COALESCE(sum(share_difficulty) FILTER (WHERE accepted_at >= bounds.ended_at - interval '24 hours'), 0)::text AS h24_difficulty,
        count(DISTINCT miner_id) FILTER (WHERE accepted_at >= bounds.ended_at - interval '3 hours') AS participants_3h
    FROM qbit_share_ledger, bounds
    WHERE accepted
      AND accepted_at >= bounds.ended_at - interval '24 hours'
      AND accepted_at <= bounds.ended_at
)
SELECT json_build_object(
    'h1_difficulty', (SELECT h1_difficulty FROM rollups),
    'h3_difficulty', (SELECT h3_difficulty FROM rollups),
    'h24_difficulty', (SELECT h24_difficulty FROM rollups),
    'participants_3h', (SELECT participants_3h FROM rollups),
    'blocks_found_total', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state <> 'reversed'),
    'prism_blocks_total', (SELECT count(*) FROM qbit_pool_blocks WHERE chain_state <> 'reversed'),
    'total_mined_bits', COALESCE((
        SELECT sum(carry.gross_amount_sats)
        FROM qbit_payout_carry_forward carry
        JOIN qbit_pool_blocks block
          ON block.block_hash = carry.block_hash
        WHERE block.chain_state = 'confirmed'
          AND block.maturity_state <> 'reversed'
          AND carry.maturity_state <> 'reversed'
    ), 0),
    'latest_block', COALESCE((
        SELECT json_build_object(
            'height', latest_block.block_height,
            'hash', latest_block.block_hash,
            'found_at', to_char(latest_block.found_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'age_seconds', GREATEST(0, floor(extract(epoch FROM (clock_timestamp() - latest_block.found_at)))::bigint),
            'solver_recipient_id', COALESCE(latest_block.solver_recipient_id, ''),
            'solver_worker_name', {_solver_worker_name_sql("latest_block.solver_share_id")}
        )
        FROM latest_block
    ), 'null'::json),
    'oldest_share_accepted_at', (SELECT oldest_share_accepted_at FROM window_summary),
    'newest_share_accepted_at', (SELECT newest_share_accepted_at FROM window_summary),
    'included_share_count', (SELECT included_share_count FROM window_summary)
);
"""
        row = self._run_read_json(sql)
        return {
            "hashrate_ths": {
                "h1": public_api.hashrate_ths_from_difficulty(row["h1_difficulty"], 60 * 60),
                "h3": public_api.hashrate_ths_from_difficulty(row["h3_difficulty"], 3 * 60 * 60),
                "h24": public_api.hashrate_ths_from_difficulty(row["h24_difficulty"], 24 * 60 * 60),
            },
            "participants_3h": int(row["participants_3h"]),
            "blocks_found_total": int(row["blocks_found_total"]),
            "prism_blocks_total": int(row["prism_blocks_total"]),
            "total_mined_bits": int(row["total_mined_bits"]),
            "latest_block": row["latest_block"],
            "reward_window": {
                "window_multiplier": 8,
                "requested_window_weight": public_api.decimal_string(window_weight),
                "oldest_share_accepted_at": row["oldest_share_accepted_at"],
                "newest_share_accepted_at": row["newest_share_accepted_at"],
                "included_share_count": int(row["included_share_count"]),
            },
        }

    def dashboard_blocks(self, *, page: int, limit: int) -> dict[str, object]:
        from lab.prism import public_api

        offset = (page - 1) * limit
        explorer_prefix = os.environ.get("PRISM_PUBLIC_EXPLORER_BLOCK_URL_PREFIX")
        sql = f"""
WITH total AS (
    SELECT count(*) AS total_count
    FROM qbit_pool_blocks
    WHERE chain_state <> 'reversed'
),
page_blocks AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256
    FROM qbit_pool_blocks block
    WHERE block.chain_state <> 'reversed'
    ORDER BY block.block_height DESC, block.found_at DESC
    LIMIT {int(limit)} OFFSET {int(offset)}
),
rows AS (
    SELECT
        block.block_hash,
        block.block_height,
        block.found_at,
        block.payout_manifest_sha256,
        COALESCE(bundle.found_block_network_difficulty::text, bundle.audit_bundle#>>'{{found_block,network_difficulty}}') AS audit_network_difficulty,
        COALESCE(bundle.found_block_bits, bundle.audit_bundle#>>'{{found_block,bits}}') AS audit_bits,
        COALESCE(bundle.found_block_coinbase_value_sats::text, bundle.audit_bundle#>>'{{found_block,coinbase_value_sats}}') AS audit_coinbase_value_sats,
        bundle.audit_bundle_sha256,
        solver.miner_id AS solver_recipient_id,
        solver.share_difficulty::text AS solver_share_difficulty,
        solver.network_difficulty::text AS solver_network_difficulty,
        solver.share_id AS solver_share_id
    FROM page_blocks block
    LEFT JOIN qbit_pool_audit_bundles bundle
      ON bundle.block_hash = block.block_hash
    LEFT JOIN LATERAL (
        SELECT share.miner_id, share.share_difficulty, share.network_difficulty, share.share_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
)
SELECT json_build_object(
    'total_count', (SELECT total_count FROM total),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'height', rows.block_height,
            'hash', rows.block_hash,
            'found_at', to_char(rows.found_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'network_difficulty', COALESCE(rows.audit_network_difficulty, rows.solver_network_difficulty, '0'),
            'bits', COALESCE(rows.audit_bits, '00000000'),
            'solver_recipient_id', COALESCE(rows.solver_recipient_id, ''),
            'solver_worker_name', {_solver_worker_name_sql("rows.solver_share_id")},
            'solver_share_difficulty', rows.solver_share_difficulty,
            'reward_window_weight', CASE
                WHEN rows.audit_network_difficulty IS NULL THEN null
                ELSE (rows.audit_network_difficulty::numeric * 8::numeric)::text
            END,
            'coinbase_value_bits', COALESCE(rows.audit_coinbase_value_sats::bigint, 0),
            'audit_bundle_sha256', rows.audit_bundle_sha256,
            'payout_manifest_sha256', rows.payout_manifest_sha256,
            'explorer_url', null
        ) ORDER BY rows.block_height DESC, rows.found_at DESC)
        FROM rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows = payload["rows"]
        if explorer_prefix:
            for row in rows:
                row["explorer_url"] = explorer_prefix.rstrip("/") + "/" + str(row["hash"])
        return {
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": rows,
        }

    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        from lab.prism import public_api

        offset = (page - 1) * limit
        search_filter = ""
        if search:
            search_filter = f"WHERE strpos(lower(miner_id), {self._text_literal(search.lower())}) > 0"
        sql = f"""
	WITH snapshot_clock AS (
	    SELECT clock_timestamp() AS ended_at
	),
	bounds AS (
	    SELECT ended_at, ended_at - interval '3 hours' AS started_at
	    FROM snapshot_clock
	),
	windowed AS (
	    SELECT ledger.*
	    FROM qbit_share_ledger ledger, bounds
	    WHERE ledger.accepted
	      AND ledger.accepted_at >= bounds.started_at
	      AND ledger.accepted_at <= bounds.ended_at
	),
grouped AS (
    SELECT
        miner_id,
        sum(share_difficulty) AS accepted_share_difficulty,
        max(accepted_at) AS last_share_at
    FROM windowed
    GROUP BY miner_id
),
filtered AS (
    SELECT *
    FROM grouped
    {search_filter}
),
blocks AS (
    SELECT solver.miner_id, count(*) AS blocks_found
    FROM qbit_pool_blocks block
    JOIN LATERAL (
        SELECT share.miner_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
    WHERE block.chain_state <> 'reversed'
    GROUP BY solver.miner_id
),
totals AS (
    SELECT
        COALESCE(sum(accepted_share_difficulty), 0) AS total_difficulty,
        count(*) AS participant_count
    FROM filtered
),
ranked AS (
    SELECT
        row_number() OVER (ORDER BY accepted_share_difficulty DESC, filtered.miner_id ASC) AS rank,
        filtered.miner_id,
        filtered.accepted_share_difficulty,
        filtered.last_share_at,
        COALESCE(blocks.blocks_found, 0) AS blocks_found
    FROM filtered
    LEFT JOIN blocks
      ON blocks.miner_id = filtered.miner_id
),
page_rows AS (
    SELECT *
    FROM ranked
    ORDER BY rank ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'started_at', (SELECT to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM bounds),
    'ended_at', (SELECT to_char(ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM bounds),
    'total_difficulty', (SELECT total_difficulty::text FROM totals),
    'participant_count', (SELECT participant_count FROM totals),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'rank', page_rows.rank,
            'recipient_id', page_rows.miner_id,
            'display_name', null,
            'accepted_share_difficulty', page_rows.accepted_share_difficulty::text,
            'share_percent', CASE
                WHEN (SELECT total_difficulty FROM totals) > 0 THEN
                    (page_rows.accepted_share_difficulty * 100::numeric / (SELECT total_difficulty FROM totals))::text
                ELSE null
            END,
            'blocks_found', page_rows.blocks_found,
            'last_share_at', to_char(page_rows.last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ) ORDER BY page_rows.rank ASC)
        FROM page_rows
    ), '[]'::json)
);
"""
        payload = self._run_read_json(sql)
        rows: list[dict[str, object]] = []
        total_difficulty = str(payload["total_difficulty"])
        pool_hashrate_ths = public_api.hashrate_ths_from_difficulty(total_difficulty, 3 * 60 * 60)
        for row in payload["rows"]:
            share_percent = row["share_percent"]
            hashrate_ths = public_api.hashrate_ths_from_difficulty(
                row["accepted_share_difficulty"],
                3 * 60 * 60,
            )
            rows.append(
                {
                    "rank": int(row["rank"]),
                    "recipient_id": row["recipient_id"],
                    "display_name": row["display_name"],
                    "hashrate_ths_3h": hashrate_ths,
                    "share_percent": public_api.decimal_string(share_percent) if share_percent is not None else None,
                    "hash_percent": _hash_percent(hashrate_ths, pool_hashrate_ths),
                    "blocks_found": int(row["blocks_found"]),
                    "last_share_at": row["last_share_at"],
                }
            )
        participant_count = int(payload["participant_count"])
        return {
            "started_at": payload["started_at"],
            "ended_at": payload["ended_at"],
            "totals": {
                "pool_hashrate_ths": pool_hashrate_ths,
                "pool_accepted_share_difficulty": total_difficulty,
                "participant_count": participant_count,
            },
            "pagination": public_api.pagination(page, limit, participant_count),
            "rows": rows,
        }

    def dashboard_reward_leaderboard(
        self,
        *,
        page: int,
        limit: int,
        current_network_difficulty: int | str | Decimal,
        search: str | None = None,
        recipient_id: str | None = None,
    ) -> dict[str, object]:
        from lab.prism import public_api

        if search is not None and recipient_id is not None:
            raise ValueError("search and recipient_id are mutually exclusive")
        offset = (page - 1) * limit
        network_difficulty = public_api.decimal_string(current_network_difficulty)
        requested_window_weight = _reward_window_weight(current_network_difficulty)
        requested_window_weight_sql = public_api.decimal_string(requested_window_weight)
        row_filter = ""
        if recipient_id is not None:
            row_filter = f"WHERE ranked.miner_id = {self._text_literal(recipient_id)}"
        elif search:
            row_filter = (
                "WHERE strpos(lower(ranked.miner_id), "
                f"{self._text_literal(search.lower())}) > 0"
            )
        sql = f"""
WITH snapshot_clock AS (
    SELECT clock_timestamp() AS ended_at
),
window_rows AS MATERIALIZED (
    SELECT window_row.*
    FROM snapshot_clock
    CROSS JOIN LATERAL qbit_prism_window(
        snapshot_clock.ended_at,
        {requested_window_weight_sql}::numeric
    ) AS window_row
),
window_summary AS (
    SELECT
        count(*) AS included_share_count,
        COALESCE(sum(counted_difficulty), 0) AS counted_window_weight,
        min(accepted_at) AS oldest_share_accepted_at
    FROM window_rows
),
grouped AS (
    SELECT
        miner_id,
        count(*) AS included_share_count,
        sum(counted_difficulty) AS counted_share_difficulty,
        max(accepted_at) AS last_share_at
    FROM window_rows
    GROUP BY miner_id
),
blocks AS (
    SELECT solver.miner_id, count(*) AS blocks_found_total
    FROM qbit_pool_blocks block
    JOIN LATERAL (
        SELECT share.miner_id
        FROM qbit_share_ledger share
        WHERE share.accepted
          AND length(share.share_id) >= 65
          AND lower(right(share.share_id, 64)) = block.block_hash
        ORDER BY share.accepted_at DESC, share.share_seq DESC
        LIMIT 1
    ) solver ON true
    WHERE block.chain_state <> 'reversed'
    GROUP BY solver.miner_id
),
ranked AS (
    SELECT
        row_number() OVER (ORDER BY grouped.counted_share_difficulty DESC, grouped.miner_id ASC) AS rank,
        grouped.miner_id,
        grouped.included_share_count,
        grouped.counted_share_difficulty,
        grouped.last_share_at,
        COALESCE(blocks.blocks_found_total, 0) AS blocks_found_total
    FROM grouped
    LEFT JOIN blocks
      ON blocks.miner_id = grouped.miner_id
),
filtered AS (
    SELECT *
    FROM ranked
    {row_filter}
),
page_rows AS (
    SELECT *
    FROM filtered
    ORDER BY rank ASC
    LIMIT {int(limit)} OFFSET {int(offset)}
)
SELECT json_build_object(
    'ended_at', to_char(snapshot_clock.ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'oldest_share_accepted_at', to_char(window_summary.oldest_share_accepted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'observed_span_seconds', CASE
        WHEN window_summary.oldest_share_accepted_at IS NULL THEN null
        ELSE GREATEST(
            0,
            floor(extract(epoch FROM (snapshot_clock.ended_at - window_summary.oldest_share_accepted_at)))::bigint
        )
    END,
    'counted_window_weight', window_summary.counted_window_weight::text,
    'included_share_count', window_summary.included_share_count,
    'participant_count', (SELECT count(*) FROM ranked),
    'total_count', (SELECT count(*) FROM filtered),
    'rows', COALESCE((
        SELECT json_agg(json_build_object(
            'rank', page_rows.rank,
            'recipient_id', page_rows.miner_id,
            'display_name', null,
            'included_share_count', page_rows.included_share_count,
            'counted_share_difficulty', page_rows.counted_share_difficulty::text,
            'share_percent', CASE
                WHEN window_summary.counted_window_weight > 0 THEN
                    (page_rows.counted_share_difficulty * 100::numeric / window_summary.counted_window_weight)::text
                ELSE null
            END,
            'blocks_found_total', page_rows.blocks_found_total,
            'last_share_at', to_char(page_rows.last_share_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ) ORDER BY page_rows.rank ASC)
        FROM page_rows
    ), '[]'::json)
)
FROM snapshot_clock
CROSS JOIN window_summary;
"""
        payload = self._run_read_json(sql)
        counted_window_weight = Decimal(str(payload["counted_window_weight"]))
        observed_span_seconds = (
            int(payload["observed_span_seconds"])
            if payload.get("observed_span_seconds") is not None
            else None
        )
        pool_hashrate_ths = None
        expected_time_to_block = None
        if observed_span_seconds is not None and observed_span_seconds > 0 and counted_window_weight > 0:
            pool_hashrate_ths = public_api.hashrate_ths_from_difficulty(
                counted_window_weight,
                observed_span_seconds,
            )
            expected_time_to_block = public_api.expected_time_to_block_seconds(
                hashrate_ths=pool_hashrate_ths,
                network_difficulty=network_difficulty,
            )

        rows: list[dict[str, object]] = []
        for row in payload["rows"]:
            counted_share_difficulty = public_api.decimal_string(row["counted_share_difficulty"])
            hashrate_ths = None
            if observed_span_seconds is not None and observed_span_seconds > 0:
                hashrate_ths = public_api.hashrate_ths_from_difficulty(
                    counted_share_difficulty,
                    observed_span_seconds,
                )
            share_percent = row["share_percent"]
            rows.append(
                {
                    "rank": int(row["rank"]),
                    "recipient_id": row["recipient_id"],
                    "display_name": row["display_name"],
                    "hashrate_ths": hashrate_ths,
                    "included_share_count": int(row["included_share_count"]),
                    "counted_share_difficulty": counted_share_difficulty,
                    "share_percent": public_api.decimal_string(share_percent) if share_percent is not None else None,
                    "blocks_found_total": int(row["blocks_found_total"]),
                    "last_share_at": row["last_share_at"],
                }
            )

        participant_count = int(payload["participant_count"])
        return {
            "window": {
                "id": "reward",
                "started_at": payload["oldest_share_accepted_at"],
                "ended_at": payload["ended_at"],
                "observed_span_seconds": observed_span_seconds,
                "network_difficulty": network_difficulty,
                "window_multiplier": 8,
                "requested_window_weight": requested_window_weight_sql,
                "counted_window_weight": public_api.decimal_string(counted_window_weight),
                "included_share_count": int(payload["included_share_count"]),
                "is_complete": requested_window_weight > 0 and counted_window_weight >= requested_window_weight,
            },
            "totals": {
                "pool_hashrate_ths": pool_hashrate_ths,
                "pool_counted_share_difficulty": public_api.decimal_string(counted_window_weight),
                "participant_count": participant_count,
                "expected_time_to_block_seconds": expected_time_to_block,
            },
            "pagination": public_api.pagination(page, limit, int(payload["total_count"])),
            "rows": rows,
        }

    def dashboard_hashrate_series(
        self,
        *,
        subject_type: str,
        subject_id: str | None,
        range_id: str,
        bucket: str,
        lookback_seconds: int = 0,
        range_anchor_epoch: int | None = None,
    ) -> list[dict[str, object]]:
        """Serve the per-bucket credited hashrate series in O(buckets).

        The series is answered from the incremental rollup tables plus a
        live tail over the shares the maintenance watermark has not folded
        in yet, so its cost tracks the number of buckets in range rather
        than the number of shares. The rollup path must stay byte-identical
        to the historical raw ``qbit_share_ledger`` aggregation for the same
        underlying shares -- same buckets, counts, difficulty strings, and
        ordering -- which pins three invariants:

        - The watermark, the rollup rows, and the tail scan are read inside
          one statement, so they share one snapshot. Reading the watermark
          in a separate statement would let a concurrent maintenance pass
          advance it in between and double-count the shares it folded in.
        - A range lower bound that is not bucket-aligned makes the raw scan
          emit a partial leading bucket (only the shares at or after the
          bound), and the bucket containing the statement's upper bound can
          hold a rollup-folded share whose coordinator-assigned accepted_at
          runs ahead of the database clock -- a share the raw scan's
          ``accepted_at <= ended_at`` excludes until the clocks catch up.
          Neither partial end can be served from full rollup buckets, so
          both are re-aggregated from the ledger over the already-rolled-up
          sequence range; each scan is bounded by one bucket span of shares
          regardless of the requested range.
        - An absent progress row gates the rollup and boundary branches off
          and widens the tail to the whole ledger (``share_seq > -1``), so
          the merged output degrades to exactly the raw scan; a database
          missing the rollup tables entirely runs the raw statement
          unchanged. Both keep pre-migration and mid-backfill states
          correct without operator action.
        """
        from lab.prism import public_api

        bucket_seconds = {"5m": 300, "1h": 3600, "1d": 86400}[bucket]
        range_interval = {
            "1w": "7 days",
            "1m": "30 days",
            "6m": "180 days",
            "window": "3 hours",
            "all": None,
        }[range_id]
        range_filter = ""
        range_start_sql: str | None = None
        if range_interval is not None:
            # Anchor the range lower bound on the caller's clock when provided
            # so it agrees with the caller's min_epoch trim even when the
            # database clock disagrees with the application clock.
            range_anchor_sql = (
                f"to_timestamp({int(range_anchor_epoch)})"
                if range_anchor_epoch is not None
                else "bounds.ended_at"
            )
            range_start_sql = f"{range_anchor_sql} - interval '{range_interval}'"
            if lookback_seconds > 0:
                # Pre-range context requested by the smoother; the caller trims
                # these buckets from the response after windowing.
                range_start_sql += f" - interval '{int(lookback_seconds)} seconds'"
            range_filter = f"AND ledger.accepted_at >= {range_start_sql}"
        subject_filter = ""
        if subject_type == "miner":
            subject_filter = f"AND ledger.miner_id = {self._text_literal(str(subject_id))}"
        if not self._hashrate_rollup_schema_present():
            sql = f"""
	WITH bounds AS (
	    SELECT clock_timestamp() AS ended_at
	),
	bucketed AS (
	    SELECT
	        floor(extract(epoch FROM ledger.accepted_at) / {int(bucket_seconds)})::bigint * {int(bucket_seconds)} AS bucket_epoch,
	        count(*) AS accepted_share_count,
	        sum(ledger.share_difficulty) AS accepted_share_difficulty
	    FROM qbit_share_ledger ledger, bounds
	    WHERE ledger.accepted
	      AND ledger.accepted_at <= bounds.ended_at
	      {range_filter}
	      {subject_filter}
	    GROUP BY bucket_epoch
	)
SELECT COALESCE(json_agg(json_build_object(
    'timestamp', to_char(to_timestamp(bucket_epoch) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'accepted_share_count', accepted_share_count,
    'accepted_share_difficulty', accepted_share_difficulty::text
) ORDER BY bucket_epoch ASC), '[]'::json)
FROM bucketed;
"""
            rows = self._run_read_json(sql)
            return [
                {
                    "timestamp": row["timestamp"],
                    "hashrate_ths": public_api.hashrate_ths_from_difficulty(row["accepted_share_difficulty"], bucket_seconds),
                    "accepted_share_count": int(row["accepted_share_count"]),
                    "accepted_share_difficulty": str(row["accepted_share_difficulty"]),
                }
                for row in rows
            ]
        if subject_type == "miner":
            rollup_table = "qbit_hashrate_rollup_miner"
            rollup_subject_filter = (
                f"\n      AND rollup.miner_id = {self._text_literal(str(subject_id))}"
            )
        else:
            rollup_table = "qbit_hashrate_rollup_pool"
            rollup_subject_filter = ""
        # The bucket containing bounds.ended_at is never served from its
        # rollup row: maintenance folds by sequence, so a coordinator clock
        # running ahead of the database clock can put a share into that
        # bucket's rollup that the raw scan's accepted_at <= ended_at would
        # exclude. Like the partial lower-bound bucket, it is re-aggregated
        # from the already-rolled-up sequence range -- one bucket span of
        # shares, whatever the requested range.
        if range_start_sql is not None:
            range_buckets_cte = f"""range_buckets AS (
    SELECT
        range_bounds.range_started_at,
        (ceil(extract(epoch FROM range_bounds.range_started_at) / {int(bucket_seconds)}))::bigint * {int(bucket_seconds)} AS first_full_bucket_epoch
    FROM (SELECT {range_start_sql} AS range_started_at FROM bounds) range_bounds
),
"""
            rollup_lower_bound = (
                "\n      AND rollup.bucket_epoch >= (SELECT first_full_bucket_epoch FROM range_buckets)"
            )
            boundary_window = """(
        ledger.accepted_at < to_timestamp((SELECT first_full_bucket_epoch FROM range_buckets))
        OR ledger.accepted_at >= to_timestamp((SELECT bucket_epoch FROM current_bucket))
      )"""
            boundary_range_filter = (
                "\n      AND ledger.accepted_at >= (SELECT range_started_at FROM range_buckets)"
            )
            tail_range_filter = (
                "\n      AND ledger.accepted_at >= (SELECT range_started_at FROM range_buckets)"
            )
        else:
            range_buckets_cte = ""
            rollup_lower_bound = ""
            boundary_window = (
                "ledger.accepted_at >= to_timestamp((SELECT bucket_epoch FROM current_bucket))"
            )
            boundary_range_filter = ""
            tail_range_filter = ""
        boundary_cte = f"""boundary AS (
    SELECT
        floor(extract(epoch FROM ledger.accepted_at) / {int(bucket_seconds)})::bigint * {int(bucket_seconds)} AS bucket_epoch,
        count(*) AS accepted_share_count,
        sum(ledger.share_difficulty) AS accepted_share_difficulty
    FROM qbit_share_ledger ledger, bounds
    WHERE (SELECT rollups_ready FROM watermark)
      AND ledger.accepted
      AND ledger.share_seq <= (SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at <= bounds.ended_at
      AND {boundary_window}{boundary_range_filter}
      {subject_filter}
    GROUP BY bucket_epoch
),
"""
        boundary_union = """        UNION ALL
        SELECT bucket_epoch, accepted_share_count, accepted_share_difficulty FROM boundary
"""
        sql = f"""
WITH bounds AS (
    SELECT clock_timestamp() AS ended_at
),
progress AS (
    SELECT last_share_seq
    FROM qbit_hashrate_rollup_progress
    WHERE singleton
),
watermark AS (
    SELECT
        COALESCE((SELECT last_share_seq FROM progress), -1) AS last_share_seq,
        EXISTS (SELECT 1 FROM progress) AS rollups_ready
),
current_bucket AS (
    SELECT floor(extract(epoch FROM bounds.ended_at) / {int(bucket_seconds)})::bigint * {int(bucket_seconds)} AS bucket_epoch
    FROM bounds
),
{range_buckets_cte}rolled AS (
    SELECT
        rollup.bucket_epoch,
        rollup.accepted_share_count,
        rollup.accepted_share_difficulty
    FROM {rollup_table} rollup, bounds
    WHERE (SELECT rollups_ready FROM watermark)
      AND rollup.grain_seconds = {int(bucket_seconds)}
      AND rollup.bucket_epoch < (SELECT bucket_epoch FROM current_bucket){rollup_lower_bound}{rollup_subject_filter}
),
{boundary_cte}tail AS (
    SELECT
        floor(extract(epoch FROM ledger.accepted_at) / {int(bucket_seconds)})::bigint * {int(bucket_seconds)} AS bucket_epoch,
        count(*) AS accepted_share_count,
        sum(ledger.share_difficulty) AS accepted_share_difficulty
    FROM qbit_share_ledger ledger, bounds
    WHERE ledger.accepted
      AND ledger.share_seq > (SELECT last_share_seq FROM watermark)
      AND ledger.accepted_at <= bounds.ended_at{tail_range_filter}
      {subject_filter}
    GROUP BY bucket_epoch
),
merged AS (
    SELECT
        parts.bucket_epoch,
        sum(parts.accepted_share_count)::bigint AS accepted_share_count,
        sum(parts.accepted_share_difficulty) AS accepted_share_difficulty
    FROM (
        SELECT bucket_epoch, accepted_share_count, accepted_share_difficulty FROM rolled
{boundary_union}        UNION ALL
        SELECT bucket_epoch, accepted_share_count, accepted_share_difficulty FROM tail
    ) parts
    GROUP BY parts.bucket_epoch
)
SELECT COALESCE(json_agg(json_build_object(
    'timestamp', to_char(to_timestamp(bucket_epoch) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'accepted_share_count', accepted_share_count,
    'accepted_share_difficulty', accepted_share_difficulty::text
) ORDER BY bucket_epoch ASC), '[]'::json)
FROM merged;
"""
        rows = self._run_read_json(sql)
        return [
            {
                "timestamp": row["timestamp"],
                "hashrate_ths": public_api.hashrate_ths_from_difficulty(row["accepted_share_difficulty"], bucket_seconds),
                "accepted_share_count": int(row["accepted_share_count"]),
                "accepted_share_difficulty": str(row["accepted_share_difficulty"]),
            }
            for row in rows
        ]

    def _hashrate_rollup_schema_present(self) -> bool:
        """Report whether the incremental hashrate rollup tables exist.

        The serving statement cannot even parse against a database that
        predates the rollup DDL, so table existence has to be settled before
        choosing a statement. A positive answer is cached for the life of
        this instance: the rollup tables are only ever created (by the
        writer's idempotent schema apply), never dropped. A negative answer
        is deliberately not cached, so a read tier pointed at a database
        whose writer applies the DDL mid-flight picks up the rollup path
        without a restart.
        """
        if getattr(self, "_hashrate_rollup_schema_ready", False):
            return True
        row = self._run_read_json(
            """
SELECT json_build_object(
    'rollup_schema_ready',
    to_regclass('qbit_hashrate_rollup_progress') IS NOT NULL
    AND to_regclass('qbit_hashrate_rollup_pool') IS NOT NULL
    AND to_regclass('qbit_hashrate_rollup_miner') IS NOT NULL
);
"""
        )
        ready = bool(row["rollup_schema_ready"])
        if ready:
            self._hashrate_rollup_schema_ready = True
        return ready

    def advance_hashrate_rollups(self, *, batch_limit: int) -> dict[str, object]:
        """Fold the next watermarked share batch into the hashrate rollups.

        One statement, one transaction. ``qbit_share_ledger`` rows are
        immutable and ``share_seq`` is append-only, so scanning strictly
        above the stored watermark in sequence order folds every share into
        its (grain, bucket) rows exactly once -- a late-clocked share still
        lands in its correct ``accepted_at`` bucket, and no re-aggregation
        window is needed. Rejected rows are read only to advance the
        watermark. The first pass seeds the progress row itself and starts
        from sequence 0, which is also how a grown ledger backfills.

        The watermark advance is a guarded upsert: it only applies while the
        progress row still holds the value this statement read, and the
        rollup upserts are gated on that advance having won. A concurrent
        advance therefore leaves this pass writing nothing at all -- the
        guarded upsert matches no row and both rollup inserts see an empty
        gate -- and the caller raises instead of double-counting, because
        two live maintenance passes mean the single-writer invariant is
        already broken.

        The whole pass is additionally fenced on the writer lease, exactly
        like every other mutation of ledger-derived state: the statement
        renews the exact ``(writer_id, writer_epoch, writer_session_token)``
        lease tuple and the advance applies only when that renewal matched,
        so a coordinator whose lease expired or was taken over cannot keep
        mutating the rollups on the strength of its process-local lock
        alone. A fenced-out pass writes nothing and raises.
        """
        batch_limit = int(batch_limit)
        if batch_limit <= 0:
            raise ValueError("hashrate rollup batch limit must be positive")
        sql = f"""
WITH lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
),
progress AS (
    SELECT COALESCE((
        SELECT last_share_seq
        FROM qbit_hashrate_rollup_progress
        WHERE singleton
    ), 0) AS last_share_seq
),
batch AS (
    SELECT
        ledger.share_seq,
        ledger.accepted,
        ledger.accepted_at,
        ledger.miner_id,
        ledger.share_difficulty
    FROM qbit_share_ledger ledger
    WHERE ledger.share_seq > (SELECT last_share_seq FROM progress)
    ORDER BY ledger.share_seq ASC
    LIMIT {batch_limit}
),
batch_stats AS (
    SELECT
        count(*) AS scanned,
        COALESCE(max(batch.share_seq), (SELECT last_share_seq FROM progress)) AS next_share_seq
    FROM batch
),
advance AS (
    INSERT INTO qbit_hashrate_rollup_progress (singleton, last_share_seq)
    SELECT true, (SELECT next_share_seq FROM batch_stats)
    WHERE EXISTS (SELECT 1 FROM lease)
    ON CONFLICT (singleton) DO UPDATE
        SET last_share_seq = EXCLUDED.last_share_seq,
            updated_at = clock_timestamp()
        WHERE qbit_hashrate_rollup_progress.last_share_seq = (SELECT last_share_seq FROM progress)
    RETURNING last_share_seq
),
grains AS (
    SELECT grain_seconds
    FROM (VALUES (300), (3600), (86400)) AS grain(grain_seconds)
),
pool_rollup AS (
    INSERT INTO qbit_hashrate_rollup_pool (
        grain_seconds,
        bucket_epoch,
        accepted_share_count,
        accepted_share_difficulty
    )
    SELECT
        grains.grain_seconds,
        floor(extract(epoch FROM batch.accepted_at) / grains.grain_seconds)::bigint * grains.grain_seconds AS bucket_epoch,
        count(*) AS accepted_share_count,
        sum(batch.share_difficulty) AS accepted_share_difficulty
    FROM batch, grains
    WHERE batch.accepted
      AND EXISTS (SELECT 1 FROM advance)
    GROUP BY grains.grain_seconds, bucket_epoch
    ON CONFLICT (grain_seconds, bucket_epoch) DO UPDATE
        SET accepted_share_count = qbit_hashrate_rollup_pool.accepted_share_count
                + EXCLUDED.accepted_share_count,
            accepted_share_difficulty = qbit_hashrate_rollup_pool.accepted_share_difficulty
                + EXCLUDED.accepted_share_difficulty
    RETURNING 1
),
miner_rollup AS (
    INSERT INTO qbit_hashrate_rollup_miner (
        grain_seconds,
        bucket_epoch,
        miner_id,
        accepted_share_count,
        accepted_share_difficulty
    )
    SELECT
        grains.grain_seconds,
        floor(extract(epoch FROM batch.accepted_at) / grains.grain_seconds)::bigint * grains.grain_seconds AS bucket_epoch,
        batch.miner_id,
        count(*) AS accepted_share_count,
        sum(batch.share_difficulty) AS accepted_share_difficulty
    FROM batch, grains
    WHERE batch.accepted
      AND EXISTS (SELECT 1 FROM advance)
    GROUP BY grains.grain_seconds, bucket_epoch, batch.miner_id
    ON CONFLICT (grain_seconds, bucket_epoch, miner_id) DO UPDATE
        SET accepted_share_count = qbit_hashrate_rollup_miner.accepted_share_count
                + EXCLUDED.accepted_share_count,
            accepted_share_difficulty = qbit_hashrate_rollup_miner.accepted_share_difficulty
                + EXCLUDED.accepted_share_difficulty
    RETURNING 1
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM lease) THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'scanned', (SELECT scanned FROM batch_stats),
            'last_share_seq', (SELECT next_share_seq FROM batch_stats),
            'advanced', (SELECT count(*) FROM advance),
            'caught_up', (SELECT scanned FROM batch_stats) < {batch_limit}
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        if int(result["advanced"]) != 1:
            raise RuntimeError(
                "hashrate rollup watermark advanced concurrently; "
                "this pass wrote nothing (writer-fencing bug)"
            )
        return {
            "scanned": int(result["scanned"]),
            "last_share_seq": int(result["last_share_seq"]),
            "caught_up": bool(result["caught_up"]),
        }

    def _audit_store(self) -> AuditArtifactStore:
        store = getattr(self, "_audit_artifact_store", None)
        if store is None:
            raise RuntimeError("audit body store is not configured")
        return store

    def _stored_canonical_audit_bundle_bytes(
        self,
        block_hash: str,
        audit_bundle_sha256: str,
    ) -> bytes | None:
        """Return a verified stored canonical bundle, or permit missing fallback.

        Missing artifacts are expected for history predating issue #158.
        Present-but-corrupt or unreadable artifacts fail closed instead of
        silently serving a non-byte-exact legacy reconstruction.
        """
        store = getattr(self, "_audit_artifact_store", None)
        reader = getattr(store, "read_canonical_audit_bundle", None)
        if not callable(reader):
            LOGGER.warning(
                "canonical audit bundle unavailable reason=missing "
                "block_hash=%s audit_bundle_sha256=%s",
                block_hash,
                audit_bundle_sha256,
            )
            return None
        try:
            canonical_bytes = reader(block_hash, audit_bundle_sha256)
        except CanonicalAuditBundleCorrupt:
            LOGGER.error(
                "canonical audit bundle unavailable reason=corrupt "
                "block_hash=%s audit_bundle_sha256=%s",
                block_hash,
                audit_bundle_sha256,
            )
            raise
        except (OSError, RuntimeError, TypeError, ValueError):
            LOGGER.exception(
                "canonical audit bundle unavailable reason=read_error "
                "block_hash=%s audit_bundle_sha256=%s",
                block_hash,
                audit_bundle_sha256,
            )
            raise
        if canonical_bytes is None:
            LOGGER.warning(
                "canonical audit bundle unavailable reason=missing "
                "block_hash=%s audit_bundle_sha256=%s",
                block_hash,
                audit_bundle_sha256,
            )
            return None
        if not isinstance(canonical_bytes, bytes):
            LOGGER.error(
                "canonical audit bundle unavailable reason=read_error "
                "block_hash=%s audit_bundle_sha256=%s",
                block_hash,
                audit_bundle_sha256,
            )
            raise TypeError("canonical audit bundle reader returned non-bytes")
        return canonical_bytes

    def _audit_reader(self, body_uri: object) -> AuditArtifactStore:
        del body_uri
        store = getattr(self, "_audit_artifact_store", None)
        if store is not None:
            return store
        # Compatibility-only dashboard resolvers historically initialized a
        # read-only PsqlShareLedger subclass with just `_audit_body_dir`.  Keep
        # that explicit adapter working while routing every read through A1.
        legacy_root = getattr(self, "_audit_body_dir", None)
        if legacy_root is not None:
            root = Path(legacy_root)
            store = AuditArtifactStore(
                AuditArtifactConfig(
                    root=root,
                    evidence_path=root / "prism-live-stratum-evidence.json",
                ),
                canonicalizer=(
                    getattr(self, "_audit_bundle_canonicalizer", None)
                    or _default_bundle_canonicalizer()
                ),
            )
            self._audit_artifact_store = store
            return store
        raise RuntimeError(
            "audit bundle body is not retrievable: audit body store is not configured"
        )

    def _externalize_audit_body(self, block_hash: str, audit_bundle_sha256: str, final_bundle: dict[str, Any]) -> str | None:
        if self._audit_artifact_store is None:
            return None
        return self._audit_store().externalize_audit_body(block_hash, audit_bundle_sha256, final_bundle)

    def _canonical_audit_body_bytes_for_sha(self, final_bundle: dict[str, Any], audit_bundle_sha256: str) -> bytes:
        return self._audit_store().canonical_audit_body_bytes_for_sha(final_bundle, audit_bundle_sha256)

    def _audit_body_ref(self, **kwargs: Any) -> dict[str, Any] | None:
        return self._audit_store().audit_body_ref(**kwargs)

    def _audit_bundle_v2(self, **kwargs: Any) -> dict[str, Any] | None:
        kwargs.setdefault("load_missing_range", self._load_audit_share_ledger_range)
        return self._audit_store().audit_bundle_v2(**kwargs)

    def _audit_share_parts(self, shares: list[Any]) -> list[dict[str, Any]] | None:
        return self._audit_store().audit_share_parts(shares)

    def _audit_share_range_parts(self, shares: list[Any]) -> list[dict[str, Any]] | None:
        return self._audit_store().audit_share_range_parts(
            shares,
            load_missing_range=self._load_audit_share_ledger_range,
        )

    def _audit_share_segment_payload(self, **kwargs: Any) -> dict[str, Any]:
        return self._audit_store().audit_share_segment_payload(**kwargs)

    def _write_audit_share_segment(self, **kwargs: Any) -> tuple[str, str]:
        return self._audit_store().write_audit_share_segment(**kwargs)

    def _write_audit_share_segment_range(self, **kwargs: Any) -> tuple[str, str]:
        kwargs.setdefault("load_missing_range", self._load_audit_share_ledger_range)
        return self._audit_store().write_audit_share_segment_range(**kwargs)

    def _merge_audit_share_ranges(self, existing_shares: list[Any], incoming_shares: list[Any], *, segment_path: Path) -> list[Any] | None:
        return self._audit_store().merge_audit_share_ranges(
            existing_shares,
            incoming_shares,
            segment_path=segment_path,
            load_missing_range=self._load_audit_share_ledger_range,
        )

    def _load_audit_share_ledger_range(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
    ) -> list[dict[str, object]]:
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'share_seq', share_seq,
    'share_id', share_id,
    'miner_id', miner_id,
    'order_key', payout_order_key,
    'p2mr_program_hex', encode(p2mr_program, 'hex'),
    'share_difficulty', share_difficulty::text,
    'network_difficulty', network_difficulty::text,
    'template_height', template_height,
    'job_id', job_id,
    'job_issued_at_ms', round(extract(epoch FROM job_issued_at) * 1000)::bigint,
    'accepted_at_ms', round(extract(epoch FROM accepted_at) * 1000)::bigint,
    'ntime', ntime,
    'credit_policy', credit_policy
) ORDER BY share_seq ASC), '[]'::json)
FROM qbit_share_ledger
WHERE accepted
  AND share_seq BETWEEN {int(first_share_seq)} AND {int(last_share_seq)};
"""
        rows = self._run_read_json(sql)
        if not isinstance(rows, list):
            raise RuntimeError("audit share ledger backfill did not return a list")
        return [self._record_from_json(row).to_prism_json() for row in rows]

    def _audit_shares_by_seq(self, shares: list[Any], *, segment_path: Path, require_contiguous: bool = True) -> dict[int, Any]:
        return self._audit_store().audit_shares_by_seq(
            shares,
            segment_path=segment_path,
            require_contiguous=require_contiguous,
        )

    def _storage_json_bytes(self, payload: dict[str, Any]) -> bytes:
        return self._audit_store().storage_json_bytes(payload)

    @staticmethod
    def _file_sha256_hex(path: Path) -> str:
        return AuditArtifactStore.file_sha256_hex(path)

    def _canonical_audit_bundle_bytes(self, final_bundle: dict[str, Any]) -> bytes:
        if self._audit_artifact_store is None:
            return canonical_audit_bundle_bytes(
                final_bundle,
                self._audit_bundle_canonicalizer,
            )
        return self._audit_store().canonical_audit_bundle_bytes(final_bundle)

    def _audit_body_path(self, block_hash: str, audit_bundle_sha256: str) -> Path:
        return self._audit_store().body_path(block_hash, audit_bundle_sha256)

    def _resolve_audit_body_path(self, body_uri: object) -> Path:
        return self._audit_store().resolve_owned_path(body_uri)

    def _external_audit_body_write_plan(self, payload: dict[str, Any]) -> str | None:
        """Refresh the writer lease and decide whether this persist may write a body.

        The audit body store lives outside Postgres, so file writes cannot be in
        the same transaction as the ledger insert. This preflight keeps stale
        writers from creating artifacts by requiring the DB lease to be current
        before any filesystem side effect.
        """
        if self._audit_artifact_store is None:
            return None
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
existing_block AS (
    SELECT
        block_hash,
        block_height,
        parent_hash,
        coinbase_txid,
        payout_manifest_sha256
    FROM qbit_pool_blocks
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_existing_block AS (
    SELECT existing_block.block_hash
    FROM existing_block, payload
    WHERE existing_block.block_height = (data->>'block_height')::bigint
      AND existing_block.parent_hash = data->>'parent_hash'
      AND existing_block.coinbase_txid = data->>'coinbase_txid'
      AND existing_block.payout_manifest_sha256 = data->>'payout_manifest_sha256'
),
existing_bundle AS (
    SELECT block_hash, audit_bundle_sha256, coinbase_tx_hex, body_uri
    FROM qbit_pool_audit_bundles
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_existing_bundle AS (
    SELECT existing_bundle.block_hash
    FROM existing_bundle, payload
    WHERE existing_bundle.audit_bundle_sha256 = data->>'audit_bundle_sha256'
      AND existing_bundle.coinbase_tx_hex = data->>'coinbase_tx_hex'
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_block) = 0 THEN
        json_build_object('error', 'existing block metadata does not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_bundle) = 0 THEN
        json_build_object('error', 'existing audit bundle does not match payload')
    ELSE
        json_build_object(
            'existing_block', (SELECT count(*) FROM existing_block) > 0,
            'existing_body_uri', (SELECT body_uri FROM existing_bundle LIMIT 1)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        existing_body_uri = result.get("existing_body_uri")
        if existing_body_uri:
            return str(existing_body_uri)
        if result.get("existing_block"):
            return None
        return str(self._audit_body_path(payload["block_hash"], payload["audit_bundle_sha256"]))

    def preflight_canonical_bundle_publication(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
    ) -> dict[str, object]:
        """Refresh the writer lease and confirm one block/digest row identity.

        The standalone canonical-bundle backfill calls this immediately before
        each publication. It is the same fence `_external_audit_body_write_plan`
        applies to the live path: a writer whose lease has lapsed must not keep
        creating artifacts under the audit root, and the digest it is about to
        publish must be the one the ledger row already advertises. Reads the
        audit tables only; the sole mutation is the writer's own lease
        heartbeat, so no schema changes.

        Returns the row's `body_uri` (None for legacy inline rows) and whether
        an inline body is still present, which is what the backfill needs to
        choose its byte source. Raises when the lease is not active or the
        block/digest identity does not match.
        """

        block_hash = canonical_hex(str(block_hash), name="block_hash", expected_bytes=32)
        digest = canonical_hex(
            str(audit_bundle_sha256),
            name="audit_bundle_sha256",
            expected_bytes=32,
        )
        payload = {
            "block_hash": block_hash,
            "audit_bundle_sha256": digest,
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
bundle AS (
    SELECT
        block_hash,
        audit_bundle_sha256,
        body_uri,
        audit_bundle IS NOT NULL AS has_inline_bundle
    FROM qbit_pool_audit_bundles
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_bundle AS (
    SELECT bundle.*
    FROM bundle, payload
    WHERE bundle.audit_bundle_sha256 = data->>'audit_bundle_sha256'
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM bundle) = 0 THEN
        json_build_object('error', 'audit bundle row does not exist')
    WHEN (SELECT count(*) FROM matching_bundle) = 0 THEN
        json_build_object('error', 'audit bundle digest does not match the stored row')
    ELSE
        json_build_object(
            'block_hash', (SELECT block_hash FROM matching_bundle LIMIT 1),
            'audit_bundle_sha256', (SELECT audit_bundle_sha256 FROM matching_bundle LIMIT 1),
            'body_uri', (SELECT body_uri FROM matching_bundle LIMIT 1),
            'has_inline_bundle', (SELECT has_inline_bundle FROM matching_bundle LIMIT 1)
        )
END;
"""
        result = self._run_fenced_json(sql)
        if not isinstance(result, dict):
            raise RuntimeError("canonical bundle preflight returned no row")
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        body_uri = result.get("body_uri")
        return {
            "block_hash": block_hash,
            "audit_bundle_sha256": digest,
            "body_uri": str(body_uri) if body_uri else None,
            "has_inline_bundle": bool(result.get("has_inline_bundle")),
        }

    def canonical_bundle_bytes_for_backfill(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        body_uri: object,
    ) -> bytes:
        """Recover verified canonical bytes for an already published body.

        Binds the ledger's append-only share loader to the store helper so the
        standalone backfill can repair share segments that have left the disk
        without reaching into ledger internals. Every recovered range is checked
        against the digest the body already commits to, and the assembled bundle
        against the advertised bundle digest.
        """

        return self._audit_store().canonical_bundle_bytes_from_external_body(
            block_hash=block_hash,
            audit_bundle_sha256=audit_bundle_sha256,
            body_uri=body_uri,
            load_missing_range=self._load_audit_share_ledger_range,
        )

    def publish_canonical_bundle_bytes(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        canonical_bytes: bytes,
    ) -> str:
        """Publish canonical bytes under the store's ownership checks.

        Callers must run `preflight_canonical_bundle_publication` immediately
        before this so the writer lease is current when the artifact lands.
        """

        return str(
            self._audit_store().write_canonical_audit_bundle(
                block_hash,
                audit_bundle_sha256,
                canonical_bytes,
            )
        )

    def _audit_body_byte_len(self, body_uri: object | None, final_bundle: dict[str, Any], canonical_bundle_path: Path | None = None) -> int:
        if self._audit_artifact_store is None:
            return len(self._canonical_audit_bundle_bytes(final_bundle))
        return self._audit_store().audit_body_byte_len(body_uri, final_bundle, canonical_bundle_path)

    def _prepare_external_audit_body(self, payload: dict[str, Any], final_bundle: dict[str, Any], *, canonical_bundle_path: Path | None = None) -> str | None:
        if self._audit_artifact_store is None:
            return None
        normalized = {
            **payload,
            "block_hash": canonical_hex(str(payload["block_hash"]), name="block_hash", expected_bytes=32),
            "audit_bundle_sha256": canonical_hex(str(payload["audit_bundle_sha256"]), name="audit_bundle_sha256", expected_bytes=32),
        }
        if canonical_bundle_path is not None:
            self._audit_store().validate_canonical_source(
                canonical_bundle_path,
                str(normalized["audit_bundle_sha256"]),
                final_bundle,
            )
        body_uri = self._external_audit_body_write_plan(normalized)
        return self._audit_store().prepare_external_audit_body(
            normalized,
            final_bundle,
            body_uri=body_uri,
            canonical_bundle_path=canonical_bundle_path,
            load_missing_range=self._load_audit_share_ledger_range,
        )

    def _read_external_body(self, body_uri: object, *, expected_sha256: object | None = None) -> dict[str, object] | None:
        return self._audit_reader(body_uri).read_external_body(body_uri, expected_sha256=expected_sha256)

    def _external_body_matches_sha(self, body_path: Path, expected_sha256: str) -> bool:
        return self._audit_reader(body_path).external_body_matches_sha(body_path, expected_sha256)

    def _external_body_available_for_sha(self, body_uri: object, expected_sha256: str) -> bool:
        try:
            reader = self._audit_reader(body_uri)
        except RuntimeError:
            return False
        return reader.external_body_available_for_sha(body_uri, expected_sha256)

    def _audit_share_segment_available(self, part: dict[str, Any], *, parent_body_uri: object) -> bool:
        try:
            self._audit_store().read_audit_share_segment(part, parent_body_uri=parent_body_uri)
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        return True

    def _resolve_audit_body_ref(self, body_ref: dict[str, Any], *, expected_sha256: object | None, body_uri: object) -> dict[str, object]:
        return self._audit_store().resolve_audit_body_ref(body_ref, expected_sha256=expected_sha256, body_uri=body_uri)

    def _resolve_audit_bundle_v2(self, body: dict[str, Any], *, expected_sha256: object | None, body_uri: object) -> dict[str, object]:
        return self._audit_store().resolve_audit_bundle_v2(body, expected_sha256=expected_sha256, body_uri=body_uri)

    def _read_audit_share_segment(self, part: dict[str, Any], *, parent_body_uri: object) -> list[Any]:
        return self._audit_store().read_audit_share_segment(part, parent_body_uri=parent_body_uri)

    def _select_audit_share_segment_range(self, shares: list[Any], *, first_share_seq: int, last_share_seq: int, parent_body_uri: object, body_uri: object) -> list[Any]:
        return self._audit_store().select_audit_share_segment_range(
            shares,
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            parent_body_uri=parent_body_uri,
            body_uri=body_uri,
        )

    def _resolve_audit_bundle_row(self, row: object) -> dict[str, object] | None:
        """Return an audit-bundle row with its body resolved inline.

        Reads the externalized body from body_uri when the inline JSONB is NULL,
        so legacy inline rows and externalized rows present an identical shape to
        callers.
        """
        if not isinstance(row, dict):
            return None
        result = dict(row)
        body = result.get("audit_bundle")
        if body is None:
            body = self._read_external_body(
                result.get("body_uri"),
                expected_sha256=result.get("audit_bundle_sha256"),
            )
        result.pop("body_uri", None)
        if body is None:
            return None
        result["audit_bundle"] = body
        return result

    def persist_accepted_block(
        self,
        *,
        block_hash: str,
        block_height: int,
        parent_hash: str,
        final_bundle: dict[str, Any],
        audit_report: dict[str, Any],
        canonical_bundle_path: Path | None = None,
    ) -> dict[str, int | str]:
        manifest = final_bundle["signed_coinbase_manifest"]["manifest"]
        found_block = final_bundle.get("found_block") or {}
        audit_bundle_sha256 = canonical_hex(
            str(audit_report["audit_bundle_sha256_hex"]),
            name="audit_bundle_sha256",
            expected_bytes=32,
        )
        block_hash = canonical_hex(str(block_hash), name="block_hash", expected_bytes=32)
        parent_hash = canonical_hex(str(parent_hash), name="parent_hash", expected_bytes=32)
        payload = {
            "block_hash": block_hash,
            "block_height": block_height,
            "parent_hash": parent_hash,
            "coinbase_txid": audit_report["coinbase_txid"],
            "payout_manifest_sha256": audit_report["coinbase_manifest_sha256_hex"],
            "audit_bundle_sha256": audit_bundle_sha256,
            "coinbase_tx_hex": audit_report["coinbase_tx_hex"],
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        body_uri = self._prepare_external_audit_body(
            payload,
            final_bundle,
            canonical_bundle_path=canonical_bundle_path,
        )
        audit_body_byte_len = self._audit_body_byte_len(
            body_uri,
            final_bundle,
            canonical_bundle_path,
        )
        payload = {
            **payload,
            # Externalized rows store the body in body_uri and NULL here; legacy
            # rows (no body store configured) keep the inline body.
            "audit_bundle": None if body_uri is not None else final_bundle,
            "body_uri": body_uri,
            "audit_body_byte_len": audit_body_byte_len,
            "schema_version": str(final_bundle.get("schema") or "qbit.prism.audit-bundle.v1"),
            "found_block_network_difficulty": found_block.get("network_difficulty"),
            "found_block_bits": found_block.get("bits"),
            "found_block_coinbase_value_sats": found_block.get("coinbase_value_sats"),
            "audit_commitment_leaves_hex": final_bundle.get("audit_commitment_leaves_hex"),
            "witness_merkle_leaves_hex": final_bundle.get("witness_merkle_leaves_hex"),
            "accounts": final_bundle["payout_policy_manifest"]["accounts"],
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
),
existing_block AS (
    SELECT
        block_hash,
        block_height,
        parent_hash,
        coinbase_txid,
        payout_manifest_sha256
    FROM qbit_pool_blocks
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
inserted_block AS (
    INSERT INTO qbit_pool_blocks (
        block_hash,
        block_height,
        parent_hash,
        coinbase_txid,
        payout_manifest_sha256
    )
    SELECT
        data->>'block_hash',
        (data->>'block_height')::bigint,
        data->>'parent_hash',
        data->>'coinbase_txid',
        data->>'payout_manifest_sha256'
    FROM payload, lease
    WHERE NOT EXISTS (SELECT 1 FROM existing_block)
    RETURNING block_hash
),
accounts AS (
    SELECT
        data,
        account->>'recipient_id' AS miner_id,
        account->>'order_key' AS payout_order_key,
        decode(account->>'p2mr_program_hex', 'hex') AS p2mr_program,
        (account->>'gross_amount_sats')::bigint AS gross_amount_sats,
        (account->>'prior_balance_sats')::numeric AS prior_balance_sats,
        (account->>'candidate_balance_sats')::numeric AS candidate_balance_sats,
        (account->>'onchain_amount_sats')::bigint AS onchain_amount_sats,
        COALESCE((account->>'settlement_fee_sats')::bigint, 0) AS settlement_fee_sats,
        (account->>'carry_forward_balance_sats')::numeric AS carry_forward_balance_sats,
        account->>'action' AS action
    FROM payload,
         jsonb_array_elements(data->'accounts') AS account
),
bundle_insert AS (
    INSERT INTO qbit_pool_audit_bundles (
        block_hash,
        audit_bundle,
        audit_bundle_sha256,
        coinbase_tx_hex,
        body_uri,
        audit_body_byte_len,
        schema_version,
        found_block_network_difficulty,
        found_block_bits,
        found_block_coinbase_value_sats,
        audit_commitment_leaves_hex,
        witness_merkle_leaves_hex
    )
    SELECT
        data->>'block_hash',
        CASE WHEN jsonb_typeof(data->'audit_bundle') = 'object' THEN data->'audit_bundle' ELSE NULL END,
        data->>'audit_bundle_sha256',
        data->>'coinbase_tx_hex',
        data->>'body_uri',
        (data->>'audit_body_byte_len')::bigint,
        data->>'schema_version',
        (data->>'found_block_network_difficulty')::numeric,
        data->>'found_block_bits',
        (data->>'found_block_coinbase_value_sats')::bigint,
        CASE WHEN jsonb_typeof(data->'audit_commitment_leaves_hex') = 'array' THEN data->'audit_commitment_leaves_hex' ELSE NULL END,
        CASE WHEN jsonb_typeof(data->'witness_merkle_leaves_hex') = 'array' THEN data->'witness_merkle_leaves_hex' ELSE NULL END
    FROM payload, inserted_block
    RETURNING block_hash
),
payout_insert AS (
    INSERT INTO qbit_pool_payout_entries (
        block_hash,
        block_height,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    )
    SELECT
        data->>'block_hash',
        (data->>'block_height')::bigint,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    FROM accounts, inserted_block
    RETURNING payout_entry_seq
),
carry_insert AS (
    INSERT INTO qbit_payout_carry_forward (
        block_height,
        block_hash,
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    )
    SELECT
        (data->>'block_height')::bigint,
        data->>'block_hash',
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    FROM accounts, inserted_block
    RETURNING carry_forward_seq
),
matching_existing_block AS (
    SELECT existing_block.block_hash
    FROM existing_block, payload
    WHERE existing_block.block_height = (data->>'block_height')::bigint
      AND existing_block.parent_hash = data->>'parent_hash'
      AND existing_block.coinbase_txid = data->>'coinbase_txid'
      AND existing_block.payout_manifest_sha256 = data->>'payout_manifest_sha256'
),
existing_bundle AS (
    SELECT block_hash, audit_bundle_sha256, coinbase_tx_hex
    FROM qbit_pool_audit_bundles
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
matching_existing_bundle AS (
    -- audit_bundle_sha256 is computed over the full bundle content, so matching
    -- it (plus the coinbase tx) proves an identical body without comparing the
    -- JSONB directly, which is NULL for externalized rows.
    SELECT existing_bundle.block_hash
    FROM existing_bundle, payload
    WHERE existing_bundle.audit_bundle_sha256 = data->>'audit_bundle_sha256'
      AND existing_bundle.coinbase_tx_hex = data->>'coinbase_tx_hex'
),
expected_payout_rows AS (
    SELECT
        data->>'block_hash' AS block_hash,
        (data->>'block_height')::bigint AS block_height,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    FROM accounts
),
existing_payout_rows AS (
    SELECT
        block_hash,
        block_height,
        miner_id,
        payout_order_key,
        p2mr_program,
        onchain_amount_sats,
        carry_forward_balance_sats,
        action
    FROM qbit_pool_payout_entries
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
payout_missing AS (
    SELECT * FROM expected_payout_rows
    EXCEPT ALL
    SELECT * FROM existing_payout_rows
),
payout_extra AS (
    SELECT * FROM existing_payout_rows
    EXCEPT ALL
    SELECT * FROM expected_payout_rows
),
expected_carry_rows AS (
    SELECT
        (data->>'block_height')::bigint AS block_height,
        data->>'block_hash' AS block_hash,
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    FROM accounts
),
existing_carry_rows AS (
    SELECT
        block_height,
        block_hash,
        miner_id,
        payout_order_key,
        p2mr_program,
        gross_amount_sats,
        prior_balance_sats,
        candidate_balance_sats,
        onchain_amount_sats,
        settlement_fee_sats,
        carry_forward_balance_sats,
        action
    FROM qbit_payout_carry_forward
    WHERE block_hash = (SELECT data->>'block_hash' FROM payload)
),
carry_missing AS (
    SELECT * FROM expected_carry_rows
    EXCEPT ALL
    SELECT * FROM existing_carry_rows
),
carry_extra AS (
    SELECT * FROM existing_carry_rows
    EXCEPT ALL
    SELECT * FROM expected_carry_rows
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_block) = 0 THEN
        json_build_object('error', 'existing block metadata does not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (SELECT count(*) FROM matching_existing_bundle) = 0 THEN
        json_build_object('error', 'existing audit bundle does not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (
          EXISTS (SELECT 1 FROM payout_missing)
          OR EXISTS (SELECT 1 FROM payout_extra)
      ) THEN
        json_build_object('error', 'existing payout entries do not match payload')
    WHEN (SELECT count(*) FROM existing_block) > 0
      AND (
          EXISTS (SELECT 1 FROM carry_missing)
          OR EXISTS (SELECT 1 FROM carry_extra)
      ) THEN
        json_build_object('error', 'existing carry-forward rows do not match payload')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'share_count', (SELECT count(*) FROM qbit_share_ledger WHERE accepted),
            'block_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM inserted_block)
                ELSE (SELECT count(*) FROM existing_block)
            END,
            'bundle_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM bundle_insert)
                ELSE (SELECT count(*) FROM existing_bundle)
            END,
            'payout_entry_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM payout_insert)
                ELSE (SELECT count(*) FROM existing_payout_rows)
            END,
            'carry_forward_count', CASE
                WHEN (SELECT count(*) FROM inserted_block) > 0 THEN (SELECT count(*) FROM carry_insert)
                ELSE (SELECT count(*) FROM existing_carry_rows)
            END,
            'onchain_output_count', {int(manifest["payout_count"])}
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "share_count": int(result["share_count"]),
            "block_count": int(result["block_count"]),
            "bundle_count": int(result["bundle_count"]),
            "payout_entry_count": int(result["payout_entry_count"]),
            "carry_forward_count": int(result["carry_forward_count"]),
            "onchain_output_count": int(result["onchain_output_count"]),
            "audit_bundle_sha256": audit_bundle_sha256,
            "body_uri": str(body_uri) if body_uri is not None else "",
            "audit_body_byte_len": audit_body_byte_len,
        }

    def reverse_immature_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'reversed_count', qbit_reverse_immature_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "reversed_count": int(result["reversed_count"]),
        }

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'rejected_count', qbit_reject_prepared_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "rejected_count": int(result["rejected_count"]),
        }

    def confirm_accepted_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'confirmed_count', qbit_confirm_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            result = self._run_json(sql)
            if "error" in result:
                raise RuntimeError(str(result["error"]))
            confirmed_count = int(result["confirmed_count"])
            publication_sequence: object | None = None
            if confirmed_count in {1, 2}:
                # Both confirming dispositions leave a confirmed row carrying
                # an ordinal: 1 allocated one in the statement above, and 2 --
                # the idempotent replay -- returns the one its original flip
                # allocated. The caller needs the ordinal either way to
                # address this block's audit publication, so read it for
                # both. Non-confirming dispositions (0, -1) have no row to
                # read and stay a single statement.
                #
                # A data-modifying PL/pgSQL function runs under the statement's
                # command snapshot, so a join in that same statement cannot see
                # the freshly assigned ordinal. Read it in the next statement
                # while retaining the ledger writer lock.
                #
                # Holding the lock across both statements is what makes this
                # step's heartbeat-silent span two statement budgets rather
                # than one: no admission slice runs in between, so a landing
                # caller's liveness monitor gets nothing until the second
                # statement returns. Report the completed first round trip
                # before starting the second (see _note_operation_progress).
                self._note_operation_progress()
                state = self._run_retry_safe_read_json(
                    f"""
SELECT json_build_object(
    'audit_publication_sequence', (
        SELECT audit_publication_sequence
        FROM qbit_pool_blocks
        WHERE block_hash = {self._text_literal(block_hash)}
          AND block_height = {int(active_tip_height)}
          AND chain_state = 'confirmed'
          AND maturity_state <> 'reversed'
    )
);
"""
                )
                publication_sequence = state.get("audit_publication_sequence")
                if publication_sequence is None:
                    raise RuntimeError(
                        "confirmed pool block has no audit publication sequence"
                    )
        response: dict[str, int | str] = {
            "backend": str(result["backend"]),
            "confirmed_count": confirmed_count,
        }
        if publication_sequence is not None:
            response["audit_publication_sequence"] = int(publication_sequence)
        return response

    def pool_block_state(self, *, block_hash: str) -> dict[str, object] | None:
        block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
        sql = f"""
SELECT json_build_object(
    'state', (
        SELECT json_build_object(
            'block_hash', block_hash,
            'block_height', block_height,
            'parent_hash', parent_hash,
            'chain_state', chain_state,
            'maturity_state', maturity_state,
            'audit_publication_sequence', audit_publication_sequence
        )
        FROM qbit_pool_blocks
        WHERE block_hash = {self._text_literal(block_hash)}
    )
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            result = self._run_retry_safe_read_json(sql)
        if not isinstance(result, dict):
            raise RuntimeError("pool block state query returned non-object JSON")
        state = result.get("state")
        if state is None:
            return None
        if not isinstance(state, dict):
            raise RuntimeError("pool block state query returned non-object JSON")
        state["block_height"] = int(state["block_height"])
        return state

    def audit_publication_sequence_floor(self) -> int:
        """Return MAX durable pool-block ordinal, excluding sequence gaps."""

        sql = """
SELECT json_build_object(
    'audit_publication_sequence_floor',
    COALESCE(MAX(audit_publication_sequence), 0)
)
FROM qbit_pool_blocks;
"""
        with self._operation_gate(self._lock, "writer lock"):
            result = self._run_retry_safe_read_json(sql)
        if not isinstance(result, dict):
            raise RuntimeError(
                "audit publication sequence floor query returned non-object JSON"
            )
        value = result.get("audit_publication_sequence_floor")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeError("audit publication sequence floor is invalid")
        return value

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        sql = """
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'block_height', block_height,
    'parent_hash', parent_hash,
    'chain_state', chain_state,
    'maturity_state', maturity_state
) ORDER BY block_height ASC, block_hash ASC), '[]'::json)
FROM qbit_pool_blocks
WHERE chain_state IN ('confirmed', 'inactive')
  AND maturity_state = 'immature'
;
"""
        with self._operation_gate(self._lock, "writer lock"):
            rows = self._run_retry_safe_read_json(sql)
        for row in rows:
            row["block_height"] = int(row["block_height"])
        return rows

    def stranded_prepared_blocks(
        self,
        *,
        active_tip_height: int,
        min_depth: int,
        limit: int = 64,
    ) -> list[dict[str, object]]:
        """Return deeply buried rows still parked in the prepared state.

        ``reorg_watch_blocks`` deliberately watches only confirmed/inactive
        rows, and a prepared row is normally resolved by the live
        submit/replay path that owns it through its outbox entry. A row
        whose outbox entry is gone (quarantined, or completed by a process
        that died before confirming) therefore has nothing left to
        re-examine it, and stays prepared forever — holding immature payout
        entries, carry-forward, and CTV fanout artifacts open with it. This
        read finds those rows; the caller decides, against the active chain,
        which ones are provably orphaned.

        The predicate leads with ``maturity_state`` and ``block_height`` so
        it rides ``qbit_pool_blocks_maturity_idx``, and the depth floor
        keeps the scan to rows the caller could actually act on.
        """
        if limit <= 0:
            return []
        depth_ceiling = int(active_tip_height) - int(min_depth)
        sql = f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'block_height', block_height,
    'parent_hash', parent_hash
) ORDER BY block_height ASC, block_hash ASC), '[]'::json)
FROM (
    SELECT block_hash, block_height, parent_hash
    FROM qbit_pool_blocks
    WHERE maturity_state = 'immature'
      AND block_height <= {depth_ceiling}
      AND chain_state = 'prepared'
    ORDER BY block_height ASC, block_hash ASC
    LIMIT {int(limit)}
) stranded;
"""
        with self._operation_gate(self._lock, "writer lock"):
            rows = self._run_retry_safe_read_json(sql)
        for row in rows:
            row["block_height"] = int(row["block_height"])
        return rows

    def mark_pool_block_inactive(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'inactive_count', qbit_mark_pool_block_inactive(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "inactive_count": int(result["inactive_count"]),
        }

    def reactivate_pool_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
SELECT json_build_object(
    'backend', 'postgres-psql',
    'reactivated_count', qbit_reactivate_pool_block(
        {self._text_literal(block_hash)},
        {int(active_tip_height)},
        {self._text_literal(self._writer_id)},
        {int(self._writer_epoch)},
        {self._text_literal(self._writer_session_token)},
        {self._lease_interval_sql}
    )
);
"""
        with self._operation_gate(self._lock, "writer lock"):
            result = self._run_json(sql)
            if "error" in result:
                raise RuntimeError(str(result["error"]))
            reactivated_count = int(result["reactivated_count"])
            publication_sequence: object | None = None
            if reactivated_count == 1:
                # Same two-statements-under-one-gate shape as
                # confirm_accepted_block: the mutating statement's command
                # snapshot cannot see the ordinal it just assigned, so the
                # read runs next while the writer lock is still held. Report
                # the completed first statement so a landing caller's monitor
                # is not asked to sit through both budgets in silence.
                self._note_operation_progress()
                state = self._run_retry_safe_read_json(
                    f"""
SELECT json_build_object(
    'audit_publication_sequence', (
        SELECT audit_publication_sequence
        FROM qbit_pool_blocks
        WHERE block_hash = {self._text_literal(block_hash)}
          AND block_height <= {int(active_tip_height)}
          AND chain_state = 'confirmed'
          AND maturity_state = 'immature'
    )
);
"""
                )
                publication_sequence = state.get("audit_publication_sequence")
                if publication_sequence is None:
                    raise RuntimeError(
                        "reactivated pool block has no audit publication sequence"
                    )
        response: dict[str, int | str] = {
            "backend": str(result["backend"]),
            "reactivated_count": reactivated_count,
        }
        if publication_sequence is not None:
            response["audit_publication_sequence"] = int(publication_sequence)
        return response

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int | str]:
        sql = f"""
WITH lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    WHERE singleton
      AND writer_id = {self._text_literal(self._writer_id)}
      AND writer_epoch = {int(self._writer_epoch)}
      AND writer_session_token = {self._text_literal(self._writer_session_token)}
    RETURNING writer_id
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'matured_count', qbit_mark_mature_pool_payouts({int(active_tip_height)})
        )
END;
"""
        result = self._run_fenced_json(sql)
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {
            "backend": str(result["backend"]),
            "matured_count": int(result["matured_count"]),
        }

    def __len__(self) -> int:
        sql = "SELECT json_build_object('count', count(*)) FROM qbit_share_ledger WHERE accepted;"
        with self._operation_gate(self._lock, "writer lock"):
            return int(self._run_retry_safe_read_json(sql)["count"])

    def _run_fenced_json(self, sql: str) -> Any:
        with self._operation_gate(self._lock, "writer lock"):
            return self._run_json(sql)

    def _run_read_json(self, sql: str) -> Any:
        with self._operation_gate(self._read_semaphore, "read slot"):
            return self._run_retry_safe_read_json(sql)

    def _run_attributed_read_json(self, sql: str, *, operation: str) -> Any:
        """Run one read-slot query, timing admission apart from execution.

        Identical to ``_run_read_json`` in what it acquires and what it
        executes -- the same bounded read semaphore, the same retry-safe
        statement, no extra connection and no extra thread -- and different
        only in what it records.

        That record is the point. Issue #211 was diagnosed against an outer
        call reporting ``replay-outbox-query exceeded 5s`` while the inner
        PostgreSQL deadline still had seconds left and the server showed zero
        blocked backends: the budget had gone to coordinator-local admission,
        and nothing on ``/metrics`` said so. One duration covering both halves
        cannot answer "database or convoy?", so the halves are counted
        separately here and exported per operation.

        Execution time is measured across the whole statement including the
        tail a server-cancelled statement spends returning, so a deadline that
        expires inside PostgreSQL is attributed to PostgreSQL (cancel lag
        included) rather than to the gate.

        Every exit path records, so a timed-out read is counted rather than
        lost, and the gate is released in ``finally`` exactly as
        ``_operation_gate`` releases it.
        """
        gate_started = self._monotonic()
        try:
            self._acquire_operation_gate(self._read_semaphore, "read slot")
        except BaseException as exc:
            # Admission itself expired: no statement was ever sent, so there
            # is no execution sample to record and the call counts as a gate
            # timeout rather than a database one.
            self._note_ledger_read_timing(
                operation,
                gate_wait_seconds=max(0.0, self._monotonic() - gate_started),
                execute_seconds=None,
                timed_out=isinstance(exc, TimeoutError),
            )
            raise
        gate_wait_seconds = max(0.0, self._monotonic() - gate_started)
        execute_started: float | None = None

        def on_statement_start() -> None:
            nonlocal execute_started
            # A retry-safe native read may dispatch more than once after an
            # ambiguous connection loss. Execution attribution begins at the
            # first dispatch and includes the retry tail rather than resetting
            # the clock for each attempt.
            if execute_started is None:
                execute_started = self._monotonic()

        timed_out = False
        try:
            return self._run_retry_safe_read_json(
                sql,
                on_statement_start=on_statement_start,
            )
        except BaseException as exc:
            timed_out = isinstance(exc, TimeoutError)
            raise
        finally:
            execute_seconds = (
                None
                if execute_started is None
                else max(0.0, self._monotonic() - execute_started)
            )
            # Released before the record is taken: a bookkeeping failure must
            # never leak the read slot it was measuring.
            self._read_semaphore.release()
            self._note_ledger_read_timing(
                operation,
                gate_wait_seconds=gate_wait_seconds,
                execute_seconds=execute_seconds,
                timed_out=timed_out,
            )

    def _ensure_ledger_read_timings(self) -> Lock:
        """Return the lock guarding this instance's read-timing record."""
        stats_lock = getattr(self, "_ledger_read_timings_lock", None)
        if stats_lock is not None:
            return stats_lock
        with PsqlShareLedger._ledger_read_timings_bootstrap:
            stats_lock = getattr(self, "_ledger_read_timings_lock", None)
            if stats_lock is None:
                self._ledger_read_timings = {}
                stats_lock = Lock()
                # Published last, so a caller that observes the lock also
                # observes the dict the lock guards.
                self._ledger_read_timings_lock = stats_lock
        return stats_lock

    def _note_ledger_read_timing(
        self,
        operation: str,
        *,
        gate_wait_seconds: float,
        execute_seconds: float | None,
        timed_out: bool,
    ) -> None:
        stats_lock = self._ensure_ledger_read_timings()
        gate_wait_seconds = max(0.0, float(gate_wait_seconds))
        with stats_lock:
            stats = self._ledger_read_timings.setdefault(
                operation,
                {
                    "calls_total": 0,
                    "gate_wait_seconds_total": 0.0,
                    "gate_wait_seconds_max": 0.0,
                    "gate_timeouts_total": 0,
                    "execute_seconds_total": 0.0,
                    "execute_seconds_max": 0.0,
                    "execute_timeouts_total": 0,
                },
            )
            stats["calls_total"] = int(stats["calls_total"]) + 1
            stats["gate_wait_seconds_total"] = (
                float(stats["gate_wait_seconds_total"]) + gate_wait_seconds
            )
            stats["gate_wait_seconds_max"] = max(
                float(stats["gate_wait_seconds_max"]), gate_wait_seconds
            )
            if execute_seconds is None:
                if timed_out:
                    stats["gate_timeouts_total"] = (
                        int(stats["gate_timeouts_total"]) + 1
                    )
                return
            execute_seconds = max(0.0, float(execute_seconds))
            stats["execute_seconds_total"] = (
                float(stats["execute_seconds_total"]) + execute_seconds
            )
            stats["execute_seconds_max"] = max(
                float(stats["execute_seconds_max"]), execute_seconds
            )
            if timed_out:
                stats["execute_timeouts_total"] = (
                    int(stats["execute_timeouts_total"]) + 1
                )

    def ledger_read_gate_stats(self) -> dict[str, dict[str, float | int]]:
        """Read-slot admission wait against SQL execution, by operation.

        The series this feeds exist so the *next* budget exhaustion is
        attributable without a live debugging session: a rising
        ``gate_wait_seconds_max`` is coordinator-local contention, a rising
        ``execute_seconds_max`` is PostgreSQL. See
        ``_run_attributed_read_json``.
        """
        stats_lock = self._ensure_ledger_read_timings()
        with stats_lock:
            return {
                operation: dict(stats)
                for operation, stats in self._ledger_read_timings.items()
            }

    def _run_retry_safe_read_json(
        self,
        sql: str,
        *,
        on_statement_start: Callable[[], None] | None = None,
    ) -> Any:
        native = getattr(self, "_native", None)
        if native is not None:
            timeout_seconds = self._remaining_operation_timeout()
            run_kwargs: dict[str, Any] = {"retry_safe": True}
            if on_statement_start is not None:
                # The native client borrows or creates its connection before
                # firing this signal, and rechecks the deadline afterward.
                # Connection setup expiry is therefore local admission, not
                # statement execution that never happened.
                run_kwargs["on_statement_start"] = on_statement_start
            if timeout_seconds is None:
                return native.run_json(sql, **run_kwargs)
            run_kwargs["timeout_seconds"] = timeout_seconds
            return native.run_json(sql, **run_kwargs)
        run_json = self._run_json
        if getattr(run_json, "__func__", None) is PsqlShareLedger._run_json:
            return run_json(sql, on_statement_start=on_statement_start)
        # Test and embedding subclasses have historically overridden this
        # private seam with the one-argument signature. Preserve that
        # compatibility while treating entry into their replacement as the
        # only observable statement-start boundary they expose.
        if on_statement_start is not None:
            on_statement_start()
        return run_json(sql)

    def _ensure_writer_lease(self) -> None:
        while True:
            acquire_started_monotonic = self._monotonic()
            result = self._try_acquire_writer_lease()
            if result.get("acquired"):
                self._writer_lease_last_refresh_monotonic = (
                    acquire_started_monotonic
                )
                return
            if self._can_adopt_writer_lease(result):
                observed_session = str(result["writer_session_token"])
                adoption_started_monotonic = self._monotonic()
                adoption = self._try_adopt_writer_lease(result)
                if adoption.get("acquired"):
                    self._writer_lease_last_refresh_monotonic = (
                        adoption_started_monotonic
                    )
                    print(
                        "prism ledger writer lease adopted from same-identity "
                        f"predecessor session={observed_session}",
                        flush=True,
                    )
                    return
                # A CAS loss may mean the predecessor renewed concurrently or
                # another replacement won. Re-observe the returned owner and
                # require a fresh full silence interval before another CAS;
                # permanently refusing this token would recreate the TTL
                # outage if that renewal were its final act before dying.
                result = adoption
            if not self._can_wait_for_writer_lease(result):
                raise RuntimeError(
                    "qbit ledger writer lease is held by "
                    f"{result.get('writer_id')} epoch={result.get('writer_epoch')} "
                    f"session={result.get('writer_session_token')} "
                    f"until {result.get('lease_expires_at')}"
                )
            wait_seconds = max(0.0, float(result.get("lease_wait_seconds") or 0.0))
            adoption_wait_seconds = self._writer_lease_adoption_wait_seconds(result)
            if adoption_wait_seconds is not None:
                wait_seconds = min(wait_seconds, adoption_wait_seconds)
            sleep_seconds = min(
                self._lease_retry_max_sleep_seconds,
                max(self._lease_retry_min_sleep_seconds, wait_seconds),
            )
            print(
                "prism ledger writer lease held until "
                f"{result.get('lease_expires_at')}; waiting {sleep_seconds:.3g}s before retry "
                f"(holder writer={result.get('writer_id')} epoch={result.get('writer_epoch')} "
                f"session={result.get('writer_session_token')})",
                flush=True,
            )
            self._lease_retry_sleep(sleep_seconds)

    def _run_lease_acquisition_json(self, sql: str, description: str) -> Any:
        """Run one lease-acquisition statement under a bounded lock deadline.

        The startup lease upsert and the adoption CAS both row-lock the
        qbit_ledger_writer_lease singleton. Unbounded, either statement
        queues behind whatever transaction already holds that row — in the
        worst case an orphaned idle-in-transaction backend of a vanished
        predecessor — inside PsqlShareLedger.__init__, before the
        coordinator's watchdog arms, for as long as the kernel keeps the
        dead peer's socket alive. Each attempt therefore runs under the
        configured lock/statement deadline (statement_timeout arms both
        SET LOCAL statement_timeout and lock_timeout on the native backend,
        and the PGOPTIONS equivalents plus a subprocess timeout on psql).

        The retry budget (5 attempts x 5s by default) is sized to outlast a
        typical orphan reap rather than to guarantee one: the session guards'
        idle_in_transaction_session_timeout runs from the moment the blocking
        transaction went idle, not from the moment this process started
        retrying, so an orphan frequently clears partway through the budget
        and a later attempt lands the lease with no operator action. A
        statement still hitting its deadline on every attempt becomes the
        fatal RuntimeError, which __init__'s close-and-reraise turns into a
        visible process exit the supervisor can restart, instead of a silent
        multi-hour hang.

        A deadline expiry names the lock conflict first because it is by far
        the most common cause, but it is not proof of one: a connect timeout,
        an exhausted pool slot, and a healthy-but-overloaded server all
        surface as the same LedgerOperationTimeout. Both the per-attempt line
        and the fatal error therefore quote the underlying exception and the
        fatal error chains it, so the operator diagnoses from the cause rather
        than from this layer's guess.
        """
        attempts = self._lease_acquire_attempts
        lock_timeout_seconds = self._lease_acquire_lock_timeout_seconds
        cause: LedgerOperationTimeout | None = None
        for attempt in range(1, attempts + 1):
            try:
                with self.statement_timeout(lock_timeout_seconds):
                    return self._run_json(sql)
            except LedgerOperationTimeout as exc:
                cause = exc
                print(
                    f"prism ledger {description}: attempt {attempt}/{attempts} "
                    f"did not complete within its {lock_timeout_seconds:g}s "
                    "deadline (commonly the qbit_ledger_writer_lease row is "
                    "lock-blocked by another transaction, but an unreachable "
                    f"or overloaded server produces the same signal): {exc}",
                    flush=True,
                )
                if attempt < attempts:
                    self._lease_retry_sleep(self._lease_retry_min_sleep_seconds)
        raise RuntimeError(
            f"qbit ledger {description} did not complete within its "
            f"{lock_timeout_seconds:g}s deadline on any of {attempts} attempts "
            "(commonly the qbit_ledger_writer_lease row is lock-blocked by "
            "another transaction, but an unreachable or overloaded server "
            f"produces the same signal): {cause}; the coordinator is exiting "
            "rather than blocking startup indefinitely"
        ) from cause

    def _try_acquire_writer_lease(self) -> dict[str, Any]:
        """Claim, renew, or observe the writer lease in one statement.

        The statement is total: every outcome is a JSON object, so nothing
        here can reach ``parse_single_json_value``'s NULL branch and surface a
        raw driver error out of ``PsqlShareLedger.__init__``. Two arms are the
        ordinary ones -- this identity took the lease, or someone else holds
        it -- and the third exists because both can be empty at once during
        the first-ever concurrent acquisition (see
        WRITER_LEASE_ACQUIRE_RETRY_KEY).

        That third arm is handled here rather than by the caller because the
        remedy is local to this statement: it carries no holder to wait on or
        adopt, and only re-running gets the fresh READ COMMITTED snapshot in
        which the winner's committed row is visible. The retry therefore
        converges to one of the two ordinary arms, and ``_ensure_writer_lease``
        never sees the sentinel.
        """
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
upsert AS (
INSERT INTO qbit_ledger_writer_lease (
    singleton,
    writer_id,
    writer_epoch,
    writer_session_token,
    lease_expires_at
)
SELECT
    true,
    data->>'writer_id',
    (data->>'writer_epoch')::bigint,
    data->>'writer_session_token',
    clock_timestamp() + {self._lease_interval_sql}
FROM payload
ON CONFLICT (singleton) DO UPDATE
SET writer_id = EXCLUDED.writer_id,
    writer_epoch = EXCLUDED.writer_epoch,
    writer_session_token = EXCLUDED.writer_session_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    updated_at = clock_timestamp()
WHERE (
        qbit_ledger_writer_lease.writer_id = EXCLUDED.writer_id
        AND qbit_ledger_writer_lease.writer_epoch = EXCLUDED.writer_epoch
        AND qbit_ledger_writer_lease.writer_session_token = EXCLUDED.writer_session_token
    )
   OR qbit_ledger_writer_lease.lease_expires_at <= clock_timestamp()
RETURNING writer_id, writer_epoch, writer_session_token
)
SELECT COALESCE(
    (
        SELECT json_build_object(
            'acquired', true,
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token
        )
        FROM upsert
    ),
    (
        SELECT json_build_object(
            'acquired', false,
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token,
            'lease_expires_at', lease_expires_at::text,
            'lease_updated_at', updated_at::text,
            'lease_age_seconds', GREATEST(
                0,
                EXTRACT(EPOCH FROM (clock_timestamp() - updated_at))
            ),
            'lease_wait_seconds', GREATEST(
                0,
                EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))
            )
        )
        FROM qbit_ledger_writer_lease
        WHERE singleton
    ),
    json_build_object(
        'acquired', false,
        '{WRITER_LEASE_ACQUIRE_RETRY_KEY}', true,
        'lease', '{WRITER_LEASE_ACQUIRE_RETRY_SUBJECT}',
        'retry_reason',
        'the {WRITER_LEASE_ACQUIRE_RETRY_SUBJECT} row was committed by a concurrent first acquisition after this statement snapshot'
    )
);
"""
        for attempt in range(1, WRITER_LEASE_ACQUIRE_RETRY_ATTEMPTS + 1):
            result = self._run_lease_acquisition_json(
                sql,
                "writer lease acquisition",
            )
            if not isinstance(result, dict):
                raise RuntimeError("psql writer lease query returned non-object JSON")
            if not result.get(WRITER_LEASE_ACQUIRE_RETRY_KEY):
                return result
            print(
                "prism ledger writer lease acquisition raced a concurrent first "
                f"acquisition (attempt {attempt}/{WRITER_LEASE_ACQUIRE_RETRY_ATTEMPTS}); "
                "retrying on a fresh statement snapshot",
                flush=True,
            )
        raise RuntimeError(
            f"{WRITER_LEASE_ACQUIRE_RETRY_SUBJECT} acquisition still could not see "
            "the lease row committed by a concurrent first acquisition after "
            f"{WRITER_LEASE_ACQUIRE_RETRY_ATTEMPTS} fresh statement snapshots; "
            "the coordinator is exiting rather than starting without the lease"
        )

    def _can_adopt_writer_lease(self, result: dict[str, Any]) -> bool:
        wait_seconds = self._writer_lease_adoption_wait_seconds(result)
        return wait_seconds is not None and wait_seconds <= 0

    def _writer_lease_adoption_wait_seconds(
        self,
        result: dict[str, Any],
    ) -> float | None:
        """Return time until one heartbeat-capable predecessor is adoptable.

        A timestamp CAS alone only orders database mutations; it does not prove
        that a live, idle predecessor cannot still perform an external wallet
        or node RPC before its next fenced write. Fast adoption is therefore
        restricted to coordinator sessions guarded by the same writer/epoch
        PostgreSQL advisory lock. This process cannot reach this method until
        that predecessor's guard session is gone. PostgreSQL must additionally
        report the exact session unchanged for a full silence interval before
        the CAS, giving a holder whose guard connection failed time to self-exit.
        A renewing live twin retains the guard and cannot reach lease polling.

        The silence interval is measured from two independent edges and the
        later one wins. The row edge (``updated_at`` age) alone is unsafe: a
        long fenced transaction such as ``persist_accepted_block`` withholds
        the predecessor's ``updated_at`` refresh until commit, so the row can
        already look minutes silent the instant the predecessor dies. The
        guard edge therefore requires a full interval to elapse after *this*
        process acquired the advisory guard, guaranteeing the predecessor its
        whole heartbeat failure budget to self-fence after losing the guard,
        no matter how stale the row already is.
        """
        observed_session = result.get("writer_session_token")
        if not (
            self.writer_lease_fast_adoption_capable
            and self._can_wait_for_writer_lease(result)
            and isinstance(observed_session, str)
            and observed_session.startswith(WRITER_LEASE_HEARTBEAT_SESSION_PREFIX)
            and observed_session != self._writer_session_token
            and result.get("lease_updated_at") is not None
        ):
            return None
        guard_acquired_monotonic = getattr(
            self,
            "_writer_lease_guard_acquired_monotonic",
            None,
        )
        if guard_acquired_monotonic is None:
            return None
        try:
            age_seconds = float(result.get("lease_age_seconds"))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(age_seconds) or age_seconds < 0:
            return None
        row_wait_seconds = max(
            0.0,
            self._lease_adoption_silence_seconds - age_seconds,
        )
        guard_held_seconds = max(
            0.0,
            self._monotonic() - guard_acquired_monotonic,
        )
        guard_wait_seconds = max(
            0.0,
            self._lease_adoption_silence_seconds - guard_held_seconds,
        )
        return max(row_wait_seconds, guard_wait_seconds)

    def _try_adopt_writer_lease(self, observed: dict[str, Any]) -> dict[str, Any]:
        """Fence and replace one observed same-identity predecessor session.

        The caller first proves the heartbeat-capable predecessor has been
        silent for the configured interval. PostgreSQL row-lock ordering then
        makes this compare-and-swap the final database fence: a predecessor
        renewal that wins first changes ``updated_at`` and makes this update
        affect zero rows; if this update wins first, every later predecessor
        mutation sees a different session and is fenced out.
        """
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
            "observed_writer_session_token": observed.get("writer_session_token"),
            "observed_lease_updated_at": observed.get("lease_updated_at"),
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
adopted AS (
    UPDATE qbit_ledger_writer_lease
    SET writer_session_token = data->>'writer_session_token',
        lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'observed_writer_session_token'
      AND qbit_ledger_writer_lease.updated_at = (data->>'observed_lease_updated_at')::timestamptz
      AND qbit_ledger_writer_lease.lease_expires_at > clock_timestamp()
    RETURNING writer_id, writer_epoch, writer_session_token
)
SELECT COALESCE(
    (
        SELECT json_build_object(
            'acquired', true,
            'adopted', true,
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token
        )
        FROM adopted
    ),
    (
        SELECT json_build_object(
            'acquired', false,
            'adopted', false,
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token,
            'lease_expires_at', lease_expires_at::text,
            'lease_updated_at', updated_at::text,
            'lease_age_seconds', GREATEST(
                0,
                EXTRACT(EPOCH FROM (clock_timestamp() - updated_at))
            ),
            'lease_wait_seconds', GREATEST(
                0,
                EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))
            )
        )
        FROM qbit_ledger_writer_lease
        WHERE singleton
    )
);
"""
        result = self._run_lease_acquisition_json(sql, "writer lease adoption")
        if not isinstance(result, dict):
            raise RuntimeError("psql writer lease adoption query returned non-object JSON")
        return result

    def _can_wait_for_writer_lease(self, result: dict[str, Any]) -> bool:
        try:
            holder_epoch = int(result.get("writer_epoch"))
        except (TypeError, ValueError):
            return False
        return (
            result.get("writer_id") == self._writer_id
            and holder_epoch == self._writer_epoch
            and result.get("lease_expires_at") is not None
        )

    def renew_writer_lease(self) -> dict[str, int | str]:
        """Refresh this writer's lease without touching any ledger rows.

        The lease is normally refreshed as a side effect of every fenced
        write. A daemon pass that produced no writes calls this instead, so an
        otherwise-idle writer's lease does not sit expired. Raises when the
        exact ``(writer_id, writer_epoch, writer_session_token)`` no longer
        holds the lease, matching the fenced-write failure mode so a fenced-out
        writer still fails fast.
        """
        return self._renew_writer_lease_with(self._run_fenced_json)

    def prove_writer_lease_guard_session(
        self,
        *,
        on_query_start: Callable[[], None] | None = None,
        on_statement_end: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """Prove ownership on one cheap read-only statement; never renew.

        The frequent half of the split issue #212 asked for.  Ownership and
        TTL renewal are two different questions asked at two very different
        rates: the heartbeat must prove *ownership* several times inside one
        adoption-silence window, but the lease TTL is 60s and only needs a
        writer-side refresh well before it lapses (every fenced write
        refreshes it too).  Paying for the renewal question on every beat is
        what made the frequent statement expensive: ``FOR NO KEY UPDATE SKIP
        LOCKED`` on the hot lease tuple, a ``pg_stat_activity`` scan to
        attribute the tuple's locker, and — in the ambiguous case — a second
        statement.  Under a rapid-block burst that statement approached the
        guard's 500ms statement timeout, which is exactly how a healthy
        coordinator ran out of server-proven envelope.

        This statement asks only the ownership question, and asks it with
        two ``EXISTS`` reads:

        * PostgreSQL still shows *this* backend holding the writer/epoch
          advisory lock, and
        * the last committed lease row still names this exact
          ``(writer_id, writer_epoch, writer_session_token)``.

        Those are the same two conditions
        :meth:`verify_writer_lease_guard_session` raises on, evaluated the
        same way, so the exact-session guarantee is unchanged: a guard
        session that died, or an identity that was fenced out, still raises
        here and still hard-exits the coordinator.  What is *not* asked is
        anything that can block or that needs live backend state, so the
        statement takes no row lock, never queues behind a fenced write, and
        never needs an attribution recheck — it is one round trip, always.

        The lease-expiry guarantee is preserved by escalation rather than by
        renewal.  The result reports ``lease_renewal_due`` whenever the
        committed row's remaining validity has fallen to the own-write
        authority margin (which ``_resolve_lease_authority_margin_seconds``
        keeps at or above half the TTL, and strictly below it), and
        ``lease_expired`` when it has lapsed outright — an expired row is
        always also renewal-due.  The heartbeat answers either flag by
        running the full renewing verification immediately, on the same
        beat.  So the cheap proof short-circuits only while the lease is
        comfortably valid, which is precisely the window in which the full
        verification had nothing to decide; the moment renewal actually
        matters — including for the whole of a long own fenced write that
        keeps skipping renewals — every beat runs the same fail-closed
        verification it ran before this split existed.

        Never call this in place of the verification before an external
        side effect.  A proof is liveness and identity, not authority: it
        deliberately does not renew, so it cannot distinguish a row this
        writer's own in-flight write will refresh from one it will roll
        back.  :meth:`require_fresh_lease_for_external_side_effect` keeps
        using the full verification for that reason.

        ``on_query_start`` fires once the guarded session's serialized query
        slot is acquired and ``on_statement_end`` once the round trip
        returns, so a caller can attribute queue wait and server time
        separately.
        """
        if not self.writer_lease_fast_adoption_capable:
            raise RuntimeError("writer session is not heartbeat-capable")
        guard = self._writer_lease_guard
        if guard is None:
            raise RuntimeError("postgres writer lease guard is not held")
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        lock_key = _writer_lease_advisory_lock_key(
            self._writer_id,
            self._writer_epoch,
        )
        lock_classid = (lock_key >> 32) & 0xFFFFFFFF
        lock_objid = lock_key & 0xFFFFFFFF
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
)
SELECT json_build_object(
    'backend', 'postgres-psql',
    'guard_advisory_lock_held', EXISTS (
        SELECT 1
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted
          AND pid = pg_backend_pid()
          AND classid = {lock_classid}::oid
          AND objid = {lock_objid}::oid
          AND objsubid = 1
    ),
    'writer_session_token_current', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
          AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
          AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    ),
    'lease_expired', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
          AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
          AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
          AND qbit_ledger_writer_lease.lease_expires_at <= clock_timestamp()
    ),
    'lease_renewal_due', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
          AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
          AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
          AND qbit_ledger_writer_lease.lease_expires_at
              <= clock_timestamp() + {self._lease_authority_margin_sql}
    )
);
"""
        run_kwargs: dict[str, Callable[[], None]] = {}
        if on_query_start is not None:
            run_kwargs["on_query_start"] = on_query_start
        if on_statement_end is not None:
            run_kwargs["on_statement_end"] = on_statement_end
        result = guard.run_json(sql, **run_kwargs)
        if not isinstance(result, dict):
            raise RuntimeError(
                "psql writer lease guard proof returned non-object JSON"
            )
        if not result.get("guard_advisory_lock_held"):
            raise RuntimeError(
                "postgres writer lease guard advisory lock is no longer held"
            )
        if not result.get("writer_session_token_current"):
            raise RuntimeError("writer lease is not active")
        # An expired row is renewal-due by construction, but state it
        # explicitly: a caller must never be able to read a stale committed
        # row as "proved and nothing to do".
        renewal_due = bool(
            result.get("lease_renewal_due") or result.get("lease_expired")
        )
        return {
            "backend": str(result["backend"]),
            "verified_count": 1,
            "renewed_count": 0,
            "proof_only": True,
            "lease_expired": bool(result.get("lease_expired")),
            "lease_renewal_due": renewal_due,
        }

    def verify_writer_lease_guard_session(
        self,
        *,
        on_query_start: Callable[[], None] | None = None,
        on_statement_progress: Callable[[], None] | None = None,
        on_statement_end: Callable[[], None] | None = None,
    ) -> dict[str, int | str]:
        """Prove the guard session live; renew the TTL only without waiting.

        The coordinator's periodic heartbeat and its external-side-effect
        fence both run this on the dedicated guard connection. It must never
        wait on the ``qbit_ledger_writer_lease`` tuple lock: every fenced
        write (share appends, ``persist_accepted_block``) row-locks that
        tuple for its whole transaction, and a guarded statement queued
        behind it hits the guard's statement timeout and hard-exits a
        healthy coordinator. Liveness is therefore proven by non-blocking
        reads: the session answers, PostgreSQL still shows this backend
        holding the writer advisory lock, and the last committed lease row
        still names this exact session. Guard-connection loss or a
        fenced-out session raises, which callers treat as loss of the
        guarded session.

        The lease TTL still needs a writer-side refresh, or an idle
        coordinator (no fenced writes, CTV broadcaster disabled) would let
        ``lease_expires_at`` lapse and any different-identity claimant could
        seize the singleton row through its expiry CAS — that identity uses
        a different advisory-lock key, so this process's guard would not
        block it. The same statement therefore renews the exact-identity
        lease row with ``FOR NO KEY UPDATE SKIP LOCKED``: renewal happens on
        every heartbeat while the tuple is uncontended and is skipped
        without queueing while a fenced transaction holds it (that
        transaction refreshes the TTL itself when it commits).

        A skipped renewal is only trustworthy while the committed row is
        unexpired. Once ``lease_expires_at`` has lapsed, a lock we skipped
        may be a different-identity ``_try_acquire_writer_lease`` taking the
        row through its expiry CAS, and the stale committed snapshot would
        still name this session until that claim commits. The locking
        renewal used to catch exactly this by queueing and re-evaluating
        after the claimant committed; the non-blocking spelling recovers it
        by failing closed whenever the renewal was lock-blocked and the
        committed row was already expired.

        The one lock-blocked expired case that must not fail closed is this
        coordinator's own fenced write outlasting the TTL (a slow
        ``persist_accepted_block``): it holds the tuple's exclusive lock, so
        no expiry claim can be in flight behind it, and its commit refreshes
        ``lease_expires_at`` before any queued claimant re-evaluates its
        expiry CAS — raising would roll back a valid write and restart-loop
        on every similarly slow block. The statement therefore attributes
        the committed row's locker through ``pg_stat_activity``: the row's
        ``xmax`` is the locker's transaction id, and a backend running that
        transaction under this process's unique pool ``application_name``
        is this writer's own fenced write. Only an expired row locked by
        anyone else raises.

        That attribution can misfire in one direction: the statement's
        lease-row reads all share one MVCC snapshot, while
        ``pg_stat_activity`` reports live backend state. An own fenced
        write committing between the snapshot and the probe clears its
        backend's ``backend_xid`` while the snapshot still shows the
        expired row that write locked, so the probe reports no own locker
        for a renewal that in fact just landed. A would-be fail-closed is
        therefore re-verified once on a fresh statement: its new snapshot
        sees the committed refresh (the renewal lands normally), a
        completed takeover (the exact-identity match fails closed), or the
        same contended expiry (still fails closed). One recheck is enough
        — each further miss needs another independent commit to land
        inside the next statement's snapshot-to-probe window, and every
        residual outcome remains fail-closed. The recheck executes inside
        the guard's already-held serialized query slot, so it cannot queue
        behind other guard callers; it adds at most one more non-blocking
        statement to this verification's execution.

        That tolerance proves liveness only, not authority to act outside
        the database: the survival argument assumes the fenced write
        commits, and a rollback instead releases the expired row unchanged,
        letting a queued different-identity claimant take it immediately.
        The result therefore reports ``renewal_deferred_to_own_write`` for
        this state, and external-side-effect fences must withhold the
        guarded RPC while it is set, retrying on their own cadence until a
        verification lands a renewal (the write committed, refreshing the
        TTL) or fails closed. The heartbeat may keep treating it as live.

        The deferral engages before expiry as well: an own-write skip over
        a committed row with less than the authority margin remaining also
        defers. An RPC authorized on a nearly-lapsed row can outlive it,
        and from expiry onward its authority degenerates into the same
        rollback-dependent argument — the TTL erodes exactly while a long
        own write withholds renewals, so the runway shrinks precisely when
        the write's fate is least certain. The margin is never below half
        the lease TTL and is raised by the coordinator to cover its
        longest fence-guarded RPC deadline plus fixed headroom (see
        ``_resolve_lease_authority_margin_seconds``), so every authorized
        effect's transmission deadline — and, within the headroom, the
        node's application of a fully received request — lands inside the
        committed row's remaining validity. A node that stalls longer
        than the headroom after complete receipt is the documented
        residual of preflight fencing between independent systems; no
        client-side deadline can cancel a handler the node already
        received.
        Skips over a row with at least the margin remaining keep
        authorizing on the committed row's own standalone validity: every
        fenced commit refreshes the TTL, so steady write traffic never
        erodes the runway, and deferring every own-write skip would
        withhold submitblock and broadcasts behind saturated append
        traffic — the submitter liveness the fast-lane submit path exists
        to protect.

        ``on_query_start`` fires once the guarded session's serialized query
        slot is acquired, letting callers budget queue wait separately from
        statement execution. The attribution recheck happens within that
        one slot acquisition, so it neither re-fires the callback nor adds
        a queue wait to the caller's execution budget.

        ``on_statement_progress`` fires when the recheck decides to run a
        second statement: the first statement's full round trip has
        completed, which is exactly the session-answers evidence a
        liveness monitor watches for. Callers whose staleness budgets are
        sized for one statement (the heartbeat monitor's failure window)
        stamp progress here so a lawful two-statement verification is not
        mistaken for a wedged heartbeat, while a genuinely stuck statement
        still produces no progress at all.

        ``on_statement_end`` is the general form of the same signal: it
        fires after *every* completed round trip, including the last one,
        which is what a caller attributing guard SQL time per phase needs.
        A caller that supplies it does not also need
        ``on_statement_progress``.
        """
        if not self.writer_lease_fast_adoption_capable:
            raise RuntimeError("writer session is not heartbeat-capable")
        guard = self._writer_lease_guard
        if guard is None:
            raise RuntimeError("postgres writer lease guard is not held")
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
            "pool_application_name": self._pool_application_name,
        }
        lock_key = _writer_lease_advisory_lock_key(
            self._writer_id,
            self._writer_epoch,
        )
        # pg_locks splits a 64-bit advisory key into classid (high word) and
        # objid (low word) with objsubid = 1.
        lock_classid = (lock_key >> 32) & 0xFFFFFFFF
        lock_objid = lock_key & 0xFFFFFFFF
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
renewable AS (
    SELECT qbit_ledger_writer_lease.singleton
    FROM qbit_ledger_writer_lease, payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    FOR NO KEY UPDATE SKIP LOCKED
),
renewed AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM renewable
    WHERE qbit_ledger_writer_lease.singleton = renewable.singleton
    RETURNING qbit_ledger_writer_lease.writer_id
)
SELECT json_build_object(
    'backend', 'postgres-psql',
    'guard_advisory_lock_held', EXISTS (
        SELECT 1
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted
          AND pid = pg_backend_pid()
          AND classid = {lock_classid}::oid
          AND objid = {lock_objid}::oid
          AND objsubid = 1
    ),
    'writer_session_token_current', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
          AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
          AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    ),
    'lease_renewed_count', (SELECT count(*) FROM renewed),
    'lease_expired', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
          AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
          AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
          AND qbit_ledger_writer_lease.lease_expires_at <= clock_timestamp()
    ),
    'lease_expiring_within_authority_margin', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
          AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
          AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
          AND qbit_ledger_writer_lease.lease_expires_at
              <= clock_timestamp() + {self._lease_authority_margin_sql}
    ),
    'lease_locked_by_this_process', EXISTS (
        SELECT 1
        FROM qbit_ledger_writer_lease, pg_stat_activity, payload
        WHERE qbit_ledger_writer_lease.singleton
          AND pg_stat_activity.application_name = data->>'pool_application_name'
          AND pg_stat_activity.backend_xid IS NOT NULL
          AND pg_stat_activity.backend_xid = qbit_ledger_writer_lease.xmax
    )
);
"""
        attribution_rechecks_left = WRITER_LEASE_VERIFICATION_MAX_STATEMENTS - 1

        def attribution_recheck(result: Any) -> str | None:
            # Runs inside the guard's held query slot, between statements.
            # The locker probe reads live pg_stat_activity state against
            # its statement's older row snapshot, so an own fenced write
            # committing mid-statement leaves an expired locked row with
            # no attributable locker — the same shape as a competing
            # claim. The next statement's fresh snapshot resolves which
            # one it was; only the ambiguous shape earns the recheck, and
            # every recheck outcome that is not a landed renewal still
            # fails closed below.
            nonlocal attribution_rechecks_left
            if attribution_rechecks_left and (
                self._lease_renewal_ambiguously_lock_blocked(result)
            ):
                attribution_rechecks_left -= 1
                if on_statement_progress is not None:
                    # The first statement's round trip just completed;
                    # liveness monitors must count it before the second
                    # statement's execution starts consuming their budget.
                    on_statement_progress()
                return sql
            return None

        run_kwargs: dict[str, Any] = {"followup": attribution_recheck}
        if on_query_start is not None:
            run_kwargs["on_query_start"] = on_query_start
        if on_statement_end is not None:
            run_kwargs["on_statement_end"] = on_statement_end
        result = guard.run_json(sql, **run_kwargs)
        if not isinstance(result, dict):
            raise RuntimeError(
                "psql writer lease guard verification returned non-object JSON"
            )
        if not result.get("guard_advisory_lock_held"):
            raise RuntimeError(
                "postgres writer lease guard advisory lock is no longer held"
            )
        if not result.get("writer_session_token_current"):
            raise RuntimeError("writer lease is not active")
        try:
            renewed_count = int(result.get("lease_renewed_count", 0))
        except (TypeError, ValueError):
            renewed_count = 0
        renewal_deferred_to_own_write = bool(
            renewed_count == 0
            and result.get("lease_locked_by_this_process")
            and (
                result.get("lease_expired")
                or result.get("lease_expiring_within_authority_margin")
            )
        )
        if (
            renewed_count == 0
            and result.get("lease_expired")
            and not renewal_deferred_to_own_write
        ):
            # The committed row still names this session but its TTL has
            # lapsed and the tuple lock we declined to wait on may belong to
            # a different-identity expiry claim whose commit would land right
            # after this snapshot. A stale token read is not proof of
            # liveness here; fail closed like the queueing renewal used to.
            # The exemption is the writer's own fenced write outlasting the
            # TTL: its exclusive tuple lock means no claim is in flight, and
            # its commit refreshes the TTL before any queued claimant
            # re-evaluates its expiry CAS, so the session provably survives.
            # It survives as a *process*; the returned deferral flag tells
            # external-side-effect fences the same state is not authority
            # for a guarded RPC, because a rollback would hand the expired
            # row to a queued claimant instead.
            raise RuntimeError(
                "writer lease is expired and its renewal was lock-blocked; "
                "a competing expiry claim may be in flight"
            )
        return {
            "backend": str(result["backend"]),
            "verified_count": 1,
            "renewed_count": renewed_count,
            "renewal_deferred_to_own_write": renewal_deferred_to_own_write,
        }

    @staticmethod
    def _lease_renewal_ambiguously_lock_blocked(result: Any) -> bool:
        """The expired, lock-blocked, unattributable-locker result shape.

        This is the only shape whose meaning a single statement cannot
        decide: a competing expiry claim and an own fenced write that
        committed mid-statement both present it. Identity failures are
        excluded — a lost advisory lock or session token is conclusive and
        must surface through the ordinary raises, never a recheck.
        """
        if not isinstance(result, dict):
            return False
        if not result.get("guard_advisory_lock_held"):
            return False
        if not result.get("writer_session_token_current"):
            return False
        try:
            renewed_count = int(result.get("lease_renewed_count", 0))
        except (TypeError, ValueError):
            renewed_count = 0
        return bool(
            renewed_count == 0
            and result.get("lease_expired")
            and not result.get("lease_locked_by_this_process")
        )

    def _renew_writer_lease_with(
        self,
        run_json: Callable[[str], Any],
    ) -> dict[str, int | str]:
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
lease AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() + {self._lease_interval_sql},
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
)
SELECT CASE
    WHEN (SELECT count(*) FROM lease) = 0 THEN
        json_build_object('error', 'writer lease is not active')
    ELSE
        json_build_object(
            'backend', 'postgres-psql',
            'renewed_count', (SELECT count(*) FROM lease)
        )
END;
"""
        result = run_json(sql)
        if not isinstance(result, dict):
            raise RuntimeError("psql writer lease renewal returned non-object JSON")
        if "error" in result:
            raise RuntimeError(str(result["error"]))
        return {"backend": str(result["backend"]), "renewed_count": int(result["renewed_count"])}

    def release_writer_lease(self) -> bool:
        """Expire this writer's lease through the normal fenced DB path."""
        return self._release_writer_lease_with(self._run_fenced_json)

    def release_writer_lease_fresh_connection(self) -> bool:
        """Expire this writer's lease through a one-shot psql connection.

        The watchdog uses this path because its shared ledger lock or native
        connection pool may be held by the subsystem that stopped making
        progress. ``_run_sql`` forks psql without touching either resource.
        """
        return self._release_writer_lease_with(self._run_fresh_connection_json)

    def _release_writer_lease_with(self, run_json: Callable[[str], Any]) -> bool:
        """Run the one exact-session release implementation with ``run_json``.

        Best-effort, intended for graceful shutdown. Only the exact
        ``(writer_id, writer_epoch, writer_session_token)`` this process holds is
        expired, so a lease already reassigned to another writer is left
        untouched. Returns True if a held lease row was expired.
        """
        payload = {
            "writer_id": self._writer_id,
            "writer_epoch": self._writer_epoch,
            "writer_session_token": self._writer_session_token,
        }
        sql = f"""
WITH payload AS (
    SELECT {self._jsonb_literal(payload)} AS data
),
released AS (
    UPDATE qbit_ledger_writer_lease
    SET lease_expires_at = clock_timestamp() - interval '1 second',
        updated_at = clock_timestamp()
    FROM payload
    WHERE qbit_ledger_writer_lease.singleton
      AND qbit_ledger_writer_lease.writer_id = data->>'writer_id'
      AND qbit_ledger_writer_lease.writer_epoch = (data->>'writer_epoch')::bigint
      AND qbit_ledger_writer_lease.writer_session_token = data->>'writer_session_token'
    RETURNING qbit_ledger_writer_lease.writer_id
)
SELECT json_build_object('released', (SELECT count(*) FROM released));
"""
        try:
            result = run_json(sql)
            if not isinstance(result, dict):
                raise RuntimeError("psql writer lease release returned non-object JSON")
            return int(result.get("released", 0)) > 0
        finally:
            # The owning coordinator calls release only after writer admission
            # and the lease heartbeat are stopped. Releasing this session lock
            # is the final handoff that lets a successor enter fast adoption.
            self._close_writer_lease_guard()

    def _run_fresh_connection_json(self, sql: str) -> Any:
        output = self._run_sql(sql).strip()
        if not output:
            raise RuntimeError("psql query returned no JSON")
        return json.loads(output.splitlines()[-1])

    def _run_json(
        self,
        sql: str,
        *,
        on_statement_start: Callable[[], None] | None = None,
    ) -> Any:
        native = getattr(self, "_native", None)
        if native is not None:
            timeout_seconds = self._remaining_operation_timeout()
            run_kwargs: dict[str, Any] = {}
            if on_statement_start is not None:
                run_kwargs["on_statement_start"] = on_statement_start
            if timeout_seconds is None:
                return native.run_json(sql, **run_kwargs)
            run_kwargs["timeout_seconds"] = timeout_seconds
            return native.run_json(sql, **run_kwargs)
        run_sql = self._run_sql
        if getattr(run_sql, "__func__", None) is PsqlShareLedger._run_sql:
            output = run_sql(
                sql,
                on_statement_start=on_statement_start,
            ).strip()
        else:
            # The A1 gate and embedders historically override this private
            # seam with the one-argument signature. Their replacement owns
            # the full execution boundary, so entry is the only statement-
            # start signal the base class can expose without breaking them.
            if on_statement_start is not None:
                on_statement_start()
            output = run_sql(sql).strip()
        if not output:
            raise RuntimeError("psql query returned no JSON")
        return json.loads(output.splitlines()[-1])

    def _run_script(self, sql: str) -> None:
        native = getattr(self, "_native", None)
        if native is not None:
            native.run_script(sql)
            return
        self._run_sql(sql)

    def _run_sql(
        self,
        sql: str,
        *,
        on_statement_start: Callable[[], None] | None = None,
    ) -> str:
        cmd = [
            *self._command,
            "--no-psqlrc",
            # One transaction per invocation. For the multi-statement schema
            # script this is belt-and-braces alongside the BEGIN/COMMIT
            # wrapper inside the script itself (psql tolerates the nested
            # BEGIN with a warning): without atomicity, a failure or
            # interruption mid-script can commit trigger definitions while
            # later statements -- including the carry-forward summary seed --
            # never run, and a live writer mutating carry state in that gap
            # leaves a permanently partial summary. For single-statement
            # queries this is semantically identical to autocommit. The SQL
            # sent here contains no \connect and no commands that cannot run
            # inside a transaction block; --no-psqlrc means no ON_ERROR_
            # ROLLBACK can be injected from a psqlrc either.
            "--single-transaction",
            "--set",
            "ON_ERROR_STOP=1",
            "--set",
            "VERBOSITY=verbose",
            "--tuples-only",
            "--no-align",
            "--quiet",
        ]
        timeout_seconds = self._remaining_operation_timeout()
        run_kwargs: dict[str, Any] = {}
        # The session guards ride every psql invocation, deadline or not: an
        # orphaned idle-in-transaction backend is exactly the failure that
        # occurs when no deadline-scoped work is running, so building
        # PGOPTIONS only for deadline-bearing statements would leave the
        # unbounded sessions — the dangerous ones — unguarded. A deadline,
        # when armed, appends its per-statement bounds after the guards.
        session_guards = getattr(self, "_session_guards", None)
        option_fragments: list[str] = []
        if session_guards is not None:
            option_fragments.append(session_guards.options_fragment())
        if timeout_seconds is not None:
            timeout_ms = max(1, int(timeout_seconds * 1000))
            option_fragments.append(
                f"-c statement_timeout={timeout_ms}ms "
                f"-c lock_timeout={timeout_ms}ms"
            )
        if option_fragments:
            subprocess_env = dict(os.environ)
            # Operator-supplied PGOPTIONS stay, ahead of the coordinator's
            # fragments so a later -c duplicate resolves in our favor.
            existing_options = subprocess_env.get("PGOPTIONS", "").strip()
            subprocess_env["PGOPTIONS"] = " ".join(
                option
                for option in (existing_options, *option_fragments)
                if option
            )
            run_kwargs["env"] = subprocess_env
        if timeout_seconds is not None:
            run_kwargs["env"]["PGCONNECT_TIMEOUT"] = str(
                max(1, math.ceil(timeout_seconds))
            )
            run_kwargs["timeout"] = timeout_seconds
        # All local deadline validation is complete. Only now does this
        # invocation count as execution: an expiry raised above never starts
        # psql and must remain attributed to coordinator-local admission.
        if on_statement_start is not None:
            on_statement_start()
        try:
            completed = subprocess.run(
                cmd,
                input=sql,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                **run_kwargs,
            )
        except subprocess.TimeoutExpired as exc:
            raise LedgerOperationTimeout(
                f"psql operation exceeded {timeout_seconds:g}s"
            ) from exc
        if completed.returncode != 0:
            stderr = completed.stderr.strip()
            if timeout_seconds is not None and _is_postgres_deadline_error(stderr):
                raise LedgerOperationTimeout(
                    f"psql operation exceeded {timeout_seconds:g}s"
                )
            raise RuntimeError(
                "psql command failed "
                f"(exit {completed.returncode}): {stderr}"
            )
        return completed.stdout

    @staticmethod
    def _record_from_json(payload: dict[str, Any]) -> AcceptedShareRecord:
        return AcceptedShareRecord(
            share_seq=int(payload["share_seq"]),
            share_id=str(payload["share_id"]),
            miner_id=str(payload["miner_id"]),
            order_key=str(payload["order_key"]),
            p2mr_program_hex=str(payload["p2mr_program_hex"]),
            share_difficulty=int(payload["share_difficulty"]),
            network_difficulty=int(payload["network_difficulty"]),
            template_height=int(payload["template_height"]),
            job_id=str(payload["job_id"]),
            job_issued_at_ms=int(payload["job_issued_at_ms"]),
            accepted_at_ms=int(payload["accepted_at_ms"]),
            ntime=int(payload["ntime"]),
            credit_policy=(
                str(payload["credit_policy"])
                if payload.get("credit_policy") is not None
                else None
            ),
            newly_inserted=bool(payload.get("newly_inserted", True)),
            candidate_outbox_state=(
                str(payload["candidate_outbox_state"])
                if payload.get("candidate_outbox_state") is not None
                else None
            ),
        )

    @staticmethod
    def _jsonb_literal(payload: object) -> str:
        raw = json.dumps(payload, separators=(",", ":"))
        tag = "qbit_prism_json"
        while f"${tag}$" in raw:
            tag += "_x"
        return f"${tag}${raw}${tag}$::jsonb"

    @staticmethod
    def _text_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @classmethod
    def _text_array_literal(cls, values: Sequence[str]) -> str:
        """Render a non-empty text set as one array literal for ``= ANY``.

        Each element goes through the same quoting the scalar literals use.
        The cast is explicit because an empty ``ARRAY[]`` has no inferable
        element type; callers never build one, and the cast keeps that a
        parse-time guarantee rather than a convention.
        """
        elements = ", ".join(cls._text_literal(value) for value in values)
        return f"ARRAY[{elements}]::text[]"


CTV_FANOUT_STATUSES = {
    "awaiting_maturity",
    "broadcastable",
    "broadcast_submitted",
    "confirmed",
    "reorged",
    "failed",
}

CTV_FANOUT_ATTEMPT_STATUSES = {"planned", "submitted", "accepted", "rejected", "failed"}


def _ctv_broadcast_summary_fields() -> tuple[str, ...]:
    return (
        "broadcast_attempt_count",
        "broadcast_attempt_detail_count",
        "first_broadcast_attempt_at",
        "last_broadcast_attempt_at",
        "last_broadcast_attempt_status",
        "last_broadcast_package_tx_hexes",
        "last_broadcast_package_txids",
        "last_broadcast_submit_result",
        "last_broadcast_error",
        "broadcast_attempt_status_counts",
        "next_broadcast_attempt_at",
        "broadcast_retry_backoff_seconds",
    )


def _empty_ctv_broadcast_summary() -> dict[str, Any]:
    return {
        "broadcast_attempt_count": 0,
        "broadcast_attempt_detail_count": 0,
        "first_broadcast_attempt_at": None,
        "last_broadcast_attempt_at": None,
        "last_broadcast_attempt_status": None,
        "last_broadcast_package_tx_hexes": [],
        "last_broadcast_package_txids": [],
        "last_broadcast_submit_result": None,
        "last_broadcast_error": None,
        "broadcast_attempt_status_counts": {},
        "next_broadcast_attempt_at": None,
        "broadcast_retry_backoff_seconds": 0,
    }


def _ctv_broadcast_attempt_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_count": int(payload.get("broadcast_attempt_count") or 0),
        "detail_count": int(payload.get("broadcast_attempt_detail_count") or 0),
        "first_attempt_at": payload.get("first_broadcast_attempt_at"),
        "last_attempt_at": payload.get("last_broadcast_attempt_at"),
        "last_attempt_status": payload.get("last_broadcast_attempt_status"),
        "last_package_tx_hexes": copy.deepcopy(payload.get("last_broadcast_package_tx_hexes") or []),
        "last_package_txids": copy.deepcopy(payload.get("last_broadcast_package_txids") or []),
        "last_submit_result": copy.deepcopy(payload.get("last_broadcast_submit_result")),
        "last_error": payload.get("last_broadcast_error"),
        "status_counts": copy.deepcopy(payload.get("broadcast_attempt_status_counts") or {}),
        "next_attempt_at": payload.get("next_broadcast_attempt_at"),
        "retry_backoff_seconds": int(payload.get("broadcast_retry_backoff_seconds") or 0),
    }


def _ctv_broadcast_attempt_due(value: object, now: datetime) -> bool:
    if value is None:
        return True
    if isinstance(value, datetime):
        candidate = value
    else:
        text = str(value).strip()
        if not text:
            return True
        if " " in text and "T" not in text:
            text = text.replace(" ", "T", 1)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        elif text.endswith("+00"):
            text = text[:-3] + "+00:00"
        try:
            candidate = datetime.fromisoformat(text)
        except ValueError:
            return True
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)
    return candidate <= now


def validate_ctv_fanout_status(status: str) -> None:
    if status not in CTV_FANOUT_STATUSES:
        raise ValueError(f"unsupported CTV fanout status: {status}")


def validate_ctv_fanout_attempt_status(status: str) -> None:
    if status not in CTV_FANOUT_ATTEMPT_STATUSES:
        raise ValueError(f"unsupported CTV fanout attempt status: {status}")


def ctv_fanout_recovery_payload(
    *,
    block_hash: str,
    manifest_set: dict[str, Any],
    manifest_set_sha256: str,
) -> dict[str, Any]:
    block_hash = canonical_hex(block_hash, name="block_hash", expected_bytes=32)
    manifest_set_sha256 = canonical_hex(
        manifest_set_sha256,
        name="manifest_set_sha256",
        expected_bytes=32,
    )
    manifests_raw = manifest_set.get("manifests")
    if not isinstance(manifests_raw, list) or not manifests_raw:
        raise ValueError("manifest_set.manifests must be a non-empty array")

    manifests = sorted(
        (require_mapping(manifest, "manifest") for manifest in manifests_raw),
        key=lambda item: int(require_mapping(item.get("precommitment"), "precommitment")["chunk_index"]),
    )
    first_precommitment = require_mapping(manifests[0].get("precommitment"), "precommitment")
    block_height_value = manifest_set.get("block_height", first_precommitment.get("block_height"))
    block_height = int(block_height_value) if block_height_value is not None else None
    fanout_count = int(manifest_set.get("fanout_count", len(manifests)))
    if fanout_count != len(manifests):
        raise ValueError("manifest_set.fanout_count must equal the number of manifests")
    settlement_mode = str(manifest_set.get("settlement_mode", first_precommitment.get("settlement_mode", "")))
    if settlement_mode not in {"hybrid_coinbase_ctv_fanout", "ctv_fanout"}:
        raise ValueError("manifest_set.settlement_mode must be a CTV settlement mode")
    parent_coinbase_txid = canonical_hex(
        str(manifest_set.get("parent_coinbase_txid", manifests[0].get("parent_coinbase_txid", ""))),
        name="parent_coinbase_txid",
        expected_bytes=32,
    )
    parent_coinbase_tx_hex = canonical_hex(
        str(manifests[0].get("parent_coinbase_tx_hex", "")),
        name="parent_coinbase_tx_hex",
    )
    fanout_output_sum_sats = int(manifest_set.get("fanout_output_sum_sats", 0))
    covenant_output_value_sats = int(manifest_set.get("covenant_output_value_sats", 0))

    artifacts: list[dict[str, Any]] = []
    for expected_index, manifest in enumerate(manifests):
        precommitment = require_mapping(manifest.get("precommitment"), "precommitment")
        precommitment_block_height = precommitment.get("block_height")
        if block_height is not None and precommitment_block_height is not None and int(precommitment_block_height) != block_height:
            raise ValueError("CTV fanout block height mismatch")
        chunk_index = int(precommitment["chunk_index"])
        chunk_count = int(precommitment["chunk_count"])
        if chunk_index != expected_index:
            raise ValueError("CTV fanout chunks must be contiguous from zero")
        if chunk_count != fanout_count:
            raise ValueError("CTV fanout chunk_count must equal fanout_count")
        artifact_parent_txid = canonical_hex(
            str(manifest.get("parent_coinbase_txid", "")),
            name="manifest.parent_coinbase_txid",
            expected_bytes=32,
        )
        if artifact_parent_txid != parent_coinbase_txid:
            raise ValueError("CTV fanout parent coinbase txid mismatch")
        fanout_fee_sats = int(precommitment.get("fanout_fee_sats", 0))
        raw_anchor_vout = precommitment.get("anchor_vout")
        if fanout_fee_sats > 0 and raw_anchor_vout is not None:
            raise ValueError("built-in-fee CTV fanout must not include a CPFP anchor")
        if fanout_fee_sats == 0 and raw_anchor_vout is None:
            raise ValueError("zero-fee CTV fanout must include a CPFP anchor")
        artifact = {
            "fanout_txid": canonical_hex(
                str(manifest["fanout_txid"]),
                name="fanout_txid",
                expected_bytes=32,
            ),
            "manifest_json": canonical_json_text(manifest),
            "manifest": copy.deepcopy(manifest),
            "manifest_sha256": sha256_json_hex(manifest),
            "precommitment_sha256": canonical_hex(
                str(manifest["precommitment_sha256_hex"]),
                name="precommitment_sha256_hex",
                expected_bytes=32,
            ),
            "ctv_hash": canonical_hex(
                str(precommitment["ctv_hash_hex"]),
                name="ctv_hash_hex",
                expected_bytes=32,
            ),
            "commitment_witness_leaf_hex": canonical_hex(
                str(manifest["commitment_witness_leaf_hex"]),
                name="commitment_witness_leaf_hex",
            ),
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "parent_coinbase_txid": artifact_parent_txid,
            "parent_coinbase_vout": int(manifest["parent_coinbase_vout"]),
            "fanout_tx_template_hex": canonical_hex(
                str(precommitment["fanout_tx_template_hex"]),
                name="fanout_tx_template_hex",
            ),
            "fanout_tx_hex": canonical_hex(str(manifest["fanout_tx_hex"]), name="fanout_tx_hex"),
            "anchor_vout": None if raw_anchor_vout is None else int(raw_anchor_vout),
            "covenant_output_value_sats": int(manifest["covenant_output_value_sats"]),
            "fanout_output_sum_sats": int(precommitment["fanout_output_sum_sats"]),
            "settlement_status": "awaiting_maturity",
        }
        if block_height is not None:
            artifact["block_height"] = block_height
        artifacts.append(artifact)

    if sum(int(artifact["fanout_output_sum_sats"]) for artifact in artifacts) != fanout_output_sum_sats:
        raise ValueError("CTV fanout output sum mismatch")
    if sum(int(artifact["covenant_output_value_sats"]) for artifact in artifacts) != covenant_output_value_sats:
        raise ValueError("CTV covenant output value sum mismatch")

    payload = {
        "schema": "qbit.prism.ctv-fanout-recovery.v1",
        "block_hash": block_hash,
        "manifest_set_sha256": manifest_set_sha256,
        "manifest_set_json": canonical_json_text(manifest_set),
        "settlement_mode": settlement_mode,
        "parent_coinbase_txid": parent_coinbase_txid,
        "parent_coinbase_tx_hex": parent_coinbase_tx_hex,
        "fanout_count": fanout_count,
        "fanout_output_sum_sats": fanout_output_sum_sats,
        "covenant_output_value_sats": covenant_output_value_sats,
        "manifest_set": copy.deepcopy(manifest_set),
        "artifacts": artifacts,
    }
    if block_height is not None:
        payload["block_height"] = block_height
    audit_bundle_sha256 = manifest_set.get("audit_bundle_sha256")
    if audit_bundle_sha256 is not None:
        payload["audit_bundle_sha256"] = canonical_hex(
            str(audit_bundle_sha256),
            name="audit_bundle_sha256",
            expected_bytes=32,
        )
    audit_bundle = manifest_set.get("audit_bundle")
    if isinstance(audit_bundle, dict):
        payload["audit_bundle"] = copy.deepcopy(audit_bundle)
    return payload


def require_mapping(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def sha256_json_hex(payload: object) -> str:
    return hashlib.sha256(canonical_json_text(payload).encode()).hexdigest()


def block_candidate_identity(candidate: dict[str, Any]) -> dict[str, Any]:
    """Candidate payload with its volatile acknowledgment stamp removed.

    A miner can resubmit the same solved block after a transient submit
    outcome, and the rebuilt intent differs from the persisted one only in
    pending_share.accepted_at_ms. That drift must stay idempotent against the
    durable outbox while any other divergence remains a hard payload
    mismatch. The stored payload keeps its original stamp; only comparisons
    use this identity form.
    """
    pending_share = candidate.get("pending_share")
    if isinstance(pending_share, dict) and "accepted_at_ms" in pending_share:
        candidate = {
            **candidate,
            "pending_share": {**pending_share, "accepted_at_ms": None},
        }
    return candidate


def block_candidate_identity_sha256(candidate: dict[str, Any]) -> str:
    return sha256_json_hex(block_candidate_identity(candidate))


def sha256_bytes_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_json_text(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def canonical_hex(value: str, *, name: str, expected_bytes: int | None = None) -> str:
    lowered = value.lower()
    if not lowered:
        raise ValueError(f"{name} must not be empty")
    try:
        bytes.fromhex(lowered)
    except ValueError as exc:
        raise ValueError(f"{name} must be hex") from exc
    if expected_bytes is not None and len(lowered) != expected_bytes * 2:
        raise ValueError(f"{name} must be {expected_bytes * 2} hex characters")
    if len(lowered) % 2 != 0:
        raise ValueError(f"{name} must have an even number of hex characters")
    return lowered
