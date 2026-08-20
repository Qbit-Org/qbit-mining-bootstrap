#!/usr/bin/env python3
"""Backfill immutable canonical audit-bundle artifacts for existing rows.

The live publication path owns one immutable canonical artifact per
``(block_hash, audit_bundle_sha256)``: the exact byte sequence the advertised
digest was computed over, published once and never rewritten.  Rows written
before that artifact existed still advertise a digest with nothing canonical
behind it.  Three shapes are in the table today:

* a pre-v2 literal external body, whose ``body_uri`` file *is* the canonical
  byte sequence -- recovering it is a copy, not a re-serialization;
* a modern compact/v2 external body, which stores the bundle without its
  shares plus content-addressed share segments, and has to be reconstructed
  through the artifact store before it can be canonicalized;
* an inline ``qbit_pool_audit_bundles.audit_bundle`` JSONB body, the oldest
  layout, which is canonicalized with the repository's own canonicalizer.

This tool walks those rows in primary-key order, recovers the canonical bytes
for each, and verifies the advertised digest *before* anything reaches the
filesystem.  It never rewrites a digest: a row whose recovered bytes do not
hash to the advertised value is reported on stderr, left untouched, and turned
into a nonzero exit status once the requested range has been processed.

Every publication is preceded, immediately, by the ledger's pre-publication
preflight (writer-lease refresh plus current row identity confirmation), so a
writer that has lost its lease -- or a row that changed underneath this scan --
cannot publish.

The run is idempotent and resumable.  Rows are paged by ``block_hash`` (the
primary key), the last visited hash is reported as ``last_checkpoint``, and
``--start-after`` resumes from it.  Revisiting an already published row is
safe: its canonical artifact is recognized and skipped.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import inspect
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.audit_artifacts import (
    AuditArtifactConfig,
    AuditArtifactStore,
    DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
)
from lab.prism.backfill_ctv_fanouts import psql_command_from_env
from lab.prism.bundle_compiler import canonical_bundle_bytes
from lab.prism.coordinator_config import (
    DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
    DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
    DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
    env,
    env_nonnegative_int,
    env_positive_float,
    env_positive_int,
)
from lab.prism.share_ledger import PsqlShareLedger, canonical_hex


BACKFILL_SUMMARY_SCHEMA = "qbit.prism.audit-bundle-canonical-backfill.v1"
DEFAULT_BATCH_SIZE = 100

# Every capability this tool needs from phase 2 is resolved by name at
# construction, first match wins, and the resolved names are reported in the
# summary.  Reconciling a final signature difference is then a one-line edit
# here plus a summary line that says exactly what the run bound to.
READ_CANONICAL_CANDIDATES = ("read_canonical_audit_bundle",)
WRITE_CANONICAL_CANDIDATES = ("write_canonical_audit_bundle",)
LITERAL_BODY_CANDIDATES = (
    "literal_canonical_bundle_bytes",
    "read_literal_audit_body",
    "read_verified_literal_audit_body",
    "read_pre_v2_audit_body",
    "read_literal_external_audit_body",
)
RECONSTRUCT_CANDIDATES = (
    "canonical_bundle_bytes_from_external_body",
    "reconstruct_external_audit_body",
    "read_external_body",
)
LEDGER_PREFLIGHT_CANDIDATES = (
    "preflight_canonical_bundle_publication",
    "preflight_audit_bundle_publication",
    "confirm_audit_bundle_publication",
    "refresh_lease_and_confirm_audit_bundle",
)
LEDGER_SHARE_RANGE_CANDIDATES = (
    "load_audit_share_ledger_range",
    "_load_audit_share_ledger_range",
)
LEDGER_READ_JSON_CANDIDATES = ("_run_read_json", "_run_json")
PREFLIGHT_DIGEST_KEYWORDS = (
    "audit_bundle_sha256",
    "digest",
    "audit_bundle_sha256_hex",
)
# Whatever the reconstruction helper hands its missing-range callback, one of
# these carries the segment digest the recovered range must reproduce.
SEGMENT_DIGEST_KEYS = (
    "expected_sha256",
    "range_sha256",
    "prefix_sha256",
    "sha256",
    "segment_sha256",
)


class CanonicalBackfillMismatch(RuntimeError):
    """Recovered bytes do not reproduce the advertised digest."""


class CanonicalBackfillUnrecoverable(RuntimeError):
    """No source for this row yielded bytes that could be verified."""


class LedgerPreflightRefused(RuntimeError):
    """The ledger declined to authorize this publication."""


class MissingCanonicalCapability(RuntimeError):
    """A required phase-2 method is not present on the injected object."""


def _sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _accepts_keyword(func: Callable[..., Any], name: str) -> bool:
    """Report whether ``func`` takes ``name`` as a keyword argument.

    The phase-2 signatures are assumed, not observed, so every optional
    keyword this tool would like to pass is probed rather than assumed.  An
    unintrospectable callable (a C builtin, an exotic mock) is given the
    benefit of the doubt: the documented signature is the contract.
    """

    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return True
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == name and parameter.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            return True
    return False


def _resolve_capability(
    owner: object,
    candidates: Sequence[str],
    *,
    description: str,
) -> tuple[str, Callable[..., Any]]:
    for name in candidates:
        value = getattr(owner, name, None)
        if callable(value):
            return name, value
    raise MissingCanonicalCapability(
        f"{description} is unavailable on {type(owner).__name__}; "
        f"tried: {', '.join(candidates)}"
    )


def _sql_text_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def audit_bundle_page_sql(
    *,
    start_after: str | None,
    limit: int,
    text_literal: Callable[[str], str] = _sql_text_literal,
) -> str:
    """One bounded, resumable page of ``qbit_pool_audit_bundles``.

    ``block_hash`` is the table's primary key, so ordering by it is stable and
    ``block_hash > <cursor>`` resumes exactly where the previous page stopped.
    The range predicate and the ordering share the column's collation, so the
    pagination stays self-consistent whatever that collation is.
    """

    if limit <= 0:
        raise ValueError("audit bundle page limit must be positive")
    predicate = (
        "" if start_after is None else f"    WHERE block_hash > {text_literal(start_after)}\n"
    )
    return f"""
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', page.block_hash,
    'audit_bundle_sha256', page.audit_bundle_sha256,
    'body_uri', page.body_uri,
    'audit_bundle', page.audit_bundle
) ORDER BY page.block_hash ASC), '[]'::json)
FROM (
    SELECT block_hash, audit_bundle_sha256, body_uri, audit_bundle
    FROM qbit_pool_audit_bundles
{predicate}    ORDER BY block_hash ASC
    LIMIT {int(limit)}
) AS page;
"""


@dataclass(frozen=True)
class AuditBundleRow:
    block_hash: str
    audit_bundle_sha256: str
    body_uri: str | None
    audit_bundle: dict[str, Any] | None


@dataclass(frozen=True)
class RecoveredCanonical:
    canonical_bytes: bytes
    source: str


class CanonicalArtifactAdapter:
    """The phase-2 canonical artifact surface, bound by name in one place.

    Phase 2 owns ``read_canonical_audit_bundle`` /
    ``write_canonical_audit_bundle``, a verified pre-v2 literal-body reader,
    and a verified external-body reconstruction helper that accepts a
    missing-range callback.  This adapter is the only place that names them,
    so a final signature difference is reconciled here and nowhere else.
    """

    def __init__(self, store: object) -> None:
        self._store = store
        self._read_name, self._read = _resolve_capability(
            store,
            READ_CANONICAL_CANDIDATES,
            description="canonical audit bundle reader",
        )
        self._write_name, self._write = _resolve_capability(
            store,
            WRITE_CANONICAL_CANDIDATES,
            description="canonical audit bundle writer",
        )
        self._literal_name, self._literal = _resolve_capability(
            store,
            LITERAL_BODY_CANDIDATES,
            description="verified pre-v2 literal audit body reader",
        )
        self._reconstruct_name, self._reconstruct = _resolve_capability(
            store,
            RECONSTRUCT_CANDIDATES,
            description="verified external audit body reconstruction helper",
        )
        self._canonicalize_name, self._canonicalize = _resolve_capability(
            store,
            ("canonical_audit_bundle_bytes",),
            description="audit bundle canonicalizer",
        )
        self._literal_takes_digest = _accepts_keyword(self._literal, "expected_sha256")
        self._reconstruct_takes_digest = _accepts_keyword(
            self._reconstruct,
            "expected_sha256",
        )
        self._reconstruct_takes_callback = _accepts_keyword(
            self._reconstruct,
            "load_missing_range",
        )

    def resolved_interface(self) -> dict[str, str]:
        return {
            "read_canonical": self._read_name,
            "write_canonical": self._write_name,
            "literal_body_reader": self._literal_name,
            "external_body_reconstruction": self._reconstruct_name,
            "canonicalizer": self._canonicalize_name,
        }

    def read_canonical(self, block_hash: str, digest: str) -> bytes | None:
        payload = self._read(block_hash, digest)
        return None if payload is None else bytes(payload)

    def write_canonical(self, block_hash: str, digest: str, payload: bytes) -> Any:
        return self._write(block_hash, digest, payload)

    def read_literal_body(self, body_uri: object, digest: str) -> bytes | None:
        """Return the pre-v2 body bytes verbatim, or None when it is not one.

        A compact or v2 wrapper is not the canonical byte sequence, so the
        reader is expected to decline it rather than hand back wrapper bytes.
        """

        if self._literal_takes_digest:
            payload = self._literal(body_uri, expected_sha256=digest)
        else:
            payload = self._literal(body_uri)
        return None if payload is None else bytes(payload)

    def canonical_bytes_from_external_body(
        self,
        block_hash: str,
        body_uri: object,
        digest: str,
        load_missing_range: Callable[..., list[Any]],
    ) -> bytes | None:
        if self._reconstruct_name == "canonical_bundle_bytes_from_external_body":
            payload = self._reconstruct(
                block_hash=block_hash,
                audit_bundle_sha256=digest,
                body_uri=body_uri,
                load_missing_range=load_missing_range,
            )
            return None if payload is None else bytes(payload)
        kwargs: dict[str, Any] = {}
        if self._reconstruct_takes_digest:
            kwargs["expected_sha256"] = digest
        if self._reconstruct_takes_callback:
            kwargs["load_missing_range"] = load_missing_range
        bundle = self._reconstruct(body_uri, **kwargs)
        if bundle is None:
            return None
        if not isinstance(bundle, dict):
            raise CanonicalBackfillUnrecoverable(
                "external audit body reconstruction did not return a bundle object"
            )
        return self.canonical_bytes(bundle)

    def canonical_bytes(self, bundle: Mapping[str, Any]) -> bytes:
        return bytes(self._canonicalize(dict(bundle)))

    def segment_storage_bytes(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> bytes:
        """Canonicalize a recovered share range with the store's own encoder."""

        payload = self._store.audit_share_segment_payload(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            shares=shares,
        )
        return bytes(self._store.storage_json_bytes(payload))


class AuditBundleLedgerAdapter:
    """The PostgreSQL surface this backfill needs, bound by name in one place."""

    def __init__(self, ledger: object) -> None:
        self._ledger = ledger
        self._read_json_name, self._read_json = _resolve_capability(
            ledger,
            LEDGER_READ_JSON_CANDIDATES,
            description="ledger read-slot JSON runner",
        )
        self._share_range_name, self._share_range = _resolve_capability(
            ledger,
            LEDGER_SHARE_RANGE_CANDIDATES,
            description="ledger audit share range reader",
        )
        self._preflight_name, self._preflight = _resolve_capability(
            ledger,
            LEDGER_PREFLIGHT_CANDIDATES,
            description="ledger publication preflight",
        )
        self._preflight_digest_keyword = next(
            (
                keyword
                for keyword in PREFLIGHT_DIGEST_KEYWORDS
                if _accepts_keyword(self._preflight, keyword)
            ),
            PREFLIGHT_DIGEST_KEYWORDS[0],
        )
        text_literal = getattr(ledger, "_text_literal", None)
        self._text_literal = (
            text_literal if callable(text_literal) else _sql_text_literal
        )

    def resolved_interface(self) -> dict[str, str]:
        return {
            "read_json": self._read_json_name,
            "share_range_reader": self._share_range_name,
            "publication_preflight": self._preflight_name,
            "preflight_digest_keyword": self._preflight_digest_keyword,
        }

    def page(self, *, start_after: str | None, limit: int) -> list[AuditBundleRow]:
        sql = audit_bundle_page_sql(
            start_after=start_after,
            limit=limit,
            text_literal=self._text_literal,
        )
        rows = self._read_json(sql)
        if not isinstance(rows, list):
            raise RuntimeError("audit bundle page did not return a JSON array")
        page: list[AuditBundleRow] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("audit bundle page returned a non-object row")
            bundle = row.get("audit_bundle")
            page.append(
                AuditBundleRow(
                    block_hash=str(row.get("block_hash") or ""),
                    audit_bundle_sha256=str(row.get("audit_bundle_sha256") or ""),
                    body_uri=(
                        None if row.get("body_uri") is None else str(row["body_uri"])
                    ),
                    audit_bundle=dict(bundle) if isinstance(bundle, Mapping) else None,
                )
            )
        return page

    def load_share_ledger_range(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
    ) -> list[Any]:
        shares = self._share_range(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
        )
        if not isinstance(shares, list):
            raise RuntimeError("audit share ledger range did not return a list")
        return shares

    def preflight_publication(self, *, block_hash: str, digest: str) -> Any:
        """Refresh the writer lease and confirm this row's identity.

        Called immediately before every filesystem publication.  A refusal --
        raised, reported through an ``error`` key, or an explicit falsy
        ``confirmed`` -- withholds the publication.
        """

        result = self._preflight(
            block_hash=block_hash,
            **{self._preflight_digest_keyword: digest},
        )
        if isinstance(result, Mapping):
            error = result.get("error")
            if error:
                raise LedgerPreflightRefused(str(error))
            confirmed = result.get("confirmed")
            if confirmed is not None and not confirmed:
                raise LedgerPreflightRefused(
                    "ledger preflight did not confirm the audit bundle row identity"
                )
        elif result is False:
            raise LedgerPreflightRefused(
                "ledger preflight did not confirm the audit bundle row identity"
            )
        return result


@dataclass
class BackfillCounters:
    visited: int = 0
    written: int = 0
    already_present: int = 0
    dry_run: int = 0
    literal_freebie: int = 0
    reconstructed: int = 0
    inline_canonicalized: int = 0
    db_segment_reloads: int = 0
    mismatch: int = 0
    errors: int = 0


class CanonicalBundleBackfill:
    """Recover and publish canonical bytes for existing audit bundle rows."""

    def __init__(
        self,
        *,
        artifacts: CanonicalArtifactAdapter,
        ledger: AuditBundleLedgerAdapter,
        dry_run: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
        limit: int | None = None,
        start_after: str | None = None,
        stderr: Any = None,
    ) -> None:
        batch_size = int(batch_size)
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if limit is not None:
            limit = int(limit)
            if limit <= 0:
                raise ValueError("limit must be positive when given")
        self._artifacts = artifacts
        self._ledger = ledger
        self._dry_run = bool(dry_run)
        self._batch_size = batch_size
        self._limit = limit
        self._start_after = start_after
        self._stderr = sys.stderr if stderr is None else stderr
        self._counters = BackfillCounters()
        self._checkpoint = start_after

    def run(self) -> dict[str, Any]:
        for row in self._iter_rows():
            self._counters.visited += 1
            # The checkpoint advances over every visited row, including the
            # ones this run refuses to publish.  Those are reported loudly and
            # make the run exit nonzero; silently rewinding to them on the next
            # resume would stall the scan on a row that needs an operator.
            self._checkpoint = row.block_hash
            self._process_row(row)
        return self.summary()

    def summary(self) -> dict[str, Any]:
        counters = asdict(self._counters)
        return {
            "schema": BACKFILL_SUMMARY_SCHEMA,
            **counters,
            "last_checkpoint": self._checkpoint,
            "start_after": self._start_after,
            "batch_size": self._batch_size,
            "limit": self._limit,
            "dry_run_mode": self._dry_run,
            "exit_code": self.exit_code(),
            "interface": {
                "artifacts": self._artifacts.resolved_interface(),
                "ledger": self._ledger.resolved_interface(),
            },
        }

    def exit_code(self) -> int:
        return 1 if (self._counters.mismatch or self._counters.errors) else 0

    def _iter_rows(self) -> Iterator[AuditBundleRow]:
        cursor = self._start_after
        remaining = self._limit
        while remaining is None or remaining > 0:
            page_size = (
                self._batch_size if remaining is None else min(self._batch_size, remaining)
            )
            page = self._ledger.page(start_after=cursor, limit=page_size)
            if not page:
                return
            for row in page:
                yield row
            next_cursor = page[-1].block_hash
            # The scan only terminates while the cursor strictly advances. A
            # page whose last row carries no usable key would otherwise reissue
            # the same query forever; the primary key makes that unreachable in
            # a healthy database, so it is reported rather than tolerated.
            if not next_cursor or (cursor is not None and next_cursor <= cursor):
                self._counters.errors += 1
                self._report(
                    "error",
                    page[-1],
                    "audit bundle page did not advance the resume checkpoint",
                )
                return
            cursor = next_cursor
            if remaining is not None:
                remaining -= len(page)
            if len(page) < page_size:
                return

    def _report(self, kind: str, row: AuditBundleRow, reason: str) -> None:
        print(
            f"prism canonical audit bundle backfill {kind} "
            f"block={row.block_hash or '<missing>'} "
            f"digest={row.audit_bundle_sha256 or '<missing>'} "
            f"reason={reason}",
            file=self._stderr,
            flush=True,
        )

    def _process_row(self, row: AuditBundleRow) -> None:
        try:
            block_hash = canonical_hex(
                row.block_hash,
                name="block_hash",
                expected_bytes=32,
            )
            digest = canonical_hex(
                row.audit_bundle_sha256,
                name="audit_bundle_sha256",
                expected_bytes=32,
            )
        except (ValueError, TypeError) as exc:
            self._counters.errors += 1
            self._report("error", row, f"row identity is not canonical: {exc}")
            return

        try:
            if self._already_published(block_hash, digest):
                self._counters.already_present += 1
                return
            recovered = self._recover(row, digest)
            actual = _sha256_hex(recovered.canonical_bytes)
            if not hmac.compare_digest(actual, digest):
                raise CanonicalBackfillMismatch(
                    f"recovered {recovered.source} bytes hash to {actual}"
                )
        except CanonicalBackfillMismatch as exc:
            self._counters.mismatch += 1
            self._report("mismatch", row, str(exc))
            return
        except Exception as exc:  # noqa: BLE001 - one bad row must not end the range
            self._counters.errors += 1
            self._report("error", row, f"{type(exc).__name__}: {exc}")
            return

        if recovered.source == "literal":
            self._counters.literal_freebie += 1
        elif recovered.source == "reconstructed":
            self._counters.reconstructed += 1
        else:
            self._counters.inline_canonicalized += 1

        if self._dry_run:
            self._counters.dry_run += 1
            return

        try:
            # Preflight is the last thing before the filesystem is touched:
            # the lease is refreshed and the row identity re-confirmed with no
            # verification work left to run in between.
            self._ledger.preflight_publication(block_hash=block_hash, digest=digest)
            self._artifacts.write_canonical(
                block_hash,
                digest,
                recovered.canonical_bytes,
            )
        except Exception as exc:  # noqa: BLE001 - report and keep scanning
            self._counters.errors += 1
            self._report("error", row, f"{type(exc).__name__}: {exc}")
            return
        self._counters.written += 1

    def _already_published(self, block_hash: str, digest: str) -> bool:
        """Report whether a verified canonical artifact already exists.

        An unreadable artifact is treated as absent -- publication is
        idempotent, so recovering and republishing is the useful answer.  An
        artifact that reads back with the wrong digest is not absent; it is a
        corrupt publication, and it is reported rather than overwritten.
        """

        try:
            existing = self._artifacts.read_canonical(block_hash, digest)
        except Exception:  # noqa: BLE001 - fall through to recovery
            return False
        if existing is None:
            return False
        actual = _sha256_hex(existing)
        if not hmac.compare_digest(actual, digest):
            raise CanonicalBackfillMismatch(
                f"existing canonical artifact hashes to {actual}"
            )
        return True

    def _recover(self, row: AuditBundleRow, digest: str) -> RecoveredCanonical:
        reasons: list[str] = []
        if row.body_uri:
            literal = self._recover_literal(row, digest, reasons)
            if literal is not None:
                return literal
            canonical = self._reconstruct(row, digest, reasons)
            if canonical is not None:
                actual = _sha256_hex(canonical)
                if not hmac.compare_digest(actual, digest):
                    raise CanonicalBackfillMismatch(
                        f"reconstructed external body canonicalizes to {actual}"
                    )
                return RecoveredCanonical(canonical, "reconstructed")
        if row.audit_bundle is not None:
            canonical = self._artifacts.canonical_bytes(row.audit_bundle)
            actual = _sha256_hex(canonical)
            if not hmac.compare_digest(actual, digest):
                raise CanonicalBackfillMismatch(
                    f"inline bundle canonicalizes to {actual}"
                )
            return RecoveredCanonical(canonical, "inline")
        if not row.body_uri:
            reasons.append("row has neither an inline bundle nor an external body")
        else:
            reasons.append("external body did not yield the advertised bytes")
        raise CanonicalBackfillUnrecoverable("; ".join(reasons))

    def _recover_literal(
        self,
        row: AuditBundleRow,
        digest: str,
        reasons: list[str],
    ) -> RecoveredCanonical | None:
        """Promote a pre-v2 literal body verbatim when it already verifies.

        These bytes *are* the canonical serialization the digest was computed
        over, so they are copied, never parsed and re-emitted: re-serializing
        would substitute this process's encoder for the one that produced the
        advertised digest.
        """

        try:
            literal = self._artifacts.read_literal_body(row.body_uri, digest)
        except Exception as exc:  # noqa: BLE001 - a compact body is not a failure
            reasons.append(f"literal body reader: {type(exc).__name__}: {exc}")
            return None
        if literal is None:
            return None
        actual = _sha256_hex(literal)
        if hmac.compare_digest(actual, digest):
            return RecoveredCanonical(literal, "literal")
        # Not a mismatch on its own: a compact or v2 wrapper never hashes to
        # the bundle digest, and reconstruction is the path that reads it.
        reasons.append(f"external body bytes hash to {actual}")
        return None

    def _reconstruct(
        self,
        row: AuditBundleRow,
        digest: str,
        reasons: list[str],
    ) -> bytes | None:
        try:
            return self._artifacts.canonical_bytes_from_external_body(
                row.block_hash,
                row.body_uri,
                digest,
                self._load_missing_range,
            )
        except CanonicalBackfillMismatch:
            raise
        except Exception as exc:  # noqa: BLE001 - recorded, then inline is tried
            reasons.append(
                f"external body reconstruction: {type(exc).__name__}: {exc}"
            )
            return None

    def _load_missing_range(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
        **extra: Any,
    ) -> list[Any]:
        """Recover an absent on-disk share segment from the append-only ledger.

        The range is read from PostgreSQL, canonicalized with the store's own
        segment encoder, and checked against the segment digest the bundle
        embeds.  A range that does not reproduce that digest is refused here,
        before it can reach a reconstruction the final bundle digest would
        then have to catch.
        """

        first_share_seq = int(first_share_seq)
        last_share_seq = int(last_share_seq)
        shares = self._ledger.load_share_ledger_range(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
        )
        try:
            sequences = [int(share["share_seq"]) for share in shares]
        except (KeyError, TypeError, ValueError) as exc:
            raise CanonicalBackfillUnrecoverable(
                "ledger share range has an invalid share_seq"
            ) from exc
        if sequences != list(range(first_share_seq, last_share_seq + 1)):
            raise CanonicalBackfillUnrecoverable(
                "ledger share range "
                f"{first_share_seq}-{last_share_seq} is incomplete or out of order"
            )
        segment_bytes = self._artifacts.segment_storage_bytes(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            shares=shares,
        )
        expected = _expected_segment_digest(extra)
        if expected is not None:
            actual = _sha256_hex(segment_bytes)
            if not hmac.compare_digest(actual, expected):
                raise CanonicalBackfillMismatch(
                    f"ledger share range {first_share_seq}-{last_share_seq} "
                    f"canonicalizes to {actual}, not the embedded {expected}"
                )
        self._counters.db_segment_reloads += 1
        return shares


def _expected_segment_digest(extra: Mapping[str, Any]) -> str | None:
    """Pull the embedded segment digest out of the callback's extra kwargs."""

    for key in SEGMENT_DIGEST_KEYS:
        value = extra.get(key)
        if value:
            return str(value).lower()
    part = extra.get("part")
    if isinstance(part, Mapping):
        for key in SEGMENT_DIGEST_KEYS:
            value = part.get(key)
            if value:
                return str(value).lower()
    return None


def build_audit_store_from_env(
    environ: Mapping[str, str] | None = None,
    *,
    read_only: bool = False,
) -> AuditArtifactStore:
    """Open the audit artifact root the way every PRISM entry point does."""

    source = os.environ if environ is None else environ
    evidence_path = Path(
        env("PRISM_EVIDENCE_PATH", "prism-live-evidence.json", environ=source)
    )
    root = Path(env("PRISM_AUDIT_DIR", str(evidence_path.parent), environ=source))
    return AuditArtifactStore(
        AuditArtifactConfig(
            root=root,
            evidence_path=evidence_path,
            share_segment_size=env_nonnegative_int(
                "PRISM_AUDIT_SHARE_SEGMENT_SIZE",
                DEFAULT_AUDIT_SHARE_SEGMENT_SIZE,
                environ=source,
            ),
        ),
        canonicalizer=canonical_bundle_bytes,
        read_only=read_only,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill immutable canonical audit-bundle artifacts for existing "
            "qbit_pool_audit_bundles rows."
        )
    )
    parser.add_argument(
        "--start-after",
        default=None,
        help="resume after this block hash checkpoint (see last_checkpoint)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=env_positive_int(
            "PRISM_AUDIT_CANONICAL_BACKFILL_BATCH_SIZE",
            DEFAULT_BATCH_SIZE,
        ),
        help="rows fetched per page",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="stop after visiting this many rows in this run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="recover and verify every row without publishing anything",
    )
    parser.add_argument(
        "--psql-command",
        help="psql command; defaults to PRISM_POSTGRES_PSQL_COMMAND/PRISM_DATABASE_URL",
    )
    parser.add_argument(
        "--writer-id",
        default=os.environ.get(
            "PRISM_LEDGER_WRITER_ID",
            "prism-audit-canonical-backfill",
        ),
    )
    parser.add_argument(
        "--writer-epoch",
        type=int,
        default=int(os.environ.get("PRISM_LEDGER_WRITER_EPOCH", "1")),
    )
    parser.add_argument(
        "--writer-session-token",
        default=os.environ.get("PRISM_LEDGER_WRITER_SESSION_TOKEN"),
    )
    parser.add_argument(
        "--lease-ttl-seconds",
        type=float,
        default=float(os.environ.get("PRISM_LEDGER_LEASE_TTL_SECONDS", "60")),
    )
    parser.add_argument(
        "--lease-acquire-lock-timeout-seconds",
        type=float,
        default=env_positive_float(
            "PRISM_LEDGER_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS",
            DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
        ),
    )
    parser.add_argument(
        "--lease-acquire-attempts",
        type=int,
        default=env_positive_int(
            "PRISM_LEDGER_LEASE_ACQUIRE_ATTEMPTS",
            DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
        ),
    )
    parser.add_argument(
        "--postgres-idle-in-transaction-timeout-seconds",
        type=float,
        default=env_positive_float(
            "PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS",
            DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
        ),
    )
    parser.add_argument(
        "--postgres-tcp-keepalives-idle-seconds",
        type=int,
        default=env_positive_int(
            "PRISM_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS",
            DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
        ),
    )
    parser.add_argument(
        "--postgres-tcp-keepalives-interval-seconds",
        type=int,
        default=env_positive_int(
            "PRISM_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS",
            DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
        ),
    )
    parser.add_argument(
        "--postgres-tcp-keepalives-count",
        type=int,
        default=env_positive_int(
            "PRISM_POSTGRES_TCP_KEEPALIVES_COUNT",
            DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
        ),
    )
    parser.add_argument(
        "--no-init-schema",
        action="store_true",
        help="do not run the idempotent PRISM schema repair before backfilling",
    )
    return parser


def ledger_from_args(
    args: argparse.Namespace,
    store: AuditArtifactStore,
) -> PsqlShareLedger:
    return PsqlShareLedger(
        psql_command=psql_command_from_env(args.psql_command),
        writer_id=args.writer_id,
        writer_epoch=args.writer_epoch,
        writer_session_token=args.writer_session_token,
        initialize_schema=not args.no_init_schema and not args.dry_run,
        read_only=args.dry_run,
        lease_ttl_seconds=args.lease_ttl_seconds,
        lease_acquire_lock_timeout_seconds=args.lease_acquire_lock_timeout_seconds,
        lease_acquire_attempts=args.lease_acquire_attempts,
        postgres_idle_in_transaction_timeout_seconds=(
            args.postgres_idle_in_transaction_timeout_seconds
        ),
        postgres_tcp_keepalives_idle_seconds=args.postgres_tcp_keepalives_idle_seconds,
        postgres_tcp_keepalives_interval_seconds=(
            args.postgres_tcp_keepalives_interval_seconds
        ),
        postgres_tcp_keepalives_count=args.postgres_tcp_keepalives_count,
        audit_artifact_store=store,
        audit_bundle_canonicalizer=canonical_bundle_bytes,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    try:
        start_after = (
            None
            if args.start_after is None
            else canonical_hex(args.start_after, name="start_after", expected_bytes=32)
        )
    except ValueError as exc:
        raise SystemExit(f"--start-after is invalid: {exc}") from exc

    store = build_audit_store_from_env(read_only=args.dry_run)
    # Bind the canonical artifact API before opening the database: a phase-2
    # signature that has not landed here should fail without first taking a
    # writer lease.
    artifacts = CanonicalArtifactAdapter(store)
    ledger = ledger_from_args(args, store)
    backfill = CanonicalBundleBackfill(
        artifacts=artifacts,
        ledger=AuditBundleLedgerAdapter(ledger),
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        limit=args.limit,
        start_after=start_after,
    )
    summary = backfill.run()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
