"""Durable retention of delivered, safe per-worker vardiff difficulties.

Storage seam only. The vardiff service keeps its in-process
``SessionDifficultyStore`` as the authorization-time source; this module gives
a later integration layer matching in-memory and PostgreSQL back ends for
retaining the last delivered, safe wire difficulty across coordinator
restarts. The caller — not this module — enforces share-backed upward moves:
the API carries the delivered decimal value and the evidence timestamp
explicitly so safety is never inferred from the username alone. Exact
usernames are public identities, not authentication secrets.

Contract shared by both implementations:

- One row per ``(listener, worker_username)``: the listener lane name plus the
  exact Stratum username, matched byte-for-byte with no normalization.
- ``difficulty`` is the delivered decimal wire difficulty, retained exactly
  (no integer or float conversion anywhere on the round trip).
- ``evidence_at_ms`` is when share-backed evidence last validated the value;
  ``updated_at_ms`` is the last write of any kind. Both are caller-supplied
  wall-clock milliseconds, so the two back ends stay deterministic and
  comparable.
- ``upsert`` is atomic with deterministic last-write/evidence semantics: a
  write whose evidence timestamp is older than the stored one is refused
  outright, equal-or-newer evidence replaces the row (last write wins on
  ties). Older evidence can therefore never displace newer evidence.
- ``apply_downward`` is the atomic lower-only correction: it reduces an
  existing row to a smaller difficulty and leaves ``evidence_at`` untouched,
  so a step-down the live session already made can never suppress later
  genuine share-backed upward evidence, and can never refresh the TTL of a
  value no share proved. A missing row, or one already at or below the
  requested difficulty, is a no-op — never an insert, because there is no
  stored value for a restart to resurrect.
- ``load_recent`` returns a bounded newest-first set whose evidence is
  strictly newer than the cutoff, deterministically ordered for preload:
  evidence timestamp descending, ties broken by (listener, worker_username)
  in byte order (the key columns collate as "C", so both back ends agree).
- ``prune`` deletes rows whose evidence is at or before the cutoff — exactly
  the complement of ``load_recent`` — entirely inside the store (an indexed
  range scan in PostgreSQL), never via an unbounded application-side sweep.

Deliberately absent: any per-key point read. Reconnect authorization must
never wait on a synchronous database round trip, so the only read is the
bounded batch preload an integration layer feeds into its in-process store.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from threading import Lock
from typing import Any, Callable, Protocol, runtime_checkable


# Byte-identical to the qbit_worker_difficulty DDL block in
# crates/qbit-prism/sql/001_share_ledger.sql (which is the canonical,
# transactional apply path); a contract test pins the match. Both statements
# are idempotent, so ensure_schema() may be re-run freely and the monolithic
# file may be reapplied over an existing table.
WORKER_DIFFICULTY_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS qbit_worker_difficulty (
    listener text COLLATE "C" NOT NULL CHECK (listener <> ''),
    worker_username text COLLATE "C" NOT NULL CHECK (worker_username <> ''),
    difficulty numeric NOT NULL CHECK (
        difficulty > 0 AND difficulty < 'Infinity'::numeric
    ),
    evidence_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (listener, worker_username)
);

CREATE INDEX IF NOT EXISTS qbit_worker_difficulty_evidence_idx
    ON qbit_worker_difficulty (evidence_at DESC, listener, worker_username);
"""

# Wall-clock bounds for every millisecond timestamp and cutoff this module
# accepts: non-negative (evidence predating the epoch is meaningless) and at
# most 9999-12-31T23:59:59.999Z, comfortably inside both PostgreSQL
# timestamptz range and exact float64 millisecond arithmetic.
MAX_TIMESTAMP_MS = 253_402_300_799_999

# Difficulty magnitude bounds enforced identically by both back ends, so the
# in-memory store cannot accept a value the PostgreSQL numeric apply would
# reject (or vice versa). Far beyond any real Stratum wire difficulty, far
# inside PostgreSQL numeric limits.
MAX_DIFFICULTY_DIGITS = 1_000
MAX_DIFFICULTY_ADJUSTED_EXPONENT = 999


@dataclass(frozen=True)
class WorkerDifficultyRecord:
    """One retained per-worker difficulty row, identical in both back ends."""

    listener: str
    worker_username: str
    difficulty: Decimal
    evidence_at_ms: int
    updated_at_ms: int


@dataclass(frozen=True)
class WorkerDifficultyUpsertResult:
    """Outcome of one upsert.

    ``applied`` is False when the write carried evidence strictly older than
    the stored row and was refused. ``stored`` is the row now standing for the
    key; it can be None only on the PostgreSQL back end, in the narrow race
    where a refused write's follow-up read finds no snapshot-visible row.
    """

    applied: bool
    stored: WorkerDifficultyRecord | None


@dataclass(frozen=True)
class WorkerDifficultyLowerResult:
    """Outcome of one lower-only correction.

    ``applied`` is True only when a stored row was actually reduced. A key with
    no stored row, or one already at or below the requested difficulty, is a
    no-op: there is nothing a restart could resurrect. ``stored`` is the row now
    standing for the key, or None when no row exists (or, on the PostgreSQL
    back end, in the narrow race where a no-op's follow-up read finds no
    snapshot-visible row).
    """

    applied: bool
    stored: WorkerDifficultyRecord | None


@runtime_checkable
class WorkerDifficultyStorePort(Protocol):
    """The exact surface an integration layer may depend on.

    Four operations, none of which is a per-key synchronous read.
    """

    def upsert(
        self,
        *,
        listener: str,
        worker_username: str,
        difficulty: Decimal,
        evidence_at_ms: int,
        now_ms: int,
    ) -> WorkerDifficultyUpsertResult: ...

    def apply_downward(
        self,
        *,
        listener: str,
        worker_username: str,
        difficulty: Decimal,
        now_ms: int,
    ) -> WorkerDifficultyLowerResult: ...

    def load_recent(
        self,
        *,
        evidence_after_ms: int,
        limit: int,
    ) -> list[WorkerDifficultyRecord]: ...

    def prune(
        self,
        *,
        evidence_cutoff_ms: int,
        limit: int | None = None,
    ) -> int: ...


def _validated_identity(value: str, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if value == "":
        raise ValueError(f"{name} must not be empty")
    if "\x00" in value:
        # PostgreSQL text cannot hold NUL; refusing it here keeps the
        # in-memory back end from accepting a key the durable one rejects.
        raise ValueError(f"{name} must not contain NUL")
    return value


def _validated_difficulty(difficulty: Decimal) -> Decimal:
    if isinstance(difficulty, bool) or not isinstance(difficulty, Decimal):
        raise ValueError("difficulty must be a Decimal (no float or int coercion)")
    if not difficulty.is_finite() or difficulty <= 0:
        raise ValueError("difficulty must be a positive finite Decimal")
    parts = difficulty.as_tuple()
    if len(parts.digits) > MAX_DIFFICULTY_DIGITS:
        raise ValueError(
            f"difficulty exceeds {MAX_DIFFICULTY_DIGITS} significant digits"
        )
    if abs(difficulty.adjusted()) > MAX_DIFFICULTY_ADJUSTED_EXPONENT:
        raise ValueError(
            "difficulty magnitude exceeds "
            f"1E±{MAX_DIFFICULTY_ADJUSTED_EXPONENT}"
        )
    return difficulty


def _validated_timestamp_ms(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer millisecond timestamp")
    if value < 0 or value > MAX_TIMESTAMP_MS:
        raise ValueError(f"{name} must be within [0, {MAX_TIMESTAMP_MS}]")
    return value


def _validated_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("limit must be an integer")
    if limit <= 0:
        raise ValueError("limit must be positive")
    return limit


class MemoryWorkerDifficultyStore:
    """In-memory reference implementation of the storage contract.

    Semantically identical to :class:`PostgresWorkerDifficultyStore` (the
    parity gate drives both in lockstep). The lock is a leaf lock: nothing
    here calls out of this module while holding it.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[tuple[str, str], WorkerDifficultyRecord] = {}

    def upsert(
        self,
        *,
        listener: str,
        worker_username: str,
        difficulty: Decimal,
        evidence_at_ms: int,
        now_ms: int,
    ) -> WorkerDifficultyUpsertResult:
        listener = _validated_identity(listener, "listener")
        worker_username = _validated_identity(worker_username, "worker_username")
        difficulty = _validated_difficulty(difficulty)
        evidence_at_ms = _validated_timestamp_ms(evidence_at_ms, "evidence_at_ms")
        now_ms = _validated_timestamp_ms(now_ms, "now_ms")
        key = (listener, worker_username)
        with self._lock:
            existing = self._entries.get(key)
            if existing is not None and evidence_at_ms < existing.evidence_at_ms:
                return WorkerDifficultyUpsertResult(applied=False, stored=existing)
            record = WorkerDifficultyRecord(
                listener=listener,
                worker_username=worker_username,
                difficulty=difficulty,
                evidence_at_ms=evidence_at_ms,
                updated_at_ms=now_ms,
            )
            self._entries[key] = record
            return WorkerDifficultyUpsertResult(applied=True, stored=record)

    def apply_downward(
        self,
        *,
        listener: str,
        worker_username: str,
        difficulty: Decimal,
        now_ms: int,
    ) -> WorkerDifficultyLowerResult:
        listener = _validated_identity(listener, "listener")
        worker_username = _validated_identity(worker_username, "worker_username")
        difficulty = _validated_difficulty(difficulty)
        now_ms = _validated_timestamp_ms(now_ms, "now_ms")
        key = (listener, worker_username)
        with self._lock:
            existing = self._entries.get(key)
            if existing is None or existing.difficulty <= difficulty:
                return WorkerDifficultyLowerResult(applied=False, stored=existing)
            # evidence_at is deliberately carried forward unchanged: this
            # correction is not new evidence, and re-stamping it would both
            # extend the TTL of an unproven value and let it refuse a later
            # share-backed write whose evidence predates the step-down.
            record = WorkerDifficultyRecord(
                listener=existing.listener,
                worker_username=existing.worker_username,
                difficulty=difficulty,
                evidence_at_ms=existing.evidence_at_ms,
                updated_at_ms=now_ms,
            )
            self._entries[key] = record
            return WorkerDifficultyLowerResult(applied=True, stored=record)

    def load_recent(
        self,
        *,
        evidence_after_ms: int,
        limit: int,
    ) -> list[WorkerDifficultyRecord]:
        evidence_after_ms = _validated_timestamp_ms(
            evidence_after_ms, "evidence_after_ms"
        )
        limit = _validated_limit(limit)
        with self._lock:
            recent = [
                record
                for record in self._entries.values()
                if record.evidence_at_ms > evidence_after_ms
            ]
        recent.sort(
            key=lambda record: (
                -record.evidence_at_ms,
                record.listener,
                record.worker_username,
            )
        )
        return recent[:limit]

    def prune(
        self,
        *,
        evidence_cutoff_ms: int,
        limit: int | None = None,
    ) -> int:
        evidence_cutoff_ms = _validated_timestamp_ms(
            evidence_cutoff_ms, "evidence_cutoff_ms"
        )
        if limit is not None:
            limit = _validated_limit(limit)
        with self._lock:
            doomed = [
                key
                for key, record in self._entries.items()
                if record.evidence_at_ms <= evidence_cutoff_ms
            ]
            if limit is not None:
                doomed.sort(
                    key=lambda key: (
                        self._entries[key].evidence_at_ms,
                        key[0],
                        key[1],
                    )
                )
                doomed = doomed[:limit]
            for key in doomed:
                del self._entries[key]
            return len(doomed)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


def _text_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _difficulty_literal(difficulty: Decimal) -> str:
    # str() of a validated finite Decimal contains only [0-9.E+-]; the quoted
    # cast keeps the rendering injection-proof regardless.
    return f"'{difficulty}'::numeric"


def _timestamp_literal(timestamp_ms: int) -> str:
    # Same ms-to-timestamptz idiom as share_ledger.py. Validated bounds keep
    # the double-precision division exact to well below the microsecond
    # resolution timestamptz stores, so ms values round-trip exactly.
    return f"to_timestamp(({timestamp_ms}::double precision / 1000.0))"


_RECORD_JSON_FIELDS = """\
'listener', {alias}.listener,
        'worker_username', {alias}.worker_username,
        'difficulty', {alias}.difficulty::text,
        'evidence_at_ms', round(extract(epoch FROM {alias}.evidence_at) * 1000)::bigint,
        'updated_at_ms', round(extract(epoch FROM {alias}.updated_at) * 1000)::bigint"""


def _record_from_json(payload: dict[str, Any]) -> WorkerDifficultyRecord:
    return WorkerDifficultyRecord(
        listener=str(payload["listener"]),
        worker_username=str(payload["worker_username"]),
        # difficulty travels as text inside the JSON payload precisely so it
        # never passes through a float.
        difficulty=Decimal(str(payload["difficulty"])),
        evidence_at_ms=int(payload["evidence_at_ms"]),
        updated_at_ms=int(payload["updated_at_ms"]),
    )


class PostgresWorkerDifficultyStore:
    """PostgreSQL implementation over an injected statement runner.

    ``run_json`` executes one self-contained SQL statement and returns its
    single JSON value — the contract ``PsqlShareLedger._run_json`` and
    ``LedgerSqlPort.run_json`` already honour, so the store rides the
    coordinator's existing pooled connections without opening its own.
    ``run_script`` (optional) executes a multi-statement script and is only
    needed for :meth:`ensure_schema`; the canonical schema path remains the
    monolithic ``001_share_ledger.sql`` apply, which creates the same table.
    """

    def __init__(
        self,
        run_json: Callable[[str], Any],
        *,
        run_script: Callable[[str], None] | None = None,
    ) -> None:
        self._run_json = run_json
        self._run_script = run_script

    @classmethod
    def from_share_ledger(
        cls,
        ledger: Any,
        *,
        timeout_seconds: float,
    ) -> "PostgresWorkerDifficultyStore":
        """Build over a bounded, admission-gated ``PsqlShareLedger`` seam.

        Worker-difficulty traffic is not writer-lease state, so it uses one
        of the ledger's bounded read/concurrent statement slots. The statement
        is still a mutation and is therefore never retried after an ambiguous
        connection failure.
        """

        def run_json(sql: str) -> Any:
            with ledger.operation_timeout(timeout_seconds):
                with ledger._operation_gate(
                    ledger._read_semaphore,
                    "worker-difficulty slot",
                ):
                    return ledger._run_json(sql)

        return cls(run_json, run_script=ledger._run_script)

    def ensure_schema(self) -> None:
        """Idempotently apply this store's DDL (subset of the monolithic file)."""
        if self._run_script is None:
            raise RuntimeError(
                "ensure_schema requires a run_script callable; the monolithic "
                "001_share_ledger.sql apply also creates this table"
            )
        self._run_script(WORKER_DIFFICULTY_SCHEMA_SQL)

    def upsert(
        self,
        *,
        listener: str,
        worker_username: str,
        difficulty: Decimal,
        evidence_at_ms: int,
        now_ms: int,
    ) -> WorkerDifficultyUpsertResult:
        listener = _validated_identity(listener, "listener")
        worker_username = _validated_identity(worker_username, "worker_username")
        difficulty = _validated_difficulty(difficulty)
        evidence_at_ms = _validated_timestamp_ms(evidence_at_ms, "evidence_at_ms")
        now_ms = _validated_timestamp_ms(now_ms, "now_ms")
        listener_literal = _text_literal(listener)
        username_literal = _text_literal(worker_username)
        applied_fields = _RECORD_JSON_FIELDS.format(alias="applied")
        current_fields = _RECORD_JSON_FIELDS.format(alias="current")
        # One atomic statement: the ON CONFLICT WHERE guard is what makes
        # older evidence unable to displace newer evidence, and equal-or-newer
        # evidence a deterministic last-write-wins replacement. The refused
        # branch reads the standing row from the statement snapshot instead
        # (a data-modifying CTE's effects are invisible to the outer query,
        # so the applied branch must come from RETURNING).
        payload = self._run_json(
            f"""
WITH applied AS (
    INSERT INTO qbit_worker_difficulty AS existing (
        listener,
        worker_username,
        difficulty,
        evidence_at,
        updated_at
    )
    VALUES (
        {listener_literal},
        {username_literal},
        {_difficulty_literal(difficulty)},
        {_timestamp_literal(evidence_at_ms)},
        {_timestamp_literal(now_ms)}
    )
    ON CONFLICT (listener, worker_username) DO UPDATE
    SET difficulty = EXCLUDED.difficulty,
        evidence_at = EXCLUDED.evidence_at,
        updated_at = EXCLUDED.updated_at
    WHERE EXCLUDED.evidence_at >= existing.evidence_at
    RETURNING listener, worker_username, difficulty, evidence_at, updated_at
)
SELECT json_build_object(
    'applied', EXISTS (SELECT 1 FROM applied),
    'stored', COALESCE(
        (SELECT json_build_object(
        {applied_fields}
        ) FROM applied),
        (SELECT json_build_object(
        {current_fields}
        )
        FROM qbit_worker_difficulty current
        WHERE current.listener = {listener_literal}
          AND current.worker_username = {username_literal})
    )
);
"""
        )
        stored = payload.get("stored")
        return WorkerDifficultyUpsertResult(
            applied=bool(payload["applied"]),
            stored=None if stored is None else _record_from_json(stored),
        )

    def apply_downward(
        self,
        *,
        listener: str,
        worker_username: str,
        difficulty: Decimal,
        now_ms: int,
    ) -> WorkerDifficultyLowerResult:
        listener = _validated_identity(listener, "listener")
        worker_username = _validated_identity(worker_username, "worker_username")
        difficulty = _validated_difficulty(difficulty)
        now_ms = _validated_timestamp_ms(now_ms, "now_ms")
        listener_literal = _text_literal(listener)
        username_literal = _text_literal(worker_username)
        difficulty_literal = _difficulty_literal(difficulty)
        lowered_fields = _RECORD_JSON_FIELDS.format(alias="lowered")
        current_fields = _RECORD_JSON_FIELDS.format(alias="current")
        # One atomic statement. The row-level UPDATE takes its own lock, so a
        # concurrent upsert either precedes it (and is then compared against)
        # or follows it (and wins on evidence); evidence_at is never written
        # here, which is what keeps a step-down from suppressing later
        # share-backed evidence. The no-op branch reads the standing row from
        # the statement snapshot for the same reason upsert does.
        payload = self._run_json(
            f"""
WITH lowered AS (
    UPDATE qbit_worker_difficulty
    SET difficulty = {difficulty_literal},
        updated_at = {_timestamp_literal(now_ms)}
    WHERE listener = {listener_literal}
      AND worker_username = {username_literal}
      AND difficulty > {difficulty_literal}
    RETURNING listener, worker_username, difficulty, evidence_at, updated_at
)
SELECT json_build_object(
    'applied', EXISTS (SELECT 1 FROM lowered),
    'stored', COALESCE(
        (SELECT json_build_object(
        {lowered_fields}
        ) FROM lowered),
        (SELECT json_build_object(
        {current_fields}
        )
        FROM qbit_worker_difficulty current
        WHERE current.listener = {listener_literal}
          AND current.worker_username = {username_literal})
    )
);
"""
        )
        stored = payload.get("stored")
        return WorkerDifficultyLowerResult(
            applied=bool(payload["applied"]),
            stored=None if stored is None else _record_from_json(stored),
        )

    def load_recent(
        self,
        *,
        evidence_after_ms: int,
        limit: int,
    ) -> list[WorkerDifficultyRecord]:
        evidence_after_ms = _validated_timestamp_ms(
            evidence_after_ms, "evidence_after_ms"
        )
        limit = _validated_limit(limit)
        recent_fields = _RECORD_JSON_FIELDS.format(alias="recent")
        # The inner ORDER BY + LIMIT walk qbit_worker_difficulty_evidence_idx
        # directly; the aggregate re-states the same ordering because
        # json_agg offers no ordering guarantee of its own.
        payload = self._run_json(
            f"""
SELECT COALESCE(
    (
        SELECT json_agg(
            json_build_object(
        {recent_fields}
            )
            ORDER BY recent.evidence_at DESC, recent.listener, recent.worker_username
        )
        FROM (
            SELECT listener, worker_username, difficulty, evidence_at, updated_at
            FROM qbit_worker_difficulty
            WHERE evidence_at > {_timestamp_literal(evidence_after_ms)}
            ORDER BY evidence_at DESC, listener, worker_username
            LIMIT {limit}
        ) recent
    ),
    '[]'::json
);
"""
        )
        return [_record_from_json(entry) for entry in payload]

    def prune(
        self,
        *,
        evidence_cutoff_ms: int,
        limit: int | None = None,
    ) -> int:
        evidence_cutoff_ms = _validated_timestamp_ms(
            evidence_cutoff_ms, "evidence_cutoff_ms"
        )
        cutoff_literal = _timestamp_literal(evidence_cutoff_ms)
        if limit is None:
            payload = self._run_json(
                f"""
WITH deleted AS (
    DELETE FROM qbit_worker_difficulty
    WHERE evidence_at <= {cutoff_literal}
    RETURNING 1
)
SELECT json_build_object('deleted', (SELECT count(*) FROM deleted));
"""
            )
            return int(payload["deleted"])
        limit = _validated_limit(limit)
        # Oldest evidence first with the same tie-break the memory store
        # applies, so a bounded prune removes a deterministic set.
        payload = self._run_json(
            f"""
WITH doomed AS (
    SELECT listener, worker_username
    FROM qbit_worker_difficulty
    WHERE evidence_at <= {cutoff_literal}
    ORDER BY evidence_at ASC, listener, worker_username
    LIMIT {limit}
),
deleted AS (
    DELETE FROM qbit_worker_difficulty target
    USING doomed
    WHERE target.listener = doomed.listener
      AND target.worker_username = doomed.worker_username
      AND target.evidence_at <= {cutoff_literal}
    RETURNING 1
)
SELECT json_build_object('deleted', (SELECT count(*) FROM deleted));
"""
        )
        return int(payload["deleted"])


__all__ = [
    "MAX_DIFFICULTY_ADJUSTED_EXPONENT",
    "MAX_DIFFICULTY_DIGITS",
    "MAX_TIMESTAMP_MS",
    "MemoryWorkerDifficultyStore",
    "PostgresWorkerDifficultyStore",
    "WORKER_DIFFICULTY_SCHEMA_SQL",
    "WorkerDifficultyLowerResult",
    "WorkerDifficultyRecord",
    "WorkerDifficultyStorePort",
    "WorkerDifficultyUpsertResult",
]
