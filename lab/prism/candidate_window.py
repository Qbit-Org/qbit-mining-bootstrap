"""Bounded, exact membership checks for a replayed candidate's share window.

Only omissions matter: the durable window must be a subset of the recorded
window. PostgreSQL performs that comparison against a private temporary table
fed through bounded COPY writes, returning a boolean rather than a window-sized
JSON value. No ledger mutation or writer-lease lock is involved. The in-memory
ledger compatibility path uses an on-disk index instead of a Python set.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from typing import Any


COPY_TEXT_CHARACTERS = 4096
COPY_TEXT_MAX_BYTES = COPY_TEXT_CHARACTERS * 4
COPY_TEXT_MAX_RECORDS = 256
_CREATE = """
CREATE TEMPORARY TABLE qbit_candidate_recorded_ids (share_id text COLLATE "C" NOT NULL)
ON COMMIT DROP;
SET LOCAL work_mem = '8MB';
"""
_COPY = "COPY pg_temp.qbit_candidate_recorded_ids (share_id) FROM STDIN"


def recorded_share_ids(shares: Iterable[Any]) -> Iterator[Any]:
    """Keep the historical dictionary/type/string conversion semantics."""
    for row in shares:
        if isinstance(row, Mapping):
            value = row.get("share_id")
            yield value if callable(getattr(value, "iter_text_chunks", None)) else str(value)


def _text_pieces(value: Any) -> Iterator[str]:
    streamed = getattr(value, "iter_text_chunks", None)
    chunks = streamed() if callable(streamed) else (value,)
    for chunk in chunks:
        for offset in range(0, len(chunk), COPY_TEXT_CHARACTERS):
            yield chunk[offset:offset + COPY_TEXT_CHARACTERS]


def copy_text_chunks(
    values: Iterable[Any], *, check: Callable[[], Any] = lambda: None,
) -> Iterator[bytes]:
    """Encode COPY text without encoding or escaping an entire field at once.

    A UTF-8 character is at most four bytes; every escaped ASCII character
    uses two. Thus even an unusually long share ID obeys the same write bound.
    Literal backslashes (including \\N and \\.) cannot become COPY commands.
    """
    buffer = bytearray()
    records = 0
    for value in values:
        for part in _text_pieces(value):
            if "\x00" in part:
                # PostgreSQL text cannot contain NUL. In psql's COPY input,
                # it can also truncate the stream without a useful diagnostic.
                raise ValueError("candidate share ID contains a NUL character")
            part = part.replace("\\", "\\\\").replace("\t", "\\t")
            part = part.replace("\n", "\\n").replace("\r", "\\r")
            encoded = part.encode("utf-8")
            if buffer and len(buffer) + len(encoded) > COPY_TEXT_MAX_BYTES:
                check()
                yield bytes(buffer)
                buffer.clear()
                records = 0
            buffer.extend(encoded)
        if len(buffer) == COPY_TEXT_MAX_BYTES:
            check()
            yield bytes(buffer)
            buffer.clear()
            records = 0
        buffer.append(10)
        records += 1
        if records == COPY_TEXT_MAX_RECORDS:
            check()
            yield bytes(buffer)
            buffer.clear()
            records = 0
    if buffer:
        check()
        yield bytes(buffer)


def disk_window_covers(recorded: Iterable[Any], durable: Iterable[Any]) -> bool:
    """Compatibility path for in-memory ledgers; never retain the ID set."""
    with tempfile.TemporaryDirectory(prefix="prism-candidate-membership-") as root, tempfile.TemporaryFile() as values:
        connection = sqlite3.connect(Path(root) / "ids.sqlite")
        try:
            connection.execute("PRAGMA cache_size = -2048")
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                "CREATE TABLE recorded (digest BLOB, start INTEGER, length INTEGER)"
            )
            connection.execute("CREATE INDEX recorded_digest ON recorded (digest)")
            for value in recorded:
                digest = hashlib.sha256()
                start = values.tell()
                for piece in _text_pieces(value):
                    encoded = piece.encode("utf-8")
                    digest.update(encoded)
                    values.write(encoded)
                connection.execute("INSERT INTO recorded VALUES (?, ?, ?)",
                                   (digest.digest(), start, values.tell() - start))
            for value in recorded_share_ids(durable):
                digest = hashlib.sha256()
                for piece in _text_pieces(value):
                    digest.update(piece.encode("utf-8"))
                matched = False
                for start, length in connection.execute(
                    "SELECT start, length FROM recorded WHERE digest = ?", (digest.digest(),),
                ):
                    # A digest only selects possible matches. Equality still
                    # compares every byte, including under a hash collision.
                    values.seek(start)
                    remaining = length
                    for piece in _text_pieces(value):
                        encoded = piece.encode("utf-8")
                        if len(encoded) > remaining or values.read(len(encoded)) != encoded:
                            break
                        remaining -= len(encoded)
                    else:
                        if remaining == 0:
                            matched = True
                            break
                if not matched:
                    return False
            return True
        finally:
            connection.close()


def _query(anchor_job_issued_at_ms: int, network_difficulty: int) -> str:
    return f"""
SELECT json_build_object('reproducible', NOT EXISTS (
    SELECT 1 FROM qbit_audit_share_window(
        to_timestamp(({int(anchor_job_issued_at_ms)}::double precision / 1000.0)),
        {int(network_difficulty)}::numeric
    ) AS durable
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_temp.qbit_candidate_recorded_ids AS recorded
        WHERE recorded.share_id = durable.share_id
    )
));
"""


def _flushing_copy_writer(cursor: Any) -> Any:
    # Keep psycopg optional for the psql-only deployment. LibpqWriter's
    # standard finish handles CopyDone/CopyFail and cancellation correctly.
    from psycopg.copy import LibpqWriter
    from psycopg.generators import copy_to

    class FlushingWriter(LibpqWriter):
        def write(self, data: bytes) -> None:
            # On Linux the default writer can grow libpq's output buffer with
            # the entire window while PostgreSQL is not consuming the socket.
            # Our producer caps each write at 16 KiB; flush it before advancing.
            self.connection.wait(copy_to(self._pgconn, data, flush=True))

    return FlushingWriter(cursor)


def postgres_window_covers(
    ledger: Any,
    shares: Iterable[Any],
    *,
    anchor_job_issued_at_ms: int,
    network_difficulty: int,
) -> bool:
    """Use one durable snapshot, bounded transport, and exact text equality.

    The COPY table is connection-private and disappears on commit or rollback.
    Server join memory can spill under work_mem. It grants neither credit nor
    replay eligibility. A lost connection fails the read; the caller's existing
    candidate retry repeats it with a fresh table and its immutable share view.
    """
    from lab.prism.share_ledger import LedgerOperationTimeout, parse_single_json_value

    query = _query(anchor_job_issued_at_ms, network_difficulty)
    with ledger._operation_gate(ledger._read_semaphore, "read slot"):
        native = getattr(ledger, "_native", None)
        if native is not None and callable(getattr(native, "connection", None)):
            timeout = ledger._remaining_operation_timeout()
            kwargs = {} if timeout is None else {"timeout_seconds": timeout}
            try:
                with native.connection(**kwargs) as connection:
                    with connection.transaction():
                        def arm_statement() -> None:
                            remaining = ledger._remaining_operation_timeout()
                            if remaining is None:
                                return
                            milliseconds = max(1, int(remaining * 1000))
                            connection.execute(
                                f"SET LOCAL statement_timeout = '{milliseconds}ms'"
                            )
                            connection.execute(
                                f"SET LOCAL lock_timeout = '{milliseconds}ms'"
                            )

                        arm_statement()
                        connection.execute(_CREATE)
                        ledger._note_operation_progress()
                        with connection.cursor() as cursor:
                            arm_statement()
                            with cursor.copy(_COPY, writer=_flushing_copy_writer(cursor)) as copy:
                                for chunk in copy_text_chunks(
                                    recorded_share_ids(shares),
                                    check=ledger._note_json_row_batch,
                                ):
                                    copy.write(chunk)
                        ledger._note_json_row_batch()
                        # Refresh the statement budget after streaming, so
                        # each phase shares the caller's original deadline.
                        arm_statement()
                        connection.execute("ANALYZE pg_temp.qbit_candidate_recorded_ids")
                        ledger._note_json_row_batch()
                        arm_statement()
                        row = connection.execute(query).fetchone()
                        result = parse_single_json_value(row[0] if row else None)
                        ledger._remaining_operation_timeout()
            except native._psycopg.OperationalError as exc:
                from lab.prism.share_ledger import _is_postgres_deadline_error

                if _is_postgres_deadline_error(exc):
                    raise LedgerOperationTimeout("candidate window check timed out") from exc
                raise RuntimeError(f"postgres candidate window check failed: {exc}") from exc
        else:
            # psql reads a boundedly-written file. communicate(input=...) would
            # reconstitute the entire recorded window in a single bytes object.
            with tempfile.TemporaryFile() as source, tempfile.TemporaryFile() as errors:
                source.write(_CREATE.encode("ascii"))
                source.write((_COPY + ";\n").encode("ascii"))
                for chunk in copy_text_chunks(
                    recorded_share_ids(shares), check=ledger._note_json_row_batch,
                ):
                    source.write(chunk)
                source.write(b"\\.\n")
                source.write(b"ANALYZE pg_temp.qbit_candidate_recorded_ids;\n")
                source.write(query.encode("ascii"))
                source.seek(0)
                command, kwargs, timeout = ledger._psql_invocation()
                try:
                    completed = subprocess.run(
                        command, stdin=source, stdout=subprocess.PIPE, stderr=errors,
                        check=False, **kwargs,
                    )
                except subprocess.TimeoutExpired as exc:
                    raise LedgerOperationTimeout("candidate window check timed out") from exc
                errors.seek(0)
                # COPY errors can echo a whole input line. Keep diagnostics
                # bounded as well; the spool itself dies with this operation.
                error_text = errors.read(64 * 1024).decode("utf-8", errors="replace")
                ledger._check_psql_exit(completed.returncode, error_text, timeout)
                ledger._remaining_operation_timeout()
                result = json.loads(completed.stdout)
        if not isinstance(result, dict) or type(result.get("reproducible")) is not bool:
            raise RuntimeError("candidate window check returned no boolean result")
        return result["reproducible"]
