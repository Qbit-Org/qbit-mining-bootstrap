"""Independent Python payout oracle in a bounded, credential-free subprocess.

The parent spools one completed ledger snapshot. Only the child constructs
AcceptedShareRecords and oracle pages. The result is a small header followed
by canonical items; neither direction pickles a Python object graph.
"""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable


WINDOW_ORACLE_BYTES = 512 * 1024 * 1024
WINDOW_ORACLE_RECORD_BYTES = 1024 * 1024
WINDOW_ORACLE_RECORDS = 1_000_000
WINDOW_ORACLE_MEMORY_BYTES = 4 * 1024 * 1024 * 1024
WINDOW_ORACLE_HEADER_BYTES = 64 * 1024
WINDOW_ORACLE_TIMEOUT_SECONDS = 120.0
_ADMISSION = threading.BoundedSemaphore(1)


class WindowOracleError(RuntimeError):
    pass


class SnapshotSink:
    def __init__(self, spool: Any, check: Callable[[], None]) -> None:
        self.spool = spool
        self.check = check
        self.reset()

    def reset(self) -> None:
        self.spool.seek(0)
        self.spool.truncate()
        self.size = self.count = 0

    def append(self, row: Any) -> None:
        self.check()
        data = json.dumps(row, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(data) > WINDOW_ORACLE_RECORD_BYTES:
            raise WindowOracleError("oracle input record exceeds byte limit")
        self.size += len(data)
        self.count += 1
        if self.size > WINDOW_ORACLE_BYTES or self.count > WINDOW_ORACLE_RECORDS:
            raise WindowOracleError("oracle snapshot exceeds resource limit")
        self.spool.write(data)

    def extend(self, rows: Any) -> None:
        for row in rows:
            self.append(row)


@dataclass(frozen=True)
class OracleResult:
    window: Any
    comparison_digest: str


def snapshot_window(
    ledger: Any, *, anchor: int, weight: int, comparison_weight: int | None = None,
    append_epoch: int = 0, check: Callable[[], None] = lambda: None,
) -> OracleResult:
    """Read once, verify independently, then validate the bounded handoff.

    The caller retains its anchor exposure and publication fences throughout.
    Admission and the read consume the same deadline as the helper. A failed
    read, child, cancellation or malformed result publishes nothing.
    """
    deadline = time.monotonic() + WINDOW_ORACLE_TIMEOUT_SECONDS

    def checkpoint() -> None:
        check()
        remaining = getattr(ledger, "_remaining_operation_timeout", None)
        if callable(remaining):
            remaining()
        if time.monotonic() >= deadline:
            raise WindowOracleError("window oracle deadline exceeded")

    checkpoint()
    while not _ADMISSION.acquire(timeout=0.05):
        checkpoint()
    try:
        with tempfile.TemporaryFile() as source, tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            sink = SnapshotSink(source, checkpoint)
            read_weight = weight if comparison_weight is None else comparison_weight
            timeout_scope = getattr(ledger, "operation_timeout", None)
            checkpoint()
            with (timeout_scope(max(0.001, deadline - time.monotonic()))
                  if callable(timeout_scope) else nullcontext()):
                stream = getattr(ledger, "spool_snapshot_at_job_issue", None)
                if callable(stream):
                    stream(anchor, window_weight=read_weight, sink=sink)
                else:
                    # Compatibility for embedded/test ledgers. The shipped SQL
                    # ledger always uses the streaming contract above.
                    records = ledger.snapshot_at_job_issue(anchor, window_weight=read_weight)
                    try:
                        for record in records:
                            sink.append(record.to_prism_json())
                    finally:
                        del records
            checkpoint()
            source.seek(0)
            request = dict(version=1, anchor=int(anchor), weight=int(weight),
                           comparison_weight=int(read_weight), append_epoch=int(append_epoch))
            process = subprocess.Popen(
                [sys.executable, "-I", str(Path(__file__).resolve()), json.dumps(request)],
                stdin=source, stdout=output, stderr=errors, close_fds=True,
                # Inherit no database, signing, SSH or service credentials.
                env={"PATH": os.defpath, "LANG": "C.UTF-8"},
            )
            try:
                while process.poll() is None:
                    checkpoint()
                    time.sleep(0.05)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
            checkpoint()
            if process.returncode != 0:
                # Child diagnostics describe input, so keep them local and
                # bounded; never echo complete rows into coordinator logs.
                raise WindowOracleError(f"window oracle exited {process.returncode}")
            output.seek(0)
            return _read_result(output, request, checkpoint)
    finally:
        _ADMISSION.release()


def _read_result(output: Any, request: dict[str, int], check: Callable[[], None]) -> OracleResult:
    from lab.prism.share_ledger import (
        DaemonShareWindowMirror, DaemonWindowMirrorDivergence,
        DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE,
        _canonical_items_record_count,
    )

    header = output.readline(WINDOW_ORACLE_HEADER_BYTES + 1)
    if len(header) > WINDOW_ORACLE_HEADER_BYTES or not header.endswith(b"\n"):
        raise WindowOracleError("invalid window oracle header framing")
    try:
        meta = json.loads(header)
        if any(type(meta[key]) is not type(value) or meta[key] != value for key, value in request.items()):
            raise ValueError("snapshot identity changed")
        count, size = meta["count"], meta["size"]
        if type(count) is not int or not 0 <= count <= WINDOW_ORACLE_RECORDS:
            raise ValueError("invalid count")
        if type(size) is not int or not 0 <= size <= WINDOW_ORACLE_BYTES:
            raise ValueError("invalid size")
        for key in ("digest", "comparison_digest"):
            value = meta[key]
            if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise ValueError("invalid digest")
        if request["weight"] == request["comparison_weight"] and meta["digest"] != meta["comparison_digest"]:
            raise ValueError("equal-weight comparison digest mismatch")
        # Read only the declared bounded payload. Hashing releases the GIL;
        # no share dictionary is decoded in the parent.
        items = output.read(size)
        if len(items) != size or output.read(1):
            raise ValueError("invalid payload framing")
        digest = hashlib.sha256(b"[")
        view = memoryview(items)
        for offset in range(0, len(view), 64 * 1024):
            check()
            digest.update(view[offset:offset + 64 * 1024])
        digest.update(b"]")
        if digest.hexdigest() != meta["digest"]:
            raise ValueError("canonical digest mismatch")
        # Structural verification walks one record at a time and retains no
        # parsed graph. Membership, sorting and the weight fold run only in
        # the independent child. This also rejects a corrupted count header.
        if _canonical_items_record_count(items) != count:
            raise ValueError("canonical record count mismatch")
        check()
        window = DaemonShareWindowMirror(
            anchor_job_issued_at_ms=request["anchor"], window_weight=request["weight"],
            page_size=DEFAULT_INCREMENTAL_SHARE_WINDOW_PAGE_SIZE, record_count=count,
            canonical_items=items, share_snapshot_sha256=meta["digest"],
        )
        return OracleResult(window, meta["comparison_digest"])
    except (KeyError, TypeError, ValueError, DaemonWindowMirrorDivergence) as exc:
        raise WindowOracleError("invalid window oracle result") from exc


def _main() -> None:
    # -I and exec start a clean interpreter: no fork of coordinator threads,
    # no inherited Python state, and no import through a caller's cwd.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import resource
    from lab.prism.helper_limits import apply_helper_memory_limit
    apply_helper_memory_limit(WINDOW_ORACLE_MEMORY_BYTES)
    resource.setrlimit(resource.RLIMIT_FSIZE, (WINDOW_ORACLE_BYTES + WINDOW_ORACLE_HEADER_BYTES,) * 2)
    resource.setrlimit(resource.RLIMIT_CPU, (int(WINDOW_ORACLE_TIMEOUT_SECONDS),) * 2)
    from lab.prism.share_ledger import IncrementalShareWindow, PsqlShareLedger

    request = json.loads(sys.argv[1])
    records = []
    size = 0
    while line := sys.stdin.buffer.readline(WINDOW_ORACLE_RECORD_BYTES + 1):
        size += len(line)
        if len(line) > WINDOW_ORACLE_RECORD_BYTES or size > WINDOW_ORACLE_BYTES or len(records) >= WINDOW_ORACLE_RECORDS:
            raise WindowOracleError("oracle input exceeds resource limit")
        records.append(PsqlShareLedger._record_from_json(json.loads(line)))
    comparison = IncrementalShareWindow.from_full_snapshot(
        records, anchor_job_issued_at_ms=request["anchor"], window_weight=request["comparison_weight"],
    )
    comparison_digest = comparison.json_records().canonical_json_sha256()
    window = comparison if request["weight"] == request["comparison_weight"] else IncrementalShareWindow.from_full_snapshot(
        records, anchor_job_issued_at_ms=request["anchor"], window_weight=request["weight"],
    )
    size = sum(len(page.canonical_json_items) for page in window.pages) + max(0, len(window.pages) - 1)
    if size > WINDOW_ORACLE_BYTES:
        raise WindowOracleError("oracle output exceeds resource limit")
    metadata = dict(request, count=window.record_count, size=size,
                    digest=window.json_records().canonical_json_sha256(), comparison_digest=comparison_digest)
    sys.stdout.buffer.write(json.dumps(metadata).encode("ascii") + b"\n")
    for index, page in enumerate(window.pages):
        if index:
            sys.stdout.buffer.write(b",")
        sys.stdout.buffer.write(page.canonical_json_items)


if __name__ == "__main__":
    _main()
