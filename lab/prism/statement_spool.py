"""Execute streamed finalization statements with bounded coordinator memory.

The existing atomic fenced SQL is unchanged. psql owns its unavoidable full
statement buffer in an isolated process; the coordinator passes an open file,
never the complete SQL string, and reads only a bounded result after success.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterable
from typing import Any


STATEMENT_CHUNK_CHARS = 4096
STATEMENT_SPOOL_BYTES = 4 * 1024 * 1024 * 1024
STATEMENT_HELPER_MEMORY_BYTES = 4 * 1024 * 1024 * 1024
STATEMENT_RESULT_BYTES = 64 * 1024
STATEMENT_HELPER_TIMEOUT_SECONDS = 600.0
# Stdlib only: the child's import path is not guaranteed. The RLIMIT_AS block
# is an inline copy of lab.prism.helper_limits.apply_helper_memory_limit.
_EXEC_LIMITED = """
import os, resource, sys
limit_bytes = int(sys.argv[1])
try:
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
except (ValueError, OSError) as exc:
    if sys.platform != "darwin":
        raise
    sys.stderr.write(
        f"prism helper: RLIMIT_AS {limit_bytes} refused on darwin ({exc}); continuing uncapped\\n"
    )
resource.setrlimit(resource.RLIMIT_FSIZE, (int(sys.argv[2]), int(sys.argv[2])))
os.execvp(sys.argv[3], sys.argv[3:])
"""


def run_fenced_statement(ledger: Any, pieces: Iterable[str]) -> Any:
    from lab.prism.audit_bundle_view import ArtifactResourcePressure, RECORD_HELPER_ADMISSION
    from lab.prism.share_ledger import LedgerOperationTimeout, parse_single_json_value

    with tempfile.TemporaryFile() as statement, tempfile.TemporaryFile() as result, tempfile.TemporaryFile() as errors:
        size = 0
        try:
            for piece in pieces:
                for offset in range(0, len(piece), STATEMENT_CHUNK_CHARS):
                    ledger._remaining_operation_timeout()
                    chunk = piece[offset:offset + STATEMENT_CHUNK_CHARS].encode("utf-8")
                    size += len(chunk)
                    if size > STATEMENT_SPOOL_BYTES:
                        raise ArtifactResourcePressure("finalization statement spool reservation exhausted")
                    statement.write(chunk)
        except OSError as exc:
            raise ArtifactResourcePressure("finalization statement spool unavailable") from exc
        statement.seek(0)
        deadline = time.monotonic() + STATEMENT_HELPER_TIMEOUT_SECONDS

        def check() -> None:
            ledger._remaining_operation_timeout()
            if time.monotonic() >= deadline:
                raise LedgerOperationTimeout("finalization statement helper exceeded its deadline")

        with ledger._operation_gate(ledger._lock, "writer lock"), RECORD_HELPER_ADMISSION.hold(check):
            command, kwargs, timeout = ledger._psql_invocation()
            environment = dict(kwargs.get("env", os.environ))
            native = getattr(ledger, "_native", None)
            if native is not None:
                # Preserve the exact native DSN, including private schemas,
                # then append the same guards and deadlines as ordinary psql.
                from psycopg.conninfo import conninfo_to_dict, make_conninfo

                connection = conninfo_to_dict(native._conninfo)
                guards = getattr(ledger, "_session_guards", None)
                fragments = [connection.get("options", "")]
                if guards is not None:
                    fragments.append(guards.options_fragment())
                if timeout is not None:
                    timeout_ms = max(1, int(timeout * 1000))
                    fragments.append(f"-c statement_timeout={timeout_ms}ms -c lock_timeout={timeout_ms}ms")
                connection["options"] = " ".join(filter(None, fragments))
                connection["application_name"] = ledger._pool_application_name
                password = connection.pop("password", None)
                if password is not None:
                    environment["PGPASSWORD"] = password
                command = ["psql", "--dbname", make_conninfo(
                    **connection,
                ),
                           *command[len(ledger._command):]]
            if timeout is not None:
                deadline = min(deadline, time.monotonic() + timeout)
            process = subprocess.Popen(
                [sys.executable, "-c", _EXEC_LIMITED,
                 str(STATEMENT_HELPER_MEMORY_BYTES), str(STATEMENT_RESULT_BYTES), *command],
                stdin=statement, stdout=result, stderr=errors, env=environment,
            )
            try:
                while process.poll() is None:
                    check()
                    time.sleep(0.05)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
            errors.seek(0)
            diagnostic = errors.read(STATEMENT_RESULT_BYTES).decode("utf-8", "replace")
            ledger._check_psql_exit(process.returncode, diagnostic, timeout)
            result.seek(0)
            output = result.read(STATEMENT_RESULT_BYTES + 1)
            if len(output) > STATEMENT_RESULT_BYTES:
                raise RuntimeError("finalization statement result exceeds its byte limit")
            return parse_single_json_value(output.decode("utf-8"))
