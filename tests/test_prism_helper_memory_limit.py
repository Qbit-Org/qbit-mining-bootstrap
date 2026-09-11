"""Isolated helpers cap their address space: strict on Linux, degraded on macOS.

Every PRISM helper that decodes unbounded input runs under ``RLIMIT_AS``. The
Linux-only tests read the limit inside real helper children launched the way
production launches them. The policy tests patch ``sys.platform`` and
``resource.setrlimit`` so both branches run on every host.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import tempfile
import threading
from typing import Any
import unittest
from unittest import mock

from lab.prism import audit_bundle_view as audit
from lab.prism import candidate_store as store
from lab.prism import statement_spool
from lab.prism.helper_limits import apply_helper_memory_limit
from lab.prism.share_ledger import PsqlShareLedger


# Distinct from every production default, so a test cannot pass by accident.
NONDEFAULT_LIMIT = 3 * 1024 * 1024 * 1024 + 4096
DARWIN_REFUSAL = ValueError("current limit exceeds maximum limit")

# Prepended to a helper child: pose as Linux and have the kernel refuse the
# address-space cap, leaving every other limit real.
_REFUSE_CAP_IN_CHILD = """
import resource, sys
sys.platform = "linux"
def _refuse(which, limits, _real=resource.setrlimit):
    if which == resource.RLIMIT_AS:
        raise ValueError("current limit exceeds maximum limit")
    _real(which, limits)
resource.setrlimit = _refuse
"""


def _setrlimit_refusing(refused: int, refusal: BaseException | None) -> tuple[Any, list[tuple[int, Any]]]:
    calls: list[tuple[int, Any]] = []

    def setrlimit(which: int, limits: Any) -> None:
        calls.append((which, limits))
        if which == refused and refusal is not None:
            raise refusal

    return setrlimit, calls


class _SpoolLedger:
    """The ledger surface ``run_fenced_statement`` uses, with ``command`` as psql."""

    _check_psql_exit = staticmethod(PsqlShareLedger._check_psql_exit)

    def __init__(self, command: list[str]) -> None:
        self._lock = threading.Lock()
        self._command = command
        self._native = None

    def _remaining_operation_timeout(self) -> None:
        return None

    @contextlib.contextmanager
    def _operation_gate(self, gate: Any, _name: str) -> Any:
        with gate:
            yield

    def _psql_invocation(self) -> tuple[list[str], dict[str, Any], None]:
        return list(self._command), {}, None


class _HelperCase(unittest.TestCase):
    def candidate_helper(self, **kwargs: Any) -> store.LegacyCandidateHelper:
        return store.LegacyCandidateHelper(
            store.LegacyTransport(database_url="unused", psql_command=()), **kwargs
        )

    def record_source(self, raw: bytes) -> audit.ArtifactSource:
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        path = os.path.join(directory, "record.json")
        with open(path, "wb") as handle:
            handle.write(raw)
        source = audit.ArtifactSource(os.open(path, os.O_RDONLY), path=Path(path))
        self.addCleanup(source.close)
        return source

    def replace_child(self, module: Any, entry: str, script: str) -> Any:
        """Run ``script`` in place of ``python -m entry``, keeping the rest."""
        real_popen = subprocess.Popen

        def spawn(args: list[str], **kwargs: Any) -> Any:
            self.assertEqual(list(args[1:3]), ["-m", entry])
            return real_popen([args[0], "-c", script, *args[3:]], **kwargs)

        return mock.patch.object(module.subprocess, "Popen", spawn)

    def prefix_exec_limited(self, prelude: str) -> Any:
        real_popen = subprocess.Popen

        def spawn(args: list[str], **kwargs: Any) -> Any:
            self.assertEqual(list(args[1:3]), ["-c", statement_spool._EXEC_LIMITED])
            return real_popen([args[0], "-c", prelude + args[2], *args[3:]], **kwargs)

        return mock.patch.object(statement_spool.subprocess, "Popen", spawn)


@unittest.skipUnless(sys.platform.startswith("linux"), "only Linux enforces a helper-sized RLIMIT_AS")
class EffectiveHelperLimitTests(_HelperCase):
    """The limit each launcher asks for is the one its child runs under."""

    def test_candidate_helper_runs_under_the_requested_limit(self) -> None:
        # Non-default limit: constructor -> request header -> helper_main.
        probe = (
            "import resource, sys\n"
            "from lab.prism import candidate_store as store\n"
            "def report(_transport, _block_hash):\n"
            "    sys.stderr.write('RLIMIT_AS %d %d\\n' % resource.getrlimit(resource.RLIMIT_AS))\n"
            "    raise RuntimeError('probe stops here')\n"
            "store._helper_fetch_legacy_text = report\n"
            "sys.exit(store.helper_main())\n"
        )
        helper = self.candidate_helper(memory_limit_bytes=NONDEFAULT_LIMIT)
        with self.replace_child(store, "lab.prism.candidate_store", probe):
            with self.assertRaisesRegex(
                store.CandidateStorageError, f"RLIMIT_AS {NONDEFAULT_LIMIT} {NONDEFAULT_LIMIT}\\b"
            ):
                helper.convert("aa" * 32, "unused.body", "unused.idx")

    def test_audit_record_helper_runs_under_its_limit(self) -> None:
        # The launcher passes no limit; the helper applies its own constant.
        probe = (
            "import resource, sys\n"
            "from lab.prism import audit_bundle_view as audit\n"
            "status = audit._helper_main(sys.argv[1:])\n"
            "sys.stderr.write('RLIMIT_AS %d %d status %d\\n' % (*resource.getrlimit(resource.RLIMIT_AS), status))\n"
            "sys.exit(3)\n"
        )
        raw = b'{"a":"' + b"x" * 100 + b'"}'
        limit = audit.RAW_RECORD_HELPER_MEMORY_BYTES
        with self.replace_child(audit, "lab.prism.audit_bundle_view", probe):
            with self.assertRaisesRegex(
                audit.ArtifactResourcePressure, f"status 3: RLIMIT_AS {limit} {limit} status 0$"
            ):
                audit.normalize_record_isolated(self.record_source(raw), 0, len(raw), timeout_seconds=30.0)

    def test_statement_helper_execs_psql_under_both_limits(self) -> None:
        # Non-default limit: module constant -> argv[1] -> the exec'd command.
        report = (
            "import json, resource; print(json.dumps({"
            "'as': resource.getrlimit(resource.RLIMIT_AS), "
            "'fsize': resource.getrlimit(resource.RLIMIT_FSIZE)}))"
        )
        ledger = _SpoolLedger([sys.executable, "-c", report])
        with mock.patch.object(statement_spool, "STATEMENT_HELPER_MEMORY_BYTES", NONDEFAULT_LIMIT):
            result = statement_spool.run_fenced_statement(ledger, ["SELECT 1;"])
        fsize = statement_spool.STATEMENT_RESULT_BYTES
        self.assertEqual(result, {"as": [NONDEFAULT_LIMIT] * 2, "fsize": [fsize] * 2})


class RefusedCapFailsClosedTests(_HelperCase):
    """A refused cap on Linux kills each real helper child before its work."""

    def test_candidate_helper_dies_before_fetching(self) -> None:
        probe = _REFUSE_CAP_IN_CHILD + (
            "from lab.prism import candidate_store as store\n"
            "def fetch(_transport, _block_hash):\n"
            "    sys.stderr.write('FETCHED\\n')\n"
            "    return None, None\n"
            "store._helper_fetch_legacy_text = fetch\n"
            "sys.exit(store.helper_main())\n"
        )
        with self.replace_child(store, "lab.prism.candidate_store", probe):
            with self.assertRaisesRegex(store.CandidateStorageError, "legacy candidate helper failed") as failure:
                self.candidate_helper().convert("aa" * 32, "unused.body", "unused.idx")
        self.assertNotIn("FETCHED", str(failure.exception))

    def test_audit_record_helper_dies_before_decoding(self) -> None:
        probe = _REFUSE_CAP_IN_CHILD + (
            "from lab.prism import audit_bundle_view as audit\n"
            "sys.exit(audit._helper_main(sys.argv[1:]))\n"
        )
        raw = b'{"a":1}'
        with self.replace_child(audit, "lab.prism.audit_bundle_view", probe):
            with self.assertRaisesRegex(
                audit.ArtifactResourcePressure, "(?s)status 1: .*ValueError: current limit exceeds maximum limit"
            ):
                audit.normalize_record_isolated(self.record_source(raw), 0, len(raw), timeout_seconds=30.0)

    def test_statement_helper_dies_before_exec(self) -> None:
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        marker = os.path.join(directory, "executed")
        ledger = _SpoolLedger(
            [sys.executable, "-c", f"open({marker!r}, 'w').close(); print('{{}}')"]
        )
        with self.prefix_exec_limited(_REFUSE_CAP_IN_CHILD):
            with self.assertRaisesRegex(RuntimeError, r"(?s)exit 1\).*ValueError: current limit exceeds maximum limit"):
                statement_spool.run_fenced_statement(ledger, ["SELECT 1;"])
        self.assertFalse(os.path.exists(marker))


class HelperLimitPolicyTests(unittest.TestCase):
    """Both platform branches, on any host, for every site."""

    def apply(self, platform: str, refusal: BaseException | None) -> tuple[Any, str, list[tuple[int, Any]]]:
        """Run the shared function; return (raised type or None, stderr, calls)."""
        setrlimit, calls = _setrlimit_refusing(resource.RLIMIT_AS, refusal)
        stderr = io.StringIO()
        with mock.patch.object(sys, "platform", platform), \
                mock.patch("resource.setrlimit", setrlimit), \
                mock.patch.object(sys, "stderr", stderr):
            try:
                apply_helper_memory_limit(NONDEFAULT_LIMIT)
            except BaseException as exc:
                return type(exc), stderr.getvalue(), calls
        return None, stderr.getvalue(), calls

    def exec_limited(
        self, platform: str, refused: int, refusal: BaseException | None
    ) -> tuple[Any, str, list[tuple[int, Any]], list[Any]]:
        """Run ``_EXEC_LIMITED`` in-process with ``execvp`` recorded, not run."""
        setrlimit, calls = _setrlimit_refusing(refused, refusal)
        stderr = io.StringIO()
        argv = ["-c", str(NONDEFAULT_LIMIT), "65536", "psql", "--version"]
        with mock.patch.object(sys, "platform", platform), \
                mock.patch("resource.setrlimit", setrlimit), \
                mock.patch.object(sys, "stderr", stderr), \
                mock.patch.object(sys, "argv", argv), \
                mock.patch("os.execvp") as execvp:
            try:
                exec(compile(statement_spool._EXEC_LIMITED, "<exec-limited>", "exec"), {"__name__": "__main__"})
            except BaseException as exc:
                return type(exc), stderr.getvalue(), calls, execvp.call_args_list
        return None, stderr.getvalue(), calls, execvp.call_args_list

    def test_refused_cap_propagates_off_darwin(self) -> None:
        for platform in ("linux", "freebsd14"):
            for refusal in (DARWIN_REFUSAL, PermissionError(1, "Operation not permitted")):
                with self.subTest(platform=platform, refusal=refusal):
                    raised, stderr, calls = self.apply(platform, refusal)
                    self.assertIs(raised, type(refusal))
                    self.assertEqual(stderr, "")
                    self.assertEqual(calls, [(resource.RLIMIT_AS, (NONDEFAULT_LIMIT, NONDEFAULT_LIMIT))])

    def test_refused_cap_on_darwin_continues_with_one_stderr_line(self) -> None:
        for refusal in (DARWIN_REFUSAL, OSError(22, "Invalid argument")):
            with self.subTest(refusal=refusal):
                raised, stderr, _calls = self.apply("darwin", refusal)
                self.assertIsNone(raised)
                self.assertEqual(len(stderr.splitlines()), 1)
                self.assertTrue(stderr.endswith("\n"))
                self.assertIn(f"RLIMIT_AS {NONDEFAULT_LIMIT} refused on darwin", stderr)

    def test_accepted_cap_is_silent_everywhere(self) -> None:
        for platform in ("linux", "darwin"):
            with self.subTest(platform=platform):
                self.assertEqual(self.apply(platform, None)[:2], (None, ""))

    def test_inline_statement_policy_matches_the_shared_function(self) -> None:
        refusals = (None, DARWIN_REFUSAL, PermissionError(1, "Operation not permitted"))
        for platform in ("linux", "darwin", "freebsd14"):
            for refusal in refusals:
                with self.subTest(platform=platform, refusal=refusal):
                    shared = self.apply(platform, refusal)
                    raised, stderr, calls, _execs = self.exec_limited(platform, resource.RLIMIT_AS, refusal)
                    self.assertEqual((raised, stderr), shared[:2])
                    self.assertEqual(calls[:1], shared[2])

    def test_statement_helper_refused_cap(self) -> None:
        # Linux: fatal, before RLIMIT_FSIZE and before psql.
        raised, stderr, calls, execs = self.exec_limited("linux", resource.RLIMIT_AS, DARWIN_REFUSAL)
        self.assertIs(raised, ValueError)
        self.assertEqual([which for which, _ in calls], [resource.RLIMIT_AS])
        self.assertEqual(execs, [])
        # Darwin: one line, RLIMIT_FSIZE still applied, then psql.
        raised, stderr, calls, execs = self.exec_limited("darwin", resource.RLIMIT_AS, DARWIN_REFUSAL)
        self.assertIsNone(raised)
        self.assertEqual(len(stderr.splitlines()), 1)
        self.assertEqual(calls[1], (resource.RLIMIT_FSIZE, (65536, 65536)))
        self.assertEqual(execs, [mock.call("psql", ["psql", "--version"])])

    def test_statement_helper_fsize_failure_is_fatal_on_darwin(self) -> None:
        for refused, refusal in (
            (resource.RLIMIT_FSIZE, DARWIN_REFUSAL),
            (resource.RLIMIT_FSIZE, PermissionError(1, "Operation not permitted")),
        ):
            with self.subTest(refusal=refusal):
                raised, stderr, _calls, execs = self.exec_limited("darwin", refused, refusal)
                self.assertIs(raised, type(refusal))
                self.assertEqual(stderr, "")
                self.assertEqual(execs, [])

    def candidate_helper_main(self, platform: str) -> tuple[int, dict[str, Any], str, list[Any]]:
        header = json.dumps({
            "mode": "compare", "transport": {}, "block_hash": "AA" * 32,
            "memory_limit_bytes": NONDEFAULT_LIMIT,
        }).encode("utf-8") + b"\n"
        setrlimit, _calls = _setrlimit_refusing(resource.RLIMIT_AS, DARWIN_REFUSAL)
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "platform", platform), \
                mock.patch("resource.setrlimit", setrlimit), \
                mock.patch.object(sys, "stderr", stderr), \
                mock.patch.object(store, "_helper_fetch_legacy_text", return_value=('{"a":1}', "sha")) as fetch:
            status = store.helper_main(io.BytesIO(header + b'{"a":1}'), stdout)
        return status, json.loads(stdout.getvalue().splitlines()[-1]), stderr.getvalue(), fetch.call_args_list

    def test_candidate_helper_refused_cap(self) -> None:
        status, verdict, stderr, fetches = self.candidate_helper_main("linux")
        self.assertEqual((status, fetches, stderr), (1, [], ""))
        self.assertEqual(verdict, {"error": "ValueError: current limit exceeds maximum limit"})
        status, verdict, stderr, fetches = self.candidate_helper_main("darwin")
        self.assertEqual((status, verdict), (0, {"equal": True}))
        self.assertEqual(len(fetches), 1)
        self.assertEqual(len(stderr.splitlines()), 1)

    def audit_helper_main(self, platform: str) -> tuple[Any, bytes, str]:
        setrlimit, _calls = _setrlimit_refusing(resource.RLIMIT_AS, DARWIN_REFUSAL)
        stdin = io.TextIOWrapper(io.BytesIO(b'{"b":2,"a":1}'))
        stdout = io.TextIOWrapper(io.BytesIO())
        stderr = io.StringIO()
        with mock.patch.object(sys, "platform", platform), \
                mock.patch("resource.setrlimit", setrlimit), \
                mock.patch.object(sys, "stdin", stdin), \
                mock.patch.object(sys, "stdout", stdout), \
                mock.patch.object(sys, "stderr", stderr):
            try:
                result: Any = audit._helper_main(["--normalize-record"])
            except BaseException as exc:
                result = type(exc)
        return result, stdout.buffer.getvalue(), stderr.getvalue()

    def test_audit_record_helper_refused_cap(self) -> None:
        self.assertEqual(self.audit_helper_main("linux"), (ValueError, b"", ""))
        status, output, stderr = self.audit_helper_main("darwin")
        self.assertEqual(status, 0)
        self.assertTrue(output.endswith(b'{"b":2,"a":1}{"a":1,"b":2}'))
        self.assertEqual(len(stderr.splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
