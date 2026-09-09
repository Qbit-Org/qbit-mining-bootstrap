"""Native-ledger finalization transport: bytes, deadlines, and exact SQL."""

from __future__ import annotations

import os
import tracemalloc
import unittest
from unittest import mock

from lab.prism.share_ledger import LedgerOperationTimeout, PsqlShareLedger
from lab.prism.statement_spool import run_fenced_statement


@unittest.skipUnless(os.environ.get("PRISM_TEST_DATABASE_URL"), "requires disposable PostgreSQL")
class StatementSpoolPostgresTests(unittest.TestCase):
    def setUp(self):
        self.ledger = PsqlShareLedger(
            database_url=os.environ["PRISM_TEST_DATABASE_URL"], psql_command="psql",
            native_client_mode="native", writer_id="candidate-statement-tests",
        )
        self.addCleanup(self.ledger.close)
        self.addCleanup(self.ledger.release_writer_lease)

    def test_large_statement_never_enters_coordinator_native_buffer(self):
        def pieces():
            yield "SELECT json_build_object('length', octet_length($payload$"
            for _ in range(4096):
                yield "x" * 4096
            yield "$payload$::text));"

        tracemalloc.start()
        try:
            with mock.patch.object(self.ledger._native, "run_json", side_effect=AssertionError("whole query")):
                result = run_fenced_statement(self.ledger, pieces())
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(result, {"length": 16 * 1024 * 1024})
        self.assertLess(peak, 3 * 1024 * 1024)

    def test_server_deadline_releases_gate_and_allows_retry(self):
        with self.ledger.operation_timeout(0.15):
            with self.assertRaises(LedgerOperationTimeout):
                run_fenced_statement(self.ledger, ["SELECT pg_sleep(10);"])
        self.assertTrue(self.ledger._lock.acquire(blocking=False))
        self.ledger._lock.release()
        self.assertEqual(run_fenced_statement(self.ledger, ["SELECT '{\"ok\":true}'::json;"]), {"ok": True})


if __name__ == "__main__":
    unittest.main()
