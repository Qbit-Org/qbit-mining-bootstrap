"""Native-ledger finalization transport: bytes, deadlines, and exact SQL."""

from __future__ import annotations

import os
import subprocess
import threading
import time
import tracemalloc
import unittest
from unittest import mock
import uuid

from lab.prism.share_ledger import LedgerOperationTimeout, PsqlShareLedger, WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
from lab.prism.statement_spool import run_fenced_statement


@unittest.skipUnless(os.environ.get("PRISM_TEST_DATABASE_URL"), "requires disposable PostgreSQL")
class StatementSpoolPostgresTests(unittest.TestCase):
    def setUp(self):
        self.ledger = PsqlShareLedger(
            database_url=os.environ["PRISM_TEST_DATABASE_URL"], psql_command="psql",
            native_client_mode="native", writer_id="candidate-statement-tests",
            writer_session_token=WRITER_LEASE_HEARTBEAT_SESSION_PREFIX + uuid.uuid4().hex,
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

    def test_spooled_connection_matches_pool_options_and_keeps_password_out_of_argv(self):
        from psycopg.conninfo import conninfo_to_dict

        sql = "SELECT json_build_object('schema', current_setting('search_path'), 'work_mem', current_setting('work_mem'), 'app', current_setting('application_name'));"
        expected = self.ledger._run_json(sql)
        with mock.patch.dict(os.environ, {"PGOPTIONS": "-c search_path=wrong_schema -c work_mem=1MB", "PGAPPNAME": "foreign"}), \
                mock.patch("lab.prism.statement_spool.subprocess.Popen", wraps=subprocess.Popen) as launch:
            self.assertEqual(run_fenced_statement(self.ledger, [sql]), expected)
        command = launch.call_args.args[0]
        dsn = command[command.index("--dbname") + 1]
        self.assertNotIn("password", conninfo_to_dict(dsn))

    def test_expired_lease_attributes_spooled_write_to_its_owner(self):
        self.ledger._run_json("UPDATE qbit_ledger_writer_lease SET lease_expires_at = clock_timestamp() - interval '1 second' RETURNING '{}'::json;")
        results, errors = [], []

        def write():
            try:
                results.append(run_fenced_statement(self.ledger, [
                    "BEGIN; UPDATE qbit_ledger_writer_lease SET lease_expires_at = clock_timestamp() + interval '60 seconds'; "
                    "SELECT json_build_object('ok', true) FROM pg_sleep(2); COMMIT;",
                ]))
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=write)
        thread.start()
        deadline = time.monotonic() + 5
        locked = False
        try:
            while thread.is_alive() and time.monotonic() < deadline:
                locked = self.ledger._run_json("""
SELECT json_build_object('locked', EXISTS (
    SELECT 1 FROM qbit_ledger_writer_lease lease, pg_stat_activity activity
    WHERE activity.backend_xid = lease.xmax AND activity.backend_xid IS NOT NULL
));
""")["locked"]
                if locked:
                    break
                time.sleep(0.01)
            self.assertTrue(locked)
            proof = self.ledger.verify_writer_lease_guard_session()
            self.assertTrue(proof["renewal_deferred_to_own_write"])
        finally:
            thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [{"ok": True}])
        self.assertFalse(self.ledger.verify_writer_lease_guard_session()["renewal_deferred_to_own_write"])


if __name__ == "__main__":
    unittest.main()
