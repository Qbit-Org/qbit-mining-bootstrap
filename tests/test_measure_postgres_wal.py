import pathlib, subprocess, sys, unittest
from unittest.mock import patch
from scripts.measure_postgres_wal import measure
ROOT = pathlib.Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts" / "measure_postgres_wal.py"

class WalHelperTests(unittest.TestCase):
    @patch("scripts.measure_postgres_wal.psql", side_effect=RuntimeError("PostgreSQL command failed"))
    def test_initial_connection_failure_is_distinct(self, _):
        with self.assertRaisesRegex(RuntimeError, "initial connection failed"):
            measure("postgresql://127.0.0.1/db", "select 1", "unit", "test")
    def test_invalid_dsn_fails_truthfully(self):
        p=subprocess.run([sys.executable, str(HELPER),"--dsn","postgresql://invalid.invalid/x","--sql","select 1","--config","test"], text=True, capture_output=True, cwd=ROOT)
        self.assertNotEqual(p.returncode, 0)
    @patch("scripts.measure_postgres_wal.psql", side_effect=["0/10", "ok", "0/30", "32"])
    def test_success_reports_positive_delta_and_scope(self, _):
        result = measure("postgresql://127.0.0.1/db", "select 1", "unit", "test")
        self.assertEqual(result["outcome"], "ok"); self.assertEqual(result["wal_bytes"], 32.0); self.assertEqual(result["measurement_scope"], "server-wide")
    @patch("scripts.measure_postgres_wal.psql", side_effect=["0/10", RuntimeError("PostgreSQL command failed")])
    def test_operation_failure_is_distinct(self, _):
        result = measure("postgresql://127.0.0.1/db", "bad", "unit", "test")
        self.assertEqual(result["outcome"], "operation_failed"); self.assertEqual(result["side_effect_status"], "unknown")
    @patch("scripts.measure_postgres_wal.psql", side_effect=["0/10", "ok", RuntimeError("PostgreSQL command failed")])
    def test_end_lsn_failure_preserves_success(self, _):
        result = measure("postgresql://127.0.0.1/db", "select 1", "unit", "test")
        self.assertEqual(result["outcome"], "measurement_failed"); self.assertEqual(result["operation_status"], "succeeded")
    @patch("scripts.measure_postgres_wal.psql", side_effect=["0/10", "ok", "0/30", RuntimeError("PostgreSQL command failed")])
    def test_delta_failure_preserves_success(self, _):
        result = measure("postgresql://127.0.0.1/db", "select 1", "unit", "test")
        self.assertEqual(result["measurement_status"], "failed"); self.assertEqual(result["execution_stage"], "post-operation")
    def test_empty_sql_rejected(self):
        p = subprocess.run([sys.executable, str(HELPER), "--dsn", "postgresql://127.0.0.1/db", "--sql", " ", "--config", "test"], capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(p.returncode, 1)
        self.assertIn("invalid_input", p.stdout)
    def test_password_and_query_dsn_rejected(self):
        for dsn in ("postgresql://u:p@127.0.0.1/db", "postgresql://127.0.0.1/db?hostaddr=evil"):
            p = subprocess.run([sys.executable, str(HELPER), "--dsn", dsn, "--sql", "select 1", "--config", "test"], capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(p.returncode, 1)
            self.assertIn("invalid_input", p.stdout)
    @patch("scripts.measure_postgres_wal.psql", side_effect=subprocess.TimeoutExpired("psql", 1))
    def test_timeout_is_sanitized(self, _):
        with self.assertRaises(RuntimeError) as ctx: measure("postgresql://127.0.0.1/db", "select 1", "u", "t", 1)
        self.assertIn("timed out", str(ctx.exception))
    @patch("scripts.measure_postgres_wal.psql", side_effect=FileNotFoundError())
    def test_missing_executable_is_sanitized(self, _):
        with self.assertRaises(RuntimeError) as ctx: measure("postgresql://127.0.0.1/db", "select 1", "u", "t", 1)
        self.assertIn("unavailable", str(ctx.exception))

if __name__ == "__main__": unittest.main()
