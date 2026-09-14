import subprocess, unittest
from unittest.mock import patch
from scripts.measure_postgres_wal import measure

class WalHelperTests(unittest.TestCase):
    def test_invalid_dsn_fails_truthfully(self):
        p=subprocess.run(["python3","scripts/measure_postgres_wal.py","--dsn","postgresql://invalid.invalid/x","--sql","select 1","--config","test"], text=True, capture_output=True)
        self.assertNotEqual(p.returncode, 0)
    def test_shape_declares_total_wal(self):
        with open("scripts/measure_postgres_wal.py") as f: self.assertIn("pg_wal_lsn_diff", f.read())
    @patch("scripts.measure_postgres_wal.psql", side_effect=["0/10", "ok", "0/30", "32"])
    def test_success_reports_positive_delta_and_scope(self, _):
        result = measure("postgresql://127.0.0.1/db", "select 1", "unit", "test")
        self.assertEqual(result["outcome"], "ok"); self.assertEqual(result["wal_bytes"], 32.0); self.assertEqual(result["measurement_scope"], "server-wide")
    @patch("scripts.measure_postgres_wal.psql", side_effect=["0/10", RuntimeError("PostgreSQL command failed")])
    def test_operation_failure_is_distinct(self, _):
        self.assertEqual(measure("postgresql://127.0.0.1/db", "bad", "unit", "test")["outcome"], "operation_failed")
    def test_empty_sql_rejected(self):
        p = subprocess.run(["python3", "scripts/measure_postgres_wal.py", "--dsn", "postgresql://127.0.0.1/db", "--sql", " ", "--config", "test"], capture_output=True, text=True)
        self.assertEqual(p.returncode, 1)
        self.assertIn("invalid_input", p.stdout)
    def test_password_and_query_dsn_rejected(self):
        for dsn in ("postgresql://u:p@127.0.0.1/db", "postgresql://127.0.0.1/db?hostaddr=evil"):
            p = subprocess.run(["python3", "scripts/measure_postgres_wal.py", "--dsn", dsn, "--sql", "select 1", "--config", "test"], capture_output=True, text=True)
            self.assertEqual(p.returncode, 1)
            self.assertIn("invalid_input", p.stdout)

if __name__ == "__main__": unittest.main()
