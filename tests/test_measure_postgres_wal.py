import json, os, subprocess, unittest

class WalHelperTests(unittest.TestCase):
    def test_invalid_dsn_fails_truthfully(self):
        p=subprocess.run(["python3","scripts/measure_postgres_wal.py","--dsn","postgresql://invalid.invalid/x","--sql","select 1","--config","test"], text=True, capture_output=True)
        self.assertNotEqual(p.returncode, 0)
    def test_shape_declares_total_wal(self):
            with open("scripts/measure_postgres_wal.py") as f: self.assertIn("pg_wal_lsn_diff", f.read())
    def test_empty_sql_rejected(self):
        p = subprocess.run(["python3", "scripts/measure_postgres_wal.py", "--dsn", "postgresql://127.0.0.1/db", "--sql", " ", "--config", "test"], capture_output=True, text=True)
        self.assertEqual(p.returncode, 1)
        self.assertIn("invalid_input", p.stdout)

if __name__ == "__main__": unittest.main()
