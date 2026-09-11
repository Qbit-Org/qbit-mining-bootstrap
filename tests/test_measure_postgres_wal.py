import json, os, subprocess, unittest

class WalHelperTests(unittest.TestCase):
    def test_invalid_dsn_fails_truthfully(self):
        p=subprocess.run(["python3","scripts/measure_postgres_wal.py","--dsn","postgresql://invalid.invalid/x","--sql","select 1"], text=True, capture_output=True)
        self.assertNotEqual(p.returncode, 0)
    def test_shape_declares_total_wal(self):
        self.assertIn("pg_wal_lsn_diff", open("scripts/measure_postgres_wal.py").read())

if __name__ == "__main__": unittest.main()
