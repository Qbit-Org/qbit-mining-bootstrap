"""Split the recovery-evidence export in order, and fail it closed (#712).

The database-gated migration_rollback test compares the parallel export
with the serial one byte for byte; these check the cut and the order.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


DRIVER = Path(__file__).resolve().parents[1] / "scripts/prism-recovery-evidence-parallel.py"
spec = importlib.util.spec_from_file_location("recovery_evidence_parallel", DRIVER)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SQL = module.SCRIPT.read_text(encoding="utf-8")

# Stands in for psql: answers the coordinator, prints one JSON line per
# export statement it runs (taking every \if branch), and fails with exit 3
# on a line containing FAKE_PSQL_FAIL. It refuses to prompt for a password,
# and to print rows anywhere but a pipe the driver reads, as the driver
# must ask of psql.
FAKE_PSQL = r'''#!{python}
import json, os, random, re, stat, sys, time
if "-XqAtw" not in sys.argv or not stat.S_ISFIFO(os.fstat(1).st_mode):
    sys.exit(2)
fail = os.environ.get("FAKE_PSQL_FAIL")
source = open(sys.argv[sys.argv.index("--file") + 1]) if "--file" in sys.argv else sys.stdin
skipped = []
statement = None
for line in source:
    if fail and fail in line:
        sys.exit(3)
    stripped = line.strip()
    if stripped.startswith("\\if"):
        skipped.append(False)
    elif stripped.startswith("\\else"):
        skipped[-1] = True
    elif stripped.startswith("\\endif"):
        skipped.pop()
    elif any(skipped):
        pass
    elif "pg_export_snapshot()" in line:
        print("00000003-0000000F-1|t", flush=True)
    elif "percentile_disc(" in line:
        print("{{10,20,30}}" if "ORDER BY share_seq" in line else "{{4,8,c}}", flush=True)
    elif stripped == "SELECT NULL;":
        print("", flush=True)
    elif statement is not None or stripped.startswith("SELECT jsonb_build_object('kind', '"):
        statement = (statement or "") + line
        if stripped.endswith(";"):
            kind = re.match(r"SELECT jsonb_build_object\('kind', '(\w+)'", statement).group(1)
            where = re.findall(r" WHERE (.*?) ORDER BY", statement)
            print(json.dumps({{"kind": kind, "where": where}}), flush=True)
            statement = None
time.sleep(random.random() / 20)
'''


def collapse(kinds):
    """The kinds in order, a run of parts of one kind counted once."""
    return [kind for index, kind in enumerate(kinds) if index == 0 or kinds[index - 1] != kind]


class ScriptTests(unittest.TestCase):
    def test_the_cut_loses_nothing_and_keeps_one_transaction(self):
        script = module.Script(SQL)
        self.assertEqual(script.preamble + module.SHARES + script.middle + script.hashes + script.tail, SQL)
        self.assertEqual(script.preamble.count(module.BEGIN), 1)
        # Every flag a later part branches on is set before the first export.
        for flag in ("has_native_share_hashes", "has_audit_snapshots", "has_policy_transitions"):
            self.assertIn(f"AS {flag}", script.preamble)
        self.assertTrue(script.tail.endswith(module.COMMIT))
        self.assertNotIn(module.COMMIT, script.preamble + script.middle + script.hashes)

    def test_a_session_imports_the_snapshot_before_any_query(self):
        session = module.Script(SQL).session("00000003-0000000F-1").splitlines()
        begin = session.index(module.BEGIN.strip())
        self.assertEqual(session[begin + 1], "SET TRANSACTION SNAPSHOT '00000003-0000000F-1';")

    def test_parts_follow_the_serial_order_and_only_ranges_sort_with_more_memory(self):
        script = module.Script(SQL)
        parts = script.parts("00000003-0000000F-1", [10, 20], ["4", "c"], "64MB")
        self.assertEqual([name.split(" (")[0] for name, _ in parts], [
            "shares part 1 of 3", "shares part 2 of 3", "shares part 3 of 3",
            "the kinds between shares and share_hashes",
            "share_hashes part 1 of 3", "share_hashes part 2 of 3", "share_hashes part 3 of 3",
            "the kinds after share_hashes",
        ])
        session = script.session("00000003-0000000F-1")
        for name, sql in parts:
            with self.subTest(part=name):
                self.assertTrue(sql.startswith(session))
                self.assertTrue(sql.endswith(module.COMMIT))
                self.assertEqual(sql.count(module.COMMIT), 1)
                self.assertEqual("SET LOCAL work_mem = '64MB';" in sql, " part " in name)
        self.assertIn("WHERE share_seq >= 10 AND share_seq < 20 ORDER BY share_seq;", parts[1][1])
        hashes = parts[5][1]
        # The native and the 2.x mapping are both restricted to the range,
        # and 2.x shares outside it are never matched against the pattern.
        self.assertEqual(hashes.count(
            """WHERE header_hash COLLATE "C" >= '4' AND header_hash COLLATE "C" < 'c' ORDER BY"""), 2)
        self.assertIn(
            """WHERE accepted AND CASE WHEN lower(right(share_id,64)) COLLATE "C" >= '4' AND """
            """lower(right(share_id,64)) COLLATE "C" < 'c' THEN share_id ~ '[0-9a-fA-F]{64}$' END\n""",
            hashes)

    def test_ranges_cover_every_key_in_order(self):
        self.assertEqual(module.ranges([]), [(None, None)])
        self.assertEqual(module.ranges(["5", "9"]), [(None, "5"), ("5", "9"), ("9", None)])
        self.assertEqual(module.predicate("k", None, None), "true")
        self.assertEqual(module.predicate("k", None, "5"), "k < 5")
        self.assertEqual(module.predicate("k", "5", "9"), "k >= 5 AND k < 9")
        # NULL sorts last, so the last range holds it.
        self.assertEqual(module.predicate("k", "9", None), "(k >= 9 OR k IS NULL)")

    def test_sampled_bounds_are_only_well_formed_keys(self):
        self.assertEqual(module.bounds("", module.SHARE_BOUND), [])
        self.assertEqual(module.bounds("{}", module.SHARE_BOUND), [])
        self.assertEqual(module.bounds("{1,5,8}", module.SHARE_BOUND), ["1", "5", "8"])
        self.assertEqual(module.bounds('{NULL,ab,"a b",AB}', module.HASH_BOUND), ["ab"])
        with self.assertRaises(module.ExportError):
            module.bounds("ERROR", module.SHARE_BOUND)

    def test_an_edited_script_is_refused_rather_than_cut_elsewhere(self):
        hashes_order = module.HASHES_ORDER
        for edited in (
            SQL.replace(module.SHARES, ""),
            SQL.replace(module.SHARES, module.SHARES * 2),
            SQL.replace(module.SHARES, module.SHARES.replace("ORDER BY share_seq", "ORDER BY share_id")),
            SQL.replace(hashes_order, hashes_order.replace("COLLATE \"C\"", ""), 1),
            SQL.replace(module.HASHES, module.HASHES + "\\if :has_cluster\n\\endif\n"),
            SQL.replace(module.LEGACY_MATCH, "share_id ~ '[0-9a-f]{64}$'"),
            SQL.replace(f"{module.LEGACY_HEADER} AS header_hash", "lower(right(share_id, 64)) AS header_hash"),
            SQL.replace(module.BEGIN, ""),
            SQL[:-len(module.COMMIT)],
        ):
            with self.assertRaises(module.ExportError):
                module.Script(edited)


class ExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.fake = Path(cls.directory.name) / "psql"
        cls.fake.write_text(FAKE_PSQL.format(python=sys.executable), encoding="utf-8")
        cls.fake.chmod(0o755)
        serial = subprocess.run([str(cls.fake), "-XqAtw"], input=SQL, capture_output=True, text=True,
                                check=True)
        cls.serial = [json.loads(line)["kind"] for line in serial.stdout.splitlines()]

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def export(self, *args, fail="", stdout=subprocess.PIPE):
        with tempfile.TemporaryDirectory() as work:
            result = subprocess.run(
                [sys.executable, str(DRIVER), "--psql", str(self.fake), "--work-dir", work, *args],
                stdout=stdout, stderr=subprocess.PIPE, text=True, timeout=120,
                env=dict(os.environ, FAKE_PSQL_FAIL=fail))
            return result, os.listdir(work)

    def test_parts_are_printed_in_the_serial_order_whenever_they_finish(self):
        self.assertEqual(self.serial[0], "shares")
        self.assertEqual(self.serial[-2:], ["integrity", "complete"])
        share_ranges = [["share_seq < 10"], ["share_seq >= 10 AND share_seq < 20"],
                        ["share_seq >= 20 AND share_seq < 30"], ["(share_seq >= 30 OR share_seq IS NULL)"]]
        key = 'header_hash COLLATE "C"'
        for jobs, hash_ranges in (
            ("1", [["true"]]),
            ("3", [[f"{key} < '4'"], [f"{key} >= '4' AND {key} < '8'"],
                   [f"{key} >= '8' AND {key} < 'c'"], [f"({key} >= 'c' OR {key} IS NULL)"]]),
        ):
            with self.subTest(jobs=jobs):
                for _ in range(3):
                    result, leftovers = self.export("--jobs", jobs)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(leftovers, [])
                    rows = [json.loads(line) for line in result.stdout.splitlines()]
                    self.assertEqual(collapse([row["kind"] for row in rows]), self.serial)
                    self.assertEqual([row["where"] for row in rows if row["kind"] == "shares"], share_ranges)
                    self.assertEqual([row["where"] for row in rows if row["kind"] == "share_hashes"],
                                     hash_ranges)

    def test_a_failed_session_fails_the_export_without_a_completion_marker(self):
        for fail, reason in (
            ("pg_export_snapshot()", "the export transaction failed (psql exited 3)"),
            ("share_seq >= 20 AND", "shares part 3 of 4"),
            ("qbit_carry_forward_integrity_report()", "the kinds after share_hashes failed"),
        ):
            with self.subTest(fail=fail):
                result, leftovers = self.export("--jobs", "3", fail=fail)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(reason, result.stderr)
                self.assertEqual(leftovers, [])
                self.assertNotIn('"complete"', result.stdout)

    @unittest.skipUnless(os.path.exists("/dev/full"), "needs /dev/full")
    def test_an_output_that_cannot_be_written_fails_the_export(self):
        with open("/dev/full", "wb") as full:
            result, leftovers = self.export("--jobs", "2", stdout=full)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("No space left on device", result.stderr)
        self.assertEqual(leftovers, [])

    def test_an_unusable_work_mem_is_refused_before_connecting(self):
        result, _ = self.export("--work-mem", "1GB'; DROP TABLE x; --")
        self.assertEqual(result.returncode, 2)
        self.assertIn("is not a work_mem", result.stderr)


if __name__ == "__main__":
    unittest.main()
