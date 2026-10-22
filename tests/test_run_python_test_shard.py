from __future__ import annotations

import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

from scripts.run_python_test_shard import main, select_shard


def test_ids(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from test_ids(test)
        else:
            yield test.id()


class PythonTestShardTests(unittest.TestCase):
    def fixture_suite(self, directory):
        for index in range(7):
            Path(directory, f"test_shard_fixture_{index}.py").write_text(
                "import unittest\n"
                "active = False\n"
                "def setUpModule():\n"
                "    global active\n"
                "    active = True\n"
                "def tearDownModule():\n"
                "    global active\n"
                "    active = False\n"
                "class Fixture(unittest.TestCase):\n"
                "    @classmethod\n"
                "    def setUpClass(cls):\n"
                "        cls.ready = True\n"
                "    def test_a(self):\n"
                "        self.assertTrue(active and self.ready)\n"
                "    def test_b(self):\n"
                "        self.assertTrue(active and self.ready)\n"
            )
        return unittest.TestLoader().discover(directory)

    def test_every_discovered_test_runs_once_with_module_fixtures_intact(self):
        with tempfile.TemporaryDirectory() as directory:
            suite = self.fixture_suite(directory)
            expected = list(test_ids(suite))
            shards = [select_shard(suite, index, 4) for index in range(4)]
            rediscovered = unittest.TestLoader().discover(directory)
            self.assertEqual(
                [list(test_ids(shard)) for shard in shards],
                [list(test_ids(select_shard(rediscovered, i, 4))) for i in range(4)],
            )
            actual = [test_id for shard in shards for test_id in test_ids(shard)]
            self.assertEqual(len(expected), 14)
            self.assertCountEqual(actual, expected)
            module_shards = {}
            for index, shard in enumerate(shards):
                for test_id in test_ids(shard):
                    module = test_id.split(".")[0]
                    self.assertEqual(module_shards.setdefault(module, index), index)
                result = unittest.TextTestRunner(stream=io.StringIO()).run(shard)
                self.assertTrue(result.wasSuccessful())

    def test_one_shard_preserves_discovery_order(self):
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(type(self))
        self.assertEqual(list(test_ids(select_shard(suite, 0, 1))), list(test_ids(suite)))

    def test_invalid_coordinates_and_empty_shards_fail_closed(self):
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(type(self))
        for index, count in [(-1, 4), (4, 4), (0, 0), (0, -1)]:
            with self.subTest(index=index, count=count), self.assertRaises(ValueError):
                select_shard(suite, index, count)
        with self.assertRaises(ValueError):
            select_shard(unittest.TestSuite(), 0, 1)
        with self.assertRaises(ValueError):
            select_shard(suite, suite.countTestCases(), suite.countTestCases() + 1)

    def test_discovery_import_errors_fail_the_assigned_shard(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "test_broken_shard_fixture.py").write_text(
                "raise ImportError('broken fixture dependency')\n"
            )
            suite = unittest.TestLoader().discover(directory)
            selected = select_shard(suite, 0, 1)
            result = unittest.TextTestRunner(stream=io.StringIO()).run(selected)
            self.assertFalse(result.wasSuccessful())
            self.assertEqual(len(result.errors), 1)

    def test_runner_exit_status_tracks_test_result(self):
        for passes in [True, False]:
            with self.subTest(passes=passes):
                class Fixture(unittest.TestCase):
                    def runTest(self):
                        self.assertTrue(passes)

                suite = unittest.TestSuite([unittest.TestSuite([Fixture()])])
                with (
                    mock.patch.object(unittest.TestLoader, "discover", return_value=suite),
                    contextlib.redirect_stdout(io.StringIO()),
                    contextlib.redirect_stderr(io.StringIO()),
                ):
                    self.assertEqual(
                        main(["--shard-index", "0", "--shard-count", "1"]),
                        0 if passes else 1,
                    )

    def run_parity_shard(self, sources, citations, *, index=0, count=1):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tests").mkdir()
            for name, source in sources.items():
                (root / "tests" / name).write_text(textwrap.dedent(source), encoding="utf-8")
            if citations is not None:
                (root / "docs").mkdir()
                (root / "docs/prism-deleted-test-map.md").write_text(
                    "\n".join(f"`tests/{name}::test_kept`" for name in citations), encoding="utf-8"
                )
            return subprocess.run(
                [sys.executable, "-c",
                 "from pathlib import Path; import sys; "
                 "from scripts import run_python_test_shard as runner; "
                 "runner.ROOT = Path(sys.argv[1]); raise SystemExit(runner.main(sys.argv[2:]))",
                 str(root), "--shard-index", str(index), "--shard-count", str(count)],
                cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, check=False,
            )

    def test_cited_tests_must_pass_despite_unittest_treating_skips_as_success(self):
        base = "import unittest\nclass Kept(unittest.TestCase):\n    def test_kept(self):\n        pass\n"
        sources = {
            "module fixture": base + '\ndef setUpModule():\n    raise unittest.SkipTest("unavailable")\n',
            "class fixture": base + '\n    @classmethod\n    def setUpClass(cls):\n        raise unittest.SkipTest("unavailable")\n',
            "case fixture": base + '\n    def setUp(self):\n        self.skipTest("unavailable")\n',
            "async fixture": base.replace("unittest.TestCase", "unittest.IsolatedAsyncioTestCase")
            + '\n    async def asyncSetUp(self):\n        self.skipTest("unavailable")\n',
            "test body": base.replace("        pass", '        self.skipTest("unavailable")'),
            "subtest": base.replace("        pass", '        with self.subTest():\n            self.skipTest("unavailable")'),
            "expected failure": base.replace("    def test_kept", "    @unittest.expectedFailure\n    def test_kept")
            .replace("        pass", "        self.fail('not implemented')"),
        }
        for label, source in sources.items():
            with self.subTest(label=label):
                result = self.run_parity_shard({"test_parity_a.py": source}, ["test_parity_a.py"])
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("tests/test_parity_a.py::test_kept", result.stderr)
                self.assertIn("no successful execution", result.stderr)

    def test_cited_success_runs_fixtures_and_body_once_and_allows_unrelated_skips(self):
        result = self.run_parity_shard({"test_parity_a.py": '''
            import unittest
            def setUpModule():
                print("MODULE READY")
            def tearDownModule():
                print("MODULE CLEANED")
            class Kept(unittest.TestCase):
                @classmethod
                def setUpClass(cls):
                    print("CLASS READY")
                @classmethod
                def tearDownClass(cls):
                    print("CLASS CLEANED")
                def setUp(self):
                    print("CASE READY")
                    self.addCleanup(print, "CASE CLEANED")
                def test_kept(self):
                    print("BODY RAN")
                @unittest.skip("optional uncited test")
                def test_optional(self):
                    self.fail("should not run")
        '''}, ["test_parity_a.py"])
        self.assertEqual(result.returncode, 0, result.stderr)
        for marker in ("MODULE READY", "MODULE CLEANED", "CLASS READY", "CLASS CLEANED",
                       "CASE READY", "CASE CLEANED", "BODY RAN"):
            self.assertEqual(result.stdout.count(marker), 1, result.stdout)

    def test_live_inherited_implementation_satisfies_citation_despite_skipped_subclass(self):
        result = self.run_parity_shard({"test_parity_a.py": '''
            import unittest
            class Kept(unittest.TestCase):
                def test_kept(self):
                    pass
            class Skipped(Kept):
                @classmethod
                def setUpClass(cls):
                    raise unittest.SkipTest("optional subclass")
        '''}, ["test_parity_a.py"])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_parity_check_requires_only_citations_assigned_to_this_shard(self):
        source = "import unittest\nclass Kept(unittest.TestCase):\n    def test_kept(self):\n        pass\n"
        sources = {
            "test_parity_a.py": source + '\n    def setUp(self):\n        self.skipTest("unavailable")\n',
            "test_parity_b.py": source,
        }
        for index, expected in ((0, 1), (1, 0)):
            with self.subTest(index=index):
                result = self.run_parity_shard(sources, list(sources), index=index, count=2)
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertNotIn("tests/test_parity_b.py::test_kept", result.stderr)
        result = self.run_parity_shard(sources, list(sources))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("tests/test_parity_a.py::test_kept", result.stderr)
        self.assertNotIn("tests/test_parity_b.py::test_kept", result.stderr)

    def test_missing_parity_map_fails_closed(self):
        result = self.run_parity_shard({"test_parity_a.py": '''
            import unittest
            class Kept(unittest.TestCase):
                def test_kept(self):
                    pass
        '''}, None)
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("prism-deleted-test-map.md", result.stderr)


if __name__ == "__main__":
    unittest.main()
