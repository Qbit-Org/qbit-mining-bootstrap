from __future__ import annotations

import contextlib
import io
from pathlib import Path
import tempfile
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


if __name__ == "__main__":
    unittest.main()
