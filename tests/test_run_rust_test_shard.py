from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import unittest
from unittest import mock

from scripts.run_rust_test_shard import IGNORED, main, shard_commands, workspace_targets


NATIVE_SCRIPT = Path(__file__).resolve().parents[1] / "test" / "prism-native-tests.sh"


def fixture_metadata():
    packages = [
        {"id": "server", "name": "qbit-prism-server", "targets": [
            {"kind": [kind], "name": name}
            for kind, name in [
                ("lib", "qbit_prism_server"), ("bin", "qbit-prism-server"),
                ("test", "stratum_admission_postgres"),
                ("test", "observability_database"), ("test", "issued_job_dependency"),
                ("test", "new_contract"),
                ("custom-build", "build-script-build"),
            ]
        ]},
        {"id": "core", "name": "core", "targets": [
            {"kind": [kind], "name": kind} for kind in ("lib", "bin", "test", "bench", "example")
        ]},
        {"id": "dependency", "name": "dependency", "targets": [
            {"kind": ["lib"], "name": "dependency"}
        ]},
    ]
    return {"packages": packages, "workspace_members": ["server", "core"]}


class RustTestShardTests(unittest.TestCase):
    def test_disjoint_shards_cover_all_targets_and_keep_ignored_runs_with_owner(self):
        metadata = fixture_metadata()
        expected = shard_commands(metadata, 0, 1)
        shards = [shard_commands(metadata, i, 4) for i in range(4)]
        self.assertCountEqual([cmd for shard in shards for cmd in shard], expected)
        ordinary = [cmd for cmd in expected if "--ignored" not in cmd]
        self.assertEqual(len(ordinary), 11)
        self.assertEqual(len({tuple(cmd) for cmd in ordinary}), 11)
        self.assertEqual(len(expected) - len(ordinary), len(IGNORED))
        for shard in shards:
            for command in shard:
                if "--ignored" in command:
                    prefix = command[:command.index("--")]
                    self.assertIn(prefix + ["--", "--nocapture"], shard)
        admission = next(cmd for cmd in expected if "--exact" in cmd)
        self.assertEqual(admission[-1], next(iter(IGNORED.values()))[-1])
        metadata["packages"].reverse()
        for package in metadata["packages"]:
            package["targets"].reverse()
        self.assertEqual(shards, [shard_commands(metadata, i, 4) for i in range(4)])

    def test_local_database_mode_selects_the_same_ignored_contracts(self):
        script = NATIVE_SCRIPT.read_text(encoding="utf-8").replace("\\\n", " ")
        database = script.split("\n  database)\n", 1)[1].split("\n    ;;", 1)[0]
        local = [shlex.split(line)[1:] for line in database.splitlines() if "--ignored" in line]
        ci = [cmd for cmd in shard_commands(fixture_metadata(), 0, 1) if "--ignored" in cmd]
        self.assertEqual(len(ci), len(IGNORED))
        self.assertCountEqual(local, ci)

    def test_bad_coordinates_empty_and_unknown_targets_fail_closed(self):
        for index, count in [(-1, 4), (4, 4), (0, 0), (0, -1), (11, 12)]:
            with self.subTest(index=index, count=count), self.assertRaises(ValueError):
                shard_commands(fixture_metadata(), index, count)
        with self.assertRaises(ValueError):
            shard_commands({"packages": [], "workspace_members": []}, 0, 1)
        metadata = fixture_metadata()
        metadata["packages"][0]["targets"][0]["kind"] = ["unknown"]
        with self.assertRaises(ValueError):
            workspace_targets(metadata)

    def test_dry_run_and_execution_use_identical_commands(self):
        metadata = fixture_metadata()
        for dry_run in [True, False]:
            with (
                mock.patch("scripts.run_rust_test_shard.subprocess.check_output", return_value=json.dumps(metadata)),
                mock.patch("scripts.run_rust_test_shard.subprocess.run") as run,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                args = ["--shard-index", "2", "--shard-count", "4"]
                self.assertEqual(main(args + (["--dry-run"] if dry_run else [])), 0)
                self.assertEqual(
                    [call.args[0] for call in run.call_args_list],
                    [] if dry_run else shard_commands(metadata, 2, 4),
                )
                for call in run.call_args_list:
                    self.assertTrue(call.kwargs["check"])

    def test_cargo_failure_stops_shard_and_preserves_nonzero_status(self):
        for phase in ["metadata", "tests"]:
            with (
                mock.patch("scripts.run_rust_test_shard.subprocess.check_output", return_value=json.dumps(fixture_metadata())) as metadata,
                mock.patch("scripts.run_rust_test_shard.subprocess.run") as run,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                (metadata if phase == "metadata" else run).side_effect = subprocess.CalledProcessError(101, ["cargo"])
                self.assertEqual(main(["--shard-index", "0", "--shard-count", "1"]), 101)
                self.assertEqual(run.call_count, 0 if phase == "metadata" else 1)


if __name__ == "__main__":
    unittest.main()
