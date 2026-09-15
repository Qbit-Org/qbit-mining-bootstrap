#!/usr/bin/env python3
"""Run a deterministic shard of workspace Rust targets in required integration CI.

Keep each test binary intact so its fixtures and process-wide locks retain
Cargo's normal semantics. Discover targets from Cargo so new suites join CI.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import subprocess


ROOT = Path(__file__).resolve().parents[1]
# These ignored database contracts must be selected explicitly. Run each only
# on the shard that owns its ordinary test target; test/prism-native-tests.sh
# runs the same selections locally. Measurement runs stay opt-in.
IGNORED = {
    ("qbit-prism-server", "test", "stratum_admission_postgres"): [
        "--exact",
        "ten_thousand_unsubscribed_connections_do_not_advance_postgres_sequence",
    ],
    ("qbit-prism-server", "test", "observability_database"): [],
    ("qbit-prism-server", "test", "issued_job_dependency"): ["--test-threads=2"],
}


def workspace_targets(metadata: dict) -> list[tuple[str, str, str]]:
    targets = []
    for package in metadata["packages"]:
        if package["id"] not in metadata["workspace_members"]:
            continue
        for target in package["targets"]:
            kinds = target["kind"]
            if kinds == ["custom-build"]:
                continue
            if len(kinds) != 1 or kinds[0] not in ("lib", "bin", "test", "bench", "example"):
                raise ValueError(f"unsupported target kind: {kinds}")
            targets.append((package["name"], kinds[0], target["name"]))
    return sorted(targets)


def shard_commands(metadata: dict, index: int, count: int) -> list[list[str]]:
    if count < 1 or not 0 <= index < count:
        raise ValueError("require shard-count > 0 and 0 <= shard-index < shard-count")
    targets = workspace_targets(metadata)[index::count]
    if not targets:
        raise ValueError("shard contains no targets; reduce shard-count")
    commands = []
    for package, kind, name in targets:
        command = ["cargo", "test", "--locked", "-p", package, f"--{kind}"]
        if kind != "lib":
            command.append(name)
        commands.append(command + ["--", "--nocapture"])
        if (package, kind, name) in IGNORED:
            commands.append(command + ["--", "--ignored", "--nocapture"] + IGNORED[package, kind, name])
    return commands


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true", help="print commands without running tests")
    args = parser.parse_args(argv)
    try:
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
            cwd=ROOT, text=True,
        ))
        commands = shard_commands(metadata, args.shard_index, args.shard_count)
        for command in commands:
            print(shlex.join(command), flush=True)
            if not args.dry_run:
                subprocess.run(command, cwd=ROOT, check=True)
    except ValueError as error:
        parser.error(str(error))
    except subprocess.CalledProcessError as error:
        return error.returncode if error.returncode > 0 else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
