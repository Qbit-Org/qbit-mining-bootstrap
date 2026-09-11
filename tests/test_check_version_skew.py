#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_version_skew.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
MAKEFILE = ROOT / "Makefile"


def write_workspace(
    root: Path,
    *,
    workspace_version: str,
    version_file: str | None,
    crates: dict[str, str | None],
) -> None:
    """Lay out a Cargo workspace; a crate inherits the workspace version unless given a literal."""
    members = "".join(f'    "crates/{name}",\n' for name in crates)
    (root / "Cargo.toml").write_text(
        "[workspace]\n"
        'resolver = "2"\n'
        f"members = [\n{members}]\n"
        "\n"
        "[workspace.package]\n"
        f'version = "{workspace_version}"\n',
        encoding="utf-8",
    )
    for name, literal in crates.items():
        crate = root / "crates" / name
        (crate / "src").mkdir(parents=True)
        (crate / "src" / "lib.rs").write_text("", encoding="utf-8")
        version_line = (
            "version.workspace = true" if literal is None else f'version = "{literal}"'
        )
        (crate / "Cargo.toml").write_text(
            f'[package]\nname = "{name}"\n{version_line}\nedition = "2021"\n',
            encoding="utf-8",
        )
    if version_file is not None:
        (root / "VERSION").write_text(version_file, encoding="utf-8")


def run_check(root: Path, **env_overrides: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


class CheckVersionSkewTests(unittest.TestCase):
    def test_matching_version_file_and_crates_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_workspace(
                root,
                workspace_version="3.0.0",
                version_file="3.0.0\n",
                crates={"alpha": None, "beta": None},
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "VERSION 3.0.0 matches every workspace crate: alpha, beta", result.stdout
        )
        self.assertEqual(result.stderr, "")

    def test_version_file_behind_the_workspace_names_every_crate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_workspace(
                root,
                workspace_version="3.0.0",
                version_file="2.0.2\n",
                crates={"alpha": None, "beta": None},
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("alpha is 3.0.0 but VERSION is 2.0.2", result.stderr)
        self.assertIn("beta is 3.0.0 but VERSION is 2.0.2", result.stderr)
        self.assertIn("2 of 2 workspace crates differ from VERSION 2.0.2", result.stderr)

    def test_crate_with_its_own_literal_version_is_named_alone(self) -> None:
        # A crate that drops `version.workspace = true` for a literal is the
        # skew this check exists for; the manifests are never grepped, so the
        # literal is caught through Cargo's own resolution.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_workspace(
                root,
                workspace_version="3.0.0",
                version_file="3.0.0\n",
                crates={"alpha": None, "beta": "2.0.0"},
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("beta is 2.0.0 but VERSION is 3.0.0", result.stderr)
        self.assertNotIn("alpha", result.stderr)
        self.assertIn("1 of 2 workspace crates differ from VERSION 3.0.0", result.stderr)

    def test_empty_missing_or_malformed_version_file_fails(self) -> None:
        for content in ("", "\n", "3.0.0\n3.0.0\n", "v3.0.0\n", " 3.0.0\n", "three\n"):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                write_workspace(
                    root,
                    workspace_version="3.0.0",
                    version_file=content,
                    crates={"alpha": None},
                )
                result = run_check(root)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn("VERSION must hold exactly one SemVer line", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_workspace(
                root, workspace_version="3.0.0", version_file=None, crates={"alpha": None}
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("VERSION is missing", result.stderr)

    def test_unusable_cargo_is_reported_rather_than_passing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_workspace(
                root,
                workspace_version="3.0.0",
                version_file="3.0.0\n",
                crates={"alpha": None},
            )
            result = run_check(root, CARGO=str(root / "no-such-cargo"))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("cannot run", result.stderr)
        self.assertIn("no-such-cargo", result.stderr)

    def test_checked_in_tree_has_one_release_identity(self) -> None:
        result = run_check(ROOT)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "matches every workspace crate: qbit-pool-builder, qbit-prism, "
            "qbit-prism-server, qbit-prism-test-gate",
            result.stdout,
        )

    def test_ci_runs_the_check_in_a_job_the_merge_gate_needs(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        rust_tests = workflow.split("name: Rust tests", 1)[1].split("\n  checks:", 1)[0]
        self.assertIn("python3 scripts/check_version_skew.py", rust_tests)
        checks = workflow.split("\n  checks:", 1)[1]
        needs = checks.split("needs: [", 1)[1].split("]", 1)[0]
        self.assertIn("rust-tests", needs.split(", "))

    def test_make_target_runs_the_check(self) -> None:
        makefile = MAKEFILE.read_text(encoding="utf-8")
        self.assertIn("check-version-skew:\n\tpython3 scripts/check_version_skew.py\n", makefile)
        phony = next(line for line in makefile.splitlines() if line.startswith(".PHONY:"))
        self.assertIn("check-version-skew", phony.split())


if __name__ == "__main__":
    unittest.main()
