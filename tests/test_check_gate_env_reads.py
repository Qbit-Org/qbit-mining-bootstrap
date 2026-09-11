#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_gate_env_reads.py"
GATE_CRATE = ROOT / "crates" / "qbit-prism-test-gate"


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_check(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


class CheckGateEnvReadsTests(unittest.TestCase):
    def test_a_direct_read_outside_the_gate_crate_fails_and_is_named(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root,
                "crates/server/tests/db.rs",
                "fn f() {\n"
                '    let _ = std::env::var("PRISM_TEST_DATABASE_URL");\n'
                '    let _ = std::env::var_os("QBITD_BIN");\n'
                "}\n",
            )
            write(root, "crates/server/src/lib.rs", 'const J: &str = "GITHUB_JOB";\n')
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("crates/server/tests/db.rs:2: PRISM_TEST_DATABASE_URL", result.stderr)
        self.assertIn("crates/server/tests/db.rs:3: QBITD_BIN", result.stderr)
        self.assertIn("crates/server/src/lib.rs:1: GITHUB_JOB", result.stderr)
        self.assertIn("3 direct read(s)", result.stderr)

    def test_the_gate_crate_itself_may_name_the_variables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root,
                "crates/qbit-prism-test-gate/src/lib.rs",
                'pub const NAME: &str = "PRISM_TEST_DATABASE_URL";\n'
                'pub const SWITCH: &str = "PRISM_TEST_REQUIRE_INTEGRATION";\n',
            )
            write(root, "crates/server/src/lib.rs", "pub fn nothing() {}\n")
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no direct reads", result.stdout)

    def test_doc_comments_and_longer_literals_are_not_reads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root,
                "crates/server/tests/db.rs",
                "//! PRISM_TEST_DATABASE_URL=postgres://x cargo test\n"
                '#[ignore = "requires disposable PRISM_TEST_DATABASE_URL; run in database CI"]\n'
                "fn f() {\n"
                '    assert!(message.contains("PRISM_TEST_DATABASE_URL is set but not readable"));\n'
                '    let _ = std::env::var("PRISM_DATABASE_URL");\n'
                "}\n",
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_target_directories_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "crates/server/src/lib.rs", "pub fn nothing() {}\n")
            write(root, "target/debug/build/x.rs", 'let _ = env::var("QBITD_BIN");\n')
            write(root, "crates/target/x.rs", 'let _ = env::var("QBITD_BIN");\n')
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_checkout_without_crates_is_an_error_not_a_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_check(Path(tmp))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("is not a directory", result.stderr)

    def test_the_checked_in_tree_reads_gate_variables_only_through_the_gate(self) -> None:
        result = run_check(ROOT)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("outside crates/qbit-prism-test-gate", result.stdout)
        self.assertTrue((GATE_CRATE / "src" / "lib.rs").is_file())

    def test_ci_runs_the_check(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("python3 scripts/check_gate_env_reads.py", workflow)


if __name__ == "__main__":
    unittest.main()
