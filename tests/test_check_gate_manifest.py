#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_gate_manifest.py"
EXPECTED_LIST = ROOT / "test" / "prism-gated-tests.txt"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
GATE_LIB = ROOT / "crates" / "qbit-prism-test-gate" / "src" / "lib.rs"

A = "qbit-prism-server::ledger_postgres::alpha"
B = "qbit-prism-server::ledger_postgres::two_x::beta"
C = "qbit-prism::window_daemon_gate::gamma"


def run_check(
    manifest: str | None,
    expected: str | None,
    *,
    log: str | None = None,
    extra: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        args = [sys.executable, str(SCRIPT)]
        manifest_path = root / "manifest.txt"
        if manifest is not None:
            manifest_path.write_text(manifest, encoding="utf-8")
        args += ["--manifest", str(manifest_path)]
        expected_path = root / "expected.txt"
        if expected is not None:
            expected_path.write_text(expected, encoding="utf-8")
        args += ["--expected", str(expected_path)]
        if log is not None:
            log_path = root / "tests.log"
            log_path.write_text(log, encoding="utf-8")
            args += ["--log", str(log_path)]
        args += extra or []
        merged = os.environ.copy()
        merged.pop("GITHUB_STEP_SUMMARY", None)
        merged.update(env or {})
        return subprocess.run(
            args, cwd=ROOT, env=merged, text=True, capture_output=True, check=False
        )


class CheckGateManifestTests(unittest.TestCase):
    def test_a_complete_manifest_passes_and_is_printed(self) -> None:
        result = run_check(f"executed {B}\nexecuted {A}\nexecuted {C}\n", f"{A}\n{B}\n{C}\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("all 3 expected gated tests executed", result.stdout)
        self.assertIn("## Gated test execution manifest", result.stdout)
        self.assertIn(f"executed {A}\nexecuted {B}\nexecuted {C}", result.stdout)
        self.assertEqual(result.stderr, "")

    def test_a_test_that_did_not_execute_fails(self) -> None:
        result = run_check(f"executed {A}\n", f"{A}\n{B}\n")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"{B} did not execute", result.stderr)
        self.assertIn("1 distinct gated tests executed, below the minimum of 2", result.stderr)

    def test_a_skipped_or_failed_line_fails_even_when_every_test_is_listed(self) -> None:
        for kind in ("skipped", "failed"):
            with self.subTest(kind=kind):
                result = run_check(f"executed {A}\n{kind} {B}\n", f"{A}\n{B}\n")
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(f"{B} was {kind}", result.stderr)

    def test_an_unlisted_executed_test_fails_so_the_list_must_grow(self) -> None:
        result = run_check(f"executed {A}\nexecuted {C}\n", f"{A}\n")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"{C} executed but is not in the expected list", result.stderr)
        self.assertIn("test/prism-gated-tests.txt", result.stderr)

    def test_repeated_executions_count_once_and_are_noted(self) -> None:
        result = run_check(f"executed {A}\nexecuted {A}\nexecuted {B}\n", f"{A}\n{B}\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"{A} executed 2 times; counted once", result.stdout)

    def test_a_malformed_manifest_line_fails(self) -> None:
        for line in ("ran x::y", f"executed  {A}", "executed", "executed not-an-id"):
            with self.subTest(line=line):
                result = run_check(f"{line}\n", f"{A}\n")
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn("is not a line the gate writes", result.stderr)

    def test_a_missing_or_empty_manifest_fails(self) -> None:
        result = run_check(None, f"{A}\n")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("manifest", result.stderr)
        self.assertIn("is missing", result.stderr)
        result = run_check("", f"{A}\n")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("is empty", result.stderr)

    def test_the_expected_list_must_be_sorted_unique_and_well_formed(self) -> None:
        cases = {
            f"{B}\n{A}\n": "must be sorted",
            f"{A}\n{A}\n": "repeats",
            "": "names no tests",
            "not an id\n": "is not a <package>::<binary>::<test> id",
        }
        for content, message in cases.items():
            with self.subTest(content=content):
                result = run_check(f"executed {A}\n", content)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)
        result = run_check(f"executed {A}\n", f"# comment\n\n{A}  # trailing\n")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_log_is_scanned_for_skip_lines(self) -> None:
        skip_lines = [
            f"[prism-test-gate] skipped {A}: PRISM_TEST_DATABASE_URL is unset or empty",
            "skipping PostgreSQL integration test; set PRISM_TEST_DATABASE_URL",
            "PRISM_TEST_DATABASE_URL not set; database contract test skipped",
            "set PRISM_TEST_DATABASE_URL for real rollup transaction tests",
            "SKIPPED: jsonb_ceiling_gate did not run.",
        ]
        for line in skip_lines:
            with self.subTest(line=line):
                result = run_check(f"executed {A}\n", f"{A}\n", log=f"test alpha ... ok\n{line}\n")
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(f"log shows a skipped gated test: {line}", result.stderr)
        benign = (
            "test alpha ... ok\n"
            "test beta ... ignored, requires disposable PRISM_TEST_DATABASE_URL; run explicitly\n"
            "test result: ok. 3 passed; 0 failed; 2 ignored\n"
        )
        result = run_check(f"executed {A}\n", f"{A}\n", log=benign)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_report_is_appended_to_the_step_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "summary.md"
            summary.write_text("# before\n", encoding="utf-8")
            result = run_check(f"executed {A}\n", f"{A}\n", env={"GITHUB_STEP_SUMMARY": str(summary)})
            self.assertEqual(result.returncode, 0, result.stderr)
            text = summary.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("# before\n## Gated test execution manifest"), text)
        self.assertIn(f"executed {A}", text)
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "explicit.md"
            result = run_check(f"failed {A}\n", f"{A}\n", extra=["--summary", str(summary)])
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("**FAILED**", summary.read_text(encoding="utf-8"))

    def test_the_skip_prefix_matches_the_gate_crate(self) -> None:
        lib = GATE_LIB.read_text(encoding="utf-8")
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('pub const SKIP_PREFIX: &str = "[prism-test-gate] skipped";', lib)
        self.assertIn('SKIP_PREFIX = "[prism-test-gate] skipped"', script)

    def test_the_checked_in_expected_list_is_valid_and_names_the_ignored_runs(self) -> None:
        result = run_check(
            "".join(f"executed {line}\n" for line in EXPECTED_LIST.read_text(encoding="utf-8").splitlines() if line and not line.startswith("#")),
            EXPECTED_LIST.read_text(encoding="utf-8"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        ids = [
            line.split("#", 1)[0].strip()
            for line in EXPECTED_LIST.read_text(encoding="utf-8").splitlines()
        ]
        ids = [i for i in ids if i]
        self.assertIn(
            "qbit-prism-server::stratum_admission_postgres::"
            "ten_thousand_unsubscribed_connections_do_not_advance_postgres_sequence",
            ids,
        )
        self.assertIn(
            "qbit-prism-server::observability_database::"
            "collector_uses_real_schema_pending_rows_and_failed_read_semantics",
            ids,
        )
        oracle = [i for i in ids if i.startswith("qbit-prism-server::window_read_oracle::")]
        self.assertEqual(len(oracle), 8, oracle)

    def test_ci_runs_the_checker_in_the_native_job_and_uploads_the_manifest(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        native = workflow.split("\n  prism-native-postgres:", 1)[1].split("\n  docker-builds:", 1)[0]
        self.assertIn('PRISM_TEST_REQUIRE_INTEGRATION: "1"', native)
        self.assertIn("PRISM_TEST_GATE_MANIFEST=", native)
        self.assertIn('>> "${GITHUB_ENV}"', native)
        self.assertIn("scripts/check_gate_manifest.py", native)
        self.assertIn("--expected test/prism-gated-tests.txt", native)
        self.assertIn("cargo test --locked --workspace --all-targets -- --nocapture", native)
        self.assertIn("actions/upload-artifact@", native)


if __name__ == "__main__":
    unittest.main()
