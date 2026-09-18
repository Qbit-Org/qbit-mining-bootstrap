#!/usr/bin/env python3

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_deleted_test_map.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
ISSUES = "https://github.com/Qbit-Org/qbit-mining-bootstrap/issues"

FILE_MANIFEST = """# Deleted files, independent of the map under test.
tests/test_a.py
tests/test_b.py
tests/test_c.py
tests/test_d.py
tests/test_e.py
"""

CASE_MANIFEST = """# Deleted cases, independent of the map under test.
test_case_one
"""

MAP = f"""# Deleted 2.x.x tests and their 3.x.x replacements

Regenerate the file list with:

```
| this fenced line | is not | a table |
```

## Summary

| Rows | Full | Partial | Open gap | Needs triage | Retired |
| --- | --- | --- | --- | --- | --- |
| 5 | 1 | 1 | 1 | 1 | 1 |

Sections and row counts:

- Ledger (3)
- Stratum (2)

## Ledger

`server::run` and `#[tokio::test]` are code, not test references. [#5]({ISSUES}/5)
shipped the ledger; prose may link a closed issue as history.

| 2.x.x file | Covered | 3.x.x replacement or status |
| --- | --- | --- |
| `tests/test_a.py` | full | `crates/demo/tests/ledger.rs::lands_one_block` (asserts `a \\| b`), `crates/demo/tests/ledger.rs::lands_two_blocks` |
| `tests/test_b.py` | partial | partial: covered by `tests/test_kept.py::test_kept`, written when [#6]({ISSUES}/6) landed; not covered: the rest (→ [#7]({ISSUES}/7)) | <!-- retired-setting: PRISM_DEMO -->
| `tests/test_c.py` | retired | retired — Python only |

| 2.x.x case | Covered | 3.x.x replacement or status |
| --- | --- | --- |
| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |

## Stratum

| 2.x.x file | Covered | 3.x.x replacement or status |
| --- | --- | --- |
| `tests/test_d.py` | open gap | open gap → [#8]({ISSUES}/8) |
| `tests/test_e.py` | needs triage | needs triage — nobody owns it, see [#9]({ISSUES}/9) |

## Needs triage

| 2.x.x file | Area | Reason |
| --- | --- | --- |
| `tests/test_e.py` | Stratum | nobody owns it |
"""

RUST = """use support::fixture;

fn helper_that_is_not_a_test() {}

#[test]
fn lands_one_block() {}

// A comment between the attribute and the function is fine.
#[tokio::test(
    flavor = "multi_thread",
    worker_threads = 2
)]
// Another comment.
async fn lands_two_blocks() {}
"""

PYTHON = """def helper():
    pass


class KeptTests:
    def test_kept(self):
        pass
"""


def write_tree(
    root: Path, text: str = MAP, *, manifest: str | None = FILE_MANIFEST, case_manifest: str | None = CASE_MANIFEST
) -> None:
    (root / "docs").mkdir()
    (root / "docs" / "prism-deleted-test-map.md").write_text(text, encoding="utf-8")
    if manifest is not None:
        (root / "docs" / "prism-deleted-test-files.txt").write_text(manifest, encoding="utf-8")
    if case_manifest is not None:
        (root / "docs" / "prism-deleted-test-cases.txt").write_text(case_manifest, encoding="utf-8")
    (root / "crates" / "demo" / "tests").mkdir(parents=True)
    (root / "crates" / "demo" / "tests" / "ledger.rs").write_text(RUST, encoding="utf-8")
    (root / "tests").mkdir()
    (root / "tests" / "test_kept.py").write_text(PYTHON, encoding="utf-8")


def edited(old: str, new: str) -> str:
    # A fixture edit that matches nothing would turn a failing-case test into a passing one.
    assert MAP.count(old) == 1, old
    return MAP.replace(old, new)


def line_of(fragment: str, text: str = MAP) -> int:
    lines = [number for number, line in enumerate(text.splitlines(), start=1) if fragment in line]
    assert len(lines) == 1, (fragment, lines)
    return lines[0]


def run_check(root: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = {key: value for key, value in os.environ.items() if key not in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_API_URL")}
    merged.update(env or {})
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), *args],
        cwd=ROOT,
        env=merged,
        text=True,
        capture_output=True,
        check=False,
    )


class GitHubStub:
    """A local stand-in for the GitHub issues API with scripted replies per issue."""

    def __init__(self, replies: dict[int, list[tuple[int, object, float]]]) -> None:
        self.replies = replies
        self.requests: list[tuple[int, str | None]] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - the base class names it
                found = re.fullmatch(r"/repos/Qbit-Org/qbit-mining-bootstrap/issues/([0-9]+)", self.path)
                number = int(found.group(1)) if found else -1
                stub.requests.append((number, self.headers.get("Authorization")))
                scripted = stub.replies.get(number, [(404, {"message": "Not Found"}, 0.0)])
                status, body, delay = scripted.pop(0) if len(scripted) > 1 else scripted[0]
                time.sleep(delay)
                payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except OSError:
                    pass  # the client gave up first, which is what the deadline test wants

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> GitHubStub:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()

    def asked(self) -> list[int]:
        return [number for number, _ in self.requests]


def state(value: str) -> list[tuple[int, object, float]]:
    return [(200, {"state": value}, 0.0)]


ALL_OPEN = {6: state("open"), 7: state("open"), 8: state("open"), 9: state("open")}


class OfflineTests(unittest.TestCase):
    def check(
        self, text: str = MAP, *, manifest: str | None = FILE_MANIFEST, case_manifest: str | None = CASE_MANIFEST
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_tree(root, text, manifest=manifest, case_manifest=case_manifest)
            return run_check(root)

    def assert_fails(self, text: str, *expected: str) -> None:
        result = self.check(text)
        self.assertEqual(result.returncode, 1, result.stderr)
        for fragment in expected:
            self.assertIn(fragment, result.stderr)

    def test_the_repository_map_holds(self) -> None:
        result = subprocess.run([sys.executable, str(SCRIPT)], cwd=ROOT, text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("142 file rows", result.stdout)
        self.assertIn("17 case rows", result.stdout)

    def test_a_consistent_map_passes_with_escaped_pipes_markers_and_fences(self) -> None:
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ok (5 file rows, 1 case rows)", result.stdout)

    def test_a_renamed_file_fails_even_when_counts_are_unchanged(self) -> None:
        self.assert_fails(
            edited("`tests/test_c.py`", "`tests/test_c_typo.py`"),
            f":{line_of('`tests/test_c.py`')}: file `tests/test_c_typo.py` is not in the deleted-file manifest",
            "deleted file `tests/test_c.py` is missing from the file tables",
        )

    def test_a_missing_file_fails_even_when_counts_are_reconciled(self) -> None:
        text = edited("| `tests/test_c.py` | retired | retired — Python only |\n", "")
        text = text.replace("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 4 | 1 | 1 | 1 | 1 | 0 |")
        text = text.replace("- Ledger (3)", "- Ledger (2)")
        self.assert_fails(text, "deleted file `tests/test_c.py` is missing from the file tables")

    def test_an_extra_file_fails_even_when_counts_are_reconciled(self) -> None:
        text = edited(
            "| `tests/test_c.py` | retired | retired — Python only |",
            "| `tests/test_c.py` | retired | retired — Python only |\n| `tests/test_invented.py` | retired | retired — invented |",
        )
        text = text.replace("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 6 | 1 | 1 | 1 | 1 | 2 |")
        text = text.replace("- Ledger (3)", "- Ledger (4)")
        self.assert_fails(text, "file `tests/test_invented.py` is not in the deleted-file manifest")

    def test_a_case_row_cannot_replace_a_file_row(self) -> None:
        text = edited("| `tests/test_c.py` | retired | retired — Python only |\n", "")
        text = text.replace("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 4 | 1 | 1 | 1 | 1 | 0 |")
        text = text.replace("- Ledger (3)", "- Ledger (2)")
        text = text.replace("`test_case_one`", "`tests/test_c.py`")
        self.assert_fails(text, "deleted file `tests/test_c.py` is missing from the file tables")

    def test_an_unavailable_or_empty_manifest_fails_closed(self) -> None:
        for manifest, message in ((None, "cannot read"), ("# No entries\n\n", "manifest has no deleted files")):
            with self.subTest(manifest=manifest):
                result = self.check(manifest=manifest)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("docs/prism-deleted-test-files.txt", result.stderr)
                self.assertIn(message, result.stderr)

    def test_duplicate_manifest_entries_fail(self) -> None:
        result = self.check(manifest=FILE_MANIFEST + "tests/test_c.py\n")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("duplicate deleted file `tests/test_c.py`", result.stderr)

    def test_a_missing_case_fails_even_when_file_counts_are_unchanged(self) -> None:
        self.assert_fails(
            edited("| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |\n", ""),
            "deleted case `test_case_one` is missing from the case tables",
        )

    def test_a_renamed_case_fails_even_when_counts_are_unchanged(self) -> None:
        self.assert_fails(
            edited("`test_case_one`", "`test_case_typo`"),
            f":{line_of('`test_case_one`')}: case `test_case_typo` is not in the deleted-case manifest",
            "deleted case `test_case_one` is missing from the case tables",
        )

    def test_an_extra_case_fails(self) -> None:
        self.assert_fails(
            edited(
                "| `test_case_one` | full |",
                "| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |\n| `test_case_two` | full |",
            ),
            "case `test_case_two` is not in the deleted-case manifest",
        )

    def test_a_file_row_cannot_replace_a_case_row(self) -> None:
        text = edited("| 2.x.x case |", "| 2.x.x file |")
        text = text.replace("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 6 | 2 | 1 | 1 | 1 | 1 |")
        text = text.replace("- Ledger (3)", "- Ledger (4)")
        result = self.check(text, manifest=FILE_MANIFEST + "test_case_one\n")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("deleted case `test_case_one` is missing from the case tables", result.stderr)

    def test_an_unavailable_or_empty_case_manifest_fails_closed(self) -> None:
        for manifest, message in ((None, "cannot read"), ("# No entries\n\n", "manifest has no deleted cases")):
            with self.subTest(manifest=manifest):
                result = self.check(case_manifest=manifest)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("docs/prism-deleted-test-cases.txt", result.stderr)
                self.assertIn(message, result.stderr)

    def test_duplicate_case_manifest_entries_fail(self) -> None:
        result = self.check(case_manifest=CASE_MANIFEST + "test_case_one\n")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("duplicate deleted case `test_case_one`", result.stderr)

    def test_duplicate_case_rows_fail(self) -> None:
        self.assert_fails(
            edited(
                "| `test_case_one` | full |",
                "| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |\n| `test_case_one` | full |",
            ),
            f"case `test_case_one` already has a row at line {line_of('`test_case_one`')}",
        )

    def test_a_missing_map_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = run_check(Path(directory))
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot read", result.stderr)

    def test_reference_to_a_missing_file_fails(self) -> None:
        self.assert_fails(
            edited("`tests/test_kept.py::test_kept`", "`tests/test_gone.py::test_kept`"),
            f"docs/prism-deleted-test-map.md:{line_of('`tests/test_b.py`')}: `tests/test_gone.py::test_kept`: no such file",
        )

    def test_reference_to_a_missing_function_fails(self) -> None:
        self.assert_fails(
            edited("ledger.rs::lands_two_blocks`", "ledger.rs::lands_three_blocks`"),
            "no `fn lands_three_blocks` in that file",
        )
        self.assert_fails(edited("test_kept.py::test_kept`", "test_kept.py::test_renamed`"), "no `def test_renamed`")

    def test_reference_to_a_function_that_is_not_a_test_fails(self) -> None:
        self.assert_fails(
            edited("ledger.rs::lands_two_blocks`", "ledger.rs::helper_that_is_not_a_test`"),
            "`fn helper_that_is_not_a_test` exists but is not a test function",
        )
        self.assert_fails(
            edited("test_kept.py::test_kept`", "test_kept.py::helper`"),
            "`def helper` exists but is not a test function",
        )

    def test_a_reference_in_prose_outside_any_table_is_checked_too(self) -> None:
        self.assert_fails(
            edited("are code, not test references.", "are code; `crates/demo/tests/ledger.rs::gone` is not."),
            f":{line_of('are code, not test references.')}: `crates/demo/tests/ledger.rs::gone`: no `fn gone`",
        )

    def test_a_malformed_reference_is_an_error_not_a_skipped_one(self) -> None:
        self.assert_fails(
            edited("`crates/demo/tests/ledger.rs::lands_two_blocks`", "`crates/demo/tests/ledger::lands_two_blocks`"),
            "looks like a test reference but is not",
        )

    def test_a_reference_may_not_leave_the_repository(self) -> None:
        self.assert_fails(
            edited("`tests/test_kept.py::test_kept`", "`tests/../../outside/test_kept.py::test_kept`"),
            "path leaves the repository",
        )

    def test_summary_total_and_status_counts_must_equal_the_rows(self) -> None:
        self.assert_fails(edited("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 6 | 1 | 1 | 1 | 1 | 1 |"), "summary says 6 rows, the file tables hold 5")
        self.assert_fails(
            edited("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 5 | 2 | 1 | 1 | 1 | 0 |"),
            "summary says 2 full, the rows say 1",
            "summary says 0 retired, the rows say 1",
        )
        self.assert_fails(edited("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 5 | one | 1 | 1 | 1 | 1 |"), "must be 6 whole numbers")

    def test_a_status_change_without_a_summary_change_fails(self) -> None:
        self.assert_fails(
            edited("| `tests/test_c.py` | retired | retired — Python only |", "| `tests/test_c.py` | partial | partial: some |"),
            "summary says 1 partial, the rows say 2",
            "summary says 1 retired, the rows say 0",
        )

    def test_case_rows_are_not_counted_as_files(self) -> None:
        text = edited("| `test_case_one` | full |", "| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |\n| `test_case_two` | full |")
        result = self.check(text, case_manifest=CASE_MANIFEST + "test_case_two\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ok (5 file rows, 2 case rows)", result.stdout)

    def test_section_list_must_match_the_sections(self) -> None:
        self.assert_fails(edited("- Ledger (3)", "- Ledger (4)"), "section list says 4 rows in 'Ledger', the section holds 3")
        self.assert_fails(edited("- Stratum (2)\n", ""), "section 'Stratum' holds 2 rows and is missing from the section list")
        self.assert_fails(edited("- Stratum (2)", "- Stratum (2)\n- Vardiff (1)"), "section list names 'Vardiff'")
        self.assert_fails(edited("- Stratum (2)", "- Stratum: 2"), "section list item must read")

    def test_row_shape_is_enforced(self) -> None:
        self.assert_fails(edited("(asserts `a \\| b`)", "(asserts `a | b`)"), "row has 4 cells, expected 3")
        self.assert_fails(edited("| `tests/test_c.py` | retired |", "| tests/test_c.py | retired |"), "first cell must be one `code` name")
        self.assert_fails(edited("| retired — Python only |", "| retired — Python only"), f":{line_of('`tests/test_c.py`')}: table line does not end with an unescaped pipe")

    def test_one_unreadable_line_does_not_hide_the_rest_of_its_table(self) -> None:
        result = self.check(edited("| open gap → [#8]", "| open gap → [#8] \\").replace("| `tests/test_e.py` | needs triage |", "| `tests/test_e.py` | someday |"))
        self.assertEqual(result.returncode, 1)
        self.assertIn("unknown status 'someday'", result.stderr)

    def test_an_unknown_table_is_an_error(self) -> None:
        self.assert_fails(
            edited("| 2.x.x case | Covered | 3.x.x replacement or status |", "| 2.x.x method | Covered | 3.x.x replacement or status |"),
            "unrecognised table header",
        )

    def test_status_rules(self) -> None:
        self.assert_fails(edited("| `tests/test_c.py` | retired | retired —", "| `tests/test_c.py` | obsolete | retired —"), "unknown status 'obsolete'")
        self.assert_fails(
            edited("| `tests/test_c.py` | retired | retired — Python only |", "| `tests/test_c.py` | retired | partial: some |"),
            "status is 'retired' but the text does not lead with it",
        )
        self.assert_fails(
            edited("| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |", "| `test_case_one` | full | covered somewhere |"),
            "a full row must cite at least one `path::name` test",
        )
        self.assert_fails(
            edited(f"open gap → [#8]({ISSUES}/8)", "open gap, someone should look"),
            "an open gap row must link the issue that closes the gap",
        )

    def test_a_partial_file_row_must_cite_a_test(self) -> None:
        for citation in ("", "the native suite", "`server::run`", "`tests/test_kept::test_kept`"):
            with self.subTest(citation=citation):
                self.assert_fails(
                    edited("`tests/test_kept.py::test_kept`", citation),
                    f":{line_of('`tests/test_b.py`')}: a partial row must cite at least one `path::name` test",
                )

    def test_a_partial_case_row_must_cite_a_test(self) -> None:
        self.assert_fails(
            edited(
                "| `test_case_one` | full | `crates/demo/tests/ledger.rs::lands_one_block` |",
                "| `test_case_one` | partial | partial: covered by the native suite |",
            ),
            f":{line_of('`test_case_one`')}: a partial row must cite at least one `path::name` test",
        )

    def test_a_partial_case_row_with_a_test_citation_passes(self) -> None:
        result = self.check(edited("| `test_case_one` | full |", "| `test_case_one` | partial | partial: covered by"))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_duplicate_rows_fail(self) -> None:
        self.assert_fails(
            edited("| `tests/test_c.py` | retired | retired — Python only |", "| `tests/test_a.py` | retired | retired — Python only |"),
            f":{line_of('`tests/test_c.py`')}: file `tests/test_a.py` already has a row at line {line_of('`tests/test_a.py`')}",
        )

    def test_needs_triage_index_must_list_exactly_the_needs_triage_rows(self) -> None:
        self.assert_fails(
            edited("| `tests/test_e.py` | Stratum | nobody owns it |\n", ""),
            "`tests/test_e.py` is needs triage but missing from the needs triage index",
        )
        self.assert_fails(
            edited("| `tests/test_e.py` | Stratum | nobody owns it |", "| `tests/test_e.py` | Stratum | nobody |\n| `tests/test_c.py` | Ledger | stale |"),
            "`tests/test_c.py` is in the needs triage index but has no needs triage row",
        )
        self.assert_fails(edited("| `tests/test_e.py` | Stratum |", "| `tests/test_e.py` | Ledger |"), "area 'Ledger' is not the section the row sits in, 'Stratum'")

    def test_issue_links_must_point_at_the_issue_they_name(self) -> None:
        self.assert_fails(edited(f"[#7]({ISSUES}/7)", f"[#7]({ISSUES}/70)"), f"[#7] links '{ISSUES}/70'")
        self.assert_fails(edited(f"[#7]({ISSUES}/7)", "[#7](https://github.com/other/repo/issues/7)"), "[#7] links")

    def test_an_owner_arrow_must_be_a_link(self) -> None:
        for arrow in ("→", "->"):
            with self.subTest(arrow=arrow):
                self.assert_fails(edited(f"(→ [#7]({ISSUES}/7))", f"({arrow} #7)"), "an owner arrow must be followed by an issue link")

    def test_summary_lists_counts_and_open_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_tree(root)
            result = run_check(root, "--summary")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("5 rows: 1 full, 1 partial, 1 retired, 1 open gap, 1 needs triage.", result.stdout)
        self.assertIn("- `tests/test_d.py` (links #8)", result.stdout)
        self.assertIn("- `tests/test_e.py` (links #9)", result.stdout)
        self.assertIn("- `tests/test_b.py` (partial, owner #6, #7)", result.stdout)


class IssueStateTests(unittest.TestCase):
    def check(
        self, replies: dict[int, list[tuple[int, object, float]]], *args: str, text: str = MAP, env: dict[str, str] | None = None
    ) -> tuple[subprocess.CompletedProcess[str], GitHubStub]:
        with tempfile.TemporaryDirectory() as directory, GitHubStub(replies) as stub:
            root = Path(directory)
            write_tree(root, text)
            return run_check(root, "--check-issues", "--api-url", stub.url, *args, env=env), stub

    def test_open_owners_pass_and_only_owners_are_asked_about(self) -> None:
        result, stub = self.check(dict(ALL_OPEN))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("4 owner issue(s) open", result.stdout)
        # #5 is history in prose. #6 and #9 carry no arrow but sit in rows, where any link is an owner.
        self.assertEqual(sorted(stub.asked()), [6, 7, 8, 9])

    def test_a_closed_owner_fails_and_names_the_lines(self) -> None:
        for number, line in ((6, line_of("`tests/test_b.py`")), (8, line_of("`tests/test_d.py`")), (9, line_of("needs triage — nobody"))):
            with self.subTest(issue=number):
                result, _ = self.check({**ALL_OPEN, number: state("closed")})
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(f"#{number} is closed but the map names it as the owner", result.stderr)
                self.assertIn(f"lines {line};", result.stderr)

    def test_an_arrow_link_in_prose_is_an_owner_in_either_spelling(self) -> None:
        for arrow in ("→", "->"):
            with self.subTest(arrow=arrow):
                text = edited("prose may link a closed issue as history.", f"the rest is {arrow} [#11]({ISSUES}/11).")
                result, _ = self.check({**ALL_OPEN, 11: state("closed")}, text=text)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("#11 is closed", result.stderr)

    def test_a_link_in_the_needs_triage_index_is_an_owner(self) -> None:
        text = edited("| Stratum | nobody owns it |", f"| Stratum | nobody owns it, [#10]({ISSUES}/10) might |")
        result, stub = self.check({**ALL_OPEN, 10: state("closed")}, text=text)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("#10 is closed", result.stderr)
        self.assertIn(10, stub.asked())

    def test_a_server_error_is_retried_and_then_reported_as_unknown(self) -> None:
        result, stub = self.check({**ALL_OPEN, 8: [(500, {"message": "boom"}, 0.0)]})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn(f"#8 (map lines {line_of('`tests/test_d.py`')}): state unknown: HTTP 500", result.stderr)
        self.assertIn("nothing is assumed open", result.stderr)
        self.assertEqual(stub.asked().count(8), 3)

    def test_a_transient_server_error_recovers(self) -> None:
        result, stub = self.check({**ALL_OPEN, 8: [(502, {}, 0.0), (200, {"state": "open"}, 0.0)]})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(stub.asked().count(8), 2)

    def test_refusals_are_unknown_without_retry(self) -> None:
        for status, fragment in ((403, "HTTP 403"), (429, "HTTP 429"), (404, "HTTP 404: no such issue")):
            with self.subTest(status=status):
                result, stub = self.check({**ALL_OPEN, 7: [(status, {"message": "no"}, 0.0)]})
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(fragment, result.stderr)
                self.assertEqual(stub.asked().count(7), 1)

    def test_an_unexpected_reply_is_unknown_not_open(self) -> None:
        for body, fragment in (
            ({"state": "reopened"}, "state 'reopened'"),
            ({"title": "no state"}, "state None"),
            ([], "state None"),
            (b"<html>gateway</html>", "reply was not JSON"),
        ):
            with self.subTest(body=body):
                result, _ = self.check({**ALL_OPEN, 9: [(200, body, 0.0)]})
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(fragment, result.stderr)

    def test_a_closed_owner_outranks_an_unknown_one_and_both_are_reported(self) -> None:
        result, _ = self.check({**ALL_OPEN, 7: state("closed"), 8: [(403, {}, 0.0)]})
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("#7 is closed", result.stderr)
        self.assertIn(f"#8 (map lines {line_of('`tests/test_d.py`')}): state unknown", result.stderr)

    def test_the_total_deadline_bounds_a_slow_api(self) -> None:
        started = time.monotonic()
        result, _ = self.check({number: [(200, {"state": "open"}, 1.5)] for number in ALL_OPEN}, "--timeout-seconds", "0.4")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("ran out of time", result.stderr)
        self.assertLess(time.monotonic() - started, 5.0)

    def test_an_unreachable_api_is_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_tree(root)
            result = run_check(root, "--check-issues", "--api-url", "http://127.0.0.1:1", "--timeout-seconds", "5")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("state unknown: request failed", result.stderr)

    def test_offline_problems_still_fail_when_every_owner_is_open(self) -> None:
        result, _ = self.check(dict(ALL_OPEN), text=edited("| 5 | 1 | 1 | 1 | 1 | 1 |", "| 9 | 1 | 1 | 1 | 1 | 1 |"))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("summary says 9 rows", result.stderr)

    def test_the_token_is_sent_only_when_one_is_set(self) -> None:
        _, anonymous = self.check(dict(ALL_OPEN))
        self.assertEqual({header for _, header in anonymous.requests}, {None})
        for variable in ("GITHUB_TOKEN", "GH_TOKEN"):
            with self.subTest(variable=variable):
                _, stub = self.check(dict(ALL_OPEN), env={variable: "fixture-token"})
                self.assertEqual({header for _, header in stub.requests}, {"Bearer fixture-token"})

    def test_arguments_are_validated_at_the_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_tree(root)
            for args, fragment in (
                (("--timeout-seconds", "nan"), "finite number of seconds above zero"),
                (("--timeout-seconds", "inf"), "finite number of seconds above zero"),
                (("--timeout-seconds", "0"), "finite number of seconds above zero"),
                (("--timeout-seconds", "-3"), "finite number of seconds above zero"),
                (("--timeout-seconds", "soon"), "is not a number"),
                (("--api-url", "file:///etc/passwd"), "must be an http(s) URL"),
                (("--api-url", "https://"), "must be an http(s) URL"),
                (("--repository", "just-a-name"), "must be <owner>/<name>"),
            ):
                with self.subTest(args=args):
                    result = run_check(root, "--check-issues", *args)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(fragment, result.stderr)
            result = run_check(root, env={"GITHUB_API_URL": "ftp://example.invalid"})
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must be an http(s) URL", result.stderr)


class WorkflowTests(unittest.TestCase):
    def jobs(self) -> dict[str, str]:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        body = workflow.split("\njobs:\n", 1)[1]
        names = re.findall(r"^  ([A-Za-z0-9_-]+):$", body, flags=re.MULTILINE)
        parts = re.split(r"^  [A-Za-z0-9_-]+:$", body, flags=re.MULTILINE)[1:]
        return dict(zip(names, parts, strict=True))

    def test_the_offline_check_is_part_of_the_required_checks(self) -> None:
        jobs = self.jobs()
        self.assertIn("run: python3 scripts/check_deleted_test_map.py\n", jobs["lint-compile-compose"])
        needs = re.search(r"^    needs: \[(.*)\]$", jobs["checks"], flags=re.MULTILINE)
        self.assertIsNotNone(needs)
        self.assertIn("lint-compile-compose", [name.strip() for name in needs.group(1).split(",")])

    def test_the_issue_state_check_runs_can_fail_and_does_not_gate_merges(self) -> None:
        jobs = self.jobs()
        job = jobs["deleted-test-map-owner-issues"]
        self.assertIn("run: python3 scripts/check_deleted_test_map.py --check-issues\n", job)
        self.assertIn("GITHUB_TOKEN: ${{ github.token }}", job)
        self.assertIn("issues: read", job)
        self.assertIsNone(re.search(r"^\s*continue-on-error:", job, flags=re.MULTILINE))
        needs = re.search(r"^    needs: \[(.*)\]$", jobs["checks"], flags=re.MULTILINE)
        self.assertNotIn("deleted-test-map-owner-issues", needs.group(1))


if __name__ == "__main__":
    unittest.main()
