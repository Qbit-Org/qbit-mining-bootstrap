#!/usr/bin/env python3
"""Prove that every gated test executed, from the gate's execution manifest.

When `PRISM_TEST_GATE_MANIFEST` names a file, the shared integration gate
(`crates/qbit-prism-test-gate`) appends one line per decision:

    executed <package>::<binary>::<test path>
    skipped  <package>::<binary>::<test path>
    failed   <package>::<binary>::<test path>

`test/prism-gated-tests.txt` is the checked-in list of every gated test the
`prism-native-postgres` job must execute; its length is the minimum count.
This check fails when:

- any expected test has no `executed` line,
- any `skipped` or `failed` line appears,
- fewer distinct tests executed than the list holds,
- a test executed that is not in the list (adding a gated test means adding
  it to the list, so the list stays the source of truth),
- the manifest holds a line the gate would not write,
- or, with `--log`, the test log contains the gate's skip prefix or the
  wording of the skip messages the gate replaced.

A test that executes more than once (a fixture opened in a loop, or one
binary run twice) counts once. The manifest is printed to standard output
and, when `--summary` names a file or `GITHUB_STEP_SUMMARY` is set, appended
to it as Markdown.

Usage: python3 scripts/check_gate_manifest.py --manifest FILE
           [--expected test/prism-gated-tests.txt] [--log FILE] [--summary FILE]
"""

from __future__ import annotations

import argparse
from collections import Counter
import os
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-gate-manifest"
DEFAULT_EXPECTED = ROOT / "test" / "prism-gated-tests.txt"
KINDS = ("executed", "skipped", "failed")
# Keep in step with SKIP_PREFIX in crates/qbit-prism-test-gate/src/lib.rs.
SKIP_PREFIX = "[prism-test-gate] skipped"
# Wording of the per-file skip messages the shared gate replaced. A test that
# grows one of these back has bypassed the gate.
LEGACY_SKIP_PATTERNS = (
    re.compile(r"skipping [^\n]*(PRISM_TEST_DATABASE_URL|PRISM_TEST_PG_BIN_DIR|QBITD_BIN)"),
    re.compile(r"PRISM_TEST_DATABASE_URL not set"),
    re.compile(r"set PRISM_TEST_DATABASE_URL for "),
    re.compile(r"SKIPPED: jsonb_ceiling_gate"),
)
TEST_ID = re.compile(r"^[A-Za-z0-9_-]+(::[A-Za-z0-9_]+)+$")


class ManifestError(Exception):
    """A reason the proof fails, worded for whoever reads the CI log."""


def read_expected(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ManifestError(f"expected list {path} is missing") from None
    ids = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if not TEST_ID.match(line):
            raise ManifestError(
                f"{path}:{number}: {line!r} is not a <package>::<binary>::<test> id"
            )
        ids.append(line)
    if not ids:
        raise ManifestError(f"expected list {path} names no tests")
    duplicates = sorted(name for name, count in Counter(ids).items() if count > 1)
    if duplicates:
        raise ManifestError(
            f"expected list {path} repeats {', '.join(duplicates)}"
        )
    if ids != sorted(ids):
        raise ManifestError(f"expected list {path} must be sorted")
    return ids


def read_manifest(path: Path) -> list[tuple[str, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ManifestError(
            f"manifest {path} is missing: no gated test ran, or "
            "PRISM_TEST_GATE_MANIFEST did not name it"
        ) from None
    lines = []
    for number, raw in enumerate(text.splitlines(), start=1):
        parts = raw.split(" ")
        if len(parts) != 2 or parts[0] not in KINDS or not TEST_ID.match(parts[1]):
            raise ManifestError(
                f"{path}:{number}: {raw!r} is not a line the gate writes "
                f"(expected '<{'|'.join(KINDS)}> <package>::<binary>::<test>')"
            )
        lines.append((parts[0], parts[1]))
    if not lines:
        raise ManifestError(f"manifest {path} is empty: no gated test ran")
    return lines


def log_problems(text: str) -> list[str]:
    """Every log line that shows a gated test skipping."""
    problems = []
    for line in text.splitlines():
        if SKIP_PREFIX in line or any(p.search(line) for p in LEGACY_SKIP_PATTERNS):
            problems.append(line.rstrip())
    return problems


def check(
    manifest: list[tuple[str, str]], expected: list[str]
) -> tuple[list[str], list[str]]:
    """Returns (problems, notes). Empty problems means the proof holds."""
    problems = []
    notes = []
    executed = Counter(test for kind, test in manifest if kind == "executed")
    for kind in ("skipped", "failed"):
        for test in sorted({t for k, t in manifest if k == kind}):
            problems.append(f"{test} was {kind}")
    for test in expected:
        if test not in executed:
            problems.append(f"{test} did not execute")
    unexpected = sorted(set(executed) - set(expected))
    for test in unexpected:
        problems.append(
            f"{test} executed but is not in the expected list; add it to "
            f"{DEFAULT_EXPECTED.relative_to(ROOT).as_posix()}"
        )
    if len(executed) < len(expected):
        problems.append(
            f"{len(executed)} distinct gated tests executed, below the minimum "
            f"of {len(expected)}"
        )
    repeated = sorted(test for test, count in executed.items() if count > 1)
    for test in repeated:
        notes.append(f"{test} executed {executed[test]} times; counted once")
    return problems, notes


def render(manifest: list[tuple[str, str]], expected: list[str], problems: list[str], notes: list[str]) -> str:
    executed = sorted({t for k, t in manifest if k == "executed"})
    lines = [
        "## Gated test execution manifest",
        "",
        f"{len(executed)} distinct gated tests executed; the expected list holds "
        f"{len(expected)}; {len(manifest)} manifest lines.",
        "",
    ]
    if problems:
        lines += ["**FAILED**", ""] + [f"- {p}" for p in problems] + [""]
    else:
        lines += ["Every expected gated test executed.", ""]
    if notes:
        lines += [f"- {n}" for n in notes] + [""]
    lines += ["```"] + [f"{kind} {test}" for kind, test in sorted(manifest)] + ["```", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--manifest", type=Path, required=True, help="the gate's manifest")
    parser.add_argument(
        "--expected",
        type=Path,
        default=DEFAULT_EXPECTED,
        help="checked-in list of gated tests (default: test/prism-gated-tests.txt)",
    )
    parser.add_argument("--log", type=Path, help="test log to scan for skip lines")
    parser.add_argument(
        "--summary",
        type=Path,
        help="Markdown file to append the manifest to (default: $GITHUB_STEP_SUMMARY)",
    )
    args = parser.parse_args(argv)

    try:
        expected = read_expected(args.expected)
        manifest = read_manifest(args.manifest)
        log_text = args.log.read_text(encoding="utf-8", errors="replace") if args.log else ""
    except (ManifestError, OSError) as error:
        print(f"{PREFIX}: {error}", file=sys.stderr)
        return 1

    problems, notes = check(manifest, expected)
    for line in log_problems(log_text):
        problems.append(f"log shows a skipped gated test: {line}")

    report = render(manifest, expected, problems, notes)
    print(report)
    summary = args.summary or (
        Path(os.environ["GITHUB_STEP_SUMMARY"]) if os.environ.get("GITHUB_STEP_SUMMARY") else None
    )
    if summary is not None:
        with summary.open("a", encoding="utf-8") as handle:
            handle.write(report)
    for problem in problems:
        print(f"{PREFIX}: {problem}", file=sys.stderr)
    if problems:
        print(f"{PREFIX}: {len(problems)} problem(s); the gated suite did not run in full", file=sys.stderr)
        return 1
    print(
        f"{PREFIX}: all {len(expected)} expected gated tests executed "
        f"({len(manifest)} manifest lines, {len(notes)} repeated)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
