#!/usr/bin/env python3
"""Fail when a Rust file reads an integration-gate variable directly.

Every test whose execution depends on `PRISM_TEST_DATABASE_URL`,
`PRISM_TEST_PG_BIN_DIR` or `QBITD_BIN` must take the value through the shared
gate crate (`crates/qbit-prism-test-gate`), which applies one decision table,
records the execution manifest and prints the skip line. The same goes for the
gate's own control variables, `PRISM_TEST_REQUIRE_INTEGRATION`,
`PRISM_TEST_GATE_MANIFEST` and `GITHUB_JOB`. A direct `std::env::var(...)` of
any of them elsewhere would let a new test pass vacuously again, unrecorded.

The rule is syntactic and deliberately broad: outside the gate crate, no Rust
source line may contain a string literal that is exactly one of those names,
whether it feeds `env::var`, `env::var_os`, `option_env!`, a `const`, or
anything else. Doc comments and longer literals such as
`"set PRISM_TEST_DATABASE_URL for ..."` are unaffected.

Usage: python3 scripts/check_gate_env_reads.py [--root DIR]

Exits 0 when no such line exists and 1 otherwise, naming every offending
file and line.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-gate-env-reads"
GATE_CRATE = Path("crates") / "qbit-prism-test-gate"
GATE_VARIABLES = (
    "PRISM_TEST_DATABASE_URL",
    "PRISM_TEST_PG_BIN_DIR",
    "QBITD_BIN",
    "PRISM_TEST_REQUIRE_INTEGRATION",
    "PRISM_TEST_GATE_MANIFEST",
    "GITHUB_JOB",
)
LITERAL = re.compile(r'"(' + "|".join(map(re.escape, GATE_VARIABLES)) + r')"')


def rust_sources(root: Path) -> list[Path]:
    crates = root / "crates"
    return sorted(
        path
        for path in crates.rglob("*.rs")
        if "target" not in path.relative_to(root).parts
    )


def direct_reads(root: Path) -> list[tuple[Path, int, str]]:
    """Every (file, line number, variable) outside the gate crate."""
    found = []
    for path in rust_sources(root):
        relative = path.relative_to(root)
        if relative.parts[: len(GATE_CRATE.parts)] == GATE_CRATE.parts:
            continue
        for number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            for match in LITERAL.finditer(line):
                found.append((relative, number, match.group(1)))
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="checkout to scan (default: this checkout)",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if not (root / "crates").is_dir():
        print(f"{PREFIX}: {root / 'crates'} is not a directory", file=sys.stderr)
        return 1
    found = direct_reads(root)
    for relative, number, variable in found:
        print(
            f"{PREFIX}: {relative.as_posix()}:{number}: {variable} is read directly; "
            f"take it through qbit_prism_test_gate instead",
            file=sys.stderr,
        )
    if found:
        print(
            f"{PREFIX}: {len(found)} direct read(s) of gate variables outside "
            f"{GATE_CRATE.as_posix()}; see docs/prism-integration-test-gate.md",
            file=sys.stderr,
        )
        return 1
    scanned = len(rust_sources(root))
    print(
        f"{PREFIX}: no direct reads of {', '.join(GATE_VARIABLES)} outside "
        f"{GATE_CRATE.as_posix()} in {scanned} Rust files"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
