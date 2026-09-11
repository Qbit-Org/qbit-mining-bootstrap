#!/usr/bin/env python3
"""Fail when VERSION and any workspace crate version diverge.

The release identity is one number held in two places that must agree: the
`VERSION` file the bootstrap ships, and `[workspace.package].version` in the
root Cargo.toml, which every crate inherits with `version.workspace = true`.
Crate versions are resolved through `cargo metadata` rather than read from the
manifests, so a crate that opts back out of the workspace version with its own
literal is reported too.

Usage: python3 scripts/check_version_skew.py [--root DIR]

Exits 0 when every workspace crate matches VERSION. Exits 1 when any crate
differs, when VERSION is missing, empty, or not exactly one version line, or
when cargo cannot describe the workspace. Every mismatching crate is named
with both versions. Set CARGO to run a cargo other than the one on PATH.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-version-skew"
# Cargo's SemVer: MAJOR.MINOR.PATCH with optional -prerelease and +build parts.
VERSION_PATTERN = re.compile(
    r"^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$"
)


class SkewError(Exception):
    """A reason the check cannot pass, worded for whoever reads the CI log."""


def read_version_file(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise SkewError(f"{path} is missing") from None
    lines = text.splitlines()
    if len(lines) != 1 or not VERSION_PATTERN.match(lines[0]):
        raise SkewError(
            f"{path} must hold exactly one SemVer line such as 3.0.0, got {text!r}"
        )
    return lines[0]


def workspace_crate_versions(root: Path) -> dict[str, str]:
    cargo = os.environ.get("CARGO", "cargo")
    command = [
        cargo,
        "metadata",
        "--no-deps",
        "--format-version",
        "1",
        "--manifest-path",
        str(root / "Cargo.toml"),
    ]
    try:
        completed = subprocess.run(
            command, cwd=root, text=True, capture_output=True, check=False
        )
    except OSError as error:
        raise SkewError(f"cannot run {cargo}: {error}") from None
    if completed.returncode != 0:
        raise SkewError(
            f"{' '.join(command)} exited with status {completed.returncode}:\n"
            f"{completed.stderr.rstrip()}"
        )
    try:
        metadata = json.loads(completed.stdout)
        members = set(metadata["workspace_members"])
        versions = {
            package["name"]: package["version"]
            for package in metadata["packages"]
            if package["id"] in members
        }
    except (ValueError, KeyError, TypeError) as error:
        raise SkewError(f"cannot read cargo metadata output: {error}") from None
    if not versions:
        raise SkewError(f"{root / 'Cargo.toml'} declares no workspace members")
    return versions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="directory holding VERSION and the workspace Cargo.toml "
        "(default: this checkout)",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    try:
        expected = read_version_file(root / "VERSION")
        crates = workspace_crate_versions(root)
    except SkewError as error:
        print(f"{PREFIX}: {error}", file=sys.stderr)
        return 1

    skewed = {
        name: actual for name, actual in sorted(crates.items()) if actual != expected
    }
    for name, actual in skewed.items():
        print(f"{PREFIX}: {name} is {actual} but VERSION is {expected}", file=sys.stderr)
    if skewed:
        print(
            f"{PREFIX}: {len(skewed)} of {len(crates)} workspace crates differ from "
            f"VERSION {expected}; bump [workspace.package].version in Cargo.toml "
            "and VERSION together",
            file=sys.stderr,
        )
        return 1
    print(
        f"{PREFIX}: VERSION {expected} matches every workspace crate: "
        f"{', '.join(sorted(crates))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
