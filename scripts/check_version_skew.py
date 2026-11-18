#!/usr/bin/env python3
"""Fail when VERSION and any workspace crate version diverge.

The release identity is one number held in places that must agree: the
`VERSION` file the bootstrap ships, `[workspace.package].version` in the
root Cargo.toml, which every crate inherits with `version.workspace = true`,
and the entries for those crates in both lockfiles: the root Cargo.lock and
the fuzz workspace's, which is built without `--locked`, so nothing else
notices it falling behind. Crate versions are resolved through `cargo
metadata` rather than read from the manifests, so a crate that opts back out
of the workspace version with its own literal is reported too. VERSION's
release notes, `doc/release-notes-<VERSION>.md`, must exist; while a release
is pending they exist undated, and the release block in docs/releasing.md
refuses to tag notes without a release date.

Usage: python3 scripts/check_version_skew.py [--root DIR]

Exits 0 when every workspace crate, in cargo's metadata and in each lockfile,
matches VERSION and the notes exist. Exits 1 when any crate differs, when
VERSION is missing, empty, or not exactly one version line, when cargo cannot
describe the workspace, when a lockfile is missing, unreadable or lists no
workspace crate, or when the notes are missing. Every mismatching crate is
named with both versions. Set CARGO to run a cargo other than the one on PATH.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-version-skew"
# Cargo's SemVer: MAJOR.MINOR.PATCH with optional -prerelease and +build parts.
VERSION_PATTERN = re.compile(
    r"^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$"
)
# docs/releasing.md: both lockfiles carry the pending release version.
LOCKFILES = ("Cargo.lock", "crates/qbit-prism-server/fuzz/Cargo.lock")
NOTES = "doc/release-notes-{version}.md"


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


def lockfile_versions(path: Path, crates: set[str]) -> list[tuple[str, str]]:
    """(name, version) of each workspace crate `path` lists: a path package, with no `source`."""
    try:
        lock = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SkewError(f"{path} is missing") from None
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise SkewError(f"cannot read {path}: {error}") from None
    packages = lock.get("package")
    listed = [
        (package.get("name"), package.get("version"))
        for package in (packages if isinstance(packages, list) else [])
        if isinstance(package, dict)
        and isinstance(package.get("name"), str)
        and package["name"] in crates
        and "source" not in package
    ]
    if not listed:
        raise SkewError(f"{path} lists no workspace crate")
    return listed


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
        locked = {name: lockfile_versions(root / name, set(crates)) for name in LOCKFILES}
    except SkewError as error:
        print(f"{PREFIX}: {error}", file=sys.stderr)
        return 1

    failed = False
    skewed = {
        name: actual for name, actual in sorted(crates.items()) if actual != expected
    }
    for name, actual in skewed.items():
        print(f"{PREFIX}: {name} is {actual} but VERSION is {expected}", file=sys.stderr)
    if skewed:
        failed = True
        print(
            f"{PREFIX}: {len(skewed)} of {len(crates)} workspace crates differ from "
            f"VERSION {expected}; bump [workspace.package].version in Cargo.toml "
            "and VERSION together",
            file=sys.stderr,
        )
    for lockfile, listed in locked.items():
        stale = sorted((name, actual) for name, actual in listed if actual != expected)
        for name, actual in stale:
            print(
                f"{PREFIX}: {lockfile} has {name} {actual} but VERSION is {expected}",
                file=sys.stderr,
            )
        if stale:
            failed = True
            print(
                f"{PREFIX}: bump {lockfile} with VERSION: both lockfiles carry the "
                "release version (docs/releasing.md)",
                file=sys.stderr,
            )
    notes = NOTES.format(version=expected)
    if not (root / notes).is_file():
        failed = True
        print(
            f"{PREFIX}: {notes} is missing: a version's notes come with it, undated "
            "while the release is pending (docs/releasing.md)",
            file=sys.stderr,
        )
    if failed:
        return 1
    print(
        f"{PREFIX}: VERSION {expected} matches every workspace crate: "
        f"{', '.join(sorted(crates))}; both lockfiles ({', '.join(LOCKFILES)}) agree, "
        f"and {notes} exists"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
