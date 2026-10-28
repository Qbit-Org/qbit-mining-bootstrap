#!/usr/bin/env python3
"""Fail unless every build still uses vendor/sqlx-core, and its advisories are reviewed.

vendor/sqlx-core is sqlx-core with launchbadge/sqlx#4251 backported (#771),
and two workspaces patch it in: the repository's (`Cargo.toml`) and the
Stratum fuzz targets' (`crates/qbit-prism-server/fuzz/Cargo.toml`). sqlx pins
its sqlx-core with `=`, so moving sqlx to another version leaves the patch
unused: Cargo only warns, and the unfixed crates.io sqlx-core comes back. For
each workspace this requires that

- its `Cargo.toml` patches sqlx-core to vendor/sqlx-core;
- its lock file holds exactly one sqlx-core, with no `source` or `checksum`
  (a path dependency), at vendor/sqlx-core's version;
- the locked sqlx is that version too, as sqlx X requires sqlx-core =X, and
  its sqlx-core dependency names no other copy;
- Cargo did not record the sqlx-core patch under `[[patch.unused]]`.

cargo-deny matches RustSec advisories only against registry crates, so it no
longer checks sqlx-core. When a RustSec advisory-db checkout is present, this
lists every advisory the database holds for sqlx-core and fails unless each
one that is not withdrawn is in vendor/sqlx-core/reviewed-advisories.toml,
with a one-line reason. The checkout is the one `cargo deny check` fetched:
deny.toml's `[advisories].db-path` (cargo-deny's `$CARGO_HOME/advisory-dbs`
when unset), or `--advisory-db` / RUSTSEC_ADVISORY_DB. Without a checkout the
advisory part is skipped, unless `--require-advisory-db` makes that a failure.

Usage: python3 scripts/check_vendored_sqlx_core.py [--root DIR]
       [--advisory-db PATH] [--require-advisory-db]

Exits 0 when everything holds and 1 otherwise, naming each problem.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-vendored-sqlx-core"
CRATE = "sqlx-core"
VENDORED = Path("vendor") / CRATE
REVIEWED = VENDORED / "reviewed-advisories.toml"
# Each workspace that builds the server, as the directory holding its
# Cargo.toml and Cargo.lock.
WORKSPACES = (Path("."), Path("crates") / "qbit-prism-server" / "fuzz")
ADVISORY_DB_ENV = "RUSTSEC_ADVISORY_DB"
ADVISORY_ID = re.compile(r"^RUSTSEC-[0-9]{4}-[0-9]{4}$")
FRONT_MATTER = re.compile(r"\A```toml\n(?P<toml>.*?)\n```", re.DOTALL)


class CheckError(Exception):
    """One problem, worded for whoever reads the CI log."""


def load_toml(path: Path, label: str | None = None) -> dict:
    label = label or str(path)
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError:
        raise CheckError(f"{label} is missing") from None
    except tomllib.TOMLDecodeError as error:
        raise CheckError(f"{label} is not valid TOML: {error}") from None


def vendored_version(root: Path) -> str:
    manifest = VENDORED / "Cargo.toml"
    package = load_toml(root / manifest, manifest.as_posix()).get("package")
    if not isinstance(package, dict) or package.get("name") != CRATE:
        raise CheckError(f"{manifest.as_posix()} is not the {CRATE} package")
    version = package.get("version")
    if not isinstance(version, str) or not version:
        raise CheckError(f"{manifest.as_posix()} has no version")
    return version


def manifest_problems(root: Path, workspace: Path) -> list[str]:
    label = (workspace / "Cargo.toml").as_posix()
    patch = load_toml(root / workspace / "Cargo.toml", label).get("patch")
    registry = patch.get("crates-io") if isinstance(patch, dict) else None
    entry = registry.get(CRATE) if isinstance(registry, dict) else None
    path = entry.get("path") if isinstance(entry, dict) else None
    if not isinstance(path, str):
        return [f"{label} does not patch {CRATE} to {VENDORED.as_posix()} under [patch.crates-io]"]
    if (root / workspace / path).resolve() != (root / VENDORED).resolve():
        return [f"{label} patches {CRATE} to {path!r}, not {VENDORED.as_posix()}"]
    return []


def dependency_parts(entry: str) -> tuple[str, str | None, str | None]:
    """A lock file dependency, `name[ version][ (source)]`, as its parts."""
    match = re.fullmatch(r"(?P<name>\S+)(?: (?P<version>[^\s(]+))?(?: \((?P<source>.+)\))?", entry)
    if match is None:
        return entry, None, None
    return match["name"], match["version"], match["source"]


def lock_problems(root: Path, workspace: Path, version: str) -> list[str]:
    label = (workspace / "Cargo.lock").as_posix()
    data = load_toml(root / workspace / "Cargo.lock", label)
    packages = data.get("package", [])
    if not isinstance(packages, list):
        return [f"{label} has no [[package]] entries"]
    problems = []
    cores = [package for package in packages if package.get("name") == CRATE]
    if len(cores) != 1:
        found = ", ".join(
            f"{core.get('version')} from {core.get('source', 'a path')}" for core in cores
        )
        problems.append(f"{label} holds {len(cores)} {CRATE} entries ({found or 'none'}), not one")
    for core in cores:
        if "source" in core or "checksum" in core:
            problems.append(
                f"{label}: {CRATE} {core.get('version')} comes from "
                f"{core.get('source', 'a registry (it has a checksum)')}, not "
                f"{VENDORED.as_posix()}: the [patch.crates-io] entry no longer applies"
            )
        elif core.get("version") != version:
            problems.append(
                f"{label}: the path {CRATE} is {core.get('version')}, but "
                f"{VENDORED.as_posix()} is {version}"
            )
    sqlxs = [package for package in packages if package.get("name") == "sqlx"]
    if len(sqlxs) != 1:
        problems.append(f"{label} holds {len(sqlxs)} sqlx entries, not one")
    for sqlx in sqlxs:
        if sqlx.get("version") != version:
            problems.append(
                f"{label}: sqlx {sqlx.get('version')} requires {CRATE} ={sqlx.get('version')}, "
                f"but {VENDORED.as_posix()} is {version}: the patch cannot apply"
            )
        wanted = [
            dependency_parts(entry)
            for entry in sqlx.get("dependencies", [])
            if dependency_parts(entry)[0] == CRATE
        ]
        if len(wanted) != 1:
            problems.append(f"{label}: sqlx lists {len(wanted)} {CRATE} dependencies, not one")
        for _, wanted_version, wanted_source in wanted:
            if wanted_source is not None or wanted_version not in (None, version):
                problems.append(
                    f"{label}: sqlx depends on {CRATE} {wanted_version} from "
                    f"{wanted_source or 'a path'}, not {VENDORED.as_posix()}"
                )
    unused = data.get("patch", {}).get("unused", []) if isinstance(data.get("patch"), dict) else []
    for entry in unused:
        if isinstance(entry, dict) and entry.get("name") == CRATE:
            problems.append(
                f"{label}: Cargo recorded the {CRATE} {entry.get('version')} patch as unused"
            )
    return problems


def reviewed_advisories(root: Path) -> dict[str, str]:
    data = load_toml(root / REVIEWED, REVIEWED.as_posix())
    unknown = sorted(set(data) - {"reviewed"})
    if unknown:
        raise CheckError(f"{REVIEWED.as_posix()}: unknown keys {', '.join(unknown)}")
    reviewed = data.get("reviewed")
    if not isinstance(reviewed, dict):
        raise CheckError(f"{REVIEWED.as_posix()} has no [reviewed] table")
    problems = []
    for advisory, reason in reviewed.items():
        if not ADVISORY_ID.match(advisory):
            problems.append(f"{advisory!r} is not a RUSTSEC-YYYY-NNNN ID")
        if not isinstance(reason, str) or not reason.strip() or "\n" in reason:
            problems.append(f"{advisory} needs a one-line reason")
    if problems:
        raise CheckError(f"{REVIEWED.as_posix()}: {'; '.join(problems)}")
    return reviewed


def default_db_path(root: Path) -> Path:
    advisories = load_toml(root / "deny.toml", "deny.toml").get("advisories", {})
    cargo_home = os.environ.get("CARGO_HOME") or str(Path.home() / ".cargo")
    configured = advisories.get("db-path") if isinstance(advisories, dict) else None
    if not configured:
        return Path(cargo_home) / "advisory-dbs"
    expanded = configured.replace("$CARGO_HOME", cargo_home)
    return Path(os.path.expanduser(expanded))


def find_checkout(path: Path) -> Path | None:
    """The advisory-db checkout at `path`, or the one directly under it."""
    if (path / "crates").is_dir():
        return path
    if not path.is_dir():
        return None
    checkouts = sorted(child for child in path.iterdir() if (child / "crates").is_dir())
    if len(checkouts) > 1:
        raise CheckError(
            f"{path} holds {len(checkouts)} advisory databases "
            f"({', '.join(child.name for child in checkouts)}); name one with "
            f"--advisory-db or {ADVISORY_DB_ENV}"
        )
    return checkouts[0] if checkouts else None


def database_advisories(checkout: Path) -> dict[str, dict]:
    """Every advisory the checkout holds for sqlx-core, by ID."""
    advisories = {}
    directory = checkout / "crates" / CRATE
    for path in sorted(directory.glob("*.md")) if directory.is_dir() else []:
        text = path.read_text(encoding="utf-8")
        match = FRONT_MATTER.match(text)
        if match is None:
            raise CheckError(f"{path} has no TOML front matter")
        try:
            data = tomllib.loads(match["toml"])
        except tomllib.TOMLDecodeError as error:
            raise CheckError(f"{path} front matter is not valid TOML: {error}") from None
        advisory = data.get("advisory")
        advisory = advisory if isinstance(advisory, dict) else {}
        if advisory.get("id") != path.stem or advisory.get("package") != CRATE:
            raise CheckError(
                f"{path}: expected id {path.stem!r} for package {CRATE!r}, got "
                f"id {advisory.get('id')!r} for package {advisory.get('package')!r}"
            )
        advisories[path.stem] = {
            "withdrawn": advisory.get("withdrawn"),
            "patched": data.get("versions", {}).get("patched", []),
            "title": next(
                (line[2:].strip() for line in text.splitlines() if line.startswith("# ")), ""
            ),
        }
    return advisories


def advisory_problems(
    root: Path, db: Path | None, required: bool
) -> tuple[list[str], list[str]]:
    """Problems, and lines that report what was checked."""
    reviewed = reviewed_advisories(root)
    source = db if db is not None else default_db_path(root)
    checkout = find_checkout(source)
    if checkout is None:
        missing = f"no RustSec advisory database at {source}"
        if required:
            return [f"{missing}; run `cargo deny check` first"], []
        return [], [f"{missing}; skipped the advisory review ({len(reviewed)} reviewed)"]
    advisories = database_advisories(checkout)
    problems = []
    report = [f"{checkout} lists {len(advisories)} {CRATE} advisories"]
    for advisory, details in advisories.items():
        if details["withdrawn"]:
            state = f"withdrawn {details['withdrawn']}"
        elif advisory in reviewed:
            state = f"reviewed: {reviewed[advisory]}"
        else:
            state = "NOT REVIEWED"
            problems.append(
                f"{advisory} ({details['title']}; patched {details['patched']}) is not in "
                f"{REVIEWED.as_posix()}: review it against {VENDORED.as_posix()} and add "
                "its ID with a one-line reason"
            )
        report.append(f"  {advisory}: {state}")
    for advisory in sorted(set(reviewed) - set(advisories)):
        problems.append(
            f"{REVIEWED.as_posix()} names {advisory}, which the database does not hold "
            f"for {CRATE}"
        )
    return problems, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, default=ROOT, help="checkout to check (default: this checkout)")
    parser.add_argument(
        "--advisory-db",
        type=Path,
        default=os.environ.get(ADVISORY_DB_ENV) or None,
        help="an advisory-db checkout, or the directory holding one "
        f"(default: ${ADVISORY_DB_ENV}, else deny.toml's db-path)",
    )
    parser.add_argument(
        "--require-advisory-db",
        action="store_true",
        help="fail, rather than skip the advisory review, when no database is found",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    problems: list[str] = []
    report: list[str] = []
    try:
        version = vendored_version(root)
        for workspace in WORKSPACES:
            problems += manifest_problems(root, workspace)
            problems += lock_problems(root, workspace, version)
        found, report = advisory_problems(root, args.advisory_db, args.require_advisory_db)
        problems += found
    except CheckError as error:
        problems.append(str(error))
    for line in report:
        print(f"{PREFIX}: {line}")
    for problem in problems:
        print(f"{PREFIX}: {problem}", file=sys.stderr)
    if problems:
        print(f"{PREFIX}: {len(problems)} problem(s)", file=sys.stderr)
        return 1
    where = " and ".join((workspace / "Cargo.lock").as_posix() for workspace in WORKSPACES)
    print(f"{PREFIX}: {where} use {VENDORED.as_posix()} {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
