#!/usr/bin/env python3
"""Fail unless every build still uses vendor/sqlx-core, its files are the recorded ones, and its advisories are reviewed.

vendor/sqlx-core is sqlx-core with launchbadge/sqlx#4251 (#771) and #3861
(#713) backported, plus a local verify-ca change (it trusts only
sslrootcert), and two workspaces patch it in: the repository's
(`Cargo.toml`) and the Stratum fuzz targets'
(`crates/qbit-prism-server/fuzz/Cargo.toml`). sqlx pins its sqlx-core with
`=`, so moving sqlx to another version leaves the patch unused: Cargo only
warns, and the unfixed crates.io sqlx-core comes back. For each workspace
this requires that

- its `Cargo.toml` patches sqlx-core to vendor/sqlx-core;
- its lock file holds exactly one sqlx-core, with no `source` or `checksum`
  (a path dependency), at vendor/sqlx-core's version;
- the locked sqlx is that version too, as sqlx X requires sqlx-core =X, and
  its sqlx-core dependency names no other copy;
- Cargo did not record the sqlx-core patch under `[[patch.unused]]`.

cargo-deny doesn't check a path crate's files either, so an unreviewed edit
to the vendored copy, for example one that trusts the public roots under
verify-ca again, would build. vendor/sqlx-core.sha256 lists every file in
vendor/sqlx-core but reviewed-advisories.toml, with its SHA-256 and whether
it is upstream's, patched from upstream's or added. This fails on a file
whose content differs, one the manifest doesn't list and one it lists that
is missing, on anything there that is not a regular file, and unless the
README names every patched or added file and records the upstream .crate
SHA-256 the manifest was generated from. It reads nothing but the checkout.

`--write-manifest --upstream-crate CRATE` regenerates the manifest from the
tree and upstream's .crate, whose SHA-256 must be the one the README records.
It refuses a change to any file while the README is unchanged, and a patched
or added file the README doesn't name. `--upstream-crate` alone checks the
committed manifest's record of what differs from upstream against the .crate.
The README's "Changing this copy" gives the procedure.

cargo-deny matches RustSec advisories only against registry crates, so it no
longer checks sqlx-core. When a RustSec advisory-db checkout is present, this
lists every advisory the database holds for sqlx-core and fails unless each
one that is not withdrawn is in vendor/sqlx-core/reviewed-advisories.toml,
with a one-line reason. The checkout is the one `cargo deny check` fetched:
deny.toml's `[advisories].db-path` (cargo-deny's `$CARGO_HOME/advisory-dbs`
when unset), or `--advisory-db` / RUSTSEC_ADVISORY_DB. Without a checkout the
advisory part is skipped, unless `--require-advisory-db` makes that a failure.

Usage: python3 scripts/check_vendored_sqlx_core.py [--root DIR]
       [--advisory-db PATH] [--require-advisory-db] [--upstream-crate CRATE]
       python3 scripts/check_vendored_sqlx_core.py [--root DIR]
       --write-manifest --upstream-crate CRATE

Exits 0 when everything holds and 1 otherwise, naming each problem.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import io
import os
from pathlib import Path
import re
import sys
import tarfile
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
MANIFEST = Path("vendor") / f"{CRATE}.sha256"
README = VENDORED / "README.md"
# The advisory review reads it; adding a reviewed advisory changes no code.
UNHASHED = frozenset({"reviewed-advisories.toml"})
SHA256 = re.compile(r"[0-9a-f]{64}")
RECORDED_CRATE = re.compile(r"SHA-256 is\s+`(?P<sha256>[0-9a-f]{64})`")
REGENERATE = (
    f"record an intended change in {README.as_posix()}'s change list, then regenerate "
    f"{MANIFEST.as_posix()} as that README's \"Changing this copy\" says"
)


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


def workspace_problems(root: Path, version: str) -> list[str]:
    problems = []
    for workspace in WORKSPACES:
        problems += manifest_problems(root, workspace)
        problems += lock_problems(root, workspace, version)
    return problems


@dataclass
class Manifest:
    version: str
    crate_sha256: str
    # Each file by its path in vendor/sqlx-core: its SHA-256, and `upstream`
    # (upstream's file), `patched:<upstream's SHA-256>` or `added`.
    files: dict[str, tuple[str, str]]


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_readme(root: Path) -> tuple[str, str]:
    """README.md's text, and the upstream .crate SHA-256 it records."""
    try:
        text = (root / README).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise CheckError(f"{README.as_posix()} is missing") from None
    recorded = RECORDED_CRATE.search(text)
    if recorded is None:
        raise CheckError(
            f"{README.as_posix()} does not record upstream's .crate as `SHA-256 is `<hex>``"
        )
    return text, recorded["sha256"]


def vendored_files(root: Path) -> dict[str, str]:
    """Every file in vendor/sqlx-core but UNHASHED, by path, with its SHA-256."""
    base = root / VENDORED
    if not base.is_dir():
        raise CheckError(f"{VENDORED.as_posix()} is missing")
    files, problems = {}, []
    for directory, dirnames, filenames in os.walk(base):
        for name in sorted(dirnames + filenames):
            path = Path(directory) / name
            relative = path.relative_to(base).as_posix()
            if path.is_symlink():
                problems.append(f"{VENDORED.as_posix()}/{relative} is a symbolic link")
            elif name in filenames and not path.is_file():
                problems.append(f"{VENDORED.as_posix()}/{relative} is not a regular file")
            elif name in filenames and relative not in UNHASHED:
                files[relative] = sha256_of(path.read_bytes())
    if problems:
        raise CheckError(f"{'; '.join(problems)}: the vendored copy holds regular files only")
    return files


def origins(files: dict[str, str], upstream: dict[str, str]) -> dict[str, tuple[str, str]]:
    """Each file with what it is relative to upstream's crate."""
    entries = {}
    for path, digest in files.items():
        if path not in upstream:
            origin = "added"
        elif upstream[path] == digest:
            origin = "upstream"
        else:
            origin = f"patched:{upstream[path]}"
        entries[path] = (digest, origin)
    return entries


def read_manifest(root: Path) -> Manifest:
    label = MANIFEST.as_posix()
    try:
        text = (root / MANIFEST).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise CheckError(f"{label} is missing") from None
    upstream, files = None, {}
    for number, line in enumerate(text.splitlines(), 1):
        if not line or line.startswith("#"):
            continue
        fields = line.split(" ")
        if fields[0] == "upstream":
            if (
                upstream is not None
                or len(fields) != 4
                or fields[1] != CRATE
                or not SHA256.fullmatch(fields[3])
            ):
                raise CheckError(
                    f"{label}:{number}: expected one `upstream {CRATE} <version> <sha256>` line"
                )
            upstream = (fields[2], fields[3])
            continue
        digest, origin, path = (line.split(" ", 2) + ["", ""])[:3]
        valid_origin = origin in ("upstream", "added") or (
            origin.startswith("patched:") and SHA256.fullmatch(origin[len("patched:") :])
        )
        if not SHA256.fullmatch(digest) or not valid_origin or not path or path in files:
            raise CheckError(
                f"{label}:{number}: expected `<sha256> upstream|added|patched:<sha256> <path>`, "
                "each path once"
            )
        files[path] = (digest, origin)
    if upstream is None:
        raise CheckError(f"{label} has no `upstream {CRATE} <version> <sha256>` line")
    return Manifest(version=upstream[0], crate_sha256=upstream[1], files=files)


def unrecorded_changes(entries: dict[str, tuple[str, str]], readme: str) -> list[str]:
    """Files that differ from upstream's but that README.md does not name."""
    return [
        f"{VENDORED.as_posix()}/{path} differs from upstream's ({origin.split(':')[0]}), but "
        f"{README.as_posix()} does not name `{path}`"
        for path, (_, origin) in sorted(entries.items())
        if origin != "upstream" and path != "README.md" and f"`{path}`" not in readme
    ]


def source_problems(root: Path, version: str) -> tuple[list[str], list[str]]:
    """Problems, and a line that reports what was checked."""
    label = MANIFEST.as_posix()
    manifest = read_manifest(root)
    readme, recorded = read_readme(root)
    files = vendored_files(root)
    problems = []
    if manifest.version != version:
        problems.append(f"{label} records {CRATE} {manifest.version}, but {VENDORED.as_posix()} is {version}")
    if manifest.crate_sha256 != recorded:
        problems.append(
            f"{label} records upstream's .crate as {manifest.crate_sha256}, but "
            f"{README.as_posix()} records {recorded}"
        )
    for path in sorted(set(manifest.files) - set(files)):
        problems.append(f"{VENDORED.as_posix()}/{path} is missing: {label} lists it")
    for path in sorted(set(files) - set(manifest.files)):
        problems.append(f"{VENDORED.as_posix()}/{path} is not in {label}")
    for path in sorted(set(files) & set(manifest.files)):
        if files[path] != manifest.files[path][0]:
            problems.append(f"{VENDORED.as_posix()}/{path} does not match its SHA-256 in {label}")
    problems += unrecorded_changes(manifest.files, readme)
    if problems:
        return problems + [REGENERATE], []
    kinds = [origin.split(":")[0] for _, origin in manifest.files.values()]
    return [], [
        f"{VENDORED.as_posix()} holds the {len(files)} files {label} records: upstream "
        f"{CRATE} {manifest.version} with {kinds.count('patched')} patched and "
        f"{kinds.count('added')} added"
    ]


def crate_files(crate: Path, version: str, recorded: str) -> dict[str, str]:
    """Each file in upstream's .crate, by path, with its SHA-256. Read in memory, never extracted."""
    try:
        data = crate.read_bytes()
    except OSError as error:
        raise CheckError(f"cannot read {crate}: {error.strerror}") from None
    if sha256_of(data) != recorded:
        raise CheckError(
            f"{crate} has SHA-256 {sha256_of(data)}, not the {recorded} {README.as_posix()} records"
        )
    prefix = f"{CRATE}-{version}/"
    files = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for member in archive.getmembers():
                if member.isdir():
                    continue
                if not member.isfile() or not member.name.startswith(prefix):
                    raise CheckError(f"{crate}: unexpected entry {member.name!r}")
                files[member.name[len(prefix) :]] = sha256_of(archive.extractfile(member).read())
    except (tarfile.TarError, EOFError, OSError) as error:
        raise CheckError(f"{crate} is not a readable .crate: {error}") from None
    return files


def upstream_problems(root: Path, crate: Path, version: str) -> tuple[list[str], list[str]]:
    """The manifest's record of what differs from upstream, checked against the .crate."""
    label = MANIFEST.as_posix()
    manifest = read_manifest(root)
    _, recorded = read_readme(root)
    upstream = crate_files(crate, version, recorded)
    found = origins({path: digest for path, (digest, _) in manifest.files.items()}, upstream)
    problems = [
        f"{label} records {path} as {manifest.files[path][1]}, but against {crate} it is {origin}"
        for path, (_, origin) in sorted(found.items())
        if manifest.files[path][1] != origin
    ]
    problems += [
        f"{label} does not list upstream's {path}" for path in sorted(set(upstream) - set(found))
    ]
    return problems, [f"{label} matches {crate}: every file it calls upstream's is upstream's"]


def write_manifest(root: Path, crate: Path) -> str:
    """Regenerate the manifest from the tree and upstream's .crate; what it wrote."""
    version = vendored_version(root)
    readme, recorded = read_readme(root)
    upstream = crate_files(crate, version, recorded)
    files = vendored_files(root)
    problems = [
        f"{VENDORED.as_posix()} lacks upstream's {path}: the manifest records no removals"
        for path in sorted(set(upstream) - set(files))
    ]
    entries = origins(files, upstream)
    problems += unrecorded_changes(entries, readme)
    if (root / MANIFEST).exists():
        old = read_manifest(root).files
        changed = sorted(
            path
            for path in set(old) | set(entries)
            if path != "README.md" and old.get(path, ("",))[0] != entries.get(path, ("",))[0]
        )
        if changed and old.get("README.md", ("",))[0] == entries["README.md"][0]:
            problems.append(
                f"{', '.join(changed)} changed, but {README.as_posix()} did not: record the "
                "change in its change list first"
            )
    if problems:
        raise CheckError("; ".join(problems))
    lines = [
        f"# Every file in {VENDORED.as_posix()} but {', '.join(sorted(UNHASHED))}, by SHA-256,",
        "# and whether it is upstream's, patched from upstream's (its SHA-256) or added.",
        "# scripts/check_vendored_sqlx_core.py fails CI on any other file or content.",
        f"# Regenerate it only as {README.as_posix()}'s \"Changing this copy\" says.",
        f"upstream {CRATE} {version} {recorded}",
    ]
    lines += [f"{digest} {origin} {path}" for path, (digest, origin) in sorted(entries.items())]
    temporary = (root / MANIFEST).with_name(f"{MANIFEST.name}.tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.replace(root / MANIFEST)
    kinds = [origin.split(":")[0] for _, origin in entries.values()]
    return (
        f"wrote {MANIFEST.as_posix()}: {len(entries)} files, upstream {CRATE} {version} with "
        f"{kinds.count('patched')} patched and {kinds.count('added')} added"
    )


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
    parser.add_argument(
        "--upstream-crate",
        type=Path,
        help=f"upstream's {CRATE} .crate, whose SHA-256 must be the one {README.as_posix()} "
        f"records: check {MANIFEST.as_posix()}'s record of what differs from it",
    )
    parser.add_argument(
        "--write-manifest",
        action="store_true",
        help=f"regenerate {MANIFEST.as_posix()} from the tree and --upstream-crate, and check "
        "nothing else",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    if args.write_manifest:
        if args.upstream_crate is None:
            parser.error("--write-manifest needs --upstream-crate")
        try:
            print(f"{PREFIX}: {write_manifest(root, args.upstream_crate)}")
        except CheckError as error:
            print(f"{PREFIX}: {error}", file=sys.stderr)
            print(f"{PREFIX}: {MANIFEST.as_posix()} is unchanged", file=sys.stderr)
            return 1
        return 0

    problems: list[str] = []
    report: list[str] = []
    parts = []
    try:
        version = vendored_version(root)
    except CheckError as error:
        problems.append(str(error))
    else:
        parts.append(lambda: (workspace_problems(root, version), []))
        parts.append(lambda: source_problems(root, version))
        if args.upstream_crate is not None:
            parts.append(lambda: upstream_problems(root, args.upstream_crate, version))
    parts.append(lambda: advisory_problems(root, args.advisory_db, args.require_advisory_db))
    # Each part reports on its own, so one that cannot run hides no other's problems.
    for part in parts:
        try:
            found, lines = part()
        except CheckError as error:
            problems.append(str(error))
            continue
        problems += found
        report += lines
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
