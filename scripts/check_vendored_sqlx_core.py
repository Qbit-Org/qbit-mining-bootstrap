#!/usr/bin/env python3
"""Fail unless every build still uses vendor/sqlx-core, it is upstream's crate plus the recorded patches, and its advisories are reviewed.

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
verify-ca again, would build. `--upstream-crate CRATE` names upstream's
.crate, which CI downloads; it is refused unless its SHA-256 is the one
vendor/sqlx-core/README.md records, and read in memory, never extracted.
The `diff -u` patches in vendor/sqlx-core.patches are applied to its files
in name order, each hunk only at the line it names and on exact context,
and this fails unless the result is vendor/sqlx-core byte for byte: no file
changed beyond the patches, none missing, none added but README.md and
reviewed-advisories.toml (which the build never reads), and nothing that is
not a regular file. A change to the code there must therefore change a
patch, where review sees it. What a patch does is for that review: this
proves only that the copy is upstream's plus the patches. Without
`--upstream-crate` it uses Cargo's cached copy of the .crate, and is
skipped when there is none. The README must name each patch, so its change
list keeps up with them. `--unrecorded-diff` prints what
the tree changes beyond the patches, as the next patch; the README's
"Changing this copy" gives the procedure.

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
       --upstream-crate CRATE --unrecorded-diff

Exits 0 when everything holds and 1 otherwise, naming each problem.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import difflib
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
README = VENDORED / "README.md"
PATCHES = Path("vendor") / f"{CRATE}.patches"
PATCH_NAME = re.compile(r"[0-9]{4}-[a-z0-9][a-z0-9-]*\.patch")
# The files upstream's .crate lacks that the vendored copy may hold, as the
# build never reads them: its README and the advisory review list.
ADDED = frozenset({"README.md", "reviewed-advisories.toml"})
RECORDED_CRATE = re.compile(r"SHA-256 is\s+`(?P<sha256>[0-9a-f]{64})`")
FILE_HEADER = re.compile(rb"--- a/(?P<old>[^\t\n]+)\n\+\+\+ b/(?P<new>[^\t\n]+)\n")
HUNK_HEADER = re.compile(rb"@@ -([0-9]+)(?:,([0-9]+))? \+([0-9]+)(?:,([0-9]+))? @@(?: [^\n]*)?\n")
NO_NEWLINE = b"\\ No newline at end of file\n"
RECORD = (
    f"record an intended change as a patch in {PATCHES.as_posix()}, as "
    f"{README.as_posix()}'s \"Changing this copy\" says (--unrecorded-diff prints it)"
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


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def lines_of(data: bytes) -> list[bytes]:
    """`data` split after each newline; a last line without one stays as it is."""
    lines = [line + b"\n" for line in data.split(b"\n")]
    lines[-1] = lines[-1][:-1]
    return lines if lines[-1] else lines[:-1]


def recorded_crate_sha256(root: Path) -> str:
    """The upstream .crate SHA-256 README.md records."""
    try:
        text = (root / README).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise CheckError(f"{README.as_posix()} is missing") from None
    recorded = RECORDED_CRATE.findall(text)
    if len(recorded) != 1:
        raise CheckError(
            f"{README.as_posix()} records {len(recorded)} upstream .crate SHA-256s, not one, "
            "as `SHA-256 is `<hex>``"
        )
    return recorded[0]


def vendored_files(root: Path) -> dict[str, bytes]:
    """Every file in vendor/sqlx-core, by path."""
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
            elif name in filenames:
                files[relative] = path.read_bytes()
    if problems:
        raise CheckError(f"{'; '.join(problems)}: the vendored copy holds regular files only")
    return files


def crate_files(crate: Path, version: str, recorded: str) -> dict[str, bytes]:
    """Each file in upstream's .crate, by path. Read in memory, never extracted."""
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
                name = member.name[len(prefix) :]
                if not member.name.startswith(prefix):
                    raise CheckError(
                        f"{crate} holds {member.name!r}, outside {prefix}: it is not the .crate "
                        f"of {CRATE} {version}, the version {VENDORED.as_posix()}/Cargo.toml names"
                    )
                if not member.isfile() or name in files:
                    raise CheckError(f"{crate}: unexpected entry {member.name!r}")
                files[name] = archive.extractfile(member).read()
    except (tarfile.TarError, EOFError, OSError) as error:
        raise CheckError(f"{crate} is not a readable .crate: {error}") from None
    return files


@dataclass
class Hunk:
    line: int  # its `@@` line in the patch
    old_start: int
    old: list[bytes]
    new_start: int
    new: list[bytes]


def parse_patch(label: str, data: bytes) -> list[tuple[str, list[Hunk]]]:
    """A `diff -u` patch with `a/` and `b/` labels, as each file it changes and its hunks.

    Free text may come before the first `--- ` line, and nothing else
    may stand between hunks, so what applies is what review reads.
    """
    if not data.endswith(b"\n"):
        raise CheckError(f"{label} does not end with a newline")
    lines = lines_of(data)

    def fail(index: int, message: str) -> CheckError:
        return CheckError(f"{label}:{index + 1}: {message}")

    index = 0
    while index < len(lines) and not lines[index].startswith(b"--- "):
        if lines[index].startswith((b"+++ ", b"@@ ")):
            raise fail(index, "a file or hunk header before the first `--- a/<path>` line")
        index += 1
    files: list[tuple[str, list[Hunk]]] = []
    while index < len(lines):
        header = FILE_HEADER.fullmatch(b"".join(lines[index : index + 2]))
        try:
            path = header["old"].decode() if header and header["old"] == header["new"] else None
        except UnicodeDecodeError:
            path = None
        if path is None:
            raise fail(
                index,
                "expected `--- a/<path>` then `+++ b/<path>` for one file, without timestamps "
                "(write patches with --unrecorded-diff, or `diff -u` with a/ and b/ labels)",
            )
        if path in (changed for changed, _ in files):
            raise fail(index, f"{path} has a second header: give each file one")
        index += 2
        hunks = []
        while index < len(lines) and lines[index].startswith(b"@@ "):
            counts = HUNK_HEADER.fullmatch(lines[index])
            if counts is None:
                raise fail(index, "malformed hunk header")
            old_start, old_count, new_start, new_count = (
                1 if value is None else int(value) for value in counts.groups()
            )
            hunk = Hunk(index + 1, old_start, [], new_start, [])
            index += 1
            while len(hunk.old) < old_count or len(hunk.new) < new_count:
                if index == len(lines):
                    raise fail(hunk.line - 1, "the patch ends inside this hunk")
                tag, content = lines[index][:1], lines[index][1:]
                if tag not in (b" ", b"-", b"+"):
                    raise fail(index, "expected a ` `, `-` or `+` line in the hunk")
                if tag != b"+":
                    hunk.old.append(content)
                if tag != b"-":
                    hunk.new.append(content)
                index += 1
                if index < len(lines) and lines[index] == NO_NEWLINE:
                    # The line just read ends its file without a newline.
                    if tag != b"+":
                        hunk.old[-1] = content[:-1]
                    if tag != b"-":
                        hunk.new[-1] = content[:-1]
                    index += 1
            if (len(hunk.old), len(hunk.new)) != (old_count, new_count):
                raise fail(hunk.line - 1, "the hunk's lines do not match its header's counts")
            hunks.append(hunk)
        if not hunks:
            raise fail(index, f"expected a hunk for {path}")
        files.append((path, hunks))
    if not files:
        raise CheckError(f"{label} changes no file")
    return files


def apply_hunks(label: str, path: str, data: bytes, hunks: list[Hunk]) -> bytes:
    """`data` with `hunks` applied, each exactly where it says: no offset, no fuzz."""
    lines = lines_of(data)
    result: list[bytes] = []
    done = 0
    for hunk in hunks:
        # An empty old range names the line the new lines follow.
        at = hunk.old_start - 1 if hunk.old else hunk.old_start
        position = len(result) + at - done
        if (
            at < done
            or at > len(lines)
            or lines[at : at + len(hunk.old)] != hunk.old
            or hunk.new_start != (position + 1 if hunk.new else position)
        ):
            raise CheckError(
                f"{label}:{hunk.line}: the hunk does not apply to {path} exactly at its line "
                f"{hunk.old_start}"
            )
        result += lines[done:at] + hunk.new
        done = at + len(hunk.old)
    result += lines[done:]
    # `\ No newline at end of file` may end only the file: on a line that
    # others follow, it would join two lines that review reads as separate.
    for number, line in enumerate(result[:-1], start=1):
        if not line.endswith(b"\n"):
            raise CheckError(
                f"{label}: {path}'s line {number} would end without a newline, joined to the "
                "next: `\\ No newline at end of file` marks only a file's last line"
            )
    return b"".join(result)


def recorded_patches(root: Path) -> list[Path]:
    """The patches, in the order they apply."""
    directory = root / PATCHES
    if not directory.is_dir():
        raise CheckError(f"{PATCHES.as_posix()} is missing")
    entries = sorted(directory.iterdir())
    stray = [
        entry.name
        for entry in entries
        if entry.is_symlink() or not entry.is_file() or not PATCH_NAME.fullmatch(entry.name)
    ]
    if stray:
        raise CheckError(
            f"{PATCHES.as_posix()} holds {', '.join(stray)}: it holds only regular files named "
            "NNNN-<name>.patch"
        )
    return entries


def patched_upstream(
    root: Path, crate: Path, version: str
) -> tuple[dict[str, bytes], dict[str, bytes], list[Path]]:
    """Upstream's files, the same with every patch applied, and the patches."""
    upstream = crate_files(crate, version, recorded_crate_sha256(root))
    files = dict(upstream)
    patches = recorded_patches(root)
    for patch in patches:
        label = (PATCHES / patch.name).as_posix()
        for path, hunks in parse_patch(label, patch.read_bytes()):
            if path not in files:
                raise CheckError(
                    f"{label} changes {path}, which upstream's .crate lacks: a patch only "
                    "changes upstream's files"
                )
            files[path] = apply_hunks(label, path, files[path], hunks)
    return upstream, files, patches


def cached_crate(version: str) -> Path | None:
    """Upstream's .crate in Cargo's download cache, where a build has fetched it."""
    cache = Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo") / "registry" / "cache"
    found = sorted(cache.glob(f"*/{CRATE}-{version}.crate"))
    return found[0] if found else None


def unnamed_patches(root: Path) -> list[str]:
    """Patches README.md's change list does not name. It describes the changes to
    reviewers; this keeps it from falling behind, and checks nothing it says."""
    try:
        readme = (root / README).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise CheckError(f"{README.as_posix()} is missing") from None
    return [
        f"{README.as_posix()} does not name `{patch.name}`: describe each patch in "
        f"{PATCHES.as_posix()} in its change list"
        for patch in recorded_patches(root)
        if f"`{patch.name}`" not in readme
    ]


def source_problems(root: Path, crate: Path | None, version: str) -> tuple[list[str], list[str]]:
    """Problems, and a line that reports what was checked."""
    unnamed = unnamed_patches(root)
    if crate is None:
        return unnamed, [
            f"skipped the source check: no {CRATE}-{version}.crate in Cargo's cache; pass "
            "--upstream-crate with upstream's, as CI does"
        ]
    upstream, expected, patches = patched_upstream(root, crate, version)
    files = vendored_files(root)
    base = VENDORED.as_posix()
    problems = unnamed + [
        f"{base}/{path} is missing: upstream's .crate has it"
        for path in sorted(set(expected) - set(files))
    ]
    problems += [
        f"{base}/{path} is not upstream's: only {' and '.join(sorted(ADDED))} may be added"
        for path in sorted(set(files) - set(expected) - ADDED)
    ]
    problems += [
        f"{base}/{path} is not upstream's with the recorded patches: {RECORD}"
        for path in sorted(set(files) & set(expected))
        if files[path] != expected[path]
    ]
    if problems:
        return problems, []
    patched = sum(expected[path] != upstream[path] for path in upstream)
    added = sorted((set(files) - set(upstream)) & ADDED)
    return [], [
        f"{base} is upstream {CRATE} {version} plus the {len(patches)} patch(es) in "
        f"{PATCHES.as_posix()}, byte for byte: {patched} of its {len(upstream)} files patched"
        + (f", and {' and '.join(added)} added" if added else "")
    ]


def unrecorded_diff(root: Path, crate: Path, version: str) -> str:
    """What the tree changes beyond upstream with the recorded patches, as the next patch."""
    _, expected, _ = patched_upstream(root, crate, version)
    files = vendored_files(root)
    unrecordable = sorted((set(expected) ^ set(files)) - ADDED)
    if unrecordable:
        raise CheckError(
            f"{', '.join(unrecordable)} added or removed: a patch only changes upstream's files"
        )
    out = []
    for path in sorted(set(expected) & set(files)):
        if expected[path] == files[path]:
            continue
        try:
            old = [line.decode() for line in lines_of(expected[path])]
            new = [line.decode() for line in lines_of(files[path])]
        except UnicodeDecodeError:
            raise CheckError(f"{VENDORED.as_posix()}/{path} is not UTF-8 text") from None
        for line in difflib.unified_diff(old, new, f"a/{path}", f"b/{path}"):
            out.append(line if line.endswith("\n") else f"{line}\n{NO_NEWLINE.decode()}")
    return "".join(out)


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
        f"records: check that {VENDORED.as_posix()} is it plus the patches in "
        f"{PATCHES.as_posix()} (default: Cargo's cached copy; skipped without one)",
    )
    parser.add_argument(
        "--unrecorded-diff",
        action="store_true",
        help=f"print what {VENDORED.as_posix()} changes beyond --upstream-crate with the "
        "recorded patches, as the next patch, and check nothing else",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    if args.unrecorded_diff:
        if args.upstream_crate is None:
            parser.error("--unrecorded-diff needs --upstream-crate")
        try:
            sys.stdout.write(unrecorded_diff(root, args.upstream_crate, vendored_version(root)))
        except CheckError as error:
            print(f"{PREFIX}: {error}", file=sys.stderr)
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
        parts.append(
            lambda: source_problems(root, args.upstream_crate or cached_crate(version), version)
        )
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
