#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_vendored_sqlx_core.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
VERSION = "0.8.6"
FUZZ = Path("crates") / "qbit-prism-server" / "fuzz"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
PATCH = '\n[patch.crates-io]\nsqlx-core = {{ path = "{path}" }}\n'
MANIFEST = Path("vendor") / "sqlx-core.sha256"
TLS = "src/net/tls/tls_rustls.rs"
# A stand-in for upstream's .crate SHA-256 where no test reads the crate.
CRATE_SHA256 = "ab" * 32


def readme(crate_sha256: str = CRATE_SHA256, changes: str = "") -> str:
    return (
        "# sqlx-core 0.8.6\n\nThis is sqlx-core as published: the `.crate` whose\n"
        f"SHA-256 is `{crate_sha256}`.\n\n{changes}"
    )


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(
    root: Path,
    *,
    version: str = VERSION,
    crate_sha256: str = CRATE_SHA256,
    origins: dict[str, str] | None = None,
) -> None:
    """The manifest for the tree as it stands: README.md added, every other file upstream's."""
    crate = root / "vendor" / "sqlx-core"
    lines = ["# written by the test", f"upstream sqlx-core {version} {crate_sha256}"]
    for path in sorted(crate.rglob("*")):
        relative = path.relative_to(crate).as_posix()
        if path.is_file() and relative != "reviewed-advisories.toml":
            origin = (origins or {}).get(relative, "added" if relative == "README.md" else "upstream")
            lines.append(f"{sha256(path)} {origin} {relative}")
    (root / MANIFEST).write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_crate(path: Path, files: dict[str, str], *, version: str = VERSION) -> str:
    """A .crate holding `files` under sqlx-core-<version>/; its SHA-256."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, text in files.items():
            data = text.encode()
            member = tarfile.TarInfo(f"sqlx-core-{version}/{name}")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    path.write_bytes(buffer.getvalue())
    return sha256(path)


def lock(
    *,
    core: str = f'name = "sqlx-core"\nversion = "{VERSION}"\n',
    sqlx_version: str = VERSION,
    sqlx_dependency: str = "sqlx-core",
    extra: str = "",
) -> str:
    return (
        "version = 4\n\n"
        f"[[package]]\nname = \"sqlx\"\nversion = \"{sqlx_version}\"\nsource = \"{REGISTRY}\"\n"
        f"checksum = \"00\"\ndependencies = [\n \"{sqlx_dependency}\",\n \"sqlx-postgres\",\n]\n\n"
        f"[[package]]\n{core}dependencies = [\n \"bytes\",\n]\n{extra}"
    )


def write_tree(
    root: Path,
    *,
    vendored: str = VERSION,
    root_lock: str | None = None,
    fuzz_lock: str | None = None,
    root_patch: str | None = PATCH.format(path="vendor/sqlx-core"),
    fuzz_patch: str | None = PATCH.format(path="../../../vendor/sqlx-core"),
    reviewed: str = "[reviewed]\n",
    db_path: str = "~/.cargo/advisory-db",
    readme_text: str | None = None,
) -> None:
    crate = root / "vendor" / "sqlx-core"
    (crate / "src" / "net" / "tls").mkdir(parents=True)
    (crate / "Cargo.toml").write_text(
        f'[package]\nname = "sqlx-core"\nversion = "{vendored}"\n', encoding="utf-8"
    )
    (crate / "src" / "lib.rs").write_text("pub mod net;\n", encoding="utf-8")
    (crate / TLS).write_text("// verify-ca trusts only sslrootcert\n", encoding="utf-8")
    (crate / "README.md").write_text(readme() if readme_text is None else readme_text, encoding="utf-8")
    (crate / "reviewed-advisories.toml").write_text(reviewed, encoding="utf-8")
    (root / "deny.toml").write_text(f'[advisories]\ndb-path = "{db_path}"\n', encoding="utf-8")
    (root / "Cargo.toml").write_text(
        '[workspace]\nmembers = ["crates/alpha"]\n' + (root_patch or ""), encoding="utf-8"
    )
    (root / "Cargo.lock").write_text(lock() if root_lock is None else root_lock, encoding="utf-8")
    fuzz = root / FUZZ
    fuzz.mkdir(parents=True)
    (fuzz / "Cargo.toml").write_text(
        '[package]\nname = "fuzz"\n\n[workspace]\nmembers = ["."]\n' + (fuzz_patch or ""),
        encoding="utf-8",
    )
    (fuzz / "Cargo.lock").write_text(lock() if fuzz_lock is None else fuzz_lock, encoding="utf-8")
    write_manifest(root, version=vendored)


def write_advisory(
    db: Path, advisory: str, *, package: str = "sqlx-core", withdrawn: str | None = None
) -> None:
    directory = db / "crates" / package
    directory.mkdir(parents=True, exist_ok=True)
    withdrawn_line = "" if withdrawn is None else f'withdrawn = "{withdrawn}"\n'
    (directory / f"{advisory}.md").write_text(
        "```toml\n[advisory]\n"
        f'id = "{advisory}"\npackage = "{package}"\ndate = "2026-01-01"\n{withdrawn_line}'
        '\n[versions]\npatched = [">= 0.8.7"]\n```\n\n# A flaw in sqlx-core\n\nDetails.\n',
        encoding="utf-8",
    )


def run_check(root: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    merged.pop("RUSTSEC_ADVISORY_DB", None)
    # No advisory database unless a test supplies one.
    merged["HOME"] = str(root / "home")
    merged.pop("CARGO_HOME", None)
    merged.update(env or {})
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), *args],
        cwd=ROOT,
        env=merged,
        text=True,
        capture_output=True,
        check=False,
    )


class CheckVendoredSqlxCoreTests(unittest.TestCase):
    def test_a_patched_tree_passes_and_skips_a_missing_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            f"Cargo.lock and {FUZZ.as_posix()}/Cargo.lock use vendor/sqlx-core {VERSION}",
            result.stdout,
        )
        self.assertIn("skipped the advisory review", result.stdout)

    def test_a_registry_sqlx_core_in_either_lock_fails(self) -> None:
        registry_core = (
            f'name = "sqlx-core"\nversion = "{VERSION}"\nsource = "{REGISTRY}"\nchecksum = "ee67"\n'
        )
        hand_edited = f'name = "sqlx-core"\nversion = "{VERSION}"\nsource = "{REGISTRY}"\n'
        checksum_only = f'name = "sqlx-core"\nversion = "{VERSION}"\nchecksum = "ee67"\n'
        for core in (registry_core, hand_edited, checksum_only):
            for name, label in (("root_lock", "Cargo.lock"), ("fuzz_lock", f"{FUZZ.as_posix()}/Cargo.lock")):
                with self.subTest(core=core, lock=label), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    write_tree(root, **{name: lock(core=core)})
                    result = run_check(root)
                    self.assertEqual(result.returncode, 1, result.stdout)
                    self.assertIn(
                        f"{label}: sqlx-core {VERSION} comes from", result.stderr
                    )
                    self.assertIn("the [patch.crates-io] entry no longer applies", result.stderr)
                    self.assertEqual(result.stderr.count("no longer applies"), 1)

    def test_moving_sqlx_past_the_vendored_version_fails(self) -> None:
        # `cargo update -p sqlx` to a later 0.8.x: sqlx pins its sqlx-core with
        # `=`, so the registry copy returns and Cargo marks the patch unused.
        moved = lock(
            core=f'name = "sqlx-core"\nversion = "0.8.7"\nsource = "{REGISTRY}"\nchecksum = "ab"\n',
            sqlx_version="0.8.7",
            extra=f'\n[[patch.unused]]\nname = "sqlx-core"\nversion = "{VERSION}"\n',
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, root_lock=moved)
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("Cargo.lock: sqlx-core 0.8.7 comes from", result.stderr)
        self.assertIn(
            f"Cargo.lock: sqlx 0.8.7 requires sqlx-core =0.8.7, but vendor/sqlx-core is {VERSION}",
            result.stderr,
        )
        self.assertIn(f"Cargo recorded the sqlx-core {VERSION} patch as unused", result.stderr)
        self.assertNotIn(f"{FUZZ.as_posix()}/Cargo.lock", result.stderr)

    def test_a_second_sqlx_core_or_a_version_mismatch_fails(self) -> None:
        second = lock(
            sqlx_dependency=f"sqlx-core 0.8.5 ({REGISTRY})",
            extra=(
                f'\n[[package]]\nname = "sqlx-core"\nversion = "0.8.5"\nsource = "{REGISTRY}"\n'
                'checksum = "cd"\n'
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, fuzz_lock=second)
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"{FUZZ.as_posix()}/Cargo.lock holds 2 sqlx-core entries", result.stderr)
        self.assertIn("sqlx depends on sqlx-core 0.8.5 from registry", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, vendored="0.8.7")
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("Cargo.lock: the path sqlx-core is 0.8.6, but vendor/sqlx-core is 0.8.7", result.stderr)

    def test_a_workspace_without_the_patch_or_patching_elsewhere_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, fuzz_patch=None)
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(
            f"{FUZZ.as_posix()}/Cargo.toml does not patch sqlx-core to vendor/sqlx-core",
            result.stderr,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, root_patch=PATCH.format(path="../sqlx-core"))
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("Cargo.toml patches sqlx-core to '../sqlx-core'", result.stderr)

    def test_an_unreviewed_advisory_fails_and_a_reviewed_one_is_listed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            db = root / "db"
            write_advisory(db, "RUSTSEC-2026-0101")
            write_advisory(db, "RUSTSEC-2026-0102", withdrawn="2026-02-01")
            write_advisory(db, "RUSTSEC-2026-0103", package="sqlx")
            result = run_check(root, "--advisory-db", str(db))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("RUSTSEC-2026-0101 (A flaw in sqlx-core; patched ['>= 0.8.7']) is not in", result.stderr)
            self.assertNotIn("RUSTSEC-2026-0102", result.stderr)
            self.assertIn("RUSTSEC-2026-0102: withdrawn 2026-02-01", result.stdout)
            self.assertNotIn("RUSTSEC-2026-0103", result.stdout + result.stderr)
            (root / "vendor" / "sqlx-core" / "reviewed-advisories.toml").write_text(
                '[reviewed]\n"RUSTSEC-2026-0101" = "fixed in the vendored copy by #900"\n',
                encoding="utf-8",
            )
            result = run_check(root, "--advisory-db", str(db))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("lists 2 sqlx-core advisories", result.stdout)
        self.assertIn("RUSTSEC-2026-0101: reviewed: fixed in the vendored copy by #900", result.stdout)

    def test_the_database_defaults_to_deny_tomls_db_path_or_the_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            checkout = root / "home" / ".cargo" / "advisory-db" / "advisory-db-3157b0e258782691"
            write_advisory(checkout, "RUSTSEC-2026-0101")
            result = run_check(root, "--require-advisory-db")
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("RUSTSEC-2026-0101", result.stderr)
            other = root / "other"
            (other / "crates").mkdir(parents=True)
            result = run_check(root, "--require-advisory-db", env={"RUSTSEC_ADVISORY_DB": str(other)})
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"{other} lists 0 sqlx-core advisories", result.stdout)
            write_advisory(checkout.parent / "advisory-db-0123456789abcdef", "RUSTSEC-2026-0104")
            result = run_check(root)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("holds 2 advisory databases", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, db_path="$CARGO_HOME/advisory-dbs")
            write_advisory(root / "cargo" / "advisory-dbs" / "advisory-db-1", "RUSTSEC-2026-0105")
            result = run_check(root, env={"CARGO_HOME": str(root / "cargo")})
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("RUSTSEC-2026-0105", result.stderr)

    def test_require_advisory_db_fails_without_a_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            result = run_check(root, "--require-advisory-db")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("no RustSec advisory database at", result.stderr)
        self.assertIn("run `cargo deny check` first", result.stderr)

    def test_a_malformed_review_list_or_advisory_fails(self) -> None:
        reviewed = {
            '[reviewed]\n"RUSTSEC-26-1" = "typo"\n': "'RUSTSEC-26-1' is not a RUSTSEC-YYYY-NNNN ID",
            '[reviewed]\n"RUSTSEC-2026-0101" = " "\n': "RUSTSEC-2026-0101 needs a one-line reason",
            '[reviewed]\n"RUSTSEC-2026-0101" = """two\nlines"""\n': "RUSTSEC-2026-0101 needs a one-line reason",
            '[allowed]\n': "unknown keys allowed",
            "": "has no [reviewed] table",
            '[reviewed]\n"RUSTSEC-2026-0199" = "gone"\n': "names RUSTSEC-2026-0199, which the database does not hold",
        }
        for text, message in reviewed.items():
            with self.subTest(text=text), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                write_tree(root, reviewed=text)
                (root / "db" / "crates").mkdir(parents=True)
                result = run_check(root, "--advisory-db", str(root / "db"))
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            db = root / "db"
            write_advisory(db, "RUSTSEC-2026-0101")
            advisory = db / "crates" / "sqlx-core" / "RUSTSEC-2026-0101.md"
            advisory.rename(advisory.with_name("RUSTSEC-2026-0109.md"))
            result = run_check(root, "--advisory-db", str(db))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("expected id 'RUSTSEC-2026-0109'", result.stderr)
            (db / "crates" / "sqlx-core" / "RUSTSEC-2026-0109.md").write_text("# no front matter\n", encoding="utf-8")
            result = run_check(root, "--advisory-db", str(db))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("has no TOML front matter", result.stderr)

    def test_the_checked_in_tree_uses_the_vendored_crate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # An empty directory stands in for the database: the review list
            # is checked against the real one in CI's cargo-deny job.
            result = run_check(ROOT, "--advisory-db", tmp)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"use vendor/sqlx-core {VERSION}", result.stdout)
        self.assertIn(
            f"records: upstream sqlx-core {VERSION} with 4 patched and 1 added", result.stdout
        )

    def test_a_modified_added_or_missing_vendored_file_fails(self) -> None:
        crate = Path("vendor") / "sqlx-core"
        cases = {
            "modified": (
                lambda base: (base / TLS).write_text("// trusts the public roots again\n", encoding="utf-8"),
                f"{crate.as_posix()}/{TLS} does not match its SHA-256 in {MANIFEST.as_posix()}",
            ),
            "added": (
                lambda base: (base / "src" / "extra.rs").write_text("\n", encoding="utf-8"),
                f"{crate.as_posix()}/src/extra.rs is not in {MANIFEST.as_posix()}",
            ),
            "registry marker": (
                lambda base: (base / ".cargo-ok").write_text('{"v":1}', encoding="utf-8"),
                f"{crate.as_posix()}/.cargo-ok is not in {MANIFEST.as_posix()}",
            ),
            "missing": (
                lambda base: (base / "src" / "lib.rs").unlink(),
                f"{crate.as_posix()}/src/lib.rs is missing: {MANIFEST.as_posix()} lists it",
            ),
            "missing manifest": (
                lambda base: (base.parent / "sqlx-core.sha256").unlink(),
                f"{MANIFEST.as_posix()} is missing",
            ),
            "symbolic link": (
                lambda base: (base / "src" / "linked.rs").symlink_to(base / "src" / "lib.rs"),
                f"{crate.as_posix()}/src/linked.rs is a symbolic link",
            ),
        }
        for name, (change, message) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                write_tree(root)
                change(root / crate)
                result = run_check(root)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)
                self.assertNotIn("use vendor/sqlx-core", result.stdout)
        with tempfile.TemporaryDirectory() as tmp:
            # The advisory review reads this file, and a review changes no code.
            root = Path(tmp)
            write_tree(root)
            (root / crate / "reviewed-advisories.toml").write_text(
                '[reviewed]\n"RUSTSEC-2026-0101" = "not reachable here"\n', encoding="utf-8"
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            f"vendor/sqlx-core holds the 4 files {MANIFEST.as_posix()} records: upstream "
            f"sqlx-core {VERSION} with 0 patched and 1 added",
            result.stdout,
        )

    def test_a_broken_workspace_manifest_hides_no_vendored_file_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            (root / FUZZ / "Cargo.toml").write_text("[package\n", encoding="utf-8")
            (root / "vendor" / "sqlx-core" / TLS).write_text("// edited\n", encoding="utf-8")
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"{FUZZ.as_posix()}/Cargo.toml is not valid TOML", result.stderr)
        self.assertIn(f"vendor/sqlx-core/{TLS} does not match its SHA-256", result.stderr)

    def test_the_manifest_must_agree_with_the_readme_and_the_crate_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            write_manifest(root, origins={TLS: f"patched:{'cd' * 32}"})
            result = run_check(root)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(
                f"vendor/sqlx-core/{TLS} differs from upstream's (patched), but "
                f"vendor/sqlx-core/README.md does not name `{TLS}`",
                result.stderr,
            )
            self.assertIn('regenerate vendor/sqlx-core.sha256 as that README\'s "Changing this copy" says', result.stderr)
            (root / "vendor" / "sqlx-core" / "README.md").write_text(
                readme(changes=f"- verify-ca trusts only sslrootcert, in `{TLS}`.\n"), encoding="utf-8"
            )
            write_manifest(root, origins={TLS: f"patched:{'cd' * 32}"})
            result = run_check(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("with 1 patched and 1 added", result.stdout)
            write_manifest(root, origins={TLS: f"patched:{'cd' * 32}"}, crate_sha256="ef" * 32)
            result = run_check(root)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(
                f"records upstream's .crate as {'ef' * 32}, but vendor/sqlx-core/README.md "
                f"records {CRATE_SHA256}",
                result.stderr,
            )
            write_manifest(root, origins={TLS: f"patched:{'cd' * 32}"}, version="0.8.5")
            result = run_check(root)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(
                f"{MANIFEST.as_posix()} records sqlx-core 0.8.5, but vendor/sqlx-core is {VERSION}",
                result.stderr,
            )
        malformed = {
            "no upstream line": "",
            "two upstream lines": f"upstream sqlx-core {VERSION} {CRATE_SHA256}\n" * 2,
            "bad origin": f"upstream sqlx-core {VERSION} {CRATE_SHA256}\n{'0' * 64} vendored src/lib.rs\n",
            "short hash": f"upstream sqlx-core {VERSION} {CRATE_SHA256}\nabc upstream src/lib.rs\n",
            "repeated path": f"upstream sqlx-core {VERSION} {CRATE_SHA256}\n"
            + f"{'0' * 64} upstream src/lib.rs\n" * 2,
        }
        for name, text in malformed.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                write_tree(root)
                (root / MANIFEST).write_text(text, encoding="utf-8")
                result = run_check(root)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(MANIFEST.as_posix(), result.stderr)
                expected = "expected" if text else "has no `upstream sqlx-core <version> <sha256>` line"
                self.assertIn(expected, result.stderr)

    def test_write_manifest_records_upstream_and_needs_the_readme_to_record_a_change(self) -> None:
        upstream = {
            "Cargo.toml": f'[package]\nname = "sqlx-core"\nversion = "{VERSION}"\n',
            "src/lib.rs": "pub mod net;\n",
            TLS: "// verify-ca trusts only sslrootcert\n",
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = root / "sqlx-core-0.8.6.crate"
            crate_sha256 = write_crate(crate, upstream)
            write_tree(root, readme_text=readme(crate_sha256))
            (root / MANIFEST).unlink()
            write = ("--write-manifest", "--upstream-crate", str(crate))
            result = run_check(root, *write)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("wrote vendor/sqlx-core.sha256: 4 files, upstream sqlx-core 0.8.6 with 0 patched and 1 added", result.stdout)
            manifest = (root / MANIFEST).read_text(encoding="utf-8")
            self.assertIn(f"upstream sqlx-core {VERSION} {crate_sha256}\n", manifest)
            self.assertIn(f"{sha256(root / 'vendor' / 'sqlx-core' / TLS)} upstream {TLS}\n", manifest)
            self.assertEqual(run_check(root).returncode, 0)
            self.assertEqual(run_check(root, "--upstream-crate", str(crate)).returncode, 0)

            # An edit fails the check, and the manifest is not regenerated
            # until the README records it.
            tls = root / "vendor" / "sqlx-core" / TLS
            tls.write_text("// verify-ca trusts only sslrootcert, as libpq does\n", encoding="utf-8")
            self.assertEqual(run_check(root).returncode, 1)
            result = run_check(root, *write)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(f"{TLS} changed, but vendor/sqlx-core/README.md did not", result.stderr)
            self.assertIn(f"README.md does not name `{TLS}`", result.stderr)
            self.assertIn("vendor/sqlx-core.sha256 is unchanged", result.stderr)
            self.assertEqual((root / MANIFEST).read_text(encoding="utf-8"), manifest)
            # A README edit that doesn't name the file is not enough.
            (root / "vendor" / "sqlx-core" / "README.md").write_text(
                readme(crate_sha256, "- A local change.\n"), encoding="utf-8"
            )
            result = run_check(root, *write)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertNotIn("did not: record", result.stderr)
            self.assertIn(f"README.md does not name `{TLS}`", result.stderr)
            (root / "vendor" / "sqlx-core" / "README.md").write_text(
                readme(crate_sha256, f"- A local change, in `{TLS}`.\n"), encoding="utf-8"
            )
            result = run_check(root, *write)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("with 1 patched and 1 added", result.stdout)
            upstream_tls = hashlib.sha256(upstream[TLS].encode()).hexdigest()
            self.assertIn(
                f"{sha256(tls)} patched:{upstream_tls} {TLS}\n",
                (root / MANIFEST).read_text(encoding="utf-8"),
            )
            self.assertEqual(run_check(root).returncode, 0)
            self.assertEqual(run_check(root, "--upstream-crate", str(crate)).returncode, 0)

            # Only the .crate the README records, and only with every file of it here.
            other = root / "other.crate"
            write_crate(other, {**upstream, "src/lib.rs": "pub mod other;\n"})
            result = run_check(root, "--write-manifest", "--upstream-crate", str(other))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(f"not the {crate_sha256} vendor/sqlx-core/README.md records", result.stderr)
            (root / "vendor" / "sqlx-core" / "src" / "lib.rs").unlink()
            result = run_check(root, *write)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("vendor/sqlx-core lacks upstream's src/lib.rs", result.stderr)
            result = run_check(root, "--write-manifest")
            self.assertEqual(result.returncode, 2, result.stdout)
            self.assertIn("--write-manifest needs --upstream-crate", result.stderr)

    def test_the_upstream_crate_check_catches_a_manifest_that_misstates_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = root / "sqlx-core-0.8.6.crate"
            crate_sha256 = write_crate(
                crate,
                {
                    "Cargo.toml": f'[package]\nname = "sqlx-core"\nversion = "{VERSION}"\n',
                    "src/lib.rs": "pub mod net;\n",
                    TLS: "// upstream: the public roots under verify-ca\n",
                    "LICENSE-MIT": "MIT\n",
                },
            )
            write_tree(root, readme_text=readme(crate_sha256))
            write_manifest(root, crate_sha256=crate_sha256)
            # Hand-written to call the patched file upstream's: offline, only
            # the hashes can be checked, so the plain check passes.
            self.assertEqual(run_check(root).returncode, 0)
            result = run_check(root, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"records {TLS} as upstream, but against {crate} it is patched:", result.stderr)
        self.assertIn(f"{MANIFEST.as_posix()} does not list upstream's LICENSE-MIT", result.stderr)

    def test_an_edit_to_the_real_vendored_tls_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree(ROOT / "vendor", root / "vendor", symlinks=True)
            for name in ("Cargo.toml", "Cargo.lock", "deny.toml", f"{FUZZ.as_posix()}/Cargo.toml", f"{FUZZ.as_posix()}/Cargo.lock"):
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(ROOT / name, root / name)
            self.assertEqual(run_check(root).returncode, 0)
            tls = root / "vendor" / "sqlx-core" / TLS
            tls.write_text(
                tls.read_text(encoding="utf-8").replace(
                    "RootCertStore::empty()", "public_roots()", 1
                ),
                encoding="utf-8",
            )
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"vendor/sqlx-core/{TLS} does not match its SHA-256", result.stderr)

    @unittest.skipUnless(
        list((Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo") / "registry" / "cache").glob(f"*/sqlx-core-{VERSION}.crate")),
        "upstream's .crate is not in Cargo's cache",
    )
    def test_the_real_tree_is_upstream_plus_the_recorded_changes(self) -> None:
        cache = Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo") / "registry" / "cache"
        crate = sorted(cache.glob(f"*/sqlx-core-{VERSION}.crate"))[0]
        with tempfile.TemporaryDirectory() as tmp:
            result = run_check(ROOT, "--advisory-db", tmp, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("every file it calls upstream's is upstream's", result.stdout)

    def test_ci_runs_the_check_and_the_advisory_review_after_cargo_deny(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("run: python3 scripts/check_vendored_sqlx_core.py\n", workflow)
        job = workflow.index("\n  cargo-deny:\n")
        next_job = re.compile(r"\n  [a-z0-9-]+:\n").search(workflow, job + 1)
        end = next_job.start() if next_job else len(workflow)
        deny = workflow.index("run: cargo deny --locked check", job, end)
        review = workflow.index(
            "run: python3 scripts/check_vendored_sqlx_core.py --require-advisory-db", job, end
        )
        self.assertLess(deny, review)

if __name__ == "__main__":
    unittest.main()
