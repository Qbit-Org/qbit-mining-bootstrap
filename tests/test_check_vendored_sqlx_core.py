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
PATCHES = Path("vendor") / "sqlx-core.patches"
TLS = "src/net/tls/tls_rustls.rs"
UPSTREAM = {
    "Cargo.toml": f'[package]\nname = "sqlx-core"\nversion = "{VERSION}"\n',
    "LICENSE-MIT": "MIT\n",
    "src/lib.rs": "pub mod net;\n",
    TLS: "fn roots() {\n    public_roots()\n}\n",
}
PATCHED_TLS = "fn roots() {\n    RootCertStore::empty()\n}\n"
TLS_PATCH = (
    "Local: verify-ca trusts only sslrootcert.\n\n"
    f"--- a/{TLS}\n+++ b/{TLS}\n@@ -1,3 +1,3 @@\n"
    " fn roots() {\n-    public_roots()\n+    RootCertStore::empty()\n }\n"
)
UPSTREAM_CRATE_ENV = "SQLX_CORE_UPSTREAM_CRATE"


def readme(crate_sha256: str) -> str:
    return (
        "# sqlx-core 0.8.6\n\nThis is sqlx-core as published: the `.crate` whose\n"
        f"SHA-256 is `{crate_sha256}`.\n\n- Local: verify-ca, `0001-verify-ca.patch`.\n"
    )


def record_patch(root: Path, name: str, text: str) -> None:
    """Add a patch, and name it in the README's change list."""
    (root / PATCHES / name).write_text(text, encoding="utf-8")
    path = root / "vendor" / "sqlx-core" / "README.md"
    path.write_text(path.read_text(encoding="utf-8") + f"- `{name}`.\n", encoding="utf-8")


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
    return hashlib.sha256(path.read_bytes()).hexdigest()


def real_upstream_crate() -> Path | None:
    """Upstream's real .crate: the one CI downloads, else Cargo's cached copy."""
    named = os.environ.get(UPSTREAM_CRATE_ENV)
    if named:
        # Named, so a missing file fails the tests that read it rather than skipping them.
        return Path(named)
    cache = Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo") / "registry" / "cache"
    found = sorted(cache.glob(f"*/sqlx-core-{VERSION}.crate"))
    return found[0] if found else None


REAL_CRATE = real_upstream_crate()
NEEDS_REAL_CRATE = unittest.skipIf(
    REAL_CRATE is None, f"no upstream sqlx-core-{VERSION}.crate: set {UPSTREAM_CRATE_ENV}"
)


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
) -> Path:
    """Upstream's .crate, and a checkout whose vendored copy is it plus TLS_PATCH; the crate."""
    crate = root / f"sqlx-core-{VERSION}.crate"
    crate_sha256 = write_crate(crate, UPSTREAM)
    base = root / "vendor" / "sqlx-core"
    for name, text in {**UPSTREAM, TLS: PATCHED_TLS}.items():
        (base / name).parent.mkdir(parents=True, exist_ok=True)
        (base / name).write_text(text, encoding="utf-8")
    (base / "Cargo.toml").write_text(
        f'[package]\nname = "sqlx-core"\nversion = "{vendored}"\n', encoding="utf-8"
    )
    (base / "README.md").write_text(readme(crate_sha256), encoding="utf-8")
    (base / "reviewed-advisories.toml").write_text(reviewed, encoding="utf-8")
    (root / PATCHES).mkdir()
    (root / PATCHES / "0001-verify-ca.patch").write_text(TLS_PATCH, encoding="utf-8")
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
    return crate


def copy_real_checkout(root: Path) -> None:
    shutil.copytree(ROOT / "vendor", root / "vendor", symlinks=True)
    for name in ("Cargo.toml", "Cargo.lock", "deny.toml", f"{FUZZ.as_posix()}/Cargo.toml", f"{FUZZ.as_posix()}/Cargo.lock"):
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / name, root / name)


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
        self.assertIn("skipped the source check: no sqlx-core-0.8.6.crate in Cargo's cache", result.stdout)

    def test_the_tree_must_be_upstream_plus_the_recorded_patches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root)
            result = run_check(root, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            f"vendor/sqlx-core is upstream sqlx-core {VERSION} plus the 1 patch(es) in "
            f"{PATCHES.as_posix()}, byte for byte: 1 of its 4 files patched, and README.md and "
            "reviewed-advisories.toml added",
            result.stdout,
        )
        base = Path("vendor") / "sqlx-core"
        not_patched = "is not upstream's with the recorded patches"
        not_added = "is not upstream's: only README.md and reviewed-advisories.toml may be added"
        cases = {
            "an upstream file edited": (
                lambda root: (root / base / "src" / "lib.rs").write_text("pub mod other;\n", encoding="utf-8"),
                f"{base.as_posix()}/src/lib.rs {not_patched}",
            ),
            # The edit #825's review made: the digest of a patched file was
            # all that recorded it, and updating it passed.
            "a patched file edited beyond its patch": (
                lambda root: (root / base / TLS).write_text(UPSTREAM[TLS], encoding="utf-8"),
                f"{base.as_posix()}/{TLS} {not_patched}",
            ),
            "the patch removed": (
                lambda root: (root / PATCHES / "0001-verify-ca.patch").unlink(),
                f"{base.as_posix()}/{TLS} {not_patched}",
            ),
            "a source file added": (
                lambda root: (root / base / "src" / "extra.rs").write_text("\n", encoding="utf-8"),
                f"{base.as_posix()}/src/extra.rs {not_added}",
            ),
            "a build script added": (
                lambda root: (root / base / "build.rs").write_text("fn main() {}\n", encoding="utf-8"),
                f"{base.as_posix()}/build.rs {not_added}",
            ),
            "the registry marker": (
                lambda root: (root / base / ".cargo-ok").write_text('{"v":1}', encoding="utf-8"),
                f"{base.as_posix()}/.cargo-ok {not_added}",
            ),
            "an upstream file missing": (
                lambda root: (root / base / "LICENSE-MIT").unlink(),
                f"{base.as_posix()}/LICENSE-MIT is missing: upstream's .crate has it",
            ),
            "a symbolic link": (
                lambda root: (root / base / "src" / "linked.rs").symlink_to(root / base / "src" / "lib.rs"),
                f"{base.as_posix()}/src/linked.rs is a symbolic link",
            ),
        }
        for name, (change, message) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                crate = write_tree(root)
                change(root)
                result = run_check(root, "--upstream-crate", str(crate))
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)
                self.assertNotIn("use vendor/sqlx-core", result.stdout)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root)
            (root / base / "README.md").write_text(readme("ef" * 32), encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"not the {'ef' * 32} vendor/sqlx-core/README.md records", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            # The README and the advisory review list change no code.
            root = Path(tmp)
            crate = write_tree(root)
            readme_path = root / base / "README.md"
            readme_path.write_text(readme_path.read_text(encoding="utf-8") + "\nMore notes.\n", encoding="utf-8")
            (root / base / "reviewed-advisories.toml").write_text(
                '[reviewed]\n"RUSTSEC-2026-0101" = "not reachable here"\n', encoding="utf-8"
            )
            result = run_check(root, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_change_passes_once_recorded_as_a_patch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root)
            tls = root / "vendor" / "sqlx-core" / TLS
            tls.write_text(PATCHED_TLS + "// verify-full keeps the public roots\n", encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(f"record an intended change as a patch in {PATCHES.as_posix()}", result.stderr)
            diff = run_check(root, "--upstream-crate", str(crate), "--unrecorded-diff")
            self.assertEqual(diff.returncode, 0, diff.stderr)
            self.assertEqual(
                diff.stdout,
                f"--- a/{TLS}\n+++ b/{TLS}\n@@ -1,3 +1,4 @@\n fn roots() {{\n"
                "     RootCertStore::empty()\n }\n+// verify-full keeps the public roots\n",
            )
            (root / PATCHES / "0002-note.patch").write_text("A note.\n\n" + diff.stdout, encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(
                "vendor/sqlx-core/README.md does not name `0002-note.patch`: describe each patch",
                result.stderr,
            )
            # The patch exists, so the hint to record one would mislead.
            self.assertNotIn("record an intended change", result.stderr)
            self.assertIn("1 problem(s)", result.stderr)
            record_patch(root, "0002-note.patch", "A note.\n\n" + diff.stdout)
            result = run_check(root, "--upstream-crate", str(crate))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("plus the 2 patch(es)", result.stdout)
            empty = run_check(root, "--upstream-crate", str(crate), "--unrecorded-diff")
            self.assertEqual((empty.returncode, empty.stdout), (0, ""))
            # A last line without a newline, on either side.
            (root / PATCHES / "0002-note.patch").unlink()
            tls.write_text(PATCHED_TLS + "// no newline", encoding="utf-8")
            diff = run_check(root, "--upstream-crate", str(crate), "--unrecorded-diff")
            self.assertIn("+// no newline\n\\ No newline at end of file\n", diff.stdout)
            record_patch(root, "0002-note.patch", diff.stdout)
            self.assertEqual(run_check(root, "--upstream-crate", str(crate)).returncode, 0)
            tls.write_text(PATCHED_TLS + "// a newline now\n", encoding="utf-8")
            diff = run_check(root, "--upstream-crate", str(crate), "--unrecorded-diff")
            self.assertIn("-// no newline\n\\ No newline at end of file\n+// a newline now\n", diff.stdout)
            record_patch(root, "0003-newline.patch", diff.stdout)
            result = run_check(root, "--upstream-crate", str(crate))
            self.assertEqual(result.returncode, 0, result.stderr)
            (root / "vendor" / "sqlx-core" / "src" / "extra.rs").write_text("\n", encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate), "--unrecorded-diff")
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("src/extra.rs added or removed: a patch only changes upstream's files", result.stderr)
            result = run_check(root, "--unrecorded-diff")
            self.assertEqual(result.returncode, 2, result.stdout)
            self.assertIn("--unrecorded-diff needs --upstream-crate", result.stderr)

    def test_a_patch_applies_only_exactly_as_written(self) -> None:
        header = f"--- a/{TLS}\n+++ b/{TLS}\n"
        body = " fn roots() {\n-    public_roots()\n+    RootCertStore::empty()\n }\n"
        label = f"{PATCHES.as_posix()}/0001-verify-ca.patch"
        cases = {
            "at another line": (
                header + "@@ -2,3 +2,3 @@\n" + body,
                f"{label}:3: the hunk does not apply to {TLS} exactly at its line 2",
            ),
            "on other context": (
                header + "@@ -1,3 +1,3 @@\n" + body.replace(" fn roots", " fn root"),
                f"{label}:3: the hunk does not apply to {TLS} exactly at its line 1",
            ),
            "with a new start that disagrees": (
                header + "@@ -1,3 +2,3 @@\n" + body,
                f"{label}:3: the hunk does not apply",
            ),
            "creating a file": (
                "--- /dev/null\n+++ b/src/new.rs\n@@ -0,0 +1 @@\n+x\n",
                f"{label}:1: expected `--- a/<path>` then `+++ b/<path>` for one file, without timestamps",
            ),
            "on a file upstream lacks": (
                "--- a/src/new.rs\n+++ b/src/new.rs\n@@ -0,0 +1 @@\n+x\n",
                f"{label} changes src/new.rs, which upstream's .crate lacks",
            ),
            "outside the crate": (
                "--- a/../Cargo.toml\n+++ b/../Cargo.toml\n@@ -0,0 +1 @@\n+x\n",
                f"{label} changes ../Cargo.toml, which upstream's .crate lacks",
            ),
            "with timestamps": (
                f"--- a/{TLS}\t2026-11-03\n+++ b/{TLS}\t2026-11-03\n@@ -1,3 +1,3 @@\n" + body,
                f"{label}:1: expected `--- a/<path>`",
            ),
            "naming two files": (
                f"--- a/{TLS}\n+++ b/src/lib.rs\n@@ -1,3 +1,3 @@\n" + body,
                f"{label}:1: expected `--- a/<path>`",
            ),
            "with a short hunk": (
                header + "@@ -1,4 +1,4 @@\n" + body,
                f"{label}:3: the patch ends inside this hunk",
            ),
            "with more lines than its header counts": (
                header + "@@ -1,2 +1,2 @@\n" + body,
                f"{label}:7: expected `--- a/<path>`",
            ),
            "with counts that disagree": (
                header + "@@ -1,3 +1,2 @@\n" + body,
                f"{label}:3: the hunk's lines do not match its header's counts",
            ),
            "with text between hunks": (
                header + "@@ -1,3 +1,3 @@\n" + body + "unreviewed text\n",
                f"{label}:8: expected `--- a/<path>`",
            ),
            "with a line that is no hunk line": (
                header + "@@ -1,3 +1,3 @@\n fn roots() {\n*    public_roots()\n",
                f"{label}:5: expected a ` `, `-` or `+` line in the hunk",
            ),
            "with a malformed hunk header": (
                header + "@@ -1,3 +1,3\n" + body,
                f"{label}:3: malformed hunk header",
            ),
            "with a hunk before any file": (
                "@@ -1,3 +1,3 @@\n" + header + body,
                f"{label}:1: a file or hunk header before the first `--- a/<path>` line",
            ),
            "without hunks": (header, f"{label}:3: expected a hunk for {TLS}"),
            "naming a file twice": (
                header + "@@ -1,3 +1,3 @@\n" + body + header + "@@ -1,3 +1,3 @@\n" + body,
                f"{label}:8: {TLS} has a second header",
            ),
            "without a last newline": (TLS_PATCH[:-1], f"{label} does not end with a newline"),
            "changing nothing": ("A description only.\n", f"{label} changes no file"),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                crate = write_tree(root)
                (root / PATCHES / "0001-verify-ca.patch").write_text(text, encoding="utf-8")
                result = run_check(root, "--upstream-crate", str(crate))
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)
        # Each of these would pass if the checker applied it loosely: the tree
        # matches what a lenient applier makes of it.
        loose = {
            # A no-newline marker mid-file joins two lines that review reads
            # as separate, so a line can hide in the one before it.
            "joining lines with a no-newline marker": (
                "@@ -1,3 +1,4 @@\n fn roots() {\n-    RootCertStore::empty()\n"
                "+    public_roots() // verify-full\n\\ No newline at end of file\n"
                "+    ; RootCertStore::empty()\n }\n",
                "fn roots() {\n    public_roots() // verify-full    ; RootCertStore::empty()\n}\n",
                f"{PATCHES.as_posix()}/0002-loose.patch: {TLS}'s line 2 would end without a "
                "newline, joined to the next",
            ),
            "inserting past the end": (
                "@@ -500,0 +501 @@\n+// appended\n",
                PATCHED_TLS + "// appended\n",
                f"{PATCHES.as_posix()}/0002-loose.patch:3: the hunk does not apply to {TLS} "
                "exactly at its line 500",
            ),
        }
        for name, (hunk, vendored, message) in loose.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                crate = write_tree(root)
                record_patch(root, "0002-loose.patch", f"--- a/{TLS}\n+++ b/{TLS}\n" + hunk)
                (root / "vendor" / "sqlx-core" / TLS).write_text(vendored, encoding="utf-8")
                result = run_check(root, "--upstream-crate", str(crate))
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            # Inserting after the last line is inserting at the line it names.
            root = Path(tmp)
            crate = write_tree(root)
            record_patch(root, "0002-append.patch", f"--- a/{TLS}\n+++ b/{TLS}\n@@ -3,0 +4 @@\n+// appended\n")
            (root / "vendor" / "sqlx-core" / TLS).write_text(PATCHED_TLS + "// appended\n", encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate))
            self.assertEqual(result.returncode, 0, result.stderr)
        strays = {
            "notes.txt": lambda path: path.write_text("notes\n", encoding="utf-8"),
            "0002-Upper.patch": lambda path: path.write_text(TLS_PATCH, encoding="utf-8"),
            "0002-linked.patch": lambda path: path.symlink_to(path.parent / "0001-verify-ca.patch"),
            "0002-directory.patch": lambda path: path.mkdir(),
        }
        for name, make in strays.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                crate = write_tree(root)
                make(root / PATCHES / name)
                result = run_check(root, "--upstream-crate", str(crate))
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(f"{PATCHES.as_posix()} holds {name}: it holds only regular files", result.stderr)

    def test_the_readme_pins_one_crate_and_the_crate_must_be_the_vendored_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root)
            path = root / "vendor" / "sqlx-core" / "README.md"
            text = path.read_text(encoding="utf-8")
            path.write_text(f"A newer .crate's SHA-256 is `{'ef' * 32}`.\n\n" + text, encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate))
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("README.md records 2 upstream .crate SHA-256s, not one", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root, vendored="0.8.7")
            result = run_check(root, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(
            "holds 'sqlx-core-0.8.6/Cargo.toml', outside sqlx-core-0.8.7/: it is not the .crate of "
            "sqlx-core 0.8.7, the version vendor/sqlx-core/Cargo.toml names",
            result.stderr,
        )

    def test_the_check_uses_cargos_cached_crate_and_names_every_patch_without_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root)
            cache = root / "home" / ".cargo" / "registry" / "cache" / "index.crates.io-test"
            cache.mkdir(parents=True)
            shutil.copy(crate, cache / crate.name)
            (root / "vendor" / "sqlx-core" / "src" / "lib.rs").write_text("pub mod other;\n", encoding="utf-8")
            result = run_check(root)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("vendor/sqlx-core/src/lib.rs is not upstream's with the recorded patches", result.stderr)
            (cache / crate.name).unlink()
            result = run_check(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("skipped the source check: no sqlx-core-0.8.6.crate in Cargo's cache", result.stdout)
            (root / PATCHES / "0002-unnamed.patch").write_text(TLS_PATCH, encoding="utf-8")
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("README.md does not name `0002-unnamed.patch`", result.stderr)

    def test_a_broken_workspace_manifest_hides_no_vendored_file_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            crate = write_tree(root)
            (root / FUZZ / "Cargo.toml").write_text("[package\n", encoding="utf-8")
            (root / "vendor" / "sqlx-core" / TLS).write_text("// edited\n", encoding="utf-8")
            result = run_check(root, "--upstream-crate", str(crate))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"{FUZZ.as_posix()}/Cargo.toml is not valid TOML", result.stderr)
        self.assertIn(f"vendor/sqlx-core/{TLS} is not upstream's with the recorded patches", result.stderr)

    @NEEDS_REAL_CRATE
    def test_the_real_tree_is_upstream_plus_the_recorded_patches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_check(ROOT, "--advisory-db", tmp, "--upstream-crate", str(REAL_CRATE))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            f"vendor/sqlx-core is upstream sqlx-core {VERSION} plus the 3 patch(es) in "
            f"{PATCHES.as_posix()}, byte for byte: 4 of its 100 files patched, and README.md and "
            "reviewed-advisories.toml added",
            result.stdout,
        )

    @NEEDS_REAL_CRATE
    def test_an_edit_to_the_real_vendored_tls_source_fails_until_a_patch_records_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            copy_real_checkout(root)
            check = ("--upstream-crate", str(REAL_CRATE))
            self.assertEqual(run_check(root, *check).returncode, 0)
            tls = root / "vendor" / "sqlx-core" / TLS
            source = tls.read_text(encoding="utf-8")
            trusting = (
                "        let mut cert_store = if verify_ca {\n"
                "            RootCertStore::empty()\n"
                "        } else {\n"
                "            public_roots()\n"
                "        };\n"
            )
            self.assertIn(trusting, source)
            tls.write_text(
                source.replace(trusting, "        let mut cert_store = public_roots();\n", 1),
                encoding="utf-8",
            )
            result = run_check(root, *check)
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(f"vendor/sqlx-core/{TLS} is not upstream's with the recorded patches", result.stderr)
            # It passes only once a patch, which review reads, says so.
            diff = run_check(root, *check, "--unrecorded-diff").stdout
            self.assertIn("-            RootCertStore::empty()\n", diff)
            self.assertIn("+        let mut cert_store = public_roots();\n", diff)
            record_patch(root, "0004-trust-the-public-roots.patch", diff)
            self.assertEqual(run_check(root, *check).returncode, 0)

    def test_ci_checks_the_source_against_upstream_and_reviews_advisories_after_cargo_deny(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")

        def job(name: str) -> tuple[int, int]:
            start = workflow.index(f"\n  {name}:\n")
            following = re.compile(r"\n  [a-z0-9-]+:\n").search(workflow, start + 1)
            return start, following.start() if following else len(workflow)

        url = f"https://static.crates.io/crates/sqlx-core/sqlx-core-{VERSION}.crate"
        start, end = job("lint-compile-compose")
        check = workflow.index(
            'python3 scripts/check_vendored_sqlx_core.py --upstream-crate "${crate}"', start, end
        )
        self.assertLess(workflow.index(url, start, end), check)
        start, end = job("python-tests")
        shard = workflow.index("run_python_test_shard.py", start, end)
        self.assertLess(workflow.index(url, start, end), shard)
        self.assertIn(
            f"{UPSTREAM_CRATE_ENV}: ${{{{ runner.temp }}}}/sqlx-core-{VERSION}.crate",
            workflow[start:shard],
        )
        start, end = job("cargo-deny")
        deny = workflow.index("run: cargo deny --locked check", start, end)
        review = workflow.index(
            "run: python3 scripts/check_vendored_sqlx_core.py --require-advisory-db", start, end
        )
        self.assertLess(deny, review)

if __name__ == "__main__":
    unittest.main()
