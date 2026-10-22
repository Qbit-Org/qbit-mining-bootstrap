#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_toolchain_pin.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PIN = "1.98.1"
DIGEST = "9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa"


def write_tree(
    root: Path,
    *,
    channel: str = PIN,
    profile: str = "minimal",
    components: str = '["rustfmt", "clippy"]',
    rust_version: str | None = PIN,
    crates: dict[str, str] | None = None,
    builder: str | None = None,
) -> None:
    (root / "rust-toolchain.toml").write_text(
        "[toolchain]\n"
        f'channel = "{channel}"\n'
        f'profile = "{profile}"\n'
        f"components = {components}\n",
        encoding="utf-8",
    )
    crates = {"alpha": "rust-version.workspace = true"} if crates is None else crates
    members = "".join(f'    "crates/{name}",\n' for name in crates)
    version_line = "" if rust_version is None else f'rust-version = "{rust_version}"\n'
    (root / "Cargo.toml").write_text(
        f"[workspace]\nmembers = [\n{members}]\n\n[workspace.package]\nversion = \"3.0.0\"\n{version_line}",
        encoding="utf-8",
    )
    for name, line in crates.items():
        crate = root / "crates" / name
        crate.mkdir(parents=True)
        (crate / "Cargo.toml").write_text(
            f'[package]\nname = "{name}"\nversion.workspace = true\n{line}\n', encoding="utf-8"
        )
    dockerfile = root / "lab" / "prism" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    builder = f"FROM rust:{PIN}-bookworm@sha256:{DIGEST} AS build" if builder is None else builder
    dockerfile.write_text(
        f"# syntax=docker/dockerfile:1.7\n\n{builder}\nWORKDIR /build\n\nFROM debian:bookworm-slim\n",
        encoding="utf-8",
    )


def run_check(root: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    merged.update(env or {})
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), *args],
        cwd=ROOT,
        env=merged,
        text=True,
        capture_output=True,
        check=False,
    )


def fake_tool(directory: Path, name: str, version_line: str) -> None:
    path = directory / name
    path.write_text(f"#!/bin/sh\nprintf '%s\\n' '{version_line}'\n", encoding="utf-8")
    path.chmod(0o755)


class CheckToolchainPinTests(unittest.TestCase):
    def test_an_agreeing_tree_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            result = run_check(root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"Rust {PIN} is pinned consistently", result.stdout)

    def test_print_pin_prints_only_the_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, rust_version="9.9.9")
            result = run_check(root, "--print-pin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, f"{PIN}\n")

    def test_rust_version_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, rust_version="1.97.0")
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(f"rust-version is '1.97.0' but rust-toolchain.toml pins {PIN}", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, rust_version=None)
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("rust-version is None", result.stderr)

    def test_a_crate_that_does_not_inherit_rust_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root, crates={"alpha": "rust-version.workspace = true", "beta": f'rust-version = "{PIN}"', "gamma": ""})
            result = run_check(root)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("crates/beta/Cargo.toml: rust-version must be inherited", result.stderr)
        self.assertIn("crates/gamma/Cargo.toml: rust-version must be inherited", result.stderr)
        self.assertNotIn("crates/alpha", result.stderr)

    def test_dockerfile_builder_drift_fails(self) -> None:
        cases = {
            f"FROM rust:1.97.0-bookworm@sha256:{DIGEST} AS build": f"rust:1.97.0-bookworm but rust-toolchain.toml pins {PIN}",
            f"FROM rust:{PIN}-bookworm AS build": "expected exactly one",
            f"FROM rust:{PIN}-bookworm@sha256:abc AS build": "digest must be 64 hex characters",
            "FROM rust:1-bookworm AS build": "expected exactly one",
        }
        for builder, message in cases.items():
            with self.subTest(builder=builder), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                write_tree(root, builder=builder)
                result = run_check(root)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(message, result.stderr)

    def test_the_toolchain_file_must_pin_an_exact_stable_with_the_components(self) -> None:
        cases = [
            dict(channel="stable"),
            dict(channel="1.98"),
            dict(profile="default"),
            dict(components='["rustfmt"]'),
        ]
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                write_tree(root, **case)
                result = run_check(root)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn("rust-toolchain.toml", result.stderr)
        with tempfile.TemporaryDirectory() as tmp:
            result = run_check(Path(tmp))
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("rust-toolchain.toml is missing", result.stderr)

    def test_installed_compares_rustc_and_cargo_against_the_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_tree(root)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake_tool(bin_dir, "rustc", f"rustc {PIN} (48a229cea 2026-09-01)")
            fake_tool(bin_dir, "cargo", f"cargo {PIN} (797e8a9bc 2026-08-05)")
            path = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
            result = run_check(root, "--installed", env={"PATH": path})
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("installed rustc and cargo", result.stdout)
            fake_tool(bin_dir, "cargo", "cargo 1.99.0 (deadbeef 2026-10-01)")
            result = run_check(root, "--installed", env={"PATH": path})
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn(f"cargo reports 'cargo 1.99.0 (deadbeef 2026-10-01)' but rust-toolchain.toml pins {PIN}", result.stderr)
            self.assertNotIn("rustc reports", result.stderr)
            result = run_check(root, "--installed", env={"PATH": str(root / "nowhere")})
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("rustc is not on PATH", result.stderr)

    def test_the_checked_in_tree_holds_one_pin(self) -> None:
        result = run_check(ROOT)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("lab/prism/Dockerfile", result.stdout)

    def test_ci_uses_the_pin_and_asserts_the_installed_toolchain(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("dtolnay/rust-toolchain@stable", workflow)
        self.assertIn("uses: ./.github/actions/rust-toolchain", workflow)
        action = (ROOT / ".github" / "actions" / "rust-toolchain" / "action.yml").read_text(encoding="utf-8")
        self.assertIn("scripts/check_toolchain_pin.py --installed", action)
        self.assertIn("python3 scripts/check_toolchain_pin.py", workflow)


if __name__ == "__main__":
    unittest.main()
