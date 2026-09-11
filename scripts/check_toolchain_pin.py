#!/usr/bin/env python3
"""Fail when the Rust toolchain pin is held in more than one version.

The pin lives in `rust-toolchain.toml` (`[toolchain].channel`, an exact stable
version). Two other places must agree with it: `[workspace.package].rust-version`
in the root `Cargo.toml`, which every workspace crate must inherit with
`rust-version.workspace = true`, and the builder stage of
`lab/prism/Dockerfile`, whose base image is `rust:<pin>-bookworm@sha256:<digest>`.

Usage: python3 scripts/check_toolchain_pin.py [--root DIR] [--installed] [--print-pin]

`--installed` additionally runs `rustc --version` and `cargo --version` and
fails unless both report the pinned version, so a CI job proves it is on the
pin rather than on whatever `stable` resolves to that day. `--print-pin`
prints the pinned version and nothing else, for shell steps that compare it
against something. Exits 0 when everything agrees and 1 otherwise, naming
each disagreement.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-toolchain-pin"
TOOLCHAIN_FILE = "rust-toolchain.toml"
DOCKERFILE = Path("lab") / "prism" / "Dockerfile"
REQUIRED_COMPONENTS = ("rustfmt", "clippy")
EXACT_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
BUILDER_FROM = re.compile(
    r"^FROM\s+rust:(?P<version>[^\s@-]+)-bookworm@sha256:(?P<digest>[0-9a-f]+)\s+AS\s+build\s*$"
)


class PinError(Exception):
    """One disagreement, worded for whoever reads the CI log."""


def load_toml(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError:
        raise PinError(f"{path} is missing") from None
    except tomllib.TOMLDecodeError as error:
        raise PinError(f"{path} is not valid TOML: {error}") from None


def pinned_version(root: Path) -> str:
    data = load_toml(root / TOOLCHAIN_FILE)
    toolchain = data.get("toolchain")
    if not isinstance(toolchain, dict):
        raise PinError(f"{TOOLCHAIN_FILE} has no [toolchain] table")
    channel = toolchain.get("channel")
    if not isinstance(channel, str) or not EXACT_VERSION.match(channel):
        raise PinError(
            f"{TOOLCHAIN_FILE}: channel must be an exact stable version such as "
            f"1.98.1, got {channel!r}"
        )
    if toolchain.get("profile") != "minimal":
        raise PinError(f"{TOOLCHAIN_FILE}: profile must be \"minimal\", got {toolchain.get('profile')!r}")
    components = toolchain.get("components")
    if not isinstance(components, list) or any(
        name not in components for name in REQUIRED_COMPONENTS
    ):
        raise PinError(
            f"{TOOLCHAIN_FILE}: components must include {' and '.join(REQUIRED_COMPONENTS)}, "
            f"got {components!r}"
        )
    return channel


def disagreements(root: Path, pin: str) -> list[str]:
    problems = []
    workspace = load_toml(root / "Cargo.toml")
    package = workspace.get("workspace", {}).get("package", {})
    rust_version = package.get("rust-version")
    if rust_version != pin:
        problems.append(
            f"Cargo.toml: [workspace.package].rust-version is {rust_version!r} but "
            f"{TOOLCHAIN_FILE} pins {pin}"
        )
    members = workspace.get("workspace", {}).get("members", [])
    if not members:
        problems.append("Cargo.toml: [workspace].members is empty")
    for member in members:
        manifest = root / member / "Cargo.toml"
        try:
            crate = load_toml(manifest)
        except PinError as error:
            problems.append(str(error))
            continue
        setting = crate.get("package", {}).get("rust-version")
        if setting != {"workspace": True}:
            problems.append(
                f"{member}/Cargo.toml: rust-version must be inherited with "
                f"`rust-version.workspace = true`, got {setting!r}"
            )
    dockerfile = root / DOCKERFILE
    try:
        lines = dockerfile.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        problems.append(f"{DOCKERFILE.as_posix()} is missing")
        return problems
    builders = [BUILDER_FROM.match(line) for line in lines if line.startswith("FROM ")]
    builders = [match for match in builders if match]
    if len(builders) != 1:
        problems.append(
            f"{DOCKERFILE.as_posix()}: expected exactly one "
            "`FROM rust:<version>-bookworm@sha256:<digest> AS build` line, "
            f"found {len(builders)}"
        )
    else:
        version = builders[0].group("version")
        digest = builders[0].group("digest")
        if version != pin:
            problems.append(
                f"{DOCKERFILE.as_posix()}: builder image is rust:{version}-bookworm but "
                f"{TOOLCHAIN_FILE} pins {pin}"
            )
        if len(digest) != 64:
            problems.append(
                f"{DOCKERFILE.as_posix()}: builder image digest must be 64 hex characters, "
                f"got {len(digest)}"
            )
    return problems


def installed_versions(pin: str) -> list[str]:
    problems = []
    for tool in ("rustc", "cargo"):
        binary = os.environ.get(tool.upper(), tool)
        if shutil.which(binary) is None:
            problems.append(f"{binary} is not on PATH")
            continue
        completed = subprocess.run(
            [binary, "--version"], text=True, capture_output=True, check=False
        )
        if completed.returncode != 0:
            problems.append(
                f"{binary} --version exited with status {completed.returncode}: "
                f"{completed.stderr.strip()}"
            )
            continue
        words = completed.stdout.split()
        reported = words[1] if len(words) > 1 else ""
        if reported != pin:
            problems.append(
                f"{tool} reports {completed.stdout.strip()!r} but {TOOLCHAIN_FILE} pins {pin}"
            )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, default=ROOT, help="checkout to check (default: this checkout)")
    parser.add_argument(
        "--installed",
        action="store_true",
        help="also require rustc and cargo on PATH to report the pinned version",
    )
    parser.add_argument(
        "--print-pin", action="store_true", help="print the pinned version and exit"
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    try:
        pin = pinned_version(root)
    except PinError as error:
        print(f"{PREFIX}: {error}", file=sys.stderr)
        return 1
    if args.print_pin:
        print(pin)
        return 0

    problems = disagreements(root, pin)
    if args.installed:
        problems += installed_versions(pin)
    for problem in problems:
        print(f"{PREFIX}: {problem}", file=sys.stderr)
    if problems:
        print(
            f"{PREFIX}: {len(problems)} disagreement(s) with the {pin} pin in "
            f"{TOOLCHAIN_FILE}",
            file=sys.stderr,
        )
        return 1
    where = f"{TOOLCHAIN_FILE}, Cargo.toml rust-version, every crate, {DOCKERFILE.as_posix()}"
    if args.installed:
        where += ", installed rustc and cargo"
    print(f"{PREFIX}: Rust {pin} is pinned consistently across {where}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
