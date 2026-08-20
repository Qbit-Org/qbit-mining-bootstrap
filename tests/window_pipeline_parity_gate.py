#!/usr/bin/env python3
"""Strict CI entry point for the frozen payout-window parity corpus."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

from lab.prism.prism_tools import prism_tool_command
from tests import window_pipeline_parity as parity


PARITY_TEST_TARGET = (
    "tests.test_window_pipeline_parity.WindowPipelineParityOracleTests"
)
RUST_DAEMON_BINARY = "qbit-prism-build-audit-bundle"


def resolve_gate_adapter() -> tuple[parity.WindowPipelineAdapter, tuple[str, ...]]:
    """Resolve the explicitly selected adapter and its required prebuilt tool."""
    requested = os.environ.get(parity.ADAPTER_ENV_VAR)
    if not requested:
        raise RuntimeError(
            f"{parity.ADAPTER_ENV_VAR} must explicitly select a parity adapter"
        )
    adapter = parity.resolve_adapter(requested)
    if adapter.name != requested:
        raise RuntimeError(
            f"resolved parity adapter {adapter.name!r}, expected {requested!r}"
        )

    if requested != "rust-daemon":
        return adapter, ()

    bin_dir = os.environ.get("PRISM_TOOL_BIN_DIR")
    if not bin_dir:
        raise RuntimeError(
            "rust-daemon parity requires PRISM_TOOL_BIN_DIR to select the"
            " prebuilt deployment binary"
        )
    expected = Path(bin_dir) / RUST_DAEMON_BINARY
    command = tuple(prism_tool_command(RUST_DAEMON_BINARY))
    if command != (str(expected),):
        raise RuntimeError(
            f"rust-daemon parity requires executable prebuilt binary {expected};"
            " refusing cargo fallback"
        )
    return adapter, command


def main() -> int:
    try:
        adapter, command = resolve_gate_adapter()
    except (RuntimeError, ValueError) as error:
        print(f"payout-window parity gate: {error}", file=sys.stderr)
        return 2

    corpus = parity.build_corpus()
    advance_cases = sum(bool(case.advances) for case in corpus)
    advance_steps = sum(len(case.advances) for case in corpus)
    print(f"payout-window parity adapter: {adapter.name}", flush=True)
    if command:
        print(f"payout-window parity daemon: {command[0]} --serve", flush=True)
    print(
        "payout-window frozen corpus:"
        f" {len(corpus)} full preparations,"
        f" {advance_steps} advance() calls across {advance_cases} cases",
        flush=True,
    )

    suite = unittest.defaultTestLoader.loadTestsFromName(PARITY_TEST_TARGET)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
