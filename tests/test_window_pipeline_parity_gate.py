#!/usr/bin/env python3
"""Fail-closed contracts for the payout-window CI parity entry point."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests import window_pipeline_parity as parity
from tests import window_pipeline_parity_gate as gate


class WindowPipelineParityGateTests(unittest.TestCase):
    def test_adapter_selection_must_be_explicit(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(
                RuntimeError,
                f"{parity.ADAPTER_ENV_VAR} must explicitly select",
            ):
                gate.resolve_gate_adapter()

    def test_python_selection_resolves_the_python_adapter(self) -> None:
        with mock.patch.dict(
            os.environ,
            {parity.ADAPTER_ENV_VAR: "python"},
            clear=True,
        ):
            adapter, command = gate.resolve_gate_adapter()
        self.assertIsInstance(adapter, parity.PythonWindowPipelineAdapter)
        self.assertEqual(adapter.name, "python")
        self.assertEqual(command, ())

    def test_rust_selection_resolves_the_prebuilt_real_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / gate.RUST_DAEMON_BINARY
            binary.touch(mode=0o755)
            binary.chmod(0o755)
            with mock.patch.dict(
                os.environ,
                {
                    parity.ADAPTER_ENV_VAR: "rust-daemon",
                    "PRISM_TOOL_BIN_DIR": tmp,
                },
                clear=True,
            ):
                adapter, command = gate.resolve_gate_adapter()
        self.assertIsInstance(adapter, parity.RustDaemonWindowPipelineAdapter)
        self.assertEqual(adapter.name, "rust-daemon")
        self.assertEqual(command, (str(binary),))

    def test_rust_selection_refuses_a_missing_binary_and_cargo_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
            os.environ,
            {
                parity.ADAPTER_ENV_VAR: "rust-daemon",
                "PRISM_TOOL_BIN_DIR": tmp,
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "refusing cargo fallback"):
                gate.resolve_gate_adapter()

    def test_rust_adapter_refuses_a_protocol_mismatch(self) -> None:
        fake_daemon = Path(__file__).parent / "fixtures" / "fake_serve_builder.py"
        case = parity.build_corpus()[0]
        with mock.patch.object(
            parity,
            "prism_tool_command",
            return_value=[sys.executable, str(fake_daemon)],
        ), mock.patch.dict(
            os.environ,
            {"FAKE_SERVE_BUILDER_PROTOCOL": "1"},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "daemon announced handshake"):
                parity.RustDaemonWindowPipelineAdapter().run(case)


if __name__ == "__main__":
    unittest.main()
