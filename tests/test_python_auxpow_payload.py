#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "examples" / "python-auxpow-payload.py"
SPEC = importlib.util.spec_from_file_location("python_auxpow_payload", SCRIPT_PATH)
assert SPEC is not None
payload_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(payload_module)


class PythonAuxpowPayloadTests(unittest.TestCase):
    def test_template_commitment_order_accepts_internal_and_display(self) -> None:
        self.assertEqual(payload_module.template_commitment_order({"commitmentorder": "internal"}), "internal")
        self.assertEqual(payload_module.template_commitment_order({"commitmentorder": "display"}), "display")

    def test_template_commitment_order_is_optional_for_legacy_templates(self) -> None:
        self.assertIsNone(payload_module.template_commitment_order({}))

    def test_template_commitment_order_rejects_unknown_values(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            payload_module.template_commitment_order({"commitmentorder": "sideways"})

        self.assertIn("unsupported commitmentorder", str(raised.exception))

    def test_requires_activation_aware_helper_when_template_exposes_commitment_order(self) -> None:
        def helper_without_commitment_order(template: dict[str, object], *, parent_time: int = 0, nonce: int = 0) -> object:
            return object()

        with self.assertRaises(SystemExit) as raised:
            payload_module.require_commitment_order_helper(
                helper_without_commitment_order,
                Path("/tmp/qbit-src"),
            )

        self.assertIn("does not support createauxblock.commitmentorder", str(raised.exception))

    def test_allows_activation_aware_helper(self) -> None:
        def helper_with_commitment_order(
            template: dict[str, object],
            *,
            parent_time: int = 0,
            nonce: int = 0,
            commitment_order: str | None = None,
        ) -> object:
            return object()

        payload_module.require_commitment_order_helper(helper_with_commitment_order, Path("/tmp/qbit-src"))

    def test_allows_helper_with_keyword_arguments(self) -> None:
        def helper_with_kwargs(template: dict[str, object], **kwargs: object) -> object:
            return object()

        payload_module.require_commitment_order_helper(helper_with_kwargs, Path("/tmp/qbit-src"))


if __name__ == "__main__":
    unittest.main()
