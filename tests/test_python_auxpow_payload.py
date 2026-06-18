#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "examples" / "python-auxpow-payload.py"
SPEC = importlib.util.spec_from_file_location("python_auxpow_payload", SCRIPT_PATH)
assert SPEC is not None
payload_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(payload_module)


class PythonAuxpowPayloadTests(unittest.TestCase):
    def write_helper(self, qbit_src: Path, source: str) -> None:
        helper_dir = qbit_src / "test" / "functional" / "test_framework"
        helper_dir.mkdir(parents=True)
        (helper_dir / "auxpow.py").write_text(source, encoding="utf-8")

    def test_rejects_known_old_qbit_helper_commitment_byte_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            qbit_src = Path(tmp)
            self.write_helper(
                qbit_src,
                "commitment = MERGED_MINING_HEADER + ser_uint256(chain_merkle_root) + b'footer'\n",
            )

            with self.assertRaises(SystemExit) as raised:
                payload_module.require_standard_auxpow_helper(qbit_src)

        self.assertIn("internal little-endian uint256 bytes", str(raised.exception))

    def test_allows_fixed_qbit_helper_commitment_byte_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            qbit_src = Path(tmp)
            self.write_helper(
                qbit_src,
                "commitment = MERGED_MINING_HEADER + ser_uint256(chain_merkle_root)[::-1] + b'footer'\n",
            )

            payload_module.require_standard_auxpow_helper(qbit_src)

    def test_helper_guard_ignores_comments_and_strings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            qbit_src = Path(tmp)
            self.write_helper(
                qbit_src,
                '''
"""MERGED_MINING_HEADER + ser_uint256(chain_merkle_root) +"""
# MERGED_MINING_HEADER + ser_uint256(chain_merkle_root) +
commitment = b""
''',
            )

            payload_module.require_standard_auxpow_helper(qbit_src)


if __name__ == "__main__":
    unittest.main()
