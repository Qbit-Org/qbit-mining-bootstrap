#!/usr/bin/env python3

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COORDINATOR = ROOT / "lab" / "prism" / "prism_coordinator.py"
SHARE_LEDGER = ROOT / "lab" / "prism" / "share_ledger.py"
RUST_LIBRARY = ROOT / "crates" / "qbit-prism" / "src" / "lib.rs"
LEDGER_SCHEMA = ROOT / "crates" / "qbit-prism" / "sql" / "001_share_ledger.sql"


class PrismRewardWindowInvariantTests(unittest.TestCase):
    def test_reward_window_multiplier_is_synchronized(self) -> None:
        coordinator = COORDINATOR.read_text(encoding="utf-8")
        share_ledger = SHARE_LEDGER.read_text(encoding="utf-8")
        rust_library = RUST_LIBRARY.read_text(encoding="utf-8")
        ledger_schema = LEDGER_SCHEMA.read_text(encoding="utf-8")

        python_multiplier = self._one(
            r"^PRISM_REWARD_WINDOW_MULTIPLIER\s*=\s*(\d+)\s*$",
            coordinator,
            "Python coordinator multiplier",
        )
        rust_multiplier = self._one(
            r"^pub const PRISM_WINDOW_MULTIPLIER:\s*u128\s*=\s*(\d+);\s*$",
            rust_library,
            "Rust multiplier",
        )
        dashboard_multiplier = self._one(
            r"return difficulty \* Decimal\((\d+)\)",
            share_ledger,
            "Python dashboard multiplier",
        )
        dashboard_sql_multiplier = self._one(
            r"audit_network_difficulty::numeric \* (\d+)::numeric",
            share_ledger,
            "Python dashboard SQL multiplier",
        )

        audit_function = self._one_text(
            r"CREATE OR REPLACE FUNCTION qbit_audit_share_window\(.*?\n\$\$;",
            ledger_schema,
            "SQL audit-window function",
        )
        sql_multipliers = [
            self._one(r"SELECT\s+(\d+)::numeric AS window_multiplier", audit_function, "SQL metadata multiplier"),
            self._one(
                r"network_difficulty \* (\d+)::numeric AS requested_window_weight",
                audit_function,
                "SQL requested-weight multiplier",
            ),
            self._one(
                r"qbit_prism_window\(anchor_job_issued_at, network_difficulty \* (\d+)::numeric\)",
                audit_function,
                "SQL selection multiplier",
            ),
        ]

        self.assertEqual(python_multiplier, 8)
        self.assertEqual(
            {
                rust_multiplier,
                dashboard_multiplier,
                dashboard_sql_multiplier,
                *sql_multipliers,
            },
            {python_multiplier},
        )

    def _one(self, pattern: str, source: str, label: str) -> int:
        matches = re.findall(pattern, source, flags=re.MULTILINE | re.DOTALL)
        self.assertEqual(len(matches), 1, f"expected exactly one {label}, found {matches!r}")
        return int(matches[0])

    def _one_text(self, pattern: str, source: str, label: str) -> str:
        matches = re.findall(pattern, source, flags=re.MULTILINE | re.DOTALL)
        self.assertEqual(len(matches), 1, f"expected exactly one {label}, found {len(matches)}")
        return matches[0]


if __name__ == "__main__":
    unittest.main()
