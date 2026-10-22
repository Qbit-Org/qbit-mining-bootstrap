#!/usr/bin/env python3
"""Write runner-probe-variance.json with scripts/prism_load_probe.py's own
`variance` (#542), from four synthetic 8 vCPU VMs of two runs each at the
regression fixture's fsync cost. tests/test_prism_load_regress.py checks the
checked-in file still equals this output, so it keeps #542's real shape.

Usage: python3 tests/fixtures/prism-load-trend/variance/generate.py [--check]
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

import prism_load_probe as probe  # noqa: E402

PRESET = "throughput-20k-window-1fe"
OUT = HERE / "runner-probe-variance.json"
# Per VM: (rate, ack p50, ack p99) for its two runs. VM 4 is a slower VM, so
# VM-to-VM spread is visible beside the run-to-run spread.
VMS = [
    [(500.0, 9.8, 39.0), (500.0, 10.1, 40.5)],
    [(499.5, 10.0, 40.0), (500.0, 9.9, 41.0)],
    [(500.0, 10.2, 39.5), (499.8, 10.0, 40.2)],
    [(498.0, 11.0, 44.0), (498.5, 11.2, 45.0)],
]


def run(rate: float, p50: float, p99: float) -> dict:
    result = {
        "harness_exit_code": 0, "gate_exit_code": 0, "timed_out": False,
        "wall_seconds": 600.0, "peak_used_mib": 9000.0, "baseline_used_mib": 1000.0,
        "mem_total_mib": 32000.0, "headroom_mib": 23000.0,
        "fsync": {"method": "fdatasync", "ops_per_second": 2000.0, "usecs_per_op": 500.0},
        "provenance": "pass", "seed_seconds": 10.0, "seed_rows": 20000,
        "phases": {"steady_state": {"shortfall": 0, "ack_p50_ms": p50, "ack_p99_ms": p99,
                                    "achieved_rate_shares_per_second": rate}},
        "tip_last_notify_p99_ms": None, "tips_missing_a_session": None,
    }
    result["unmeasured_reason"] = probe.unmeasured_reason(result)
    return result


def document() -> dict:
    rows = [{"schema": probe.ROW_SCHEMA, "kind": "probe", "class": 8, "runner": probe.runner_label(8),
             "target_cache": "sticky-disk", "repeat": repeat, "commit": "f1x7ure",
             "target": {}, "build": {},
             "host": {"block_device": {"write_cache": "write back", "fua": "0", "model": "m"}},
             "runs": {PRESET: [run(*r) for r in runs]}}
            for repeat, runs in enumerate(VMS, start=1)]
    expected = probe.matrices("8", "sticky-disk", "0", PRESET, str(len(VMS)), "2")
    presets, _ = probe.load_presets()
    return probe.variance(rows, expected, presets)


def text() -> str:
    return json.dumps(document(), indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    if sys.argv[1:] == ["--check"]:
        raise SystemExit(0 if OUT.read_text(encoding="utf-8") == text() else 1)
    OUT.write_text(text(), encoding="utf-8")
