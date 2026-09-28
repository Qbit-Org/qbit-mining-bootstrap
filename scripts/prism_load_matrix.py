#!/usr/bin/env python3
"""Plan the nightly load-harness matrix from the checked-in presets (#521).

Reads `crates/qbit-prism-load/presets/*.json` and prints the GitHub Actions
matrix for a selection:

- `nightly` (the schedule's selection): every preset whose schedule is
  `nightly`;
- `all`: every `nightly` and `manual` preset;
- otherwise a comma-separated list of preset names, any schedule.

Each entry carries the preset's name, runner and timeout, so which machine a
preset runs on and for how long is checked in beside the flags it pins.

Usage: python3 scripts/prism_load_matrix.py <selection> [--presets DIR]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
PRESETS = ROOT / "crates" / "qbit-prism-load" / "presets"
SCHEMA = "qbit.prism.load-preset.v1"


class SelectionError(Exception):
    """A selection the presets cannot satisfy, worded for the workflow log."""


def load(directory: Path) -> dict[str, dict]:
    presets = {}
    for path in sorted(directory.glob("*.json")):
        preset = json.loads(path.read_text(encoding="utf-8"))
        if preset.get("schema") != SCHEMA:
            raise SelectionError(f"{path}: schema is {preset.get('schema')!r}, not {SCHEMA}")
        if preset.get("name") != path.stem:
            raise SelectionError(f"{path}: name {preset.get('name')!r} is not the file's stem")
        presets[path.stem] = preset
    if not presets:
        raise SelectionError(f"no presets in {directory}")
    return presets


def select(presets: dict[str, dict], selection: str) -> list[dict]:
    selection = selection.strip()
    if selection == "nightly":
        names = [name for name, p in presets.items() if p["schedule"] == "nightly"]
    elif selection == "all":
        names = [name for name, p in presets.items() if p["schedule"] in ("nightly", "manual")]
    else:
        names = [name.strip() for name in selection.split(",") if name.strip()]
        unknown = [name for name in names if name not in presets]
        if unknown:
            raise SelectionError(
                f"unknown preset(s) {', '.join(unknown)}; choose from {', '.join(presets)}"
            )
    if not names:
        raise SelectionError(f"selection {selection!r} names no preset")
    return [
        {
            "preset": name,
            "runner": presets[name]["runner"],
            "timeout_minutes": presets[name]["timeout_minutes"],
        }
        for name in sorted(set(names))
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("selection")
    parser.add_argument("--presets", type=Path, default=PRESETS)
    args = parser.parse_args(argv)
    try:
        matrix = {"include": select(load(args.presets), args.selection)}
    except SelectionError as error:
        print(f"prism-load-matrix: {error}", file=sys.stderr)
        return 2
    print(json.dumps(matrix, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
