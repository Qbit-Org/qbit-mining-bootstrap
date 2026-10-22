#!/usr/bin/env python3
"""Plan the nightly load-harness matrix from the checked-in presets (#521).

Reads `crates/qbit-prism-load/presets/*.json` and prints the GitHub Actions
matrix for a selection:

- `nightly` (the nightly schedule's selection): every preset whose schedule
  is `nightly`;
- `weekly` (the weekly schedule's selection): every preset whose schedule is
  `weekly`, the long soak (#575) and the long fault set (#554);
- `all`: every `nightly` and `manual` preset (not the weekly soak, which
  takes most of a runner's six hours);
- `suite:<name>`: a suite from `presets/suites.toml` (#550), which names its
  presets whatever their schedules, how many times each runs and, optionally,
  one runner for all of them;
- otherwise a comma-separated list of preset names, any schedule. A name
  listed in `presets/aliases.txt` (a preset's deprecated name) selects the
  preset it was renamed to, with a warning.

Each entry carries the preset's name, runner and timeout, so which machine a
preset runs on and for how long is checked in beside the flags it pins. A
suite's entries also carry `repeat` (1-based) and `id`, `<preset>-r<repeat>`,
one job per repeat, and the suite's runner where it sets one.

Usage: python3 scripts/prism_load_matrix.py <selection> [--presets DIR]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
PRESETS = ROOT / "crates" / "qbit-prism-load" / "presets"
SCHEMA = "qbit.prism.load-preset.v1"
SUITES_SCHEMA = "qbit.prism.load-suites.v1"
SUITE_KEYS = {"description", "lane", "issues", "repeats", "runner", "presets"}
SUITE_REQUIRED = SUITE_KEYS - {"runner"}
MAX_REPEATS = 5
ISSUE = re.compile(r"^#[1-9][0-9]*$")


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


def load_aliases(directory: Path) -> dict[str, str]:
    """Deprecated preset names, `old new` per line, from `aliases.txt`."""
    path = directory / "aliases.txt"
    aliases = {}
    if not path.exists():
        return aliases
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        fields = line.split()
        if len(fields) != 2:
            raise SelectionError(f"{path}:{number}: {raw!r} is not `old new`")
        aliases[fields[0]] = fields[1]
    return aliases


def load_suites(directory: Path, presets: dict[str, dict]) -> dict[str, dict]:
    """The suites in `suites.toml`, each checked against the presets (#550).

    A suite names presets by their current names only, each once: a
    deprecated name would stop resolving when its alias is dropped, and a
    repeat belongs in `repeats`. An unknown key, a missing one, a schema other
    than this script's or a `repeats` outside 1..5 is refused, so a suite
    never runs other than as written.
    """
    path = directory / "suites.toml"
    if not path.exists():
        return {}
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as error:
        raise SelectionError(f"{path}: {error}") from error
    if data.get("schema") != SUITES_SCHEMA:
        raise SelectionError(f"{path}: schema is {data.get('schema')!r}, not {SUITES_SCHEMA}")
    extra = sorted(set(data) - {"schema", "suites"})
    if extra:
        raise SelectionError(f"{path}: unknown top-level key(s) {', '.join(extra)}")
    suites = data.get("suites")
    if not isinstance(suites, dict) or not suites:
        raise SelectionError(f"{path}: no [suites.<name>] tables")
    for name, suite in suites.items():
        where = f"{path}: suite {name}"
        if not isinstance(suite, dict):
            raise SelectionError(f"{where} is not a table")
        unknown = sorted(set(suite) - SUITE_KEYS)
        if unknown:
            raise SelectionError(f"{where}: unknown key(s) {', '.join(unknown)}")
        missing = sorted(SUITE_REQUIRED - set(suite))
        if missing:
            raise SelectionError(f"{where}: missing {', '.join(missing)}")
        for key in ("description", "lane"):
            if not (isinstance(suite[key], str) and suite[key].strip()):
                raise SelectionError(f"{where}: {key} must be a non-empty string")
        if "runner" in suite and not (
            isinstance(suite["runner"], str) and suite["runner"].startswith("blacksmith-")
        ):
            raise SelectionError(f"{where}: runner must be a blacksmith- runner label")
        issues = suite["issues"]
        if not (isinstance(issues, list) and issues and all(
            isinstance(i, str) and ISSUE.match(i) for i in issues
        )):
            raise SelectionError(f"{where}: issues must be a non-empty list of #<issue>")
        repeats = suite["repeats"]
        if not (type(repeats) is int and 1 <= repeats <= MAX_REPEATS):
            raise SelectionError(f"{where}: repeats must be an integer 1..{MAX_REPEATS}")
        names = suite["presets"]
        if not (isinstance(names, list) and names and all(isinstance(n, str) for n in names)):
            raise SelectionError(f"{where}: presets must be a non-empty list of names")
        if len(set(names)) != len(names):
            raise SelectionError(f"{where}: presets repeat a name; use repeats")
        unknown = [n for n in names if n not in presets]
        if unknown:
            raise SelectionError(f"{where}: unknown preset(s) {', '.join(unknown)}")
    return suites


def select_suite(presets: dict[str, dict], suites: dict[str, dict], name: str) -> list[dict]:
    suite = suites.get(name)
    if suite is None:
        raise SelectionError(
            f"unknown suite {name!r}; choose from {', '.join(suites) or 'none (no suites.toml)'}"
        )
    return [
        {
            "preset": preset,
            "runner": suite.get("runner", presets[preset]["runner"]),
            "timeout_minutes": presets[preset]["timeout_minutes"],
            "repeat": repeat,
            "id": f"{preset}-r{repeat}",
        }
        for preset in suite["presets"]
        for repeat in range(1, suite["repeats"] + 1)
    ]


def select(
    presets: dict[str, dict],
    selection: str,
    aliases: dict[str, str] | None = None,
    suites: dict[str, dict] | None = None,
) -> list[dict]:
    selection = selection.strip()
    if selection.startswith("suite:"):
        return select_suite(presets, suites or {}, selection.removeprefix("suite:").strip())
    if selection in ("nightly", "weekly"):
        names = [name for name, p in presets.items() if p["schedule"] == selection]
    elif selection == "all":
        names = [name for name, p in presets.items() if p["schedule"] in ("nightly", "manual")]
    else:
        names = []
        for name in (name.strip() for name in selection.split(",")):
            if not name:
                continue
            if name not in presets and name in (aliases or {}):
                print(
                    f"prism-load-matrix: {name} is deprecated; running {aliases[name]}",
                    file=sys.stderr,
                )
                name = aliases[name]
            names.append(name)
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
        presets = load(args.presets)
        # Only a suite selection reads suites.toml, so the nightly's plan
        # never depends on the L3 suites.
        suites = (
            load_suites(args.presets, presets)
            if args.selection.strip().startswith("suite:")
            else {}
        )
        matrix = {
            "include": select(presets, args.selection, load_aliases(args.presets), suites)
        }
    except SelectionError as error:
        print(f"prism-load-matrix: {error}", file=sys.stderr)
        return 2
    print(json.dumps(matrix, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
