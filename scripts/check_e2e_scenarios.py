#!/usr/bin/env python3
"""Hold the end-to-end and load scenario manifest to what the lanes run (#543).

`test/e2e-scenarios.toml` lists every end-to-end and load scenario (#487's
catalogue, #521's scenarios, #474's), each with its lanes, its owner issue,
its pass criteria and whether it runs today. A scenario that runs cites its
evidence, and the evidence decides its lanes:

- `tests`: `<package>::<binary>::<test path>` ids of gated tests. An id in
  `test/prism-gated-tests.txt` runs in `pr` (the required
  `prism-native-postgres` shards prove it executed); one in
  `test/prism-nightly-gated-tests.txt` runs in `nightly`.
- `presets`: load-harness presets in `crates/qbit-prism-load/presets`. A
  `nightly` preset runs in `nightly`, a `weekly` one (the long soak, #575)
  in `weekly`, a `manual` one in `dispatch`, and a `smoke` preset in `pr`
  through the gated `load_smoke` or `faults` test (`SMOKE_BINARIES`).
- `unit_tests`: `<file>::<fn>` for an ungated `#[test]` or `#[tokio::test]`
  that is not `#[ignore]`d; `cargo test --workspace` runs it in `pr`.
- `lane_checks`: names from `CHECKS` in `scripts/prism_shipped_image_lane.py`,
  the checks lane L6 (#544) holds the shipped images to; they run in `L6`.

This check fails when:

- the manifest is malformed: an unknown key, a missing or empty field, a
  duplicate id, an owner that is not `#<issue>`, or an undefined lane;
- a running scenario cites evidence no lane runs (an id in no gated list, an
  unknown preset, a missing or ignored test, an L6 check the lane does not
  define), or declares lanes other than
  the ones its evidence runs in;
- an unexercised scenario cites evidence, or lacks a reason (#487's
  definition of done: nothing unexercised without a linked issue and a
  reason; the owner is the linked issue);
- a lane runs a scenario the manifest does not name: a gated test in one of
  the end-to-end binaries (`SCENARIO_BINARIES`), an opt-in nightly test, a
  preset, or an L6 check that no running scenario cites;
- a lane the manifest marks as running no longer has the workflow wiring
  that runs it.

CI runs it through tests/test_check_e2e_scenarios.py in the required
python-tests shards.

Usage: python3 scripts/check_e2e_scenarios.py [--manifest test/e2e-scenarios.toml]
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter
from pathlib import Path
import re
import sys
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_gate_manifest import ManifestError, read_expected  # noqa: E402
from prism_load_matrix import SelectionError, load as load_presets  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "check-e2e-scenarios"
DEFAULT_MANIFEST = ROOT / "test" / "e2e-scenarios.toml"
SCHEMA = "qbit.e2e-scenarios.v1"
PR_LIST = Path("test/prism-gated-tests.txt")
NIGHTLY_LIST = Path("test/prism-nightly-gated-tests.txt")
PRESETS = Path("crates/qbit-prism-load/presets")
L6_DRIVER = Path("scripts/prism_shipped_image_lane.py")
# Gated test binaries whose every test is an end-to-end scenario and so must
# be named in the manifest. The rest of the gated list is component tests.
SCENARIO_BINARIES = (
    "qbit-prism-load::faults::",
    "qbit-prism-server::live_regtest::",
    "qbit-prism-load::load_smoke::",
)
PRESET_LANES = {"nightly": "nightly", "weekly": "weekly", "manual": "dispatch", "smoke": "pr"}
# The gated test binaries that run the `smoke` presets in the PR shards:
# `load_smoke` runs `pr-smoke`, and `faults` runs `faults-pr-smoke` (#554).
SMOKE_BINARIES = ("qbit-prism-load::load_smoke::", "qbit-prism-load::faults::")
# The lanes this check can enumerate, and the workflow text that has to be
# present for each to run at all. A lane marked running that is not here needs
# this check taught how to read it.
LANE_WIRING = {
    "pr": (
        ".github/workflows/ci.yml",
        ("--expected test/prism-gated-tests.txt", "scripts/run_rust_test_shard.py"),
    ),
    "nightly": (
        ".github/workflows/prism-load-nightly.yml",
        (
            "cron:",
            "--expected test/prism-nightly-gated-tests.txt",
            "scripts/prism_load_matrix.py",
        ),
    ),
    "dispatch": (
        ".github/workflows/prism-load-nightly.yml",
        ("workflow_dispatch:", "scripts/prism_load_matrix.py"),
    ),
    # The long soak's schedule (#575), whose plan selects the `weekly` presets.
    "weekly": (
        ".github/workflows/prism-load-nightly.yml",
        ('cron: "41 5 * * 6"', "github.event.schedule == '41 5 * * 6' && 'weekly'"),
    ),
    "L6": (
        ".github/workflows/prism-load-nightly.yml",
        ('cron: "43 3 * * 0"', "pull_request:", f"{L6_DRIVER} run"),
    ),
}
LANE_KEYS = {"title", "runs", "owner", "workflow"}
SCENARIO_KEYS = {
    "id", "title", "owner", "lanes", "criteria", "runs", "reason",
    "tests", "presets", "unit_tests", "lane_checks", "notes",
}
EVIDENCE_KEYS = ("tests", "presets", "unit_tests", "lane_checks")
ID = re.compile(r"^[a-z0-9][a-z0-9-]*$")
ISSUE = re.compile(r"^#[1-9][0-9]*$")
UNIT_TEST = re.compile(r"^(crates/[A-Za-z0-9_./-]+\.rs)::([A-Za-z_][A-Za-z0-9_]*)$")
TEST_ATTRIBUTE = re.compile(r"^#\[(tokio::)?test\b")


class Lanes:
    """What each running lane executes, read from the files that drive it."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.pr = read_expected(root / PR_LIST)
        self.nightly = read_expected(root / NIGHTLY_LIST)
        self.presets = load_presets(root / PRESETS)
        self.l6_checks = read_lane_checks(root / L6_DRIVER)

    def test_lanes(self, test_id: str) -> set[str]:
        lanes = set()
        if test_id in self.pr:
            lanes.add("pr")
        if test_id in self.nightly:
            lanes.add("nightly")
        return lanes

    def preset_lanes(self, name: str) -> set[str]:
        preset = self.presets.get(name)
        if preset is None:
            return set()
        lane = PRESET_LANES.get(preset.get("schedule"))
        if lane == "pr" and not any(t.startswith(SMOKE_BINARIES) for t in self.pr):
            return set()
        return {lane} if lane else set()

    def unit_test_problem(self, reference: str) -> str | None:
        """Why `reference` is not a test `cargo test` runs, or None."""
        match = UNIT_TEST.match(reference)
        if not match:
            return f"{reference!r} is not <crates/...rs>::<fn>"
        path = self.root / match.group(1)
        if not path.is_file():
            return f"{match.group(1)} does not exist"
        lines = path.read_text(encoding="utf-8").splitlines()
        definition = re.compile(
            rf"^\s*(pub(\([a-z]+\))?\s+)?(async\s+)?fn\s+{re.escape(match.group(2))}\s*[(<]"
        )
        found = [number for number, line in enumerate(lines) if definition.match(line)]
        if not found:
            return f"{match.group(1)} defines no fn {match.group(2)}"
        for number in found:
            attributes = []
            above = number - 1
            while above >= 0 and lines[above].strip().startswith(("#[", "//")):
                attributes.append(lines[above].strip())
                above -= 1
            if any(a.startswith("#[ignore") for a in attributes):
                return f"{reference} is #[ignore]d, so no lane runs it"
            if any(TEST_ATTRIBUTE.match(a) for a in attributes):
                return None
        return f"{reference} is not a #[test] or #[tokio::test]"


def read_lane_checks(path: Path) -> tuple[str, ...]:
    """The driver's `CHECKS` tuple, read without importing the driver."""
    if not path.is_file():
        return ()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "CHECKS" for target in node.targets
        ):
            value = ast.literal_eval(node.value)
            if not (isinstance(value, tuple) and all(text(item) for item in value)):
                raise ValueError(f"{path}: CHECKS must be a tuple of names")
            return value
    raise ValueError(f"{path} defines no CHECKS")


def text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def string_list(value: object) -> bool:
    return isinstance(value, list) and all(text(item) for item in value)


def check_lanes(manifest: dict, root: Path) -> tuple[dict, list[str]]:
    problems = []
    lanes = manifest.get("lanes")
    if not isinstance(lanes, dict) or not lanes:
        return {}, ["[lanes] is missing or empty"]
    for name, lane in lanes.items():
        where = f"lane {name}"
        if not isinstance(lane, dict):
            problems.append(f"{where} is not a table")
            continue
        for key in sorted(set(lane) - LANE_KEYS):
            problems.append(f"{where}: unknown key {key!r}")
        if not text(lane.get("title")):
            problems.append(f"{where}: title is missing or empty")
        if not isinstance(lane.get("runs"), bool):
            problems.append(f"{where}: runs must be true or false")
            continue
        if "owner" in lane and not (text(lane["owner"]) and ISSUE.match(lane["owner"])):
            problems.append(f"{where}: owner {lane['owner']!r} is not #<issue>")
        if lane["runs"]:
            wiring = LANE_WIRING.get(name)
            if wiring is None:
                problems.append(
                    f"{where} is marked running, but this check cannot enumerate it; "
                    "teach scripts/check_e2e_scenarios.py what it runs"
                )
                continue
            workflow, needles = wiring
            if lane.get("workflow") != workflow:
                problems.append(f"{where}: workflow must be {workflow!r}")
            path = root / workflow
            body = path.read_text(encoding="utf-8") if path.is_file() else ""
            for needle in needles:
                if needle not in body:
                    problems.append(f"{where}: {workflow} no longer contains {needle!r}")
        elif "owner" not in lane:
            problems.append(f"{where} does not run yet and names no owner issue")
    return lanes, problems


def check(manifest: dict, lanes_run: Lanes, root: Path) -> list[str]:
    """Every reason the manifest and the lanes disagree; empty when they agree."""
    problems = []
    if manifest.get("schema") != SCHEMA:
        problems.append(f"schema is {manifest.get('schema')!r}, not {SCHEMA!r}")
    for key in sorted(set(manifest) - {"schema", "lanes", "scenario"}):
        problems.append(f"unknown top-level key {key!r}")
    lanes, lane_problems = check_lanes(manifest, root)
    problems += lane_problems
    scenarios = manifest.get("scenario")
    if not isinstance(scenarios, list) or not scenarios:
        return problems + ["no [[scenario]] entries"]

    ids = Counter(s.get("id") for s in scenarios if isinstance(s, dict))
    for name, count in sorted(ids.items(), key=lambda item: str(item[0])):
        if count > 1:
            problems.append(f"scenario id {name!r} appears {count} times")
    cited: dict[str, set[str]] = {key: set() for key in EVIDENCE_KEYS}

    for index, scenario in enumerate(scenarios, start=1):
        if not isinstance(scenario, dict):
            problems.append(f"scenario {index} is not a table")
            continue
        name = scenario.get("id")
        where = f"scenario {name}" if text(name) else f"scenario {index}"
        if not (text(name) and ID.match(name)):
            problems.append(f"{where}: id {name!r} is not lower-case kebab")
        for key in sorted(set(scenario) - SCENARIO_KEYS):
            problems.append(f"{where}: unknown key {key!r}")
        for key in ("title", "criteria"):
            if not text(scenario.get(key)):
                problems.append(f"{where}: {key} is missing or empty")
        owner = scenario.get("owner")
        if not (text(owner) and ISSUE.match(owner)):
            problems.append(f"{where}: owner {owner!r} is not #<issue>")
        declared = scenario.get("lanes")
        if not (string_list(declared) and declared):
            problems.append(f"{where}: lanes must be a non-empty list of lane names")
            declared = []
        for lane in declared:
            if lane not in lanes:
                problems.append(f"{where}: lane {lane!r} is not defined under [lanes]")
        if len(set(declared)) != len(declared):
            problems.append(f"{where}: lanes repeat")
        evidence = {}
        for key in EVIDENCE_KEYS:
            value = scenario.get(key, [])
            if not string_list(value):
                problems.append(f"{where}: {key} must be a list of non-empty strings")
                value = []
            if len(set(value)) != len(value):
                problems.append(f"{where}: {key} repeats an entry")
            evidence[key] = value
        runs = scenario.get("runs")
        if not isinstance(runs, bool):
            problems.append(f"{where}: runs must be true or false")
            continue

        if not runs:
            if not text(scenario.get("reason")):
                problems.append(f"{where} is unexercised and gives no reason")
            if any(evidence.values()):
                problems.append(
                    f"{where} is unexercised but cites evidence; set runs = true or drop it"
                )
            continue

        if "reason" in scenario:
            problems.append(f"{where} runs; reason is only for an unexercised scenario")
        if not any(evidence.values()):
            problems.append(f"{where} runs but cites no tests, presets, unit_tests or lane_checks")
            continue
        derived: set[str] = set()
        for test_id in evidence["tests"]:
            found = lanes_run.test_lanes(test_id)
            if not found:
                problems.append(
                    f"{where}: {test_id} is in neither {PR_LIST} nor {NIGHTLY_LIST}, "
                    "so no lane runs it"
                )
            derived |= found
        for preset in evidence["presets"]:
            found = lanes_run.preset_lanes(preset)
            if not found:
                problems.append(f"{where}: preset {preset!r} is unknown or runs in no lane")
            derived |= found
        for reference in evidence["unit_tests"]:
            problem = lanes_run.unit_test_problem(reference)
            if problem:
                problems.append(f"{where}: {problem}")
            else:
                derived.add("pr")
        for name in evidence["lane_checks"]:
            if name in lanes_run.l6_checks:
                derived.add("L6")
            else:
                problems.append(f"{where}: {L6_DRIVER} defines no check {name!r}, so L6 does not run it")
        for key in EVIDENCE_KEYS:
            cited[key] |= set(evidence[key])
        for lane in sorted(set(declared) - derived):
            problems.append(f"{where} claims lane {lane!r}, but none of its evidence runs there")
        for lane in sorted(derived - set(declared)):
            problems.append(f"{where}: its evidence runs in {lane!r}; add it to lanes")
        for lane in declared:
            if lane in lanes and isinstance(lanes[lane], dict) and lanes[lane].get("runs") is False:
                problems.append(f"{where} runs, but lane {lane!r} is marked as not running")

    for test_id in lanes_run.pr:
        if test_id.startswith(SCENARIO_BINARIES) and test_id not in cited["tests"]:
            problems.append(f"pr runs {test_id}, which no running scenario names")
    for test_id in lanes_run.nightly:
        if test_id not in cited["tests"]:
            problems.append(f"nightly runs {test_id}, which no running scenario names")
    for name in lanes_run.l6_checks:
        if name not in cited["lane_checks"]:
            problems.append(f"L6 runs check {name}, which no running scenario names")
    for name, preset in sorted(lanes_run.presets.items()):
        lane = PRESET_LANES.get(preset.get("schedule"))
        if lane and name not in cited["presets"]:
            problems.append(f"{lane} runs preset {name}, which no running scenario names")
    return problems


def summary(manifest: dict) -> str:
    scenarios = [s for s in manifest.get("scenario", []) if isinstance(s, dict)]
    running = [s for s in scenarios if s.get("runs") is True]
    lines = [f"{len(scenarios)} scenarios: {len(running)} run, {len(scenarios) - len(running)} unexercised"]
    for lane in manifest.get("lanes", {}):
        here = [s for s in scenarios if lane in s.get("lanes", [])]
        runs = sum(1 for s in here if s.get("runs") is True)
        lines.append(f"  {lane}: {runs} run, {len(here) - runs} planned")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        with args.manifest.open("rb") as handle:
            manifest = tomllib.load(handle)
        lanes_run = Lanes(args.root)
    except FileNotFoundError as error:
        print(f"{PREFIX}: {error.filename} is missing", file=sys.stderr)
        return 1
    except (tomllib.TOMLDecodeError, ManifestError, SelectionError, OSError, ValueError) as error:
        print(f"{PREFIX}: {error}", file=sys.stderr)
        return 1
    problems = check(manifest, lanes_run, args.root)
    for problem in problems:
        print(f"{PREFIX}: {problem}", file=sys.stderr)
    if problems:
        print(f"{PREFIX}: {len(problems)} problem(s) in {args.manifest}", file=sys.stderr)
        return 1
    print(summary(manifest))
    print(f"{PREFIX}: the manifest names every scenario the lanes run, and each claim holds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
