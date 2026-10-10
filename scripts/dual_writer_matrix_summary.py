#!/usr/bin/env python3
"""Summarise a run of the dual-writer scenario matrix (crates/qbit-prism-dual-sim).

Each scenario writes `<report dir>/<scenario>/report.json`. This reads every
one and writes a single Markdown table: each scenario's verdict, its
duration, the miner-visible gap after each fault, the acknowledged shares
lost, the documented tail it excused, and what failed (expectations and
invariant checks), followed by every failed scenario's error, if any.

The table is the matrix report: the nightly job appends it to the job
summary and keeps it in the artifact. It reports; the test step is what
fails the job.

With `--manifest test/e2e-scenarios.toml --lane <lane>`, it also lists the
dual-writer scenarios of that lane that do not run yet (`runs = false`, an
id starting `dual-writer-`) with their reason, and prints a GitHub warning
annotation for each, so a lane that runs only part of the matrix says so
instead of passing it silently.

Usage: python3 scripts/dual_writer_matrix_summary.py <report dir> [--out FILE]
           [--manifest test/e2e-scenarios.toml --lane pr|nightly]

Exit 1 when the directory holds no report (nothing ran, or the reports were
not written where the job looked); 0 otherwise.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tomllib


PREFIX = "dual-writer-matrix-summary"


def load_reports(root: Path) -> list[dict]:
    reports = []
    for path in sorted(root.glob("*/report.json")):
        try:
            reports.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as error:
            reports.append({"scenario": path.parent.name, "passed": False, "error": f"unreadable report: {error}"})
    return reports


def seconds(milliseconds: object) -> str:
    if not isinstance(milliseconds, (int, float)):
        return "none"
    return f"{milliseconds / 1000:.1f} s"


def gaps(report: dict) -> str:
    found = report.get("gaps") or []
    if not found:
        return "no fault"
    return ", ".join(seconds(gap.get("fault_to_first_accept_ms")) for gap in found)


def failures(report: dict) -> list[str]:
    failed = [e["name"] for e in report.get("expectations") or [] if not e.get("passed")]
    if report.get("error"):
        failed.append("did not complete")
    return failed


def cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def pending(manifest: Path, lane: str) -> list[tuple[str, str]]:
    """The dual-writer scenarios planned for `lane` that do not run yet."""
    document = tomllib.loads(manifest.read_text(encoding="utf-8"))
    return [
        (str(scenario["id"]), str(scenario.get("reason", "")))
        for scenario in document.get("scenario", [])
        if str(scenario.get("id", "")).startswith("dual-writer-")
        and scenario.get("runs") is False
        and lane in scenario.get("lanes", [])
    ]


def render_pending(waiting: list[tuple[str, str]], lane: str) -> str:
    if not waiting:
        return ""
    lines = [
        "",
        f"### Not run yet in the `{lane}` lane",
        "",
        "These scenarios are planned for this lane but need code the branch does not have yet "
        "(test/e2e-scenarios.toml, `runs = false`):",
        "",
    ]
    lines += [f"- `{scenario}`: {cell(reason)}" for scenario, reason in waiting]
    return "\n".join(lines) + "\n"


def render(reports: list[dict]) -> str:
    passed = sum(1 for report in reports if report.get("passed"))
    lines = [
        "## Dual-writer scenario matrix",
        "",
        f"{passed} of {len(reports)} scenarios passed.",
        "",
        "| Scenario | Verdict | Duration | First share after each fault | Shares lost | Excused tail | Failed |",
        "| --- | --- | ---: | --- | ---: | ---: | --- |",
    ]
    for report in reports:
        shares = report.get("shares") or {}
        failed = failures(report)
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} |".format(
                cell(str(report.get("scenario", "?"))),
                "pass" if report.get("passed") else "**FAIL**",
                seconds(report.get("duration_ms")),
                gaps(report),
                shares.get("lost", "?"),
                shares.get("excused_tail", "?"),
                cell("; ".join(failed)) if failed else "",
            )
        )
    errors = [report for report in reports if report.get("error")]
    if errors:
        lines += ["", "### Errors", ""]
        for report in errors:
            lines.append(f"- {cell(str(report.get('scenario')))}: {cell(str(report['error']))}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", type=Path, help="the scenarios' report directory")
    parser.add_argument("--out", type=Path, help="write the table here instead of standard output")
    parser.add_argument("--manifest", type=Path, help="test/e2e-scenarios.toml, to list what does not run yet")
    parser.add_argument("--lane", default="pr", help="the lane whose pending scenarios to list")
    args = parser.parse_args(argv)
    reports = load_reports(args.root)
    if not reports:
        print(f"{PREFIX}: no report.json under {args.root}", file=sys.stderr)
        return 1
    text = render(reports)
    if args.manifest:
        waiting = pending(args.manifest, args.lane)
        text += render_pending(waiting, args.lane)
        for scenario, reason in waiting:
            print(f"::warning title=Dual-writer scenario not run yet::{scenario}: {reason}")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
