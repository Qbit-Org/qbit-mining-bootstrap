#!/usr/bin/env python3
"""The bridging lane's comparison row: what the real node costs (#552, #487 S12).

The nightly workflow's `bridging` job runs `--plan short` twice on one
runner, once on the fake node (`short-plan-fake-node`) and once on a real
regtest qbitd (`short-plan-real-node`). The two presets differ only in
`--node`. This script reads the two run directories that
`.github/scripts/prism-load-run.sh` wrote and reports, for each metric,
real minus fake:

- `client_ack_latency` p50 and p99 in each phase both runs drove;
- the time to usable work on the external tips: the slowest session's time
  (`time_to_usable_work.tips[].all_sessions_milliseconds`), p50 and max over
  the tips;
- the pool node's `submitblock` and `getblocktemplate` latency through the
  harness's relay. The fake node has no relay and records no latency, so its
  side and the difference are unknown, never 0;
- peak RSS (`VmHWM`) of each frontend and of the frontends together, and of
  the pool qbitd. The fake node runs inside the harness process, which is
  not sampled, so its node RSS is unknown.

It is a cost statement on this runner and this commit, not a D1 verdict
(#487 decisions 1 and 5): real-node rates are never D1 evidence, and the
short plan runs at 50 shares/s.

A figure a side could not measure stays null with the reason. A side whose
harness or gate did not exit 0 still gets its numbers, but the row's
`trusted` is false and names why. This script only reports; the workflow
fails the job on the sides' gate exit codes.

Writes the row as JSON (`--out`) and the Markdown table (`--summary`, which
is appended to, as GITHUB_STEP_SUMMARY is). Exits 0 once both are written,
2 on bad arguments or an unwritable output.

Usage:
    python3 scripts/prism_load_bridge.py --fake DIR --real DIR --order fake,real \
        --out bridge-row.json [--summary FILE] [--commit SHA] [--run-url URL]
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys


SCHEMA = "qbit.prism.load-bridge.v1"
REPORT_SCHEMA = "qbit.prism.load-harness.v1"
PRESETS = {"fake": "short-plan-fake-node", "real": "short-plan-real-node"}
STATEMENT = (
    "What the real node adds to the short plan on this runner and commit: real minus fake. "
    "Not a D1 verdict (#487 decisions 1 and 5)."
)
MS = "milliseconds"
KIB = "KiB"
CLIENT_CLOCK = "client monotonic"
FAKE_HAS_NO_RELAY = "the fake node has no RPC relay and records no RPC latency"
FAKE_NODE_IN_HARNESS = "the fake node runs inside the harness process, whose RSS is not sampled"


def read_side(directory: Path, side: str) -> dict:
    """One side's exit codes, report and host, or the reason each is missing."""
    result: dict = {
        "preset": PRESETS[side],
        "directory": str(directory),
        "harness_exit_code": read_code(directory / "harness-exit-code"),
        "gate_exit_code": read_code(directory / "gate-exit-code"),
        "runner": None,
        "report": None,
        "report_error": None,
    }
    try:
        host = json.loads((directory / "host.json").read_text(encoding="utf-8"))
        result["runner"] = host.get("runner_label") if isinstance(host, dict) else None
    except (OSError, ValueError):
        pass
    path = directory / "load-harness-report.json"
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        result["report_error"] = f"{path.name} is missing"
        return result
    except (OSError, ValueError) as error:
        result["report_error"] = f"{path.name} is unreadable: {error}"
        return result
    if not isinstance(report, dict) or report.get("schema") != REPORT_SCHEMA:
        schema = report.get("schema") if isinstance(report, dict) else None
        result["report_error"] = f"{path.name} has schema {schema!r}, not {REPORT_SCHEMA}"
        return result
    name = (report.get("preset") or {}).get("name")
    if name != PRESETS[side]:
        result["report_error"] = f"{path.name} is from preset {name!r}, not {PRESETS[side]}"
        return result
    result["report"] = report
    return result


def read_code(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


class Figure:
    """A measured value, or None with the reason it is missing."""

    def __init__(self, value: float | None, reason: str | None = None) -> None:
        self.value = value
        self.reason = None if value is not None else (reason or "not reported")


def missing_report(side: dict) -> Figure | None:
    if side["report"] is None:
        return Figure(None, side["report_error"])
    return None


def phase_names(side: dict) -> list[str]:
    report = side["report"] or {}
    return [
        phase["name"]
        for phase in report.get("phases") or []
        if isinstance(phase, dict) and isinstance(phase.get("name"), str)
    ]


def ack_latency(side: dict, phase_name: str, statistic: str) -> Figure:
    missing = missing_report(side)
    if missing:
        return missing
    phase = next(
        (p for p in side["report"].get("phases") or [] if isinstance(p, dict) and p.get("name") == phase_name),
        None,
    )
    if phase is None:
        return Figure(None, f"the run has no {phase_name} phase")
    latency = phase.get("client_ack_latency") or {}
    if latency.get("unit") not in (None, MS):
        return Figure(None, f"client_ack_latency is in {latency.get('unit')!r}, not {MS}")
    value = number(latency.get(statistic))
    return Figure(value, latency.get("unavailable_reason") or f"client_ack_latency.{statistic} is null")


def tip_last_notify(side: dict, statistic: str) -> Figure:
    """p50 or max over the external tips of the slowest session's time.

    A tip some session never got work on has no figure, so neither does the
    statistic: the gate reads that as a failure, and a partial p50 would
    report the run faster than it was.
    """
    missing = missing_report(side)
    if missing:
        return missing
    tips = (side["report"].get("time_to_usable_work") or {}).get("tips")
    if not isinstance(tips, list) or not tips:
        return Figure(None, "the run reported no external tip")
    values = []
    for index, tip in enumerate(tips):
        value = number(tip.get("all_sessions_milliseconds")) if isinstance(tip, dict) else None
        if value is None:
            reason = tip.get("all_sessions_unavailable_reason") if isinstance(tip, dict) else None
            return Figure(None, f"tip {index}: {reason or 'no figure'}")
        values.append(value)
    values.sort()
    if statistic == "max":
        return Figure(values[-1])
    rank = max(1, math.ceil(0.5 * len(values)))
    return Figure(values[rank - 1])


def relay_latency(side: dict, method: str, statistic: str) -> Figure:
    missing = missing_report(side)
    if missing:
        return missing
    node = side["report"].get("node") or {}
    if node.get("mode") != "qbitd":
        return Figure(None, FAKE_HAS_NO_RELAY)
    summary = ((node.get("relay") or {}).get("rpc_latency_milliseconds") or {}).get(method)
    if not isinstance(summary, dict) or not summary.get("count"):
        return Figure(None, f"no {method} went through the relay")
    return Figure(number(summary.get(statistic)), f"relay {method} {statistic} is null")


def frontend_peaks(side: dict) -> tuple[list[Figure], str | None]:
    """Each frontend's peak RSS over the run, in frontend order.

    `VmHWM` is a kernel peak per process, so the run's peak is the largest
    any phase saw; a frontend restarted in `reconnect` is a new process, and
    the maximum covers both lifetimes.
    """
    if side["report"] is None:
        return [], side["report_error"]
    count = (side["report"].get("topology") or {}).get("frontends")
    if not isinstance(count, int) or count < 1:
        return [], "the report does not say how many frontends ran"
    peaks: list[list[float]] = [[] for _ in range(count)]
    for phase in side["report"].get("phases") or []:
        for index, process in enumerate((phase or {}).get("processes") or []):
            value = number((process or {}).get("peak_rss_kib"))
            if index < count and value is not None:
                peaks[index].append(value)
    return [
        Figure(max(values)) if values else Figure(None, f"frontend {index}: no peak RSS was sampled")
        for index, values in enumerate(peaks)
    ], None


def node_peak(side: dict) -> Figure:
    missing = missing_report(side)
    if missing:
        return missing
    node = side["report"].get("node") or {}
    if node.get("mode") != "qbitd":
        return Figure(None, FAKE_NODE_IN_HARNESS)
    value = number(((node.get("qbitd") or {}).get("peak_rss_kib") or {}).get("pool"))
    return Figure(value, "the pool qbitd's VmHWM could not be read")


def metric(name: str, unit: str, clock: str | None, fake: Figure, real: Figure) -> dict:
    difference = None
    reason = None
    if fake.value is not None and real.value is not None:
        difference = real.value - fake.value
    else:
        reasons = [f"{side}: {figure.reason}" for side, figure in (("fake", fake), ("real", real)) if figure.value is None]
        reason = "; ".join(reasons)
    return {
        "metric": name,
        "unit": unit,
        "clock": clock,
        "fake": fake.value,
        "real": real.value,
        "real_minus_fake": difference,
        "unavailable_reason": reason,
    }


def metrics(fake: dict, real: dict) -> list[dict]:
    rows = []
    phases = phase_names(fake) + [name for name in phase_names(real) if name not in phase_names(fake)]
    for phase in phases:
        for statistic in ("p50", "p99"):
            rows.append(metric(
                f"client_ack_latency {statistic} ({phase})", MS,
                "client monotonic, submit written to acknowledgement read",
                ack_latency(fake, phase, statistic), ack_latency(real, phase, statistic),
            ))
    for statistic in ("p50", "max"):
        rows.append(metric(
            f"time to usable work, slowest session, {statistic} over external tips", MS,
            "client monotonic against the node's tip stamp",
            tip_last_notify(fake, statistic), tip_last_notify(real, statistic),
        ))
    for method in ("submitblock", "getblocktemplate"):
        for statistic in ("p50", "p99"):
            rows.append(metric(
                f"node {method} latency {statistic}", MS, "relay monotonic, request forwarded to reply",
                relay_latency(fake, method, statistic), relay_latency(real, method, statistic),
            ))
    fake_peaks, fake_error = frontend_peaks(fake)
    real_peaks, real_error = frontend_peaks(real)
    for index in range(max(len(fake_peaks), len(real_peaks), 1)):
        rows.append(metric(
            f"frontend {index} peak RSS", KIB, None,
            fake_peaks[index] if index < len(fake_peaks) else Figure(None, fake_error or f"no frontend {index}"),
            real_peaks[index] if index < len(real_peaks) else Figure(None, real_error or f"no frontend {index}"),
        ))
    rows.append(metric(
        "frontends peak RSS, summed", KIB, None, summed(fake_peaks, fake_error), summed(real_peaks, real_error),
    ))
    rows.append(metric("pool node peak RSS", KIB, None, node_peak(fake), node_peak(real)))
    return rows


def summed(peaks: list[Figure], error: str | None) -> Figure:
    if error or not peaks:
        return Figure(None, error or "no frontend peak RSS")
    unknown = [figure.reason for figure in peaks if figure.value is None]
    if unknown:
        return Figure(None, "; ".join(unknown))
    return Figure(sum(figure.value for figure in peaks))


def trust(sides: dict[str, dict]) -> list[str]:
    """Why the row cannot be taken at face value; empty when it can."""
    reasons = []
    for name, side in sides.items():
        if side["report"] is None:
            reasons.append(f"{name}: {side['report_error']}")
        for key in ("harness_exit_code", "gate_exit_code"):
            code = side[key]
            if code != 0:
                reasons.append(f"{name}: {key} is {'missing' if code is None else code}")
    runners = {side["runner"] for side in sides.values()}
    if len(runners) != 1 or None in runners:
        reasons.append(f"the runner labels differ or are missing: {sorted(map(str, runners))}")
    return reasons


def build_row(fake_dir: Path, real_dir: Path, order: list[str], commit: str | None, run_url: str | None) -> dict:
    sides = {"fake": read_side(fake_dir, "fake"), "real": read_side(real_dir, "real")}
    untrusted = trust(sides)
    return {
        "schema": SCHEMA,
        "statement": STATEMENT,
        "commit": commit,
        "run_url": run_url,
        "runner": sides["fake"]["runner"] if sides["fake"]["runner"] == sides["real"]["runner"] else None,
        "order": order,
        "trusted": not untrusted,
        "untrusted_reasons": untrusted,
        "sides": {
            name: {key: value for key, value in side.items() if key != "report"}
            for name, side in sides.items()
        },
        "metrics": metrics(sides["fake"], sides["real"]),
    }


def show(value: float | None, unit: str) -> str:
    if value is None:
        return "unknown"
    if unit == KIB:
        return f"{value / 1024:,.1f} MiB"
    return f"{value:,.1f} ms"


def show_difference(value: float | None, unit: str) -> str:
    if value is None:
        return "unknown"
    text = show(abs(value), unit)
    return f"+{text}" if value > 0 else (f"-{text}" if value < 0 else text)


def markdown(row: dict) -> str:
    lines = [
        "### Bridging: what the real node costs (real minus fake)",
        "",
        f"{row['statement']} Commit `{row['commit'] or 'unknown'}` on `{row['runner'] or 'unknown runner'}`; "
        f"order {' then '.join(row['order'])}.",
        "",
    ]
    if row["trusted"]:
        lines.append("Both sides completed, reconciled and passed their gates.")
    else:
        lines.append("**Not trusted:** " + "; ".join(row["untrusted_reasons"]) + ".")
    lines += ["", "| Metric | Fake | Real | Real − fake |", "|---|---|---|---|"]
    notes = []
    for entry in row["metrics"]:
        difference = show_difference(entry["real_minus_fake"], entry["unit"])
        if entry["unavailable_reason"]:
            notes.append(entry["unavailable_reason"])
            difference += f" [{len(notes)}]"
        lines.append(
            f"| {entry['metric']} | {show(entry['fake'], entry['unit'])} | "
            f"{show(entry['real'], entry['unit'])} | {difference} |"
        )
    if notes:
        lines.append("")
        lines += [f"{index}. {note}" for index, note in enumerate(notes, start=1)]
    return "\n".join(lines) + "\n\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fake", type=Path, required=True, help="the short-plan-fake-node run directory")
    parser.add_argument("--real", type=Path, required=True, help="the short-plan-real-node run directory")
    parser.add_argument("--order", required=True, help="the order the sides ran in: fake,real or real,fake")
    parser.add_argument("--out", type=Path, required=True, help="where to write the JSON row")
    parser.add_argument("--summary", type=Path, help="a Markdown file to append the table to")
    parser.add_argument("--commit")
    parser.add_argument("--run-url")
    args = parser.parse_args(argv)
    order = [side.strip() for side in args.order.split(",")]
    if sorted(order) != ["fake", "real"]:
        print(f"prism-load-bridge: --order {args.order!r} is not fake,real or real,fake", file=sys.stderr)
        return 2
    row = build_row(args.fake, args.real, order, args.commit, args.run_url)
    try:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        table = markdown(row)
        if args.summary:
            with args.summary.open("a", encoding="utf-8") as handle:
                handle.write(table)
    except OSError as error:
        print(f"prism-load-bridge: {error}", file=sys.stderr)
        return 2
    print(table, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
