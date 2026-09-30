#!/usr/bin/env python3
"""The load lanes' regression rule, report-only (#551, S6 of #487).

Compares a run's trend rows (scripts/prism_load_trend.py) with the trailing
rows of the same series on the `ci-evidence` branch, under the thresholds in
`test/prism-load-regression.toml` (its header states the rule). A series is
one lane, preset, preset sha256, runner class and fsync band; only runs whose
harness exited 0 count toward a baseline, and each run counts once, at its
latest attempt.

For a regressed number the verdict names the commit range it appeared in:
from the newest earlier run of the series that was within the threshold (the
last good commit) to the oldest run of the unbroken regressed streak that
ends at this run (the first bad commit). On the first night that is the last
trended commit and this one; on later nights of the same regression the range
stays where it began. Flagged runs are left out of later baselines, so a
regression that persists does not become its own baseline, until it has
lasted `window` runs: the new level is then taken as accepted.

Nothing here passes by default (EP-OBSERVABILITY): a number the run did not
measure, a run with no fsync cost (so no band), a series with fewer than
`min_baseline` usable runs and a run whose harness did not exit 0 each get an
explicit `unknown` or `not evaluated` with the reason.

#542's variance document (`scripts/prism_load_probe.py variance`, schema
VARIANCE_SCHEMA), when given with --variance, supplies the spread. Its groups
are per runner class, preset and fsync band (#542's own doubling bands, from
prism_load_probe.fsync_band), and each metric carries `all` (every run, so
across VMs), `vm_to_vm` (the per-VM medians) and `run_to_run` (within each
VM) statistics. A nightly runs once on whichever VM it gets, so the spread a
night is compared with is `all.cv`, the coefficient of variation over every
run, as a fraction of the median. It is read from the group of the run's own
#542 band, or else the class's all-band group (the summary says which); a
number #542 does not measure (VARIANCE_METRICS lists the ones it does), or a
group with fewer than two runs of it, uses the provisional spread, and the
verdict stays provisional and names it. When `vm_to_vm.cv` is over `ab_ratio`
times `run_to_run.median_cv` the summary points at #511's same-VM A/B.

A document of another schema, or another version of this one, is refused by
name in the verdict (EP-COMPAT) and the rule falls back to the provisional
spread.

Usage:
  prism_load_regress.py --history DIR --rows DIR [--config FILE]
      [--variance FILE] [--json OUT] [--markdown OUT] [--github-output FILE]

Exits 0 whether or not anything regressed (the rule is report-only), and 2
when an input cannot be read.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
from pathlib import Path
import statistics
from typing import Callable
import sys
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))

import prism_load_probe as probe  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "test" / "prism-load-regression.toml"
CONFIG_SCHEMA = "qbit.prism.load-regression-config.v1"
# The trend row this rule reads, written by scripts/prism_load_trend.py.
ROW_SCHEMA = "qbit.prism.load-trend-row.v1"
VERDICT_SCHEMA = "qbit.prism.load-regression-verdict.v1"
# #542's document, written by scripts/prism_load_probe.py `variance`.
VARIANCE_SCHEMA = probe.VARIANCE_SCHEMA
VARIANCE_SCHEMA_PREFIX = "qbit.prism.runner-probe-variance."
# This rule's metric keys and #542's names for them (a phase metric is
# `<phase>.<name>` there). The server-side ACK, lock waiters and frontend RSS
# are not in #542's runs, so they keep the provisional spread.
VARIANCE_METRICS = {
    "achieved_rate_shares_per_second": "achieved_rate_shares_per_second",
    "client_ack_p50_ms": "ack_p50_ms",
    "client_ack_p99_ms": "ack_p99_ms",
    "tip_last_notify_p99_ms": "tip_last_notify_p99_ms",
}
# 1.4826 * MAD estimates a normal sample's standard deviation.
MAD_SCALE = 1.4826
WORSE = ("higher", "lower")
SCOPES = ("phase", "run")


class RegressError(Exception):
    """An input the rule cannot use, worded for the workflow log."""


@dataclass(frozen=True)
class Metric:
    key: str
    scope: str
    worse: str
    min_relative_change: float
    min_absolute_change: float


@dataclass(frozen=True)
class Config:
    provisional: bool
    lanes: tuple[str, ...]
    window: int
    min_baseline: int
    k: float
    ab_ratio: float
    band_edges: tuple[float, ...]
    band_names: tuple[str, ...]
    metrics: tuple[Metric, ...]


# --- config -------------------------------------------------------------


def finite(value, where: str, minimum: float = 0.0, strict: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise RegressError(f"{where} is {value!r}, not a finite number")
    if value < minimum or (strict and value == minimum):
        raise RegressError(f"{where} is {value}, not {'above' if strict else 'at least'} {minimum}")
    return float(value)


def whole(value, where: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RegressError(f"{where} is {value!r}, not a whole number of at least {minimum}")
    return value


def known_keys(table: dict, allowed: set[str], where: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise RegressError(f"{where}: unknown key(s) {', '.join(unknown)}")


def load_config(path: Path) -> Config:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RegressError(f"{path}: {error}") from error
    if data.get("schema") != CONFIG_SCHEMA:
        raise RegressError(f"{path}: schema is {data.get('schema')!r}, not {CONFIG_SCHEMA}")
    known_keys(data, {"schema", "provisional", "rule", "fsync_bands", "metric"}, str(path))
    if not isinstance(data.get("provisional"), bool):
        raise RegressError(f"{path}: provisional must be true or false")
    rule = data.get("rule") or {}
    known_keys(rule, {"lanes", "window", "min_baseline", "k", "ab_ratio"}, f"{path} [rule]")
    lanes = rule.get("lanes")
    if not isinstance(lanes, list) or not lanes or not all(isinstance(x, str) and x for x in lanes):
        raise RegressError(f"{path}: rule.lanes must be a non-empty list of lane names")
    window = whole(rule.get("window"), f"{path}: rule.window", 1)
    min_baseline = whole(rule.get("min_baseline"), f"{path}: rule.min_baseline", 1)
    if min_baseline > window:
        raise RegressError(f"{path}: rule.min_baseline {min_baseline} exceeds rule.window {window}")
    k = finite(rule.get("k"), f"{path}: rule.k", strict=True)
    ab_ratio = finite(rule.get("ab_ratio"), f"{path}: rule.ab_ratio", minimum=1.0)
    bands = data.get("fsync_bands") or {}
    known_keys(bands, {"edges_usecs", "names"}, f"{path} [fsync_bands]")
    edges = bands.get("edges_usecs")
    names = bands.get("names")
    if not isinstance(edges, list) or not isinstance(names, list):
        raise RegressError(f"{path}: fsync_bands needs edges_usecs and names lists")
    edges = [finite(e, f"{path}: fsync_bands.edges_usecs", strict=True) for e in edges]
    if any(b <= a for a, b in zip(edges, edges[1:])):
        raise RegressError(f"{path}: fsync_bands.edges_usecs must increase strictly")
    if len(names) != len(edges) + 1 or len(set(names)) != len(names) \
            or not all(isinstance(n, str) and n and n != "unknown" for n in names):
        raise RegressError(
            f"{path}: fsync_bands.names must be {len(edges) + 1} distinct names other than 'unknown'"
        )
    metrics = []
    for index, raw in enumerate(data.get("metric") or []):
        where = f"{path}: metric[{index}]"
        known_keys(raw, {"key", "scope", "worse", "min_relative_change", "min_absolute_change"}, where)
        if not isinstance(raw.get("key"), str) or not raw["key"]:
            raise RegressError(f"{where}: key is missing")
        if raw.get("scope") not in SCOPES:
            raise RegressError(f"{where}: scope is {raw.get('scope')!r}, not one of {SCOPES}")
        if raw.get("worse") not in WORSE:
            raise RegressError(f"{where}: worse is {raw.get('worse')!r}, not one of {WORSE}")
        metrics.append(Metric(
            key=raw["key"],
            scope=raw["scope"],
            worse=raw["worse"],
            min_relative_change=finite(raw.get("min_relative_change"), f"{where}.min_relative_change"),
            min_absolute_change=finite(raw.get("min_absolute_change"), f"{where}.min_absolute_change"),
        ))
    if not metrics:
        raise RegressError(f"{path}: no [[metric]] to compare")
    if len({(m.scope, m.key) for m in metrics}) != len(metrics):
        raise RegressError(f"{path}: a metric is listed twice")
    return Config(
        provisional=data["provisional"],
        lanes=tuple(lanes),
        window=window,
        min_baseline=min_baseline,
        k=k,
        ab_ratio=ab_ratio,
        band_edges=tuple(edges),
        band_names=tuple(names),
        metrics=tuple(metrics),
    )


def fsync_band(usecs_per_op, config: Config) -> str:
    """The run's fsync band, or `unknown` when its cost was not measured."""
    if isinstance(usecs_per_op, bool) or not isinstance(usecs_per_op, (int, float)) \
            or not math.isfinite(usecs_per_op) or usecs_per_op <= 0:
        return "unknown"
    for edge, name in zip(config.band_edges, config.band_names):
        if usecs_per_op < edge:
            return name
    return config.band_names[-1]


# --- variance (#542) ----------------------------------------------------


def load_variance(path: Path | None) -> tuple[dict | None, str | None]:
    """The variance document, or None and why it is not used."""
    if path is None:
        return None, "no variance file given (#542's document is not checked in); the spread is provisional"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return None, f"variance file {path} could not be read ({error}); the spread is provisional"
    schema = data.get("schema") if isinstance(data, dict) else None
    if schema != VARIANCE_SCHEMA:
        kind = ("another version of #542's document" if isinstance(schema, str)
                and schema.startswith(VARIANCE_SCHEMA_PREFIX) else "not #542's document")
        return None, (f"variance file {path} has schema {schema!r}, {kind}; this rule reads "
                      f"{VARIANCE_SCHEMA} only, so the spread is provisional")
    if not isinstance(data.get("groups"), list):
        return None, f"variance file {path} has no groups list; the spread is provisional"
    return data, None


@dataclass(frozen=True)
class Spread:
    """#542's spreads for one number, as fractions of its median."""
    all_cv: float
    vm_to_vm_cv: float | None
    run_to_run_cv: float | None
    source: str


def variance_spread(variance: dict | None, runner: str | None, preset: str | None,
                    metric: Metric, phase: str | None, usecs_per_op) -> Spread | None:
    """The run's spread from #542's document, or None when it has none."""
    name = VARIANCE_METRICS.get(metric.key)
    if variance is None or name is None:
        return None
    full = name if metric.scope == "run" else f"{phase}.{name}"
    band = probe.fsync_band(number(usecs_per_op))
    for wanted in (band, probe.ALL_BANDS):
        if wanted == probe.UNKNOWN_BAND:
            continue
        for group in variance["groups"]:
            if not isinstance(group, dict) or (group.get("runner"), group.get("preset"),
                                               group.get("fsync_band")) != (runner, preset, wanted):
                continue
            stats = (group.get("metrics") or {}).get(full) or {}
            every = stats.get("all") or {}
            cv = positive(every.get("cv"))
            if cv is None or not isinstance(every.get("n"), int) or every["n"] < 2:
                continue
            label = f"{wanted} band" if wanted != probe.ALL_BANDS else "all bands"
            return Spread(
                all_cv=cv,
                vm_to_vm_cv=positive((stats.get("vm_to_vm") or {}).get("cv")),
                run_to_run_cv=positive((stats.get("run_to_run") or {}).get("median_cv")),
                source=f"variance (#542, {label}, n {every['n']})",
            )
    return None


def positive(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value) if value >= 0 else None


# --- rows ---------------------------------------------------------------


def number(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def read_row_files(directory: Path) -> list[dict]:
    """The run's own rows: one JSON object per `*.json` file."""
    rows = []
    for path in sorted(directory.glob("*.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise RegressError(f"{path}: {error}") from error
        if not isinstance(row, dict) or row.get("schema") != ROW_SCHEMA:
            raise RegressError(f"{path}: not a {ROW_SCHEMA} row")
        rows.append(row)
    if not rows:
        # No rows is no comparison, not "nothing regressed" (EP-OBSERVABILITY).
        raise RegressError(f"{directory}: no trend rows to compare (the run left none)")
    return rows


def read_history(directory: Path) -> tuple[list[dict], list[str]]:
    """Every row under trend/*/*.jsonl, and notes on lines that were skipped.

    Lines of another schema, or that do not parse, are counted and left out:
    they are kept on the branch (it is append-only) but never read as data of
    this schema (EP-COMPAT)."""
    rows, notes = [], []
    unparsed = foreign = 0
    for path in sorted((directory / "trend").glob("*/*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                unparsed += 1
                continue
            if not isinstance(row, dict) or row.get("schema") != ROW_SCHEMA:
                foreign += 1
                continue
            rows.append(row)
    if unparsed:
        notes.append(f"{unparsed} history line(s) did not parse and were skipped")
    if foreign:
        notes.append(f"{foreign} history row(s) of a schema other than {ROW_SCHEMA} were skipped")
    return rows, notes


def latest_attempts(rows: list[dict]) -> list[dict]:
    """One row per run and preset: its latest attempt, a measurement or a
    `missing` placeholder (EP-STATE: an earlier attempt never stands in for
    a later one, so a re-run that left no row hides the earlier measurement).
    Only `preset` rows are returned; a run whose latest attempt is missing
    has none."""
    return [r for r in latest_outcomes(rows) if r.get("kind") == "preset"]


def latest_outcomes(rows: list[dict]) -> list[dict]:
    def rank(row: dict) -> tuple:
        # At one attempt a measurement outranks a placeholder.
        return (row.get("run_attempt") or 0, row.get("kind") == "preset")

    best: dict[tuple, dict] = {}
    for row in rows:
        if row.get("kind") not in ("preset", "missing"):
            continue
        # Not by lane: a re-run whose preset moved lanes still supersedes.
        key = (row.get("repository"), row.get("run_id"), row.get("preset"))
        if key not in best or rank(row) > rank(best[key]):
            best[key] = row
    return list(best.values())


def order_key(row: dict) -> tuple:
    return (row.get("recorded_at") or "", row.get("run_id") or 0, row.get("run_attempt") or 0)


def series_key(row: dict, config: Config) -> tuple:
    fsync = row.get("fsync") or {}
    return (
        row.get("lane"),
        row.get("preset"),
        row.get("preset_sha256"),
        row.get("runner_class"),
        fsync_band(fsync.get("usecs_per_op"), config),
    )


def completed(row: dict) -> bool:
    return (row.get("outcome") or {}).get("harness_exit_code") == 0


def value_of(row: dict, metric: Metric, phase: str | None) -> float | None:
    headline = row.get("headline") or {}
    if metric.scope == "run":
        return number((headline.get("run") or {}).get(metric.key))
    return number(((headline.get("phases") or {}).get(phase) or {}).get(metric.key))


def headline_of(row: dict, phase: str | None) -> dict:
    headline = row.get("headline") or {}
    if phase is None:
        return dict(headline.get("run") or {})
    return dict((headline.get("phases") or {}).get(phase) or {})


def artifact_phases(row: dict) -> list[str]:
    phases = (row.get("headline") or {}).get("phases") or {}
    return [name for name, phase in phases.items() if (phase or {}).get("in_artifact") is not False]


# --- the rule -----------------------------------------------------------


@dataclass(frozen=True)
class Threshold:
    median: float
    spread: float
    allowed: float
    n: int
    source: str

    def worse(self, value: float, worse: str) -> bool:
        if worse == "higher":
            return value > self.median + self.allowed
        return value < self.median - self.allowed

    def limit(self, worse: str) -> float:
        return self.median + self.allowed if worse == "higher" else self.median - self.allowed


def threshold(values: list[float], metric: Metric, config: Config,
              measured: Spread | None) -> Threshold:
    median = statistics.median(values)
    if measured is not None:
        spread, source = measured.all_cv * abs(median), measured.source
    else:
        mad = statistics.median(abs(v - median) for v in values)
        spread, source = MAD_SCALE * mad, "provisional: the baseline's scaled MAD"
    allowed = max(config.k * spread, metric.min_relative_change * abs(median),
                  metric.min_absolute_change)
    return Threshold(median, spread, allowed, len(values), source)


def point(row: dict, value: float | None) -> dict:
    return {
        "run_id": row.get("run_id"),
        "run_attempt": row.get("run_attempt"),
        "run_url": row.get("run_url"),
        "commit": row.get("commit"),
        "recorded_at": row.get("recorded_at"),
        "value": value,
    }


def evaluate_number(current: dict, value: float, history: list[tuple[dict, float]],
                    metric: Metric, config: Config,
                    spread_for: Callable[[dict], Spread | None]) -> dict:
    """One number of one run against its series. `history` is the series'
    usable earlier runs with this number measured, oldest first.
    `spread_for(row)` is #542's spread for a run (its own fsync band), or
    None; each replayed run is judged with its own.

    The series is replayed oldest first so that every run's verdict is the
    one its own trailing baseline gave it, and a flagged run is left out of
    later baselines: a lasting regression stays flagged, with its range
    where it began, instead of drifting into the median that judges it.
    Once a streak of flagged runs is `window` long the new level is taken
    as accepted and the streak becomes the baseline."""
    series = history + [(current, value)]
    flagged = [False] * len(series)
    # Each run's own verdict: `within`, `flagged`, `accepted` (a flagged run
    # whose streak lasted a window) or `unjudged` (too few runs before it).
    verdicts = ["unjudged"] * len(series)
    # Whether any judgement, a replayed run's or this one's, fell back to the
    # provisional spread: that keeps the verdict provisional.
    provisional_spread = False
    limit = None
    for index, (_, observed) in enumerate(series):
        streak = 0
        while streak < index and flagged[index - 1 - streak]:
            streak += 1
        if streak >= config.window:
            for accepted in range(index - streak, index):
                flagged[accepted] = False
                verdicts[accepted] = "accepted"
        base = [v for j, (_, v) in enumerate(series[:index]) if not flagged[j]][-config.window:]
        if len(base) < config.min_baseline:
            limit = None
            continue
        spread = spread_for(series[index][0])
        provisional_spread = provisional_spread or spread is None
        limit = threshold(base, metric, config, spread)
        flagged[index] = limit.worse(observed, metric.worse)
        verdicts[index] = "flagged" if flagged[index] else "within"
    if limit is None:
        usable = len([f for f in flagged[:-1] if not f])
        return {"status": "unknown",
                "reason": f"the series has {usable} usable earlier run(s) with this number; "
                          f"the rule needs {config.min_baseline}"}
    if not flagged[-1]:
        return {"status": "within", "threshold": limit, "provisional_spread": provisional_spread}
    streak = 1
    while streak < len(series) and flagged[-1 - streak]:
        streak += 1
    # The last good run is the one before the streak, if it was judged within
    # its own baseline, or else (never judged, or an accepted level) if it is
    # within the limit that flagged this run; the verdict says which.
    good, basis = None, None
    if streak < len(series):
        candidate = series[-1 - streak]
        if verdicts[-1 - streak] == "within":
            good, basis = candidate, "within its own baseline"
        elif not limit.worse(candidate[1], metric.worse):
            good, basis = candidate, ("within this run's limit (not itself judged: too few runs "
                                      "before it)" if verdicts[-1 - streak] == "unjudged"
                                      else "within this run's limit (an accepted level)")
    return {"status": "regressed", "threshold": limit, "good": good, "good_basis": basis,
            "first_bad": series[-streak], "streak": streak, "provisional_spread": provisional_spread}


def evaluate(current_rows: list[dict], history_rows: list[dict], config: Config,
             variance: dict | None, variance_note: str | None) -> dict:
    current = latest_attempts(current_rows)
    current_ids = {(r.get("repository"), r.get("run_id")) for r in current}
    history = [r for r in latest_attempts(history_rows)
               if (r.get("repository"), r.get("run_id")) not in current_ids]
    history.sort(key=order_key)
    regressions, entries, notes = [], [], []
    # Numbers compared on the provisional spread: any keeps the verdict
    # provisional, however the config is marked.
    fallbacks: set[str] = set()
    # A planned preset whose job left no row is shown, never silently absent.
    for row in latest_outcomes(current_rows):
        if row.get("kind") == "missing":
            entries.append({"lane": row.get("lane"), "preset": row.get("preset"), "runner_class": None,
                            "fsync_band": "unknown", "run_id": row.get("run_id"),
                            "run_attempt": row.get("run_attempt"), "commit": row.get("commit"),
                            "status": "not evaluated", "missing": True,
                            "reason": "the preset's job left no row, so nothing was measured"})
    if variance_note:
        notes.append(variance_note)
    for row in sorted(current, key=lambda r: (r.get("lane") or "", r.get("preset") or "")):
        lane, preset = row.get("lane"), row.get("preset")
        runner = row.get("runner_class")
        band = series_key(row, config)[4]
        head = {"lane": lane, "preset": preset, "runner_class": runner, "fsync_band": band,
                "run_id": row.get("run_id"), "run_attempt": row.get("run_attempt"),
                "commit": row.get("commit")}
        if lane not in config.lanes:
            entries.append({**head, "status": "not evaluated",
                            "reason": f"the rule reads lanes {', '.join(config.lanes)}"})
            continue
        if not completed(row):
            code = (row.get("outcome") or {}).get("harness_exit_code")
            entries.append({**head, "status": "not evaluated",
                            "reason": f"the harness exited {code if code is not None else 'unknown'}, "
                                      "so its numbers are not a measurement of the commit"})
            continue
        if band == "unknown":
            entries.append({**head, "status": "unknown",
                            "reason": "pg_test_fsync's cost was not measured, so the run has no "
                                      "fsync band to compare within"})
            continue
        key = series_key(row, config)
        series = [r for r in history if completed(r) and series_key(r, config) == key
                  and order_key(r) < order_key(row)]
        counts = {"within": 0, "regressed": 0, "unknown": 0}
        reasons: list[str] = []
        for metric in config.metrics:
            phases = [None] if metric.scope == "run" else artifact_phases(row)
            for phase in phases:
                value = value_of(row, metric, phase)
                label = metric.key if phase is None else f"{phase}.{metric.key}"
                if value is None:
                    counts["unknown"] += 1
                    reasons.append(f"{label}: not measured in this run")
                    continue
                measured = variance_spread(variance, runner, preset, metric, phase,
                                           (row.get("fsync") or {}).get("usecs_per_op"))
                if measured and measured.run_to_run_cv is not None and measured.vm_to_vm_cv is not None \
                        and measured.vm_to_vm_cv > config.ab_ratio * measured.run_to_run_cv:
                    notes.append(
                        f"{runner} {preset} {label}: VM-to-VM spread {measured.vm_to_vm_cv:.3g} is over "
                        f"{config.ab_ratio:g}x the run-to-run spread {measured.run_to_run_cv:.3g}; "
                        "#511's interleaved A/B on one VM is the comparison to trust for it")
                earlier = [(r, v) for r in series if (v := value_of(r, metric, phase)) is not None]
                def spread_for(judged: dict, metric=metric, phase=phase) -> Spread | None:
                    return variance_spread(variance, judged.get("runner_class"), judged.get("preset"),
                                           metric, phase, (judged.get("fsync") or {}).get("usecs_per_op"))

                result = evaluate_number(row, value, earlier, metric, config, spread_for)
                if result["status"] == "unknown":
                    counts["unknown"] += 1
                    reasons.append(f"{label}: {result['reason']}")
                    continue
                counts[result["status"]] += 1
                if result["provisional_spread"]:
                    fallbacks.add(f"{runner} {preset} {label}")
                if result["status"] != "regressed":
                    continue
                limit: Threshold = result["threshold"]
                good, first_bad = result["good"], result["first_bad"]
                regressions.append({
                    **head,
                    "phase": phase,
                    "metric": metric.key,
                    "worse": metric.worse,
                    "current": point(row, value),
                    "baseline": {"median": limit.median, "spread": limit.spread,
                                 "allowed_change": limit.allowed, "limit": limit.limit(metric.worse),
                                 "runs": limit.n, "spread_source": limit.source},
                    "last_good": {**point(*good), "basis": result["good_basis"]} if good else None,
                    "first_bad": point(*first_bad),
                    "runs_in_streak": result["streak"],
                    # Both runs' headline numbers for the phase (or the run),
                    # for the tracking issue's side-by-side summary.
                    "summaries": {"current": headline_of(row, phase),
                                  "last_good": headline_of(good[0], phase) if good else None},
                    "commit_range": {
                        "from": good[0].get("commit") if good else None,
                        "to": first_bad[0].get("commit"),
                    },
                })
        entries.append({**head, "status": "evaluated", "numbers": counts, "unknown": reasons})
    if variance is not None and fallbacks:
        notes.append("the variance file has no spread (two or more runs) for "
                     + "; ".join(sorted(fallbacks)) + "; those use the provisional spread")
    return {
        "schema": VERDICT_SCHEMA,
        "report_only": True,
        "provisional": config.provisional or variance is None or bool(fallbacks),
        "rule": {"window": config.window, "min_baseline": config.min_baseline, "k": config.k,
                 "lanes": list(config.lanes), "fsync_band_edges_usecs": list(config.band_edges),
                 "fsync_band_names": list(config.band_names)},
        "history_rows": len(history),
        "regressions": regressions,
        "runs": entries,
        "notes": sorted(set(notes)),
    }


# --- output -------------------------------------------------------------


def fmt(value) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, float):
        return f"{value:,.3f}".rstrip("0").rstrip(".")
    return str(value)


def short(commit: str | None) -> str:
    return commit[:12] if commit else "unknown"


def compare_link(repository: str | None, server: str, commit_range: dict) -> str:
    start, end = commit_range.get("from"), commit_range.get("to")
    if not start or not end:
        return f"`{short(start)}..{short(end)}` (no earlier run within the threshold)"
    text = f"`{short(start)}..{short(end)}`"
    if repository:
        return f"[{text}]({server}/{repository}/compare/{start}...{end})"
    return text


def run_link(p: dict | None) -> str:
    if not p:
        return "none"
    label = f"run {p.get('run_id')}" + (f" (attempt {p['run_attempt']})" if (p.get("run_attempt") or 1) > 1 else "")
    return f"[{label}]({p['run_url']})" if p.get("run_url") else label


def markdown(verdict: dict, repository: str | None, server: str) -> str:
    lines = ["### Load regression rule (#551): report-only", ""]
    if verdict["provisional"]:
        lines += ["Thresholds are **provisional** (the config is not yet calibrated against "
                  "#542's measured variance, or a number used the provisional spread; see the "
                  "notes); nothing here fails a job or blocks a merge.", ""]
    rule = verdict["rule"]
    lines += [f"Trailing median of up to {rule['window']} runs of the same lane, preset, runner "
              f"class and fsync band (at least {rule['min_baseline']}), k = {rule['k']:g}; "
              f"{verdict['history_rows']} earlier runs read from `ci-evidence`.", ""]
    regressions = verdict["regressions"]
    if regressions:
        lines += [f"#### {len(regressions)} number(s) regressed", "",
                  "| lane | preset | runner | fsync band | phase | number | this run | last good run | "
                  "baseline median | limit | commit range |",
                  "|---|---|---|---|---|---|---|---|---|---|---|"]
        for r in regressions:
            good = r["last_good"]
            lines.append(
                f"| {r['lane']} | {r['preset']} | {r['runner_class']} | {r['fsync_band']} | "
                f"{r['phase'] or 'run'} | {r['metric']} ({r['worse']} is worse) | "
                f"{fmt(r['current']['value'])} ({run_link(r['current'])}) | "
                f"{fmt(good['value']) if good else 'none'} ({run_link(good)}"
                f"{'; ' + good['basis'] if good and good.get('basis') != 'within its own baseline' else ''}) | "
                f"{fmt(r['baseline']['median'])} over {r['baseline']['runs']} runs | "
                f"{fmt(r['baseline']['limit'])} ({r['baseline']['spread_source']}) | "
                f"{compare_link(repository, server, r['commit_range'])} |")
        lines.append("")
        lines += ["The range runs from the newest earlier run within the threshold to the first "
                  "run of the unbroken regressed streak ending at this run.", ""]
        shown = set()
        for r in regressions:
            key = (r["lane"], r["preset"], r["phase"], r["current"]["run_id"])
            if key in shown:
                continue
            shown.add(key)
            current, good = r["summaries"]["current"], r["summaries"]["last_good"] or {}
            lines += [f"<details><summary>{r['preset']} {r['phase'] or 'run'}: this run and the last "
                      "good run</summary>", "",
                      f"| number | last good ({run_link(r['last_good'])}) | this run ({run_link(r['current'])}) |",
                      "|---|---|---|"]
            for name in sorted(set(current) | set(good)):
                lines.append(f"| {name} | {fmt(good.get(name))} | {fmt(current.get(name))} |")
            lines += ["", "</details>", ""]
    else:
        counted = [e["numbers"] for e in verdict["runs"] if e["status"] == "evaluated"]
        within = sum(n["within"] for n in counted)
        unknown = sum(n["unknown"] for n in counted)
        if not within:
            # Every number unknown is no verdict, not a pass (EP-OBSERVABILITY).
            lines += ["**No verdict:** no number could be compared "
                      f"({unknown} without a verdict; see the reasons below).", ""]
        else:
            lines += [f"No number regressed: {within} within the threshold, {unknown} without a verdict.", ""]
    missing = [e for e in verdict["runs"] if e.get("missing")]
    if missing:
        lines += [f"**{len(missing)} planned preset(s) left no measurement:** "
                  + ", ".join(e["preset"] or "unknown" for e in missing) + ".", ""]
    lines += ["#### Runs", "", "| lane | preset | runner | fsync band | status |", "|---|---|---|---|---|"]
    for entry in verdict["runs"]:
        if entry["status"] == "evaluated":
            n = entry["numbers"]
            status = f"{n['within']} within, {n['regressed']} regressed, {n['unknown']} unknown"
        else:
            status = f"{entry['status']}: {entry['reason']}"
        lines.append(f"| {entry['lane']} | {entry['preset']} | {entry['runner_class']} | "
                     f"{entry['fsync_band']} | {status} |")
    unknown = [(e, u) for e in verdict["runs"] for u in e.get("unknown") or []]
    if unknown:
        lines += ["", "<details><summary>Numbers with no verdict</summary>", ""]
        lines += [f"- {e['preset']}: {u}" for e, u in unknown]
        lines += ["", "</details>"]
    if verdict["notes"]:
        lines += ["", "#### Notes", ""] + [f"- {note}" for note in verdict["notes"]]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--history", type=Path, required=True,
                        help="a checkout of ci-evidence (trend/<lane>/*.jsonl); may be empty")
    parser.add_argument("--rows", type=Path, required=True, help="this run's row files (*.json)")
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--variance", type=Path, help="#542's variance file")
    parser.add_argument("--json", type=Path, help="write the verdict here")
    parser.add_argument("--markdown", type=Path, help="write the summary here")
    parser.add_argument("--github-output", type=Path, help="append regressions=<n> here")
    parser.add_argument("--repository", help="owner/name, for compare links")
    parser.add_argument("--server-url", default="https://github.com")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        history, history_notes = read_history(args.history)
        rows = read_row_files(args.rows)
    except (RegressError, OSError) as error:
        print(f"prism-load-regress: {error}", file=sys.stderr)
        return 2
    variance, variance_note = load_variance(args.variance)
    verdict = evaluate(rows, history, config, variance, variance_note)
    verdict["notes"] = sorted(set(verdict["notes"]) | set(history_notes))
    text = markdown(verdict, args.repository, args.server_url)
    if args.json:
        args.json.write_text(json.dumps(verdict, indent=2) + "\n", encoding="utf-8")
    if args.markdown:
        args.markdown.write_text(text, encoding="utf-8")
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as out:
            out.write(f"regressions={len(verdict['regressions'])}\n")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
