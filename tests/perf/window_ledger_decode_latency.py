#!/usr/bin/env python3
"""Payout-window database decode latency and monitor responsiveness (#236).

The 2026-09 union-mainnet incident (issue #236) attributes coordinator lease
exits to the coordinator's own scheduling stalls while a ~210k-share payout
window was being decoded: ``snapshot_at_job_issue`` returned one ``json_agg``
value the size of the whole window, and decoding it was one C call that never
released the GIL, so the lease monitor thread went unscheduled for as long as
the decode took. PR 1 of the fix plan turns both payout-window reads into one
JSON object per row, decoded in bounded batches on both database backends.

This driver measures that database/decode phase alone -- ledger admission,
statement execution, result receipt, row decoding and record conversion --
for the three read shapes the coordinator issues, on both backends, at the
incident window size and the stress size, with a monitor thread beside it:

=================  ==========================================================
read               call
=================  ==========================================================
``bounded``        ``snapshot_at_job_issue(anchor, window_weight=<all rows>)``
``unbounded``      ``snapshot_at_job_issue(anchor)`` (full history)
``delta``          ``snapshot_between_job_issues(prev, anchor)`` (all rows)
=================  ==========================================================

Per call it records wall time, the calling thread's CPU time, whole-process
CPU time, RSS before/after/high-water, the record count, and the ledger's
own admission/execution attribution. Beside every call a **monitor thread**
sleeps 10 ms in a loop and records how late each wake-up was: the maximum
lateness, and how many wake-ups were more than 100, 250 and 500 ms late. It
is a proxy for the lease monitor's wake lateness, not the lease monitor
itself: the real heartbeat needs a running coordinator and a guard session,
and the quantity that decides whether it can run is exactly this one --
whether another Python thread can get the interpreter while the read is in
progress. Server-side time is measured separately with ``EXPLAIN (ANALYZE)``
of the statement the ledger actually sent, so the client-side share of the
wall time (transfer + decode + convert) is attributable as ``wall - server``
without labelling any of it as PostgreSQL time.

**Baseline versus patch is the same script on two checkouts**: ``--lab-root``
selects the tree ``lab.prism.share_ledger`` is imported from, and the result
records that tree's commit. The statements are captured from whatever that
checkout sends (``json_agg`` at the baseline, one object per row after), so
the server-side comparison is of the real statements, not a re-typed copy.

The fixture is generated, production-shaped and disposable: ``username.rig:
block_hash_hex`` share ids (``share_writer.py``'s form), 62-character payout
addresses as miner id and order key, 32-byte P2MR programs, the live-host
difficulty values ``window_pipeline_gil_scaling.py`` uses, a distinct job id
per 64 shares, a ``stale-grace`` credit policy on ~1% of rows, and miner skew
in the shape ``test-prism-postgres-scale.sh`` seeds. Nothing is exported from
a production ledger. Point it at a disposable database; it truncates the
share ledger between sizes.

This is an **on-demand instrument, not a test**: it asserts no thresholds and
is not named ``test_*``, so the discovery run never executes it. Example::

    python3 tests/perf/window_ledger_decode_latency.py \\
        --psql-command "psql -p 55436 -d qbit_bench" \\
        --database-url "postgresql://ubuntu@/qbit_bench?host=/var/run/postgresql&port=55436" \\
        --shares 210000 400000 --reps 3 --json patched.json
    python3 tests/perf/window_ledger_decode_latency.py ... --lab-root /path/to/baseline-checkout --json baseline.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]


def _install_lab_root(lab_root: Path) -> None:
    """Make ``lab`` import from the chosen checkout, before anything imports it."""
    for name in list(sys.modules):
        if name == "lab" or name.startswith("lab."):
            raise SystemExit("lab.* was imported before --lab-root was applied")
    sys.path.insert(0, str(lab_root))


def _git_head(root: Path) -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


# --------------------------------------------------------------------------
# fixture
# --------------------------------------------------------------------------

SHARE_DIFFICULTY = 16384
NETWORK_DIFFICULTY = 226646186
EPOCH_SECONDS = 1_700_000_000
SHARE_SPACING_MS = 250
ACCEPT_LAG_MS = 400


def seed_sql(share_rows: int, miners: int) -> str:
    miners = max(4, int(miners))
    return f"""
TRUNCATE qbit_share_ledger CASCADE;
ALTER SEQUENCE qbit_share_ledger_share_seq_seq RESTART WITH 1;
INSERT INTO qbit_share_ledger (
    share_id, miner_id, payout_order_key, p2mr_program,
    share_difficulty, network_difficulty, template_height, job_id,
    job_issued_at, ntime, accepted_at, accepted, writer_id, writer_epoch,
    credit_policy
)
SELECT
    'bench-miner-' || miner_idx || '.rig' || (g % 512) || ':'
        || encode(sha256(('bench-share:' || g)::bytea), 'hex'),
    'qbit1' || substr(encode(sha256(('bench-miner:' || miner_idx)::bytea), 'hex'), 1, 58),
    'qbit1' || substr(encode(sha256(('bench-miner:' || miner_idx)::bytea), 'hex'), 1, 58),
    sha256(('bench-program:' || miner_idx)::bytea),
    {SHARE_DIFFICULTY},
    {NETWORK_DIFFICULTY},
    1000000 + (g / 64),
    'bench-job-' || lpad((g / 64)::text, 12, '0'),
    to_timestamp({EPOCH_SECONDS}) + (g * interval '{SHARE_SPACING_MS} milliseconds'),
    {EPOCH_SECONDS} + (g / 4),
    to_timestamp({EPOCH_SECONDS}) + (g * interval '{SHARE_SPACING_MS} milliseconds')
        + interval '{ACCEPT_LAG_MS} milliseconds',
    TRUE,
    'bench-seed',
    1,
    CASE WHEN g % 97 = 0 THEN 'stale-grace' ELSE NULL END
FROM (
    SELECT
        gs AS g,
        CASE
            WHEN mod(gs, 10) < 3 THEN 1
            WHEN mod(gs, 10) < 5 THEN 2
            WHEN mod(gs, 10) < 6 THEN 3
            ELSE 4 + mod(gs, {miners} - 3)
        END AS miner_idx
    FROM generate_series(1, {int(share_rows)}) AS gs
) AS seeded;
ANALYZE qbit_share_ledger;
"""


def anchors_for(share_rows: int) -> tuple[int, int, int]:
    """(anchor_ms, previous_anchor_ms, window_weight covering every row)."""
    last_accepted_ms = (
        EPOCH_SECONDS * 1000 + share_rows * SHARE_SPACING_MS + ACCEPT_LAG_MS
    )
    anchor_ms = last_accepted_ms + 10_000
    previous_anchor_ms = EPOCH_SECONDS * 1000 - 1
    return anchor_ms, previous_anchor_ms, share_rows * SHARE_DIFFICULTY


# --------------------------------------------------------------------------
# measurement
# --------------------------------------------------------------------------


class WakeMonitor(threading.Thread):
    """Sleep 10 ms in a loop and record how late each wake-up was."""

    def __init__(self, interval: float = 0.010) -> None:
        super().__init__(daemon=True, name="wake-monitor")
        self.interval = interval
        self._stop_event = threading.Event()
        self.samples = 0
        self.max_late = 0.0
        self.late_100 = 0
        self.late_250 = 0
        self.late_500 = 0
        self.late_total = 0.0

    def run(self) -> None:
        while not self._stop_event.is_set():
            started = time.perf_counter()
            time.sleep(self.interval)
            late = max(0.0, time.perf_counter() - started - self.interval)
            self.samples += 1
            self.late_total += late
            if late > self.max_late:
                self.max_late = late
            if late > 0.100:
                self.late_100 += 1
            if late > 0.250:
                self.late_250 += 1
            if late > 0.500:
                self.late_500 += 1

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        self.join(timeout=5)
        return {
            "samples": self.samples,
            "max_late_ms": round(self.max_late * 1000, 1),
            "mean_late_ms": round(
                (self.late_total / self.samples * 1000) if self.samples else 0.0, 2
            ),
            "late_over_100ms": self.late_100,
            "late_over_250ms": self.late_250,
            "late_over_500ms": self.late_500,
        }


def _rss_kb() -> tuple[int, int]:
    rss = hwm = 0
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
            elif line.startswith("VmHWM:"):
                hwm = int(line.split()[1])
    except OSError:
        pass
    return rss, hwm


def capture_statement(ledger: Any, call: Callable[[], Any]) -> tuple[str, Any]:
    """Run ``call`` and return the statement the ledger sent for it.

    Works on both checkouts: the row-result read helper after #236, the
    single-value read helper before it.
    """
    statements: list[str] = []
    for name in ("_run_retry_safe_read_json_rows", "_run_retry_safe_read_json"):
        original = getattr(ledger, name, None)
        if original is None:
            continue

        def recording(sql: str, *args: Any, _original=original, **kwargs: Any) -> Any:
            statements.append(sql)
            return _original(sql, *args, **kwargs)

        setattr(ledger, name, recording)
        try:
            result = call()
        finally:
            delattr(ledger, name)
        if len(statements) != 1:
            raise SystemExit(f"expected one statement, captured {len(statements)}")
        return statements[0], result
    raise SystemExit("no read helper to capture on this ledger")


def timed_call(ledger: Any, call: Callable[[], Any]) -> dict[str, Any]:
    stats_before = ledger.ledger_read_gate_stats()
    rss_before, _ = _rss_kb()
    monitor = WakeMonitor()
    monitor.start()
    time.sleep(0.05)  # let the monitor establish its cadence
    process_cpu = time.process_time()
    thread_cpu = time.thread_time()
    started = time.perf_counter()
    result = call()
    wall = time.perf_counter() - started
    thread_cpu = time.thread_time() - thread_cpu
    process_cpu = time.process_time() - process_cpu
    monitor_stats = monitor.stop()
    rss_after, hwm_after = _rss_kb()
    stats_after = ledger.ledger_read_gate_stats()
    attribution: dict[str, float] = {}
    for operation, after in stats_after.items():
        before = stats_before.get(operation, {})
        delta = float(after.get("execute_seconds_total", 0.0)) - float(
            before.get("execute_seconds_total", 0.0)
        )
        gate_delta = float(after.get("gate_wait_seconds_total", 0.0)) - float(
            before.get("gate_wait_seconds_total", 0.0)
        )
        if delta or gate_delta:
            attribution[operation] = {
                "execute_ms": round(delta * 1000, 1),
                "gate_wait_ms": round(gate_delta * 1000, 1),
            }
    return {
        "records": len(result),
        "wall_ms": round(wall * 1000, 1),
        "thread_cpu_ms": round(thread_cpu * 1000, 1),
        "process_cpu_ms": round(process_cpu * 1000, 1),
        "rss_before_mb": round(rss_before / 1024, 1),
        "rss_after_mb": round(rss_after / 1024, 1),
        "rss_hwm_mb": round(hwm_after / 1024, 1),
        "monitor": monitor_stats,
        "ledger_attribution": attribution,
    }


def explain(psql_ledger: Any, sql: str) -> dict[str, Any]:
    """Server-side planning + execution time and the plan's node summary."""
    output = psql_ledger._run_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
    plan = json.loads(output)[0]
    nodes: list[str] = []

    def walk(node: dict[str, Any], depth: int) -> None:
        label = node.get("Node Type", "?")
        if node.get("Strategy"):
            label += f"/{node['Strategy']}"
        if node.get("Index Name"):
            label += f"[{node['Index Name']}]"
        nodes.append(f"{'  ' * depth}{label} rows={node.get('Actual Rows')}")
        for child in node.get("Plans", []) or []:
            walk(child, depth + 1)

    walk(plan["Plan"], 0)
    return {
        "planning_ms": round(float(plan.get("Planning Time", 0.0)), 1),
        "execution_ms": round(float(plan["Execution Time"]), 1),
        "plan": nodes,
    }


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------


def run(args: argparse.Namespace) -> dict[str, Any]:
    lab_root = Path(args.lab_root).resolve()
    _install_lab_root(lab_root)
    from lab.prism.share_ledger import PsqlShareLedger  # noqa: E402

    results: dict[str, Any] = {
        "lab_root": str(lab_root),
        "lab_commit": _git_head(lab_root),
        "driver_commit": _git_head(REPO_ROOT),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "switch_interval_ms": sys.getswitchinterval() * 1000,
        "miners": args.miners,
        "reps": args.reps,
        "backends": list(args.backends),
        "sizes": [],
    }
    sizes_for_json = results["sizes"]

    seed_ledger = PsqlShareLedger(
        psql_command=args.psql_command,
        native_client_mode="psql",
        writer_id="bench-seed",
        writer_epoch=1,
        initialize_schema=True,
    )
    try:
        server_version = seed_ledger._run_json(
            "SELECT json_build_object('v', version());"
        )["v"]
    finally:
        pass
    results["postgres"] = server_version

    for share_rows in args.shares:
        print(f"== {share_rows} shares: seeding", flush=True)
        seed_started = time.perf_counter()
        seed_ledger._run_script(seed_sql(share_rows, args.miners))
        seed_seconds = time.perf_counter() - seed_started
        count = int(
            seed_ledger._run_json(
                "SELECT json_build_object('n', count(*)) FROM qbit_share_ledger WHERE accepted;"
            )["n"]
        )
        if count != share_rows:
            raise SystemExit(f"seeded {count} rows, expected {share_rows}")
        anchor_ms, previous_anchor_ms, window_weight = anchors_for(share_rows)
        size_result: dict[str, Any] = {
            "shares": share_rows,
            "seed_seconds": round(seed_seconds, 1),
            "anchor_ms": anchor_ms,
            "window_weight": window_weight,
            "server": {},
            "backends": {},
        }
        sizes_for_json.append(size_result)

        read_shapes: dict[str, Callable[[Any], Callable[[], Any]]] = {
            "bounded": lambda ledger: lambda: ledger.snapshot_at_job_issue(
                anchor_ms, window_weight=window_weight
            ),
            "unbounded": lambda ledger: lambda: ledger.snapshot_at_job_issue(anchor_ms),
            "delta": lambda ledger: lambda: ledger.snapshot_between_job_issues(
                previous_anchor_ms, anchor_ms
            ),
        }

        for backend in args.backends:
            if backend == "native":
                ledger = PsqlShareLedger(
                    psql_command=args.psql_command,
                    database_url=args.database_url,
                    native_client_mode="1",
                    writer_id="bench-read",
                    writer_epoch=1,
                    read_only=True,
                )
            else:
                ledger = PsqlShareLedger(
                    psql_command=args.psql_command,
                    native_client_mode="psql",
                    writer_id="bench-read",
                    writer_epoch=1,
                    read_only=True,
                )
            backend_result: dict[str, Any] = {
                "execution_backend": ledger.execution_backend,
                "reads": {},
            }
            size_result["backends"][backend] = backend_result
            try:
                for shape, make_call in read_shapes.items():
                    call = make_call(ledger)
                    # One warm-up per shape captures the statement (and warms
                    # the buffer cache); measured reps follow.
                    statement, warm = capture_statement(ledger, call)
                    if shape not in size_result["server"] and args.explain:
                        size_result["server"][shape] = {
                            "statement_uses_json_agg": "json_agg" in statement,
                            "statement_sha256_prefix": hashlib.sha256(
                                statement.encode()
                            ).hexdigest()[:12],
                            **explain(seed_ledger, statement),
                        }
                    reps = [timed_call(ledger, call) for _ in range(args.reps)]
                    walls = [rep["wall_ms"] for rep in reps]
                    backend_result["reads"][shape] = {
                        "records": len(warm),
                        "wall_ms_median": round(statistics.median(walls), 1),
                        "wall_ms_max": round(max(walls), 1),
                        "monitor_max_late_ms_max": max(
                            rep["monitor"]["max_late_ms"] for rep in reps
                        ),
                        "monitor_late_over_100ms_total": sum(
                            rep["monitor"]["late_over_100ms"] for rep in reps
                        ),
                        "monitor_late_over_250ms_total": sum(
                            rep["monitor"]["late_over_250ms"] for rep in reps
                        ),
                        "monitor_late_over_500ms_total": sum(
                            rep["monitor"]["late_over_500ms"] for rep in reps
                        ),
                        "reps": reps,
                    }
                    print(
                        f"   {backend:>6} {shape:>9}: records={len(warm)} "
                        f"wall median={statistics.median(walls):.0f}ms "
                        f"max={max(walls):.0f}ms "
                        f"monitor max-late={backend_result['reads'][shape]['monitor_max_late_ms_max']:.0f}ms "
                        f">100ms={backend_result['reads'][shape]['monitor_late_over_100ms_total']} "
                        f">250ms={backend_result['reads'][shape]['monitor_late_over_250ms_total']}",
                        flush=True,
                    )
            finally:
                ledger.close()
        for shape, server in size_result["server"].items():
            print(
                f"   server {shape:>9}: planning={server['planning_ms']}ms "
                f"execution={server['execution_ms']}ms json_agg={server['statement_uses_json_agg']}",
                flush=True,
            )
    seed_ledger.close()
    results["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    return results


def render_comparison(baseline: dict[str, Any], patched: dict[str, Any]) -> str:
    """Markdown tables comparing two result files, size by size."""
    lines: list[str] = []
    lines.append(
        f"Baseline `{(baseline.get('lab_commit') or '?')[:12]}` vs patched "
        f"`{(patched.get('lab_commit') or '?')[:12]}` (driver "
        f"`{(patched.get('driver_commit') or '?')[:12]}`); reps={patched.get('reps')}; "
        f"miners={patched.get('miners')}; {patched.get('postgres', '?')}; "
        f"Python {patched.get('python')}; {patched.get('cpu_count')} CPUs."
    )
    lines.append("")
    by_size_base = {size["shares"]: size for size in baseline["sizes"]}
    for size in patched["sizes"]:
        shares = size["shares"]
        base = by_size_base.get(shares)
        lines.append(f"### {shares:,} shares")
        lines.append("")
        lines.append(
            "| backend | read | records | wall median (ms) base -> patch | wall max (ms) base -> patch "
            "| monitor max lateness (ms) base -> patch | wake-ups > 100 ms base -> patch "
            "| wake-ups > 250 ms base -> patch | wake-ups > 500 ms base -> patch |"
        )
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
        for backend, patched_backend in size["backends"].items():
            base_backend = (base or {}).get("backends", {}).get(backend, {})
            for shape, after in patched_backend["reads"].items():
                before = base_backend.get("reads", {}).get(shape)

                def pair(key: str, fmt: str = "{:.0f}") -> str:
                    if before is None:
                        return "n/a -> " + fmt.format(after[key])
                    return fmt.format(before[key]) + " -> " + fmt.format(after[key])

                lines.append(
                    f"| {backend} | {shape} | {after['records']:,} | {pair('wall_ms_median')} "
                    f"| {pair('wall_ms_max')} | {pair('monitor_max_late_ms_max')} "
                    f"| {pair('monitor_late_over_100ms_total', '{}')} "
                    f"| {pair('monitor_late_over_250ms_total', '{}')} "
                    f"| {pair('monitor_late_over_500ms_total', '{}')} |"
                )
        lines.append("")
        lines.append("| read | server planning+execution (ms) base -> patch | statement uses json_agg base -> patch |")
        lines.append("|---|---:|---|")
        for shape, after in size.get("server", {}).items():
            before = (base or {}).get("server", {}).get(shape)
            after_ms = after["planning_ms"] + after["execution_ms"]
            if before is None:
                lines.append(f"| {shape} | n/a -> {after_ms:.0f} | n/a -> {after['statement_uses_json_agg']} |")
            else:
                before_ms = before["planning_ms"] + before["execution_ms"]
                lines.append(
                    f"| {shape} | {before_ms:.0f} -> {after_ms:.0f} "
                    f"| {before['statement_uses_json_agg']} -> {after['statement_uses_json_agg']} |"
                )
        lines.append("")
        rss_rows = []
        for backend, patched_backend in size["backends"].items():
            base_backend = (base or {}).get("backends", {}).get(backend, {})
            for shape, after in patched_backend["reads"].items():
                before = base_backend.get("reads", {}).get(shape)
                after_rss = max(rep["rss_hwm_mb"] for rep in after["reps"])
                after_cpu = statistics.median(rep["thread_cpu_ms"] for rep in after["reps"])
                if before is None:
                    rss_rows.append(f"| {backend} | {shape} | n/a -> {after_cpu:.0f} | n/a -> {after_rss:.0f} |")
                else:
                    before_rss = max(rep["rss_hwm_mb"] for rep in before["reps"])
                    before_cpu = statistics.median(rep["thread_cpu_ms"] for rep in before["reps"])
                    rss_rows.append(
                        f"| {backend} | {shape} | {before_cpu:.0f} -> {after_cpu:.0f} "
                        f"| {before_rss:.0f} -> {after_rss:.0f} |"
                    )
        lines.append("| backend | read | caller-thread CPU median (ms) base -> patch | process RSS high-water (MB) base -> patch |")
        lines.append("|---|---|---:|---:|")
        lines.extend(rss_rows)
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("BASELINE_JSON", "PATCHED_JSON"),
        help="render a markdown comparison of two --json results and exit (no database needed)",
    )
    parser.add_argument("--psql-command", help="psql invocation for the disposable database")
    parser.add_argument("--database-url", help="DSN for the native (psycopg) backend")
    parser.add_argument("--shares", type=int, nargs="+", default=[210_000, 400_000])
    parser.add_argument("--miners", type=int, default=8, help="distinct miners (skewed like the scale gate)")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--backends", nargs="+", default=["native", "psql"], choices=["native", "psql"])
    parser.add_argument("--lab-root", default=str(REPO_ROOT), help="checkout to import lab.prism.share_ledger from")
    parser.add_argument("--no-explain", dest="explain", action="store_false")
    parser.add_argument("--json", help="write structured results here")
    args = parser.parse_args(argv)
    if args.compare:
        baseline = json.loads(Path(args.compare[0]).read_text())
        patched = json.loads(Path(args.compare[1]).read_text())
        print(render_comparison(baseline, patched))
        return 0
    if not args.psql_command:
        parser.error("--psql-command is required unless --compare is given")
    if "native" in args.backends and not args.database_url:
        parser.error("--database-url is required for the native backend")
    results = run(args)
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, sort_keys=True))
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
