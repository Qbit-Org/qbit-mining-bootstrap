#!/usr/bin/env python3
"""Exercise the exact D3 SQL on disposable asynchronous PostgreSQL 16 clusters.

Requires PyYAML, PostgreSQL server tools, and promtool. The recorded SQL samples
feed PromQL at the required one-second cadence; exporter status is controlled by
the harness, not measured from a running postgres_exporter. Never connects to a
deployed database. Optional --report writes the observations and assertion count.
"""

import argparse
import getpass
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin-dir", type=Path, default=os.environ.get("PRISM_TEST_PG_BIN_DIR"))
    parser.add_argument("--promtool", default=shutil.which("promtool"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not args.pg_bin_dir or not args.promtool:
        parser.error("provide --pg-bin-dir (or PRISM_TEST_PG_BIN_DIR) and --promtool")
    query = yaml.safe_load((ROOT / "docs/prism-postgres-exporter-queries.yaml").read_text())["pg_stat_replication"]["query"].strip().rstrip(";")

    def run(binary, *arguments):
        result = subprocess.run([str(args.pg_bin_dir / binary), *map(str, arguments)],
                                env={**os.environ, "LC_ALL": "C"}, capture_output=True,
                                text=True, timeout=45)
        if result.returncode:
            raise RuntimeError(f"{binary}: {result.stdout}\n{result.stderr}")
        return result.stdout.strip()

    def sql(port, statement):
        return run("psql", "-X", "-h", "127.0.0.1", "-p", port, "-d", "postgres",
                   "-v", "ON_ERROR_STOP=1", "-At", "-c", statement)

    with tempfile.TemporaryDirectory(prefix="prism-d3-sql-") as directory:
        root = Path(directory)
        primary, standby = root / "primary", root / "standby"
        primary_port, standby_port = free_port(), free_port()
        while standby_port == primary_port:
            standby_port = free_port()
        active = []
        samples = []
        cases = []
        started = time.monotonic()

        def observe():
            row = json.loads(sql(primary_port, f"SELECT row_to_json(r) FROM ({query}) r"))
            raw = sql(primary_port, "SELECT EXTRACT(EPOCH FROM replay_lag) FROM pg_stat_replication WHERE application_name='prism_standby_1'")
            samples.append({"elapsed_seconds": round(time.monotonic() - started, 3),
                            "metrics": row, "raw_replay_lag": float(raw) if raw else None})
            return row

        def collect(seconds):
            for _ in range(seconds):
                tick = time.monotonic()
                observe()
                time.sleep(max(0, 1 - (time.monotonic() - tick)))

        def checkpoint(name, expected, alerts=None):
            cases.append({"name": name, "sample": len(samples) - 1,
                          "expect": expected, "alerts": alerts})
            print(f"{name}: {samples[-1]}", flush=True)

        def start(data, port):
            options = (f"-h 127.0.0.1 -p {port} -k {root} -c wal_level=replica "
                       "-c max_wal_senders=4 -c synchronous_standby_names= "
                       "-c wal_receiver_status_interval=1s -c checkpoint_timeout=1h -c autovacuum=off")
            run("pg_ctl", "-D", data, "-l", root / (data.name + ".log"), "-o", options, "-w", "start")
            active.append(data)

        try:
            run("initdb", "-D", primary, "-A", "trust", "--no-locale", "-E", "UTF8")
            start(primary, primary_port)
            run("pg_basebackup", "-D", standby, "-d",
                f"host=127.0.0.1 port={primary_port} user={getpass.getuser()} application_name=prism_standby_1",
                "-R", "-X", "stream", "-c", "fast")
            start(standby, standby_port)
            sql(primary_port, "CREATE TABLE d3_probe(id integer); INSERT INTO d3_probe VALUES (1)")
            for _ in range(30):
                row = observe()
                if row["count"] == 1 and row["replay_lag"] == 0 and samples[-1]["raw_replay_lag"] is None:
                    break
                time.sleep(1)
            else:
                raise AssertionError("standby never became caught-up/idle with NULL raw replay_lag")
            assert sql(primary_port, "SELECT sync_state FROM pg_stat_replication WHERE application_name='prism_standby_1'") == "async"
            samples.clear()
            collect(7)
            checkpoint("SQL healthy idle NULL lag has caught-up WAL proof", (0, 0), (False, False))
            sql(standby_port, "SELECT pg_wal_replay_pause()")
            sql(primary_port, "INSERT INTO d3_probe VALUES (2)")
            collect(4)
            checkpoint("SQL recent write remains inside five-second allowance", (0, 0), (False, False))
            collect(3)
            checkpoint("SQL paused replay exceeds allowance but not dwell", (1, 0), (False, False))
            collect(62)
            checkpoint("SQL paused replay passes one-minute dwell", (1, 0), (True, False))
            sql(standby_port, "SELECT pg_wal_replay_resume()")
            for _ in range(15):
                collect(1)
                if samples[-1]["metrics"]["replay_lag"] == 0:
                    break
            assert samples[-1]["metrics"]["replay_lag"] == 0
            checkpoint("SQL resumed replay is caught up despite old reported latency", (0, 0), (False, False))
            # The same primary-only query must fail on a recovery server, rather
            # than silently produce a healthy zero when deployed to the wrong role.
            try:
                sql(standby_port, query)
            except RuntimeError as error:
                assert "recovery" in str(error), error
            else:
                raise AssertionError("primary query unexpectedly succeeded during recovery")
            run("pg_ctl", "-D", standby, "-m", "immediate", "-w", "stop")
            active.remove(standby)
            collect(2)
            row = samples[-1]["metrics"]
            assert row["count"] == 0 and row["replay_lsn_hi"] == -1 and row["replay_lag"] == -1
            checkpoint("SQL disconnected standby is unknown and unavailable", (1, 1))

            target = 'job="qbit-postgres-primary",instance="primary",network="mainnet"'
            labels = target + ',application_name="prism_standby_1"'
            series = {name + "{" + target + "}": value for name, value in
                      [("up", 1), ("pg_up", 1), ("pg_exporter_last_scrape_error", 0)]}
            for name in samples[0]["metrics"]:
                if name != "application_name":
                    series["pg_stat_replication_" + name + "{" + labels + "}"] = " ".join(
                        str(sample["metrics"][name]) for sample in samples)
            titles = ["PrismHAStandbyReplayLagHigh", "PrismHAStandbyDisconnected"]
            scenarios = [{"name": case["name"], "series": series, "eval_time": f"{case['sample']}s",
                          "expect": dict(zip(titles, case["expect"]))} for case in cases]
            for scenario, case in zip(scenarios, cases):
                if case["alerts"] is not None:
                    scenario["alerts"] = dict(zip(titles, case["alerts"]))
            fixture = root / "scenarios.json"
            fixture.write_text(json.dumps(scenarios))
            subprocess.run([sys.executable, str(ROOT / "scripts/test_prism_alert_promql.py"),
                            "--promtool", args.promtool, "--scenarios", str(fixture)], check=True)
            if args.report:
                args.report.write_text(json.dumps({"postgres": run("postgres", "--version"),
                                                   "samples": samples, "cases": cases,
                                                   "exporter": "not started; status inputs controlled by harness",
                                                   "wrong_role_query": "failed as expected"}, indent=2) + "\n")
            print("D3 SQL and derived PromQL passed; disposable clusters cleaned on exit", flush=True)
        finally:
            for data in reversed(active):
                run("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop")


if __name__ == "__main__":
    main()
