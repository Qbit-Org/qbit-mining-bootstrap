#!/usr/bin/env python3
"""Measure server-wide WAL generated during one caller-controlled operation."""
from __future__ import annotations
import argparse, json, os, subprocess, time
from urllib.parse import urlparse

def psql(dsn: str, sql: str) -> str:
    try:
        env = {"PATH": os.environ.get("PATH", "")}
        return subprocess.run(["psql", dsn, "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", sql], check=True, text=True, capture_output=True, env=env).stdout.strip()
    except FileNotFoundError as exc:
        raise RuntimeError("psql executable is unavailable") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("PostgreSQL command failed") from exc

def measure(dsn: str, sql: str, source: str, config: str) -> dict:
    parsed = urlparse(dsn)
    if not sql.strip():
        raise ValueError("SQL operation must not be empty")
    if parsed.query or parsed.password or parsed.hostname not in (None, "localhost", "127.0.0.1", "::1") or not dsn.startswith("postgresql://"):
        raise ValueError("refusing non-local or ambiguous DSN; use explicit loopback target")
    setup = time.monotonic()
    try: start = psql(dsn, "select pg_current_wal_lsn()")
    except RuntimeError as exc: raise RuntimeError(f"initial connection failed: {exc}") from exc
    setup_seconds = time.monotonic()-setup
    began = time.monotonic()
    try:
        psql(dsn, sql)
    except RuntimeError as exc:
        return {"outcome":"operation_failed", "execution_stage":"operation", "side_effect_status":"unknown", "measurement_scope":"server-wide", "source":source, "config":config, "operation_seconds":time.monotonic()-began, "setup_seconds":setup_seconds, "error":str(exc)}
    try: end = psql(dsn, "select pg_current_wal_lsn()")
    except RuntimeError as exc: return {"outcome":"measurement_failed", "operation_status":"succeeded", "measurement_status":"failed", "execution_stage":"post-operation", "measurement_scope":"server-wide", "source":source, "config":config, "operation_seconds":time.monotonic()-began, "setup_seconds":setup_seconds, "error":str(exc)}
    try: delta = float(psql(dsn, f"select pg_wal_lsn_diff('{end}','{start}')"))
    except RuntimeError as exc: return {"outcome":"measurement_failed", "operation_status":"succeeded", "measurement_status":"failed", "execution_stage":"post-operation", "measurement_scope":"server-wide", "source":source, "config":config, "operation_seconds":time.monotonic()-began, "setup_seconds":setup_seconds, "error":str(exc)}
    return {"outcome":"ok", "operation_status":"succeeded", "measurement_status":"succeeded", "measurement_scope":"server-wide", "source":source, "config":config, "start_lsn":start, "end_lsn":end, "wal_bytes":delta, "operation_seconds":time.monotonic()-began, "setup_seconds":setup_seconds}

def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("--dsn", required=True); ap.add_argument("--sql", required=True); ap.add_argument("--source", default="caller-operation"); ap.add_argument("--config", required=True)
    try: result=measure(**vars(ap.parse_args()))
    except RuntimeError as exc: print(json.dumps({"outcome":"connection_failed", "error":str(exc)})); return 1
    except ValueError as exc: print(json.dumps({"outcome":"invalid_input", "error":str(exc)})); return 1
    print(json.dumps(result, sort_keys=True)); return 0 if result["outcome"] == "ok" else 1
if __name__ == "__main__": raise SystemExit(main())
