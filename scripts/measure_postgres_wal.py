#!/usr/bin/env python3
"""Measure server-wide WAL for one caller-controlled operation.

Each psql invocation has its own finite timeout; the measurement as a whole
may contain four bounded calls. WAL is server-wide for the bracket interval,
and operation_seconds ends immediately after the operation call. A timeout or
connection loss after an attempted operation leaves side effects uncertain;
this helper never retries or claims rollback.
"""
from __future__ import annotations
import argparse, json, os, subprocess, time
from urllib.parse import urlparse

def psql(dsn: str, sql: str, timeout: float) -> str:
    try:
        env = {"PATH": os.environ.get("PATH", "")}
        return subprocess.run(["psql", dsn, "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", sql], check=True, text=True, capture_output=True, env=env, timeout=timeout).stdout.strip()
    except FileNotFoundError as exc:
        raise RuntimeError("psql executable is unavailable") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("PostgreSQL command failed") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("PostgreSQL command timed out") from exc

def measure(dsn: str, sql: str, source: str, config: str, timeout_seconds: float = 10.0) -> dict:
    if not (0 < timeout_seconds <= 300) or not float(timeout_seconds) == timeout_seconds:
        raise ValueError("timeout_seconds must be finite and between 0 and 300")
    parsed = urlparse(dsn)
    if not sql.strip():
        raise ValueError("SQL operation must not be empty")
    if parsed.query or parsed.password or parsed.hostname not in (None, "localhost", "127.0.0.1", "::1") or not dsn.startswith("postgresql://"):
        raise ValueError("refusing non-local or ambiguous DSN; use explicit loopback target")
    setup = time.monotonic()
    try: start = psql(dsn, "select pg_current_wal_lsn()", timeout_seconds)
    except RuntimeError as exc: raise RuntimeError(f"initial connection failed: {exc}") from exc
    setup_seconds = time.monotonic()-setup
    began = time.monotonic()
    try:
        psql(dsn, sql, timeout_seconds)
    except RuntimeError as exc:
        return {"outcome":"operation_failed", "execution_stage":"operation", "side_effect_status":"unknown", "measurement_scope":"server-wide", "source":source, "config":config, "operation_seconds":time.monotonic()-began, "setup_seconds":setup_seconds, "error":str(exc)}
    operation_seconds = time.monotonic()-began
    try: end = psql(dsn, "select pg_current_wal_lsn()", timeout_seconds)
    except RuntimeError as exc: return {"outcome":"measurement_failed", "operation_status":"succeeded", "measurement_status":"failed", "execution_stage":"post-operation", "measurement_scope":"server-wide", "source":source, "config":config, "operation_seconds":operation_seconds, "setup_seconds":setup_seconds, "error":str(exc)}
    try: delta = float(psql(dsn, f"select pg_wal_lsn_diff('{end}','{start}')", timeout_seconds))
    except RuntimeError as exc: return {"outcome":"measurement_failed", "operation_status":"succeeded", "measurement_status":"failed", "execution_stage":"post-operation", "measurement_scope":"server-wide", "source":source, "config":config, "operation_seconds":operation_seconds, "setup_seconds":setup_seconds, "error":str(exc)}
    return {"outcome":"ok", "operation_status":"succeeded", "measurement_status":"succeeded", "measurement_scope":"server-wide", "source":source, "config":config, "start_lsn":start, "end_lsn":end, "wal_bytes":delta, "operation_seconds":operation_seconds, "setup_seconds":setup_seconds}

def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("--dsn", required=True); ap.add_argument("--sql", required=True); ap.add_argument("--source", default="caller-operation"); ap.add_argument("--config", required=True); ap.add_argument("--timeout-seconds", type=float, default=10.0)
    try: result=measure(**vars(ap.parse_args()))
    except RuntimeError as exc: print(json.dumps({"outcome":"connection_failed", "error":str(exc)})); return 1
    except ValueError as exc: print(json.dumps({"outcome":"invalid_input", "error":str(exc)})); return 1
    print(json.dumps(result, sort_keys=True)); return 0 if result["outcome"] == "ok" else 1
if __name__ == "__main__": raise SystemExit(main())
