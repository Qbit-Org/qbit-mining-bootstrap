#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-throughput-pg-$$}"
SHARE_COUNT="${QBIT_PRISM_THROUGHPUT_SHARES:-20000}"
MIN_SHARES_PER_SECOND="${QBIT_PRISM_MIN_SHARES_PER_SEC:-0}"
REPORT_PATH="${QBIT_PRISM_THROUGHPUT_REPORT:-${ROOT_DIR}/build/prism-postgres-throughput.json}"

require_executable() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required executable: $1" >&2
    exit 1
  }
}

cleanup() {
  docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_executable docker
require_executable python3

docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
docker run \
  --rm \
  --detach \
  --name "${POSTGRES_CONTAINER}" \
  -e POSTGRES_USER=qbit \
  -e POSTGRES_PASSWORD=qbit \
  -e POSTGRES_DB=qbit \
  "${POSTGRES_IMAGE}" >/dev/null

deadline=$((SECONDS + 60))
until docker exec "${POSTGRES_CONTAINER}" pg_isready -U qbit -d qbit >/dev/null 2>&1; do
  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "timed out waiting for PRISM Postgres container" >&2
    docker logs "${POSTGRES_CONTAINER}" >&2 || true
    exit 1
  fi
  sleep 1
done

mkdir -p "$(dirname "${REPORT_PATH}")"

(
  cd "${ROOT_DIR}"
  POSTGRES_CONTAINER="${POSTGRES_CONTAINER}" \
  SHARE_COUNT="${SHARE_COUNT}" \
  MIN_SHARES_PER_SECOND="${MIN_SHARES_PER_SECOND}" \
  REPORT_PATH="${REPORT_PATH}" \
    python3 <<'PY'
from __future__ import annotations

import json
import os
import platform
import subprocess
import time
from pathlib import Path


ROOT_DIR = Path.cwd()
SCHEMA_PATH = ROOT_DIR / "crates/qbit-prism/sql/001_share_ledger.sql"
container = os.environ["POSTGRES_CONTAINER"]
share_count = int(os.environ["SHARE_COUNT"])
min_shares_per_second = float(os.environ["MIN_SHARES_PER_SECOND"])
report_path = Path(os.environ["REPORT_PATH"])

if share_count <= 0:
    raise SystemExit("QBIT_PRISM_THROUGHPUT_SHARES must be positive")
if min_shares_per_second < 0:
    raise SystemExit("QBIT_PRISM_MIN_SHARES_PER_SEC must be non-negative")


def psql(sql: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container,
            "psql",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--tuples-only",
            "--no-align",
            "--quiet",
            "-U",
            "qbit",
            "-d",
            "qbit",
        ],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "psql command failed "
            f"(exit {completed.returncode}): {completed.stderr.strip()}"
        )
    return completed.stdout


psql(SCHEMA_PATH.read_text(encoding="utf-8"))
postgres_version = psql("SELECT version();").strip()

insert_sql = f"""
TRUNCATE qbit_share_ledger RESTART IDENTITY CASCADE;
INSERT INTO qbit_share_ledger (
    share_id,
    miner_id,
    payout_order_key,
    p2mr_program,
    share_difficulty,
    network_difficulty,
    template_height,
    job_id,
    job_issued_at,
    ntime,
    accepted_at,
    accepted,
    writer_id,
    writer_epoch
)
SELECT
    'bench-' || gs::text,
    'miner-' || (gs % 1024)::text,
    lpad((gs % 1024)::text, 8, '0'),
    decode(lpad(to_hex(gs), 64, '0'), 'hex'),
    1 + (gs % 4096),
    1000000,
    1000 + (gs / 1000),
    'job-' || (gs / 1000)::text,
    to_timestamp(1700000000) + make_interval(secs => gs::double precision / 1000.0),
    1700000000 + gs,
    to_timestamp(1700000000) + make_interval(secs => gs::double precision / 1000.0),
    true,
    'throughput',
    1
FROM generate_series(1, {share_count}) AS gs;
"""

started = time.perf_counter()
psql(insert_sql)
insert_seconds = time.perf_counter() - started
observed_count = int(psql("SELECT count(*) FROM qbit_share_ledger WHERE accepted;").strip())
shares_per_second = observed_count / max(insert_seconds, 0.001)

anchor_seconds = 1700000000 + (share_count / 1000.0)
explain_sql = f"""
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT *
FROM qbit_audit_share_window(to_timestamp({anchor_seconds:.3f}), 100000::numeric);
"""
explain_text = psql(explain_sql).strip()

report = {
    "schema": "qbit.prism.postgres-throughput.v1",
    "share_count": observed_count,
    "insert_seconds": insert_seconds,
    "shares_per_second": shares_per_second,
    "min_shares_per_second": min_shares_per_second,
    "passed_minimum": shares_per_second >= min_shares_per_second,
    "postgres_version": postgres_version,
    "machine": {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    },
    "audit_window_explain_analyze": explain_text,
}
report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(
    "prism postgres throughput "
    f"shares={observed_count} seconds={insert_seconds:.3f} "
    f"shares_per_second={shares_per_second:.1f} report={report_path}"
)
if shares_per_second < min_shares_per_second:
    raise SystemExit(
        "Postgres throughput below QBIT_PRISM_MIN_SHARES_PER_SEC: "
        f"{shares_per_second:.1f} < {min_shares_per_second:.1f}"
    )
PY
)
