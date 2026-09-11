#!/usr/bin/env python3
"""Non-discovered public read replica integration gate (issue #145).

Invoked by ``test/test-prism-public-read-replica.sh``. Lives outside unittest
discovery because it requires an explicitly provisioned primary plus streaming
hot-standby replica. Proves, against real containers (the #134 pattern):

- the extracted public read service serves a moved endpoint with data that
  reached it by streaming replication, and publishes the observed lag;
- the replica contract is enforced rather than described: a replication stream
  silent past the configured bound answers 503, and a backing server that is
  not in recovery is refused outright;
- the contract is scoped by what a route actually reads -- the environment-only
  mining-configuration route keeps serving 200 while replica-backed routes
  refuse.

The unit-level route split and the freshness rules under a fake clock live in
tests/test_prism_public_read_service.py; this gate proves the container-side
serving contract those tests model.
"""

from __future__ import annotations

import contextlib
import json
import os
import shlex
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from lab.prism import public_read_service

PRIMARY_PSQL_COMMAND = os.environ.get("GATE_PRIMARY_PSQL_COMMAND", "")
REPLICA_PSQL_COMMAND = os.environ.get("GATE_REPLICA_PSQL_COMMAND", "")
if not PRIMARY_PSQL_COMMAND or not REPLICA_PSQL_COMMAND:
    raise SystemExit(
        "GATE_PRIMARY_PSQL_COMMAND and GATE_REPLICA_PSQL_COMMAND are required"
    )

BLOCK_HASH = "aa" * 32


class GateFailure(RuntimeError):
    pass


def assert_equal(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise GateFailure(f"{message}: expected {expected!r}, got {actual!r}")


def http_get(url: str) -> tuple[int, object, dict[str, str]]:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read()
            return (
                response.status,
                json.loads(body) if body else None,
                {k.lower(): v for k, v in response.headers.items()},
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        return (
            exc.code,
            json.loads(body) if body else None,
            {k.lower(): v for k, v in exc.headers.items()},
        )


def http_get_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read().decode()


def run_psql(command: str, sql: str) -> None:
    process = subprocess.run(
        shlex.split(command) + ["-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if process.returncode != 0:
        raise GateFailure(
            f"psql failed ({process.returncode}): {process.stderr.strip()}"
        )


def psql_scalar(command: str, sql: str) -> str | None:
    """Scalar result, or None when the query could not run.

    None rather than an exception because callers poll for state that is still
    replicating: before the DDL replays, the query fails with "relation does
    not exist", which is a "not yet", not a gate failure.
    """
    process = subprocess.run(
        shlex.split(command) + ["-tA", "-c", sql],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if process.returncode != 0:
        return None
    return process.stdout.strip()


def wait_for_replica_row(deadline_seconds: float = 90.0) -> None:
    """Poll the standby until the row arrives, tolerating the schema's absence.

    Both the schema and the row reach the standby by streaming replication --
    the shell wrapper starts the replica before this gate runs, so the standby
    was cloned from a primary that had neither. Until the CREATE TABLE replays,
    the count query fails rather than returning 0, so a failed query has to be
    treated as "not yet" inside the deadline instead of aborting the gate.
    """
    deadline = time.monotonic() + deadline_seconds
    last_seen: str | None = None
    while time.monotonic() < deadline:
        last_seen = psql_scalar(
            REPLICA_PSQL_COMMAND,
            "SELECT count(*) FROM qbit_pool_blocks "
            f"WHERE block_hash = '{BLOCK_HASH}'",
        )
        if last_seen == "1":
            return
        time.sleep(0.5)
    raise GateFailure(
        f"replica did not replay the qbit_pool_blocks row within "
        f"{deadline_seconds}s (last count query returned {last_seen!r})"
    )


GATE_ENVIRONMENT = {
    "QBIT_RPC_HOST": "127.0.0.1",
    "QBIT_RPC_PORT": "18452",
    "QBIT_RPC_USER": "gate",
    "QBIT_RPC_PASSWORD": "gate",
    # Required at startup: this process runs no Stratum listener.
    "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://gate.invalid:3340",
    # psql transport, because the DSN is a `docker exec` into the container.
    "PRISM_POSTGRES_NATIVE_CLIENT": "0",
    "PRISM_DATABASE_URL": "",
    "PRISM_PUBLIC_REPLICA_MODE": "require",
}


@contextlib.contextmanager
def serving(**overrides: str):
    """Build the real service from the environment and serve it on a free port.

    Goes through build_service() rather than assembling the collaborators by
    hand: the point of a container gate is that the process an operator starts
    is the process under test, including every refusal build_service makes.
    """
    audit_dir = Path(tempfile.mkdtemp(prefix="prism-public-read-gate-"))
    environment = dict(GATE_ENVIRONMENT)
    environment["PRISM_AUDIT_DIR"] = str(audit_dir)
    environment["PRISM_POSTGRES_PSQL_COMMAND"] = REPLICA_PSQL_COMMAND
    environment.update(overrides)

    previous = dict(os.environ)
    os.environ.update(environment)
    try:
        coordinator, readiness, metrics, replica = public_read_service.build_service()
    finally:
        os.environ.clear()
        os.environ.update(previous)

    readiness.start()
    service = public_read_service.PublicReadService(
        coordinator,
        metrics=metrics,
        readiness=readiness,
        replica=replica,
    )
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), public_read_service.make_handler(service)
    )
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        readiness.stop()
        thread.join(timeout=5)


def check_serves_replicated_data() -> None:
    with serving() as base_url:
        status, payload, _ = http_get(f"{base_url}/healthz")
        assert_equal(status, 200, "health status")
        assert_equal(payload["ok"], True, "health ok")
        assert_equal(payload["replica"]["in_recovery"], True, "in_recovery")
        if payload["replica"]["receiver_heartbeat_age_seconds"] is None:
            raise GateFailure("health reported no walreceiver heartbeat")

        status, payload, headers = http_get(f"{base_url}/public/v1/blocks")
        assert_equal(status, 200, "blocks status")
        assert_equal(payload["schema"], "prism.dashboard.blocks.v1", "blocks schema")
        assert_equal(payload["pagination"]["total_count"], 1, "blocks total_count")
        assert_equal(payload["rows"][0]["hash"], BLOCK_HASH, "blocks row hash")
        if public_read_service.REPLICA_LAG_HEADER.lower() not in headers:
            raise GateFailure(
                f"public response missing {public_read_service.REPLICA_LAG_HEADER}"
            )

        metrics = http_get_text(f"{base_url}/metrics")
        for gauge in (
            "qbit_prism_public_replica_in_recovery 1",
            "qbit_prism_public_replica_heartbeat_age_seconds",
            "qbit_prism_public_replica_replay_lag_seconds",
            "qbit_prism_public_replica_apply_backlog_bytes",
        ):
            if gauge not in metrics:
                raise GateFailure(f"metrics missing {gauge!r}")


def check_refuses_past_the_bound() -> None:
    """Any live heartbeat is older than this bound, so every probe fails it."""

    with serving(PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS="0.0001") as base_url:
        status, payload, _ = http_get(f"{base_url}/public/v1/blocks")
        assert_equal(status, 503, "degraded blocks status")
        assert_equal(payload["error"]["code"], "upstream_unavailable", "degraded code")
        if "silent for" not in payload["error"]["message"]:
            raise GateFailure(f"unexpected degraded message: {payload}")

        status, _, _ = http_get(f"{base_url}/healthz")
        assert_equal(status, 503, "degraded health status")

        # Scoped by what the route reads: mining-configuration is assembled
        # from the environment and touches no read slot, so the replica being
        # stale is not a reason to refuse it.
        status, payload, _ = http_get(f"{base_url}/public/v1/mining-configuration")
        assert_equal(status, 200, "mining-configuration under a stale replica")


def check_refuses_a_writable_primary() -> None:
    with serving(PRISM_POSTGRES_PSQL_COMMAND=PRIMARY_PSQL_COMMAND) as base_url:
        status, payload, _ = http_get(f"{base_url}/public/v1/blocks")
        assert_equal(status, 503, "primary refusal status")
        if "not in recovery" not in payload["error"]["message"]:
            raise GateFailure(f"unexpected primary refusal message: {payload}")

        status, _, _ = http_get(f"{base_url}/healthz")
        assert_equal(status, 503, "primary refusal health status")


def main() -> int:
    # The schema is created on the primary *after* the standby was cloned --
    # the shell wrapper brings the replica up before invoking this gate -- so
    # both the DDL and the row below reach the standby by streaming, which is
    # what wait_for_replica_row polls for.
    run_psql(
        PRIMARY_PSQL_COMMAND,
        Path("crates/qbit-prism/sql/001_share_ledger.sql").read_text(encoding="utf-8"),
    )
    run_psql(
        PRIMARY_PSQL_COMMAND,
        f"""
INSERT INTO qbit_pool_blocks
    (block_hash, block_height, parent_hash, coinbase_txid,
     payout_manifest_sha256, chain_state)
VALUES ('{BLOCK_HASH}', 101, '{"bb" * 32}', '{"cc" * 32}',
        '{"dd" * 32}', 'confirmed')
ON CONFLICT (block_hash) DO NOTHING;
""",
    )
    wait_for_replica_row()

    check_serves_replicated_data()
    check_refuses_past_the_bound()
    check_refuses_a_writable_primary()

    print("public read replica gate: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
