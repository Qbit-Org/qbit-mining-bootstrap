#!/usr/bin/env bash
# Regression test for the carry-forward summary seed guard (#124).
#
# Reproduces the poisoning mechanism against a real Postgres: a per-statement
# autocommit schema apply commits the carry-forward summary sync triggers
# before the seeding block at the end of the file runs, a still-running writer
# confirms a block in that gap, and the summary ends up holding only the
# post-trigger delta. The old emptiness-based guard then treats that partial
# summary as already seeded and locks the damage in permanently. The hardened
# guard compares the summary against the carry history it summarizes and
# repairs it on the next apply, on both the psql-subprocess and the native
# psycopg apply paths. A second scenario proves the other half of the fix:
# a failing schema apply commits nothing on either path (the script wraps
# itself in one BEGIN/COMMIT transaction, and the psql backend additionally
# passes --single-transaction).
#
# Requires docker and a host python3 with psycopg installed
# (python3 -m pip install 'psycopg[binary]').
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-seed-guard-pg-$$}"

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

# Both apply paths are exercised, so the native psycopg client is mandatory
# here rather than opportunistic.
python3 -c 'import psycopg' 2>/dev/null || {
  echo "host python3 lacks psycopg; install with: python3 -m pip install 'psycopg[binary]'" >&2
  exit 1
}

docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
docker run \
  --rm \
  --detach \
  --name "${POSTGRES_CONTAINER}" \
  -p 127.0.0.1:0:5432 \
  -e POSTGRES_USER=qbit \
  -e POSTGRES_PASSWORD=qbit \
  -e POSTGRES_DB=qbit \
  "${POSTGRES_IMAGE}" >/dev/null

# The official image runs initdb against a temporary server that binds no TCP
# address, so a socket-only `pg_isready -U qbit -d qbit` reports ready during
# bootstrap and races the restart into the durable server. Gate on the TCP
# listener instead, and confirm with a real query rather than trusting the
# liveness probe alone.
deadline=$((SECONDS + 60))
until docker exec "${POSTGRES_CONTAINER}" \
    pg_isready -h 127.0.0.1 -U qbit -d qbit >/dev/null 2>&1 \
  && echo 'SELECT 1;' | docker exec -i "${POSTGRES_CONTAINER}" \
    psql --no-psqlrc --set ON_ERROR_STOP=1 -U qbit -d qbit >/dev/null 2>&1; do
  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "timed out waiting for PRISM Postgres container" >&2
    docker logs "${POSTGRES_CONTAINER}" >&2 || true
    exit 1
  fi
  sleep 1
done

HOST_PORT="$(docker port "${POSTGRES_CONTAINER}" 5432/tcp | head -n 1 | awk -F: '{print $NF}')"
if [[ -z "${HOST_PORT}" ]]; then
  echo "unable to resolve published Postgres port" >&2
  exit 1
fi
DATABASE_URL="postgresql://qbit:qbit@127.0.0.1:${HOST_PORT}/qbit"

# The gate above ran inside the container; the native apply path connects over
# the published port from the host, so prove that reaches the same server.
deadline=$((SECONDS + 60))
until python3 - "$DATABASE_URL" <<'PY' >/dev/null 2>&1
import sys
import psycopg

with psycopg.connect(sys.argv[1], connect_timeout=3) as conn:
    conn.execute("SELECT 1")
PY
do
  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "timed out waiting for published PRISM Postgres port" >&2
    docker logs "${POSTGRES_CONTAINER}" >&2 || true
    exit 1
  fi
  sleep 1
done

(
  cd "${ROOT_DIR}"
  PRISM_TEST_ROOT_DIR="${ROOT_DIR}" \
  PRISM_TEST_POSTGRES_CONTAINER="${POSTGRES_CONTAINER}" \
  PRISM_TEST_POSTGRES_PORT="${HOST_PORT}" \
    python3 <<'PY'
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT_DIR = os.environ["PRISM_TEST_ROOT_DIR"]
sys.path.insert(0, ROOT_DIR)

from lab.prism.share_ledger import PsqlShareLedger

CONTAINER = os.environ["PRISM_TEST_POSTGRES_CONTAINER"]
HOST_PORT = os.environ["PRISM_TEST_POSTGRES_PORT"]
SCHEMA_PATH = Path(ROOT_DIR) / "crates" / "qbit-prism" / "sql" / "001_share_ledger.sql"

# The hardened guard's first comment line. Splitting the schema here yields a
# head that installs every table, function and carry-forward summary sync
# trigger but no seeding block at all -- exactly the state a non-atomic apply
# leaves behind when it is interrupted between the triggers and the seed.
SEED_GUARD_MARKER = (
    "-- Seed or repair the summary from carry history whenever the summary"
)

# The pre-#124 guard, verbatim. It seeds only when the summary is completely
# empty, so a partially populated summary reads as "already seeded".
OLD_SEED_GUARD = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM qbit_payout_carry_forward_current)
       AND EXISTS (
           SELECT 1
           FROM qbit_payout_carry_forward carry
           JOIN qbit_pool_blocks block
             ON block.block_hash = carry.block_hash
           WHERE carry.maturity_state <> 'reversed'
             AND block.chain_state = 'confirmed'
             AND block.maturity_state <> 'reversed'
       )
    THEN
        PERFORM qbit_rebuild_carry_forward_current_balances();
    END IF;
END
$$;
"""

CONFIRMED_BLOCK_HASH = "aa" * 32
PREPARED_BLOCK_HASH = "ee" * 32


def assert_equal(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise SystemExit(f"{message}: expected {expected!r}, got {actual!r}")


def psql_command(database: str) -> str:
    return f"docker exec -i {CONTAINER} psql -U qbit -d {database}"


def database_url(database: str) -> str:
    return f"postgresql://qbit:qbit@127.0.0.1:{HOST_PORT}/{database}"


def _psql(sql: str, *, database: str, extra_args: list[str]) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            CONTAINER,
            "psql",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            *extra_args,
            "-U",
            "qbit",
            "-d",
            database,
        ],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"psql on {database} failed (exit {completed.returncode}): "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout


def psql_autocommit_apply(sql: str, *, database: str = "qbit") -> None:
    """Apply SQL with no --single-transaction: one commit per statement.

    This is the operator error the old ops doc allowed, and the shape that
    poisons the summary seed.
    """
    _psql(sql, database=database, extra_args=[])


def psql_json(sql: str, *, database: str = "qbit") -> object:
    output = _psql(
        sql,
        database=database,
        extra_args=["--tuples-only", "--no-align", "--quiet"],
    ).strip()
    if not output:
        raise SystemExit(f"psql query on {database} returned no JSON")
    return json.loads(output.splitlines()[-1])


def summary_rows(*, database: str = "qbit") -> object:
    return psql_json(
        """
SELECT COALESCE(json_agg(json_build_object(
    'miner_id', miner_id,
    'balance_sats', balance_sats::text,
    'active_row_count', active_row_count
) ORDER BY miner_id), '[]'::json)
FROM qbit_payout_carry_forward_current;
""",
        database=database,
    )


def reported_balances(*, database: str = "qbit") -> object:
    return psql_json(
        """
SELECT COALESCE(json_agg(json_build_object(
    'miner_id', miner_id,
    'balance_sats', balance_sats::text
) ORDER BY miner_id), '[]'::json)
FROM qbit_current_carry_forward_balances();
""",
        database=database,
    )


POISONED_SUMMARY = [
    {"miner_id": "miner-b", "balance_sats": "500", "active_row_count": 1}
]
REPAIRED_SUMMARY = [
    {"miner_id": "miner-a", "balance_sats": "1000", "active_row_count": 1},
    {"miner_id": "miner-b", "balance_sats": "500", "active_row_count": 1},
]
REPAIRED_BALANCES = [
    {"miner_id": "miner-a", "balance_sats": "1000"},
    {"miner_id": "miner-b", "balance_sats": "500"},
]


def assert_repaired(ledger: PsqlShareLedger, label: str) -> None:
    assert_equal(summary_rows(), REPAIRED_SUMMARY, f"{label}: summary rebuilt from history")
    assert_equal(
        reported_balances(),
        REPAIRED_BALANCES,
        f"{label}: reported balances no longer under-report the pre-upgrade carry",
    )
    assert_equal(
        ledger._run_json(
            """
SELECT json_build_object(
    'drift_rows', (SELECT count(*) FROM qbit_carry_forward_current_drift()),
    'balance_rows', (SELECT count(*) FROM qbit_current_carry_forward_balances())
);
"""
        ),
        {"drift_rows": 0, "balance_rows": 2},
        f"{label}: summary agrees with the recomputation it summarizes",
    )


# --------------------------------------------------------------------------
# Scenario A: poison via a non-atomic apply, old guard locks it in, a new
# apply repairs it on both execution backends.
# --------------------------------------------------------------------------

schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
schema_lines = schema_text.splitlines(keepends=True)
marker_indexes = [
    index
    for index, line in enumerate(schema_lines)
    if line.rstrip("\n") == SEED_GUARD_MARKER
]
assert_equal(len(marker_indexes), 1, "seed guard marker comment appears exactly once")
marker_index = marker_indexes[0]
schema_head = "".join(schema_lines[:marker_index])
schema_tail = "".join(schema_lines[marker_index:])
if "CREATE TRIGGER qbit_pool_blocks_carry_forward_current_sync" not in schema_head:
    raise SystemExit("schema head is missing the carry-forward summary sync triggers")
if "CREATE OR REPLACE FUNCTION qbit_carry_forward_current_drift" not in schema_head:
    raise SystemExit("schema head is missing the drift function the guard depends on")
if "qbit_rebuild_carry_forward_current_balances" not in schema_tail:
    raise SystemExit("schema tail is missing the seed/repair guard")

# The script wraps itself in one BEGIN/COMMIT transaction (that wrapper is
# half of the #124 fix). The interrupted-apply reproduction must strip the
# leading BEGIN so the head really does autocommit statement by statement,
# exactly like the pre-fix schema file did under a plain `psql -f`.
head_lines = schema_head.splitlines(keepends=True)
begin_indexes = [
    index for index, line in enumerate(head_lines) if line.rstrip("\n") == "BEGIN;"
]
assert_equal(len(begin_indexes), 1, "schema head contains exactly one leading BEGIN")
unwrapped_head = "".join(head_lines[begin_indexes[0] + 1 :])

# 1. The interrupted operator apply: every statement commits on its own and
#    the run stops before the seeding block at the end of the file.
psql_autocommit_apply(unwrapped_head)

# 2. Pre-upgrade carry history that never flowed through the summary triggers.
#    session_replication_role = replica suppresses them for this session the
#    way history written before they existed never reached them. It also
#    suppresses the BEFORE trigger that assigns the publication ordinal, so
#    the confirmed row carries one explicitly to satisfy the CHECK constraint.
psql_autocommit_apply(
    """
SET session_replication_role = replica;

INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state,
    audit_publication_sequence
) VALUES (
    '"""
    + CONFIRMED_BLOCK_HASH
    + """',
    100,
    '"""
    + "00" * 32
    + """',
    '"""
    + "ab" * 32
    + """',
    '"""
    + "ac" * 32
    + """',
    'confirmed',
    nextval('qbit_audit_publication_sequence_seq')
);

INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state
) VALUES (
    '"""
    + PREPARED_BLOCK_HASH
    + """',
    101,
    '"""
    + CONFIRMED_BLOCK_HASH
    + """',
    '"""
    + "ef" * 32
    + """',
    '"""
    + "e0" * 32
    + """',
    'prepared'
);

INSERT INTO qbit_payout_carry_forward (
    block_height,
    block_hash,
    miner_id,
    payout_order_key,
    p2mr_program,
    gross_amount_sats,
    prior_balance_sats,
    candidate_balance_sats,
    onchain_amount_sats,
    carry_forward_balance_sats,
    action,
    maturity_state
) VALUES
    (
        100,
        '"""
    + CONFIRMED_BLOCK_HASH
    + """',
        'miner-a',
        'a',
        decode('11' || repeat('00', 31), 'hex'),
        1000,
        0,
        1000,
        0,
        1000,
        'accrued',
        'immature'
    ),
    (
        101,
        '"""
    + PREPARED_BLOCK_HASH
    + """',
        'miner-b',
        'b',
        decode('22' || repeat('00', 31), 'hex'),
        500,
        0,
        500,
        0,
        500,
        'accrued',
        'immature'
    );

SET session_replication_role = origin;
"""
)
assert_equal(
    summary_rows(),
    [],
    "pre-upgrade carry history left the summary empty",
)

# 3. The gap mutation: the still-running writer confirms the prepared block
#    while the apply sits between the committed triggers and the seed. The
#    summary receives that delta and nothing else.
psql_autocommit_apply(
    "UPDATE qbit_pool_blocks SET chain_state = 'confirmed' "
    "WHERE block_hash = '" + PREPARED_BLOCK_HASH + "';"
)
assert_equal(
    summary_rows(),
    POISONED_SUMMARY,
    "the gap mutation leaves a summary holding only the post-trigger delta",
)
assert_equal(
    reported_balances(),
    [{"miner_id": "miner-b", "balance_sats": "500"}],
    "the poisoned summary silently omits every pre-upgrade carry balance",
)

# 4. The pre-#124 guard sees a non-empty summary and calls the poison seeded.
psql_autocommit_apply(OLD_SEED_GUARD)
assert_equal(
    summary_rows(),
    POISONED_SUMMARY,
    "the old emptiness guard locks the poisoned summary in",
)

# 5. Repair through the psql-subprocess apply path.
psql_ledger = PsqlShareLedger(
    psql_command=psql_command("qbit"),
    native_client_mode="0",
    writer_id="seed-guard-psql",
    writer_epoch=1,
    initialize_schema=True,
)
assert_equal(
    psql_ledger.execution_backend, "psql-subprocess", "psql apply path selected"
)
assert_repaired(psql_ledger, "psql apply path")
psql_ledger.release_writer_lease()
psql_ledger.close()

# 6. Re-poison the summary the same way and repair through the native apply
#    path, which wraps the script in the simple-query-protocol transaction
#    rather than passing --single-transaction.
psql_autocommit_apply(
    "DELETE FROM qbit_payout_carry_forward_current WHERE miner_id = 'miner-a';"
)
assert_equal(
    summary_rows(),
    POISONED_SUMMARY,
    "re-poisoned summary before the native apply",
)
native_ledger = PsqlShareLedger(
    psql_command=psql_command("qbit"),
    database_url=database_url("qbit"),
    native_client_mode="1",
    writer_id="seed-guard-native",
    writer_epoch=1,
    initialize_schema=True,
)
assert_equal(
    native_ledger.execution_backend, "psycopg-pool", "native apply path selected"
)
assert_repaired(native_ledger, "native apply path")
native_ledger.release_writer_lease()
native_ledger.close()

# 7. The guard's drift-only branch: a summary whose active row-count total
#    still matches the carry history but whose balances were corrupted.
#    Row counts alone cannot catch this; the drift comparison must.
psql_autocommit_apply(
    "UPDATE qbit_payout_carry_forward_current "
    "SET balance_sats = balance_sats + 1 WHERE miner_id = 'miner-a';"
)
assert_equal(
    reported_balances(),
    [
        {"miner_id": "miner-a", "balance_sats": "1001"},
        {"miner_id": "miner-b", "balance_sats": "500"},
    ],
    "balance-only corruption is visible in the reported balances",
)
drift_ledger = PsqlShareLedger(
    psql_command=psql_command("qbit"),
    native_client_mode="0",
    writer_id="seed-guard-drift",
    writer_epoch=1,
    initialize_schema=True,
)
assert_repaired(drift_ledger, "drift-only corruption repair")
drift_ledger.release_writer_lease()
drift_ledger.close()

# --------------------------------------------------------------------------
# Scenario B: a failing apply commits nothing on either path. The failure is
# injected just before the script's own final COMMIT so it lands inside the
# apply's transaction: without atomicity (a pre-fix autocommit apply, or a
# client that neither honors the wrapper nor passes --single-transaction)
# every earlier statement would already be durable when the failure lands,
# which is exactly what opens the poisoning gap.
# --------------------------------------------------------------------------

ATOMIC_DATABASES = (
    ("qbit_psql_atomic", "0", "seed-guard-atomic-psql", False),
    ("qbit_native_atomic", "1", "seed-guard-atomic-native", True),
)

for atomic_database, _mode, _writer_id, _needs_url in ATOMIC_DATABASES:
    # CREATE DATABASE cannot run inside a transaction block, so it goes
    # through the maintenance command rather than the ledger.
    psql_autocommit_apply(f"CREATE DATABASE {atomic_database};")

schema_body_lines = schema_text.splitlines(keepends=True)
commit_indexes = [
    index
    for index, line in enumerate(schema_body_lines)
    if line.rstrip("\n") == "COMMIT;"
]
assert_equal(
    len(commit_indexes),
    1,
    "schema script contains exactly one top-level COMMIT",
)
failing_schema_text = (
    "".join(schema_body_lines[: commit_indexes[0]])
    + "SELECT 1/0;\n"
    + "".join(schema_body_lines[commit_indexes[0] :])
)

with tempfile.TemporaryDirectory() as temporary_directory:
    failing_schema_path = Path(temporary_directory) / "001_share_ledger_failing.sql"
    failing_schema_path.write_text(failing_schema_text, encoding="utf-8")
    for atomic_database, mode, writer_id, needs_url in ATOMIC_DATABASES:
        failure: BaseException | None = None
        try:
            PsqlShareLedger(
                psql_command=psql_command(atomic_database),
                database_url=database_url(atomic_database) if needs_url else None,
                native_client_mode=mode,
                writer_id=writer_id,
                writer_epoch=1,
                initialize_schema=True,
                schema_path=failing_schema_path,
            )
        except Exception as exc:  # noqa: BLE001 - the failure mode is the assertion
            failure = exc
        if failure is None:
            raise SystemExit(
                f"{atomic_database}: a failing schema apply unexpectedly succeeded"
            )
        assert_equal(
            psql_json(
                """
SELECT json_build_object(
    'pool_blocks', to_regclass('qbit_pool_blocks')::text,
    'carry_forward_current', to_regclass('qbit_payout_carry_forward_current')::text
);
""",
                database=atomic_database,
            ),
            {"pool_blocks": None, "carry_forward_current": None},
            f"{atomic_database}: the failed apply committed nothing",
        )

print("prism postgres seed guard: OK")
PY
)

echo "test-prism-postgres-seed-guard: PASS"
