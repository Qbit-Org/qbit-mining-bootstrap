#!/usr/bin/env bash
set -euo pipefail

# Named, reusable production-scale PRISM ledger fixture (issue #188 fix 6).
#
# Every latent O(history) charge in the 2026-08-14 landing livelock was
# invisible on the empty databases the ordinary tests run against. This
# harness seeds a scaled qbit_payout_carry_forward / qbit_share_ledger
# fixture with realistic miner skew, then asserts plan shape and per-query
# budgets for the hot query classes, timeout cancellation cleanliness, and
# the durable outbox format contract. Budgets are enforced on server-side
# planning+execution time (EXPLAIN ANALYZE) so per-call psql / docker-exec
# transport overhead on shared CI runners cannot flake the gate; wall-clock
# figures are printed alongside for the record.
#
# Scale knobs (defaults keep CI fast; raise them for production-shaped runs):
#   QBIT_PRISM_SCALE_CARRY_ROWS   carry-forward rows        (default 120000; prod ~5000000)
#   QBIT_PRISM_SCALE_SHARE_ROWS   accepted share rows       (default 200000; prod ~50000000)
#   QBIT_PRISM_SCALE_MINERS       distinct miners           (default 13)
#   QBIT_PRISM_SCALE_BLOCKS       confirmed pool blocks     (default 40)
#   QBIT_PRISM_SCALE_PRIOR_BALANCES_BUDGET_MS   warm budget (default 50)
#   QBIT_PRISM_SCALE_OUTBOX_POLL_BUDGET_MS      warm budget (default 50)
#   QBIT_PRISM_SCALE_WINDOW_READ_BUDGET_MS      warm budget (default 2000)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=test/prism-postgres-lib.sh
source "${ROOT_DIR}/test/prism-postgres-lib.sh"

POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-scale-pg-$$}"

require_executable() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required executable: $1" >&2
    exit 1
  }
}

EXTERNAL_PSQL="${QBIT_PRISM_EXTERNAL_PSQL_COMMAND:-}"

cleanup() {
  if [[ -z "${EXTERNAL_PSQL}" ]]; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_executable python3

if [[ -n "${EXTERNAL_PSQL}" ]]; then
  PSQL_COMMAND="${EXTERNAL_PSQL}"
  deadline=$((SECONDS + 60))
  until echo 'SELECT 1;' | ${PSQL_COMMAND} >/dev/null 2>&1; do
    if [[ "${SECONDS}" -ge "${deadline}" ]]; then
      echo "timed out waiting for external PRISM Postgres" >&2
      exit 1
    fi
    sleep 1
  done
else
  require_executable docker
  docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
  docker run \
    --rm \
    --detach \
    --name "${POSTGRES_CONTAINER}" \
    -e POSTGRES_USER=qbit \
    -e POSTGRES_PASSWORD=qbit \
    -e POSTGRES_DB=qbit \
    "${POSTGRES_IMAGE}" >/dev/null

  wait_for_prism_postgres_container "${POSTGRES_CONTAINER}"
  PSQL_COMMAND="docker exec -i ${POSTGRES_CONTAINER} psql -U qbit -d qbit"
fi

(
  cd "${ROOT_DIR}"
  PRISM_PSQL_COMMAND="${PSQL_COMMAND}" \
    python3 <<'PY'
from __future__ import annotations

import json
import os
import statistics
import time

from lab.prism.share_ledger import LedgerOperationTimeout, PsqlShareLedger

CARRY_ROWS = int(os.environ.get("QBIT_PRISM_SCALE_CARRY_ROWS", "120000"))
SHARE_ROWS = int(os.environ.get("QBIT_PRISM_SCALE_SHARE_ROWS", "200000"))
MINERS = max(3, int(os.environ.get("QBIT_PRISM_SCALE_MINERS", "13")))
BLOCKS = max(2, int(os.environ.get("QBIT_PRISM_SCALE_BLOCKS", "40")))
PRIOR_BUDGET_MS = float(
    os.environ.get("QBIT_PRISM_SCALE_PRIOR_BALANCES_BUDGET_MS", "50")
)
OUTBOX_BUDGET_MS = float(
    os.environ.get("QBIT_PRISM_SCALE_OUTBOX_POLL_BUDGET_MS", "50")
)
WINDOW_BUDGET_MS = float(
    os.environ.get("QBIT_PRISM_SCALE_WINDOW_READ_BUDGET_MS", "2000")
)


def assert_true(condition: object, message: str) -> None:
    if not condition:
        raise SystemExit(f"scale fixture assertion failed: {message}")


def server_execution_ms(sql: str) -> float:
    """Planning + execution time measured inside PostgreSQL.

    Wall-clock timing through the harness includes per-call psql (and, in
    CI, docker exec) transport overhead of tens of milliseconds, which
    would dominate the budgets on shared runners. Budgets gate the query
    cost itself, so they are enforced on the server-side measurement;
    wall-clock values are still reported for the record.
    """
    plan = json.loads(ledger._run_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + sql))[0]
    return float(plan.get("Planning Time", 0.0)) + float(plan["Execution Time"])


PRIOR_BALANCES_SQL = (
    "SELECT COALESCE(json_agg(json_build_object("
    "'m', miner_id, 'b', balance_sats::text)), '[]'::json) "
    "FROM qbit_current_carry_forward_balances();"
)


ledger = PsqlShareLedger(
    psql_command=os.environ["PRISM_PSQL_COMMAND"],
    writer_id="scale-writer",
    writer_epoch=1,
    initialize_schema=True,
)

print(
    f"scale fixture: seeding carry={CARRY_ROWS} shares={SHARE_ROWS} "
    f"miners={MINERS} blocks={BLOCKS}",
    flush=True,
)
seed_started = time.monotonic()

# Bulk seed with the summary trigger disabled, then rebuild through the ops
# resync hook -- exactly the documented migration/repair path.
#
# Miner skew: the three heaviest miners take ~60% of rows; the remainder is
# spread across the tail, approximating the production distribution.
ledger._run_sql(
    f"""
ALTER TABLE qbit_payout_carry_forward DISABLE TRIGGER qbit_payout_carry_forward_current_sync;

INSERT INTO qbit_pool_blocks (
    block_hash, block_height, parent_hash, coinbase_txid,
    payout_manifest_sha256, chain_state
)
SELECT
    'scale-block-' || lpad(gs::text, 8, '0'),
    gs,
    'scale-block-' || lpad((gs - 1)::text, 8, '0'),
    md5('coinbase' || gs::text),
    md5('manifest' || gs::text),
    'confirmed'
FROM generate_series(1, {BLOCKS}) AS gs
ON CONFLICT (block_hash) DO NOTHING;

INSERT INTO qbit_payout_carry_forward (
    block_height, block_hash, miner_id, payout_order_key, p2mr_program,
    gross_amount_sats, prior_balance_sats, candidate_balance_sats,
    onchain_amount_sats, carry_forward_balance_sats, action, maturity_state
)
SELECT
    1 + mod(gs, {BLOCKS}),
    'scale-block-' || lpad((1 + mod(gs, {BLOCKS}))::text, 8, '0'),
    'scale-miner-' || lpad(miner_idx::text, 4, '0'),
    lpad(miner_idx::text, 4, '0'),
    decode(lpad(to_hex(miner_idx + 1), 64, '0'), 'hex'),
    1000 + mod(gs, 50),
    0,
    1000 + mod(gs, 50),
    CASE WHEN mod(gs, 7) = 0 THEN 1000 + mod(gs, 50) ELSE 0 END,
    CASE WHEN mod(gs, 7) = 0 THEN 0 ELSE 1000 + mod(gs, 50) END,
    CASE WHEN mod(gs, 7) = 0 THEN 'onchain' ELSE 'accrued' END,
    'immature'
FROM (
    SELECT
        gs,
        CASE
            WHEN mod(gs, 10) < 3 THEN 1
            WHEN mod(gs, 10) < 5 THEN 2
            WHEN mod(gs, 10) < 6 THEN 3
            ELSE 4 + mod(gs, {MINERS} - 3)
        END AS miner_idx
    FROM generate_series(1, {CARRY_ROWS}) AS gs
) AS seeded;

ALTER TABLE qbit_payout_carry_forward ENABLE TRIGGER qbit_payout_carry_forward_current_sync;

INSERT INTO qbit_share_ledger (
    share_id, miner_id, payout_order_key, p2mr_program,
    share_difficulty, network_difficulty, template_height, job_id,
    job_issued_at, ntime, accepted_at, accepted, writer_id, writer_epoch
)
SELECT
    'scale-share-' || lpad(gs::text, 10, '0'),
    'scale-miner-' || lpad(miner_idx::text, 4, '0'),
    lpad(miner_idx::text, 4, '0'),
    decode(lpad(to_hex(miner_idx + 1), 64, '0'), 'hex'),
    64 + mod(gs, 512),
    100000,
    1 + mod(gs, {BLOCKS}),
    'scale-job-' || mod(gs, 1024)::text,
    now() - make_interval(secs => ({SHARE_ROWS} - gs) * 0.25),
    1700000000 + gs,
    now() - make_interval(secs => ({SHARE_ROWS} - gs) * 0.25),
    true,
    'scale-writer',
    1
FROM (
    SELECT
        gs,
        CASE
            WHEN mod(gs, 10) < 3 THEN 1
            WHEN mod(gs, 10) < 5 THEN 2
            WHEN mod(gs, 10) < 6 THEN 3
            ELSE 4 + mod(gs, {MINERS} - 3)
        END AS miner_idx
    FROM generate_series(1, {SHARE_ROWS}) AS gs
) AS seeded;

SELECT qbit_rebuild_carry_forward_current_balances();
ANALYZE qbit_payout_carry_forward;
ANALYZE qbit_payout_carry_forward_current;
ANALYZE qbit_share_ledger;
ANALYZE qbit_pool_blocks;
"""
)
print(
    f"scale fixture: seeded in {time.monotonic() - seed_started:.1f}s",
    flush=True,
)


def timed_json(sql: str) -> tuple[float, object]:
    started = time.monotonic()
    result = ledger._run_json(sql)
    return (time.monotonic() - started) * 1000.0, result


# --- Prior balances: budget + equivalence -----------------------------------

cold_ms, cold_balances = timed_json(PRIOR_BALANCES_SQL)
cold_server_ms = server_execution_ms(PRIOR_BALANCES_SQL)
warm_wall_samples = []
warm_server_samples = []
for _ in range(5):
    warm_ms, _ = timed_json(PRIOR_BALANCES_SQL)
    warm_wall_samples.append(warm_ms)
    warm_server_samples.append(server_execution_ms(PRIOR_BALANCES_SQL))
warm_wall_median = statistics.median(warm_wall_samples)
warm_server_median = statistics.median(warm_server_samples)
print(
    f"scale fixture: prior balances cold wall={cold_ms:.1f}ms "
    f"server={cold_server_ms:.1f}ms; warm medians "
    f"wall={warm_wall_median:.1f}ms server={warm_server_median:.1f}ms "
    f"budget={PRIOR_BUDGET_MS:.0f}ms",
    flush=True,
)
assert_true(len(cold_balances) >= 3, "prior balances returned miner rows")
assert_true(
    warm_server_median <= PRIOR_BUDGET_MS,
    f"warm prior-balances server time {warm_server_median:.1f}ms exceeds "
    f"{PRIOR_BUDGET_MS:.0f}ms budget",
)

recompute_started = time.monotonic()
drift = ledger._run_json(
    "SELECT json_build_object("
    "'drift_count', (SELECT count(*) FROM qbit_carry_forward_current_drift()));"
)
recompute_ms = (time.monotonic() - recompute_started) * 1000.0
assert_true(
    int(drift["drift_count"]) == 0,
    "summary balances drift from recompute at scale",
)
print(
    f"scale fixture: recompute cross-check {recompute_ms:.1f}ms drift=0",
    flush=True,
)

# --- Prior balances: plan shape ----------------------------------------------
# Inline copy of the qbit_current_carry_forward_balances() body; the plan
# must never touch the O(history) qbit_payout_carry_forward table.

plan_text = ledger._run_sql(
    """
EXPLAIN (FORMAT TEXT)
WITH balances AS (
    SELECT
        (array_agg(current_balance.miner_id ORDER BY current_balance.payout_order_key, current_balance.miner_id))[1] AS miner_id,
        (array_agg(current_balance.payout_order_key ORDER BY current_balance.payout_order_key, current_balance.miner_id))[1] AS payout_order_key,
        current_balance.p2mr_program,
        SUM(current_balance.balance_sats) AS balance_sats
    FROM qbit_payout_carry_forward_current current_balance
    WHERE current_balance.active_row_count > 0
    GROUP BY current_balance.p2mr_program
    HAVING SUM(current_balance.balance_sats) <> 0
)
SELECT balances.miner_id, balances.payout_order_key, balances.p2mr_program, balances.balance_sats
FROM balances
ORDER BY balances.payout_order_key, balances.miner_id, balances.p2mr_program;
"""
)
assert_true(
    "qbit_payout_carry_forward " not in plan_text
    and "on qbit_payout_carry_forward\n" not in plan_text
    and not plan_text.rstrip().endswith("on qbit_payout_carry_forward"),
    f"prior-balances plan touches the O(history) table:\n{plan_text}",
)
assert_true(
    "qbit_payout_carry_forward_current" in plan_text,
    f"prior-balances plan does not read the summary table:\n{plan_text}",
)

# --- Mid-history reversal equivalence under trigger maintenance --------------
# Reverse one seeded block the same way qbit_reverse_immature_pool_block
# does (block first, then carry rows) and require the trigger-maintained
# summary to keep matching the recompute.

ledger._run_sql(
    """
UPDATE qbit_pool_blocks
SET chain_state = 'reversed', maturity_state = 'reversed',
    disconnected_at = clock_timestamp()
WHERE block_hash = 'scale-block-00000002';

UPDATE qbit_payout_carry_forward
SET maturity_state = 'reversed'
WHERE block_hash = 'scale-block-00000002'
  AND maturity_state = 'immature';
"""
)
post_reversal_server_ms = statistics.median(
    server_execution_ms(PRIOR_BALANCES_SQL) for _ in range(3)
)
drift = ledger._run_json(
    "SELECT json_build_object("
    "'drift_count', (SELECT count(*) FROM qbit_carry_forward_current_drift()));"
)
assert_true(
    int(drift["drift_count"]) == 0,
    "summary drifts from recompute after mid-history reversal at scale",
)
assert_true(
    post_reversal_server_ms <= PRIOR_BUDGET_MS,
    f"post-reversal prior-balances server time {post_reversal_server_ms:.1f}ms "
    f"exceeds {PRIOR_BUDGET_MS:.0f}ms budget",
)
print(
    f"scale fixture: mid-history reversal kept drift=0 "
    f"server={post_reversal_server_ms:.1f}ms",
    flush=True,
)

# --- Outbox poll: plan shape + budget ---------------------------------------

intent = {
    "schema": "qbit.prism.block-candidate-intent.v1",
    "block_hash_hex": "ab" * 32,
    "block_hex": "00",
}
assert_true(
    ledger.persist_block_candidate_intent(intent),
    "outbox intent persist",
)
poll_started = time.monotonic()
pending_rows = ledger.pending_block_candidate_rows()
poll_wall_ms = (time.monotonic() - poll_started) * 1000.0
poll_server_ms = server_execution_ms(
    "SELECT block_hash, candidate FROM qbit_block_candidate_outbox "
    "WHERE state = 'pending' ORDER BY created_at, block_hash LIMIT 32;"
)
print(
    f"scale fixture: outbox poll wall={poll_wall_ms:.1f}ms "
    f"server={poll_server_ms:.1f}ms budget={OUTBOX_BUDGET_MS:.0f}ms",
    flush=True,
)
assert_true(
    poll_server_ms <= OUTBOX_BUDGET_MS,
    f"outbox poll server time {poll_server_ms:.1f}ms exceeds "
    f"{OUTBOX_BUDGET_MS:.0f}ms budget",
)
outbox_plan_text = ledger._run_sql(
    """
EXPLAIN (FORMAT TEXT)
SELECT block_hash, candidate
FROM qbit_block_candidate_outbox
WHERE state = 'pending'
ORDER BY created_at, block_hash
LIMIT 32;
"""
)
assert_true(
    "qbit_block_candidate_outbox_pending_idx" in outbox_plan_text,
    f"outbox poll does not use the pending partial index:\n{outbox_plan_text}",
)

# --- Durable outbox format contract (replay compatibility) -------------------
# A rollback replays rows written by the newer build; the payload must keep
# its schema-versioned shape and authoritative key so the previous release
# can digest it.

assert_true(len(pending_rows) == 1, "one pending outbox row")
durable_row = pending_rows[0]
assert_true(
    durable_row["block_hash"] == "ab" * 32,
    "outbox row keeps the authoritative block-hash key",
)
candidate_payload = durable_row["candidate"]
assert_true(
    candidate_payload.get("schema") == "qbit.prism.block-candidate-intent.v1",
    "outbox candidate payload keeps the v1 intent schema tag",
)
assert_true(
    candidate_payload.get("block_hash_hex") == "ab" * 32
    and "block_hex" in candidate_payload,
    "outbox candidate payload keeps replay-critical fields",
)
assert_true(
    ledger.mark_block_candidate_submitted(block_hash="ab" * 32),
    "outbox terminal update",
)

# --- Window read budget ------------------------------------------------------

anchor = ledger._run_json(
    "SELECT json_build_object('anchor', to_char(max(job_issued_at), "
    "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')) FROM qbit_share_ledger;"
)["anchor"]
window_sql = (
    "SELECT json_build_object('rows', count(*)) FROM qbit_audit_share_window("
    f"'{anchor}'::timestamptz, 100000::numeric);"
)
window_started = time.monotonic()
window_rows = ledger._run_json(window_sql)
window_wall_ms = (time.monotonic() - window_started) * 1000.0
window_server_ms = server_execution_ms(window_sql)
assert_true(int(window_rows["rows"]) > 0, "window read returned shares")
assert_true(
    window_server_ms <= WINDOW_BUDGET_MS,
    f"window read server time {window_server_ms:.1f}ms exceeds "
    f"{WINDOW_BUDGET_MS:.0f}ms budget",
)
print(
    f"scale fixture: window read wall={window_wall_ms:.1f}ms "
    f"server={window_server_ms:.1f}ms rows={window_rows['rows']}",
    flush=True,
)

# --- Timeout cancellation leaves no invisible query --------------------------

marker = "scale-cancellation-marker"
try:
    with ledger.operation_timeout(0.2):
        ledger._run_json(
            f"SELECT json_build_object('{marker}', pg_sleep(5));"
        )
    raise SystemExit("padded statement unexpectedly completed under 0.2s budget")
except LedgerOperationTimeout:
    pass
deadline = time.monotonic() + 5.0
lingering = None
while time.monotonic() < deadline:
    lingering = ledger._run_json(
        "SELECT json_build_object('active', count(*)) FROM pg_stat_activity "
        f"WHERE state = 'active' AND query LIKE '%{marker}%' "
        "AND query NOT LIKE '%pg_stat_activity%';"
    )
    if int(lingering["active"]) == 0:
        break
    time.sleep(0.1)
assert_true(
    lingering is not None and int(lingering["active"]) == 0,
    f"cancelled statement still active on the server: {lingering}",
)
follow_up = ledger._run_json("SELECT json_build_object('ok', 1);")
assert_true(int(follow_up["ok"]) == 1, "ledger unusable after cancellation")
print("scale fixture: timeout cancellation left no active query", flush=True)

ledger.close()
print(
    "prism postgres scale PASS "
    f"carry={CARRY_ROWS} shares={SHARE_ROWS} miners={MINERS} "
    f"prior_cold_server_ms={cold_server_ms:.1f} "
    f"prior_warm_server_ms={warm_server_median:.1f} "
    f"window_server_ms={window_server_ms:.1f}",
    flush=True,
)
PY
)
