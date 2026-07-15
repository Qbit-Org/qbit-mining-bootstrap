#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-ledger-pg-$$}"

require_executable() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required executable: $1" >&2
    exit 1
  }
}

# When QBIT_PRISM_EXTERNAL_PSQL_COMMAND is set, run against an already-running
# Postgres (e.g. a local cluster) instead of provisioning a Docker container.
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

  deadline=$((SECONDS + 60))
  until docker exec "${POSTGRES_CONTAINER}" pg_isready -U qbit -d qbit >/dev/null 2>&1; do
    if [[ "${SECONDS}" -ge "${deadline}" ]]; then
      echo "timed out waiting for PRISM Postgres container" >&2
      docker logs "${POSTGRES_CONTAINER}" >&2 || true
      exit 1
    fi
    sleep 1
  done
  PSQL_COMMAND="docker exec -i ${POSTGRES_CONTAINER} psql -U qbit -d qbit"
fi

(
  cd "${ROOT_DIR}"
  PRISM_PSQL_COMMAND="${PSQL_COMMAND}" \
    python3 <<'PY'
from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from pathlib import Path

from lab.prism.share_ledger import PendingShare, PsqlShareLedger


def pending(
    index: int,
    *,
    job_ms: int,
    accepted_ms: int,
    share_id: str | None = None,
    share_difficulty: int | None = None,
    network_difficulty: int = 1000,
    template_height: int = 10,
) -> PendingShare:
    return PendingShare(
        share_id=share_id or f"share-{index}",
        miner_id=f"miner-{index}",
        order_key=f"{index:04d}",
        p2mr_program_hex=f"{index:02x}" * 32,
        share_difficulty=share_difficulty if share_difficulty is not None else 100 + index,
        network_difficulty=network_difficulty,
        template_height=template_height,
        job_id=f"job-{index}",
        job_issued_at_ms=job_ms,
        accepted_at_ms=accepted_ms,
        ntime=1_700_000_000 + index,
    )


def fake_audit_bundle_bytes(payload) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def fake_audit_bundle_sha256(payload) -> str:
    return hashlib.sha256(fake_audit_bundle_bytes(payload)).hexdigest()


def assert_equal(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise SystemExit(f"{message}: expected {expected!r}, got {actual!r}")


def force_expired_idle_lease(runner: PsqlShareLedger) -> None:
    runner._run_sql(
        """
UPDATE qbit_ledger_writer_lease
SET updated_at = clock_timestamp() - interval '6 minutes',
    lease_expires_at = clock_timestamp() - interval '1 minute';
"""
    )


psql = os.environ["PRISM_PSQL_COMMAND"]
ledger = PsqlShareLedger(
    psql_command=psql,
    writer_id="writer-a",
    writer_epoch=1,
    initialize_schema=True,
)
schema_smoke = ledger._run_json(
    """
SELECT json_build_object(
    'carry_forward_balance_rows', (SELECT count(*) FROM qbit_current_carry_forward_balances()),
    'owed_balance_rows', (SELECT count(*) FROM qbit_current_owed_balances())
);
"""
)
assert_equal(
    schema_smoke,
    {"carry_forward_balance_rows": 0, "owed_balance_rows": 0},
    "schema initialization exposes current balance functions",
)

# The live ACK path uses append_batch, including ordinary shares whose
# candidate value is JSON null. Exercise the real Postgres statement so JSONB
# null handling, FIFO sequence assignment, exact replay, and terminal outbox
# compaction cannot regress behind SQL-string unit tests.
batch_a = pending(90, job_ms=900, accepted_ms=901, share_id="batch-a")
batch_b = pending(91, job_ms=902, accepted_ms=903, share_id="batch-b")
batch_rows = ledger.append_batch([(batch_a, None), (batch_b, None)])
assert_equal([row.share_id for row in batch_rows], ["batch-a", "batch-b"], "batch FIFO ids")
assert_equal([row.share_seq for row in batch_rows], [1, 2], "batch FIFO sequences")
assert_equal(ledger.pending_block_candidates(), [], "ordinary shares create no outbox rows")

batch_c = pending(92, job_ms=904, accepted_ms=905, share_id="batch-c")
candidate_intent = {
    "schema": "qbit.prism.block-candidate-intent.v1",
    "block_hash_hex": "ab" * 32,
    "block_hex": "00",
}
candidate_row = ledger.append_batch([(batch_c, candidate_intent)])[0]
assert_equal(candidate_row.share_seq, 3, "candidate share sequence")
assert_equal(ledger.append_batch([(batch_c, candidate_intent)])[0].share_seq, 3, "exact replay")
assert_equal(ledger.pending_block_candidates(), [candidate_intent], "pending candidate replay")
assert_equal(
    ledger.pending_block_candidate_rows(),
    [{"block_hash": "ab" * 32, "candidate": candidate_intent}],
    "pending candidate replay retains authoritative outbox key",
)
assert ledger.mark_block_candidate_submitted(block_hash="ab" * 32)
assert_equal(ledger.pending_block_candidates(), [], "submitted candidate leaves pending set")
assert_equal(ledger.append_batch([(batch_c, candidate_intent)])[0].share_seq, 3, "compact exact replay")
batch_d = pending(93, job_ms=906, accepted_ms=907, share_id="batch-d")
candidate_only = {
    **candidate_intent,
    "block_hash_hex": "cd" * 32,
    "credit_share_on_accept": True,
}
assert ledger.persist_block_candidate_intent(candidate_only)
assert_equal(ledger.pending_block_candidates(), [candidate_only], "candidate-only intent")
assert_equal(ledger.append_batch([(batch_d, candidate_only)])[0].share_seq, 4, "linked solver credit")
assert ledger.mark_block_candidate_submitted(block_hash="cd" * 32)
assert_equal(ledger.pending_block_candidates(), [], "linked candidate completion")
poison_hash = "ef" * 32
poison_candidate = {
    **candidate_intent,
    "block_hash_hex": poison_hash,
}
assert ledger.persist_block_candidate_intent(poison_candidate)
ledger._run_sql(
    "UPDATE qbit_block_candidate_outbox "
    "SET candidate = candidate - 'block_hash_hex' "
    f"WHERE block_hash = '{poison_hash}';"
)
poison_rows = ledger.pending_block_candidate_rows()
assert_equal(poison_rows[0]["block_hash"], poison_hash, "poison row keeps durable key")
assert_equal(
    poison_rows[0]["candidate"].get("block_hash_hex"),
    None,
    "poison fixture removes payload block hash",
)
assert ledger.mark_block_candidate_abandoned(
    block_hash=poison_rows[0]["block_hash"],
    error="invalid durable candidate intent",
)
assert_equal(ledger.pending_block_candidates(), [], "poison row quarantined by durable key")
ledger._run_sql(
    """
DELETE FROM qbit_block_candidate_outbox;
DELETE FROM qbit_share_ledger;
ALTER SEQUENCE qbit_share_ledger_share_seq_seq RESTART WITH 1;
"""
)

ledger._run_sql(
    """
ALTER TABLE qbit_ctv_fanout_artifacts
    ALTER COLUMN anchor_vout SET NOT NULL;
"""
)
legacy_anchor_nullable = ledger._run_json(
    """
SELECT to_json(is_nullable)
FROM information_schema.columns
WHERE table_name = 'qbit_ctv_fanout_artifacts'
  AND column_name = 'anchor_vout';
"""
)
assert_equal(legacy_anchor_nullable, "NO", "old schema simulation makes anchor_vout not nullable")
ledger.release_writer_lease()
ledger = PsqlShareLedger(
    psql_command=psql,
    writer_id="writer-a",
    writer_epoch=1,
    initialize_schema=True,
)
repaired_anchor_nullable = ledger._run_json(
    """
SELECT to_json(is_nullable)
FROM information_schema.columns
WHERE table_name = 'qbit_ctv_fanout_artifacts'
  AND column_name = 'anchor_vout';
"""
)
assert_equal(repaired_anchor_nullable, "YES", "schema init repairs nullable CTV anchor_vout")

first = ledger.append(pending(1, job_ms=1_700_000_000_500, accepted_ms=1_700_000_001_000))
second = ledger.append(pending(2, job_ms=1_700_000_000_000, accepted_ms=1_700_000_001_100))
assert_equal([first.share_seq, second.share_seq], [1, 2], "initial share sequence")

snapshot = ledger.snapshot_at_job_issue(1_700_000_001_100)
assert_equal([share.share_seq for share in snapshot], [1, 2], "snapshot preserves insert order")

try:
    ledger.append(pending(99, job_ms=1_700_000_000_000, accepted_ms=1_700_000_001_200, share_id="share-1"))
except RuntimeError as exc:
    if "duplicate share_id" not in str(exc):
        raise
else:
    raise SystemExit("duplicate share replay unexpectedly appended")

third = ledger.append(pending(3, job_ms=1_700_000_000_000, accepted_ms=1_700_000_001_300))
assert_equal(third.share_seq, 3, "duplicate replay must not consume a sequence value")

future = ledger.append(pending(4, job_ms=1_700_000_000_000, accepted_ms=1_700_000_005_000))
assert_equal(future.share_seq, 4, "future accepted share sequence")

window = ledger.audit_share_window(anchor_job_issued_at_ms=1_700_000_001_300, network_difficulty=50)
assert_equal(
    [row["share_id"] for row in window],
    ["share-3", "share-2", "share-1"],
    "audit window excludes future-accepted old-job share",
)

try:
    PsqlShareLedger(psql_command=psql, writer_id="writer-a", writer_epoch=2)
except RuntimeError as exc:
    if "writer-a" not in str(exc):
        raise
else:
    raise SystemExit("same writer id with a different live epoch stole the lease")

try:
    PsqlShareLedger(psql_command=psql, writer_id="writer-b", writer_epoch=1)
except RuntimeError as exc:
    if "writer-a" not in str(exc):
        raise
else:
    raise SystemExit("different writer stole an unexpired lease")

force_expired_idle_lease(ledger)
idle_share = ledger.append(pending(50, job_ms=1_700_000_005_500, accepted_ms=1_700_000_005_600))
assert_equal(idle_share.share_seq, 5, "same writer refreshes expired lease before append")

try:
    PsqlShareLedger(psql_command=psql, writer_id="writer-b", writer_epoch=1)
except RuntimeError as exc:
    if "writer-a" not in str(exc):
        raise
else:
    raise SystemExit("different writer stole an unexpired lease after idle refresh")

force_expired_idle_lease(ledger)
replacement = PsqlShareLedger(
    psql_command=psql,
    writer_id="writer-b",
    writer_epoch=1,
    audit_bundle_canonicalizer=fake_audit_bundle_bytes,
)
try:
    ledger.append(pending(5, job_ms=1_700_000_006_000, accepted_ms=1_700_000_006_100))
except RuntimeError as exc:
    if "writer lease is not active" not in str(exc):
        raise
else:
    raise SystemExit("stale writer appended after replacement acquired the lease")

force_expired_idle_lease(replacement)
try:
    ledger.append(pending(51, job_ms=1_700_000_006_200, accepted_ms=1_700_000_006_300))
except RuntimeError as exc:
    if "writer lease is not active" not in str(exc):
        raise
else:
    raise SystemExit("stale writer reacquired after replacement lease expired")

fifth = replacement.append(pending(5, job_ms=1_700_000_006_000, accepted_ms=1_700_000_006_100))
assert_equal(fifth.share_seq, 6, "replacement writer resumes next sequence")

below_height = replacement.append(
    pending(6, job_ms=1_700_000_007_000, accepted_ms=1_700_000_007_100, template_height=19)
)
height_hit = replacement.append(
    pending(7, job_ms=1_700_000_007_000, accepted_ms=1_700_000_007_100, template_height=20)
)
height_high = replacement.append(
    pending(8, job_ms=1_700_000_007_000, accepted_ms=1_700_000_007_100, template_height=21)
)
assert_equal(
    [below_height.share_seq, height_hit.share_seq, height_high.share_seq],
    [7, 8, 9],
    "template-height fixture sequence",
)
replacement._run_sql(
    """
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
    reject_reason,
    writer_id,
    writer_epoch
) VALUES (
    'share-rejected-height-22',
    'miner-rejected',
    'rejected',
    decode('99' || repeat('00', 31), 'hex'),
    1,
    1000,
    22,
    'job-rejected',
    to_timestamp(1700000007.000),
    1700000009,
    to_timestamp(1700000007.100),
    false,
    'low-difficulty',
    'writer-b',
    1
);
"""
)
since_height = replacement._run_json(
    """
SELECT COALESCE(json_agg(json_build_object(
    'share_id', share_id,
    'share_seq', share_seq,
    'template_height', template_height,
    'accepted', accepted
) ORDER BY share_seq ASC), '[]'::json)
FROM qbit_shares_since_template_height(20);
"""
)
assert_equal(
    [row["share_id"] for row in since_height],
    ["share-7", "share-8"],
    "template-height query returns accepted shares at or above threshold in sequence order",
)

replacement.append(
    pending(10, job_ms=1_700_000_008_000, accepted_ms=1_700_000_008_000, share_difficulty=40)
)
replacement.append(
    pending(11, job_ms=1_700_000_008_000, accepted_ms=1_700_000_008_000, share_difficulty=60)
)
replacement.append(
    pending(12, job_ms=1_700_000_008_000, accepted_ms=1_700_000_008_000, share_difficulty=100)
)
exact_window = replacement.audit_share_window(
    anchor_job_issued_at_ms=1_700_000_008_000,
    network_difficulty=25,
)
assert_equal(
    [row["share_id"] for row in exact_window],
    ["share-12", "share-11", "share-10"],
    "exact 8x window share order",
)
assert_equal(
    [row["counted_difficulty"] for row in exact_window],
    [100, 60, 40],
    "exact 8x window counted difficulties",
)
assert_equal(sum(row["counted_difficulty"] for row in exact_window), 200, "exact 8x window total weight")

replacement.append(
    pending(13, job_ms=1_700_000_009_000, accepted_ms=1_700_000_009_000, share_difficulty=90)
)
replacement.append(
    pending(14, job_ms=1_700_000_009_000, accepted_ms=1_700_000_009_000, share_difficulty=150)
)
partial_window = replacement.audit_share_window(
    anchor_job_issued_at_ms=1_700_000_009_000,
    network_difficulty=25,
)
assert_equal(
    [row["share_id"] for row in partial_window],
    ["share-14", "share-13"],
    "partial oldest-share window order",
)
assert_equal([row["share_difficulty"] for row in partial_window], [150, 90], "partial window original difficulties")
assert_equal([row["counted_difficulty"] for row in partial_window], [150, 50], "partial oldest-share counted difficulty")
assert_equal(sum(row["counted_difficulty"] for row in partial_window), 200, "partial window total weight")

bundle = {
    "signed_coinbase_manifest": {"manifest": {"payout_count": 2}},
    "payout_policy_manifest": {
        "accounts": [
            {
                "recipient_id": "miner-a",
                "order_key": "a",
                "p2mr_program_hex": "aa" * 32,
                "gross_amount_sats": 1000,
                "prior_balance_sats": 0,
                "candidate_balance_sats": 1000,
                "onchain_amount_sats": 0,
                "carry_forward_balance_sats": 1000,
                "action": "accrued",
            },
            {
                "recipient_id": "miner-b",
                "order_key": "b",
                "p2mr_program_hex": "bb" * 32,
                "gross_amount_sats": 50000,
                "prior_balance_sats": 0,
                "candidate_balance_sats": 50000,
                "onchain_amount_sats": 50000,
                "settlement_fee_sats": 500,
                "carry_forward_balance_sats": 0,
                "action": "onchain",
            },
        ]
    },
}
def audit_report_for(
    final_bundle,
    *,
    coinbase_txid: str = "11" * 32,
    coinbase_manifest_sha256_hex: str = "22" * 32,
    coinbase_tx_hex: str = "00",
):
    return {
        "coinbase_txid": coinbase_txid,
        "coinbase_manifest_sha256_hex": coinbase_manifest_sha256_hex,
        "audit_bundle_sha256_hex": fake_audit_bundle_sha256(final_bundle),
        "coinbase_tx_hex": coinbase_tx_hex,
    }


report = audit_report_for(bundle)
force_expired_idle_lease(replacement)
first_persist = replacement.persist_accepted_block(
    block_hash="44" * 32,
    block_height=7,
    parent_hash="55" * 32,
    final_bundle=bundle,
    audit_report=report,
)
assert_equal(
    [
        first_persist["block_count"],
        first_persist["bundle_count"],
        first_persist["payout_entry_count"],
        first_persist["carry_forward_count"],
    ],
    [1, 1, 2, 2],
    "initial accepted block persistence counts",
)
fanout_manifest_set = {
    "settlement_mode": "ctv_fanout",
    "parent_coinbase_txid": "11" * 32,
    "fanout_count": 1,
    "fanout_output_sum_sats": 487,
    "covenant_output_value_sats": 500,
    "manifests": [
        {
            "fanout_txid": "12" * 32,
            "parent_coinbase_txid": "11" * 32,
            "parent_coinbase_tx_hex": "00",
            "parent_coinbase_vout": 0,
            "fanout_tx_hex": "02",
            "commitment_witness_leaf_hex": "03",
            "precommitment_sha256_hex": "13" * 32,
            "covenant_output_value_sats": 500,
            "precommitment": {
                "settlement_mode": "ctv_fanout",
                "chunk_index": 0,
                "chunk_count": 1,
                "ctv_hash_hex": "14" * 32,
                "fanout_tx_template_hex": "04",
                "fanout_fee_sats": 13,
                "fanout_output_sum_sats": 487,
            },
        }
    ],
}
force_expired_idle_lease(replacement)
fanout_persist = replacement.persist_ctv_fanout_manifest_set(
    block_hash="44" * 32,
    manifest_set=fanout_manifest_set,
    manifest_set_sha256="15" * 32,
)
assert_equal(
    [fanout_persist["fanout_set_count"], fanout_persist["fanout_artifact_count"]],
    [1, 1],
    "same writer refreshes expired lease before CTV fanout persistence",
)
fanout_anchor_row = replacement._run_json(
    """
SELECT json_build_object(
    'anchor_vout_is_null', anchor_vout IS NULL,
    'fanout_output_sum_sats', fanout_output_sum_sats
)
FROM qbit_ctv_fanout_artifacts
WHERE fanout_txid = '""" + "12" * 32 + """';
"""
)
assert_equal(
    fanout_anchor_row,
    {"anchor_vout_is_null": True, "fanout_output_sum_sats": 487},
    "fee-bearing CTV fanout persists with NULL anchor_vout",
)
replacement._run_sql("DELETE FROM qbit_ctv_fanout_artifacts WHERE fanout_txid = '" + "12" * 32 + "';")
force_expired_idle_lease(replacement)
fanout_backfill_lease_stealer = PsqlShareLedger(
    psql_command=psql,
    writer_id="fanout-backfill-lease-stealer",
    writer_epoch=1,
)
try:
    replacement.persist_ctv_fanout_manifest_set(
        block_hash="44" * 32,
        manifest_set=fanout_manifest_set,
        manifest_set_sha256="15" * 32,
    )
except RuntimeError as exc:
    if "writer lease is not active" not in str(exc):
        raise
else:
    raise SystemExit("stale CTV fanout backfill unexpectedly succeeded")
assert_equal(
    fanout_backfill_lease_stealer._run_json(
        """
SELECT json_build_object(
    'artifact_count',
    (SELECT count(*) FROM qbit_ctv_fanout_artifacts WHERE fanout_txid = '""" + "12" * 32 + """')
);
"""
    )["artifact_count"],
    0,
    "stale CTV fanout backfill does not insert missing artifacts",
)
force_expired_idle_lease(fanout_backfill_lease_stealer)
replacement = PsqlShareLedger(
    psql_command=psql,
    writer_id="writer-b",
    writer_epoch=1,
    audit_bundle_canonicalizer=fake_audit_bundle_bytes,
)
fanout_backfill = replacement.persist_ctv_fanout_manifest_set(
    block_hash="44" * 32,
    manifest_set=fanout_manifest_set,
    manifest_set_sha256="15" * 32,
)
assert_equal(
    [fanout_backfill["fanout_set_count"], fanout_backfill["fanout_artifact_count"]],
    [1, 1],
    "matching CTV fanout set backfills a missing artifact row",
)
force_expired_idle_lease(replacement)
fanout_status_update = replacement.update_ctv_fanout_status(
    fanout_txid="12" * 32,
    settlement_status="broadcastable",
)
assert_equal(fanout_status_update["updated_count"], 1, "same writer refreshes expired lease before CTV status update")
force_expired_idle_lease(replacement)
fanout_attempt = replacement.record_ctv_fanout_broadcast_attempt(
    fanout_txid="12" * 32,
    attempt_status="submitted",
    package_tx_hexes=["02"],
    package_txids=["16" * 32],
    submit_result={"accepted": True},
)
assert_equal(
    [fanout_attempt["attempt_count"], fanout_attempt["updated_count"]],
    [1, 1],
    "same writer refreshes expired lease before CTV broadcast attempt",
)
fanout_status = replacement.ctv_fanout_status(fanout_txid="12" * 32)
assert_equal(fanout_status["settlement_status"], "broadcast_submitted", "CTV broadcast attempt updates status")
assert_equal(
    [attempt["attempt_status"] for attempt in fanout_status["broadcast_attempts"]],
    ["submitted"],
    "CTV broadcast attempt is journaled",
)
duplicate_persist = replacement.persist_accepted_block(
    block_hash="44" * 32,
    block_height=7,
    parent_hash="55" * 32,
    final_bundle=bundle,
    audit_report=report,
)
assert_equal(
    [
        duplicate_persist["block_count"],
        duplicate_persist["bundle_count"],
        duplicate_persist["payout_entry_count"],
        duplicate_persist["carry_forward_count"],
    ],
    [1, 1, 2, 2],
    "duplicate accepted block verifies existing rows instead of reporting zero inserts",
)
bad_bundle = copy.deepcopy(bundle)
bad_bundle["found_block"] = {"bits": "207fffff"}
bad_report = audit_report_for(bad_bundle)
try:
    replacement.persist_accepted_block(
        block_hash="44" * 32,
        block_height=7,
        parent_hash="55" * 32,
        final_bundle=bad_bundle,
        audit_report=bad_report,
    )
except RuntimeError as exc:
    if "existing audit bundle does not match payload" not in str(exc):
        raise
else:
    raise SystemExit("duplicate accepted block with mismatched audit digest unexpectedly succeeded")
owed = replacement.current_owed_balances()
assert_equal(owed, [], "prepared block must not affect owed balances before chain confirmation")
replacement._run_sql(
    """
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
        70,
        NULL,
        'miner-unanchored-null',
        'unanchored-null',
        decode('dd' || repeat('00', 31), 'hex'),
        777,
        0,
        777,
        0,
        777,
        'accrued',
        'immature'
    ),
    (
        71,
        '""" + "99" * 32 + """',
        'miner-unanchored-missing',
        'unanchored-missing',
        decode('ee' || repeat('00', 31), 'hex'),
        888,
        0,
        888,
        0,
        888,
        'accrued',
        'immature'
    );
"""
)
unanchored_balances = replacement._run_json(
    """
SELECT COALESCE(json_agg(miner_id ORDER BY miner_id), '[]'::json)
FROM qbit_current_carry_forward_balances()
WHERE miner_id LIKE 'miner-unanchored-%';
"""
)
assert_equal(unanchored_balances, [], "unanchored carry rows require a confirmed pool block")
confirmed = replacement.confirm_accepted_block(block_hash="44" * 32, active_tip_height=7)
assert_equal(confirmed["confirmed_count"], 1, "confirmed block count")
assert_equal(
    replacement.dashboard_miner_pending_maturity_bits(recipient_id="miner-b"),
    49500,
    "pending maturity totals net immature onchain outputs",
)
assert_equal(
    replacement.dashboard_miner_pending_maturity_bits(recipient_id="miner-a"),
    0,
    "pending maturity excludes accrued balances",
)
owed = replacement.current_owed_balances()
assert_equal([(row["recipient_id"], row["balance_sats"]) for row in owed], [("miner-a", 1000)], "owed balance before reversal")

alias_block_hash = "ef" * 32
alias_program_hex = "fa" * 32
replacement._run_sql(
    """
INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state
) VALUES (
    '""" + alias_block_hash + """',
    72,
    '""" + "44" * 32 + """',
    '""" + "46" * 32 + """',
    '""" + "47" * 32 + """',
    'confirmed'
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
        72,
        '""" + alias_block_hash + """',
        'alias-miner-b',
        '02',
        decode('""" + alias_program_hex + """', 'hex'),
        200,
        0,
        200,
        0,
        200,
        'accrued',
        'immature'
    ),
    (
        72,
        '""" + alias_block_hash + """',
        'alias-miner-a',
        '00',
        decode('""" + alias_program_hex + """', 'hex'),
        100,
        0,
        100,
        0,
        100,
        'accrued',
        'immature'
    );
"""
)
alias_balances = replacement._run_json(
    """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'order_key', payout_order_key,
    'balance_sats', balance_sats::text
) ORDER BY payout_order_key, miner_id), '[]'::json)
FROM qbit_current_carry_forward_balances()
WHERE p2mr_program = decode('""" + alias_program_hex + """', 'hex');
"""
)
assert_equal(
    alias_balances,
    [{"recipient_id": "alias-miner-a", "order_key": "00", "balance_sats": "300"}],
    "current carry balances aggregate same-payout-program aliases",
)
replacement._run_sql(
    """
UPDATE qbit_payout_carry_forward
SET maturity_state = 'reversed'
WHERE block_hash = '""" + alias_block_hash + """';

UPDATE qbit_pool_blocks
SET chain_state = 'reversed',
    maturity_state = 'reversed',
    disconnected_at = clock_timestamp()
WHERE block_hash = '""" + alias_block_hash + """';
"""
)

zero_net_bundle = copy.deepcopy(bundle)
zero_net_bundle["signed_coinbase_manifest"]["manifest"]["payout_count"] = 1
zero_net_bundle["payout_policy_manifest"]["accounts"] = [
    {
        "recipient_id": "miner-a",
        "order_key": "a",
        "p2mr_program_hex": "aa" * 32,
        "gross_amount_sats": 0,
        "prior_balance_sats": 1000,
        "candidate_balance_sats": 1000,
        "onchain_amount_sats": 1000,
        "carry_forward_balance_sats": 0,
        "action": "onchain",
    }
]
zero_net_report = audit_report_for(zero_net_bundle)
replacement.persist_accepted_block(
    block_hash="45" * 32,
    block_height=8,
    parent_hash="44" * 32,
    final_bundle=zero_net_bundle,
    audit_report=zero_net_report,
)
force_expired_idle_lease(replacement)
replacement.confirm_accepted_block(block_hash="45" * 32, active_tip_height=8)
zero_net_balances = replacement._run_json(
    """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'balance_sats', balance_sats::text
) ORDER BY miner_id), '[]'::json)
FROM qbit_current_carry_forward_balances()
WHERE miner_id = 'miner-a';
"""
)
assert_equal(zero_net_balances, [], "exact zero net carry replay omits current balance row")
force_expired_idle_lease(replacement)
replacement.reverse_immature_block(block_hash="45" * 32, active_tip_height=8)

replacement.persist_accepted_block(
    block_hash="46" * 32,
    block_height=9,
    parent_hash="45" * 32,
    final_bundle=zero_net_bundle,
    audit_report=zero_net_report,
)
rejected_count = replacement.reject_prepared_block(block_hash="46" * 32, active_tip_height=8)["rejected_count"]
assert_equal(rejected_count, 3, "reject prepared block/payout/carry row count")
rejected_state = replacement._run_json(
    """
SELECT json_build_object(
    'chain_state', chain_state,
    'maturity_state', maturity_state
)
FROM qbit_pool_blocks
WHERE block_hash = '""" + "46" * 32 + """';
"""
)
assert_equal(
    rejected_state,
    {"chain_state": "rejected", "maturity_state": "reversed"},
    "rejected prepared block state",
)

try:
    replacement._run_json("SELECT json_build_object('count', qbit_reverse_immature_pool_block('" + "44" * 32 + "', 7));")
except RuntimeError as exc:
    if "function qbit_reverse_immature_pool_block" not in str(exc):
        raise
else:
    raise SystemExit("unfenced raw reversal function unexpectedly exists")

inactive_count = replacement.mark_pool_block_inactive(block_hash="44" * 32, active_tip_height=7)["inactive_count"]
assert_equal(inactive_count, 1, "inactive block quarantine count")
assert_equal(replacement.current_owed_balances(), [], "inactive owed balances are excluded")
reactivated_count = replacement.reactivate_pool_block(block_hash="44" * 32, active_tip_height=7)["reactivated_count"]
assert_equal(reactivated_count, 1, "inactive block reactivation count")
if not replacement.current_owed_balances():
    raise SystemExit("reactivated block did not restore owed balances")
inactive_count = replacement.mark_pool_block_inactive(block_hash="44" * 32, active_tip_height=7)["inactive_count"]
assert_equal(inactive_count, 1, "inactive block can be quarantined again before final reversal")
reversed_count = replacement.reverse_immature_block(block_hash="44" * 32, active_tip_height=7)["reversed_count"]
assert_equal(reversed_count, 6, "reverse immature block/payout/carry/fanout row count")
assert_equal(replacement.current_owed_balances(), [], "reversed owed balances are excluded")

replacement.persist_accepted_block(
    block_hash="66" * 32,
    block_height=8,
    parent_hash="44" * 32,
    final_bundle=bundle,
    audit_report=report,
)
replacement.confirm_accepted_block(block_hash="66" * 32, active_tip_height=8)
inactive_count = replacement.mark_pool_block_inactive(block_hash="66" * 32, active_tip_height=8)["inactive_count"]
assert_equal(inactive_count, 1, "inactive block 66 quarantine count")
matured_while_inactive = replacement._run_json("SELECT json_build_object('count', qbit_mark_mature_pool_payouts(1008));")["count"]
assert_equal(matured_while_inactive, 0, "mature payout sweep ignores inactive blocks")
inactive_count = replacement.mark_pool_block_inactive(block_hash="66" * 32, active_tip_height=1008)["inactive_count"]
assert_equal(inactive_count, 0, "height-mature inactive block quarantine is idempotent")
reactivated_count = replacement.reactivate_pool_block(block_hash="66" * 32, active_tip_height=1008)["reactivated_count"]
assert_equal(reactivated_count, 1, "height-mature inactive block reactivation count")
inactive_count = replacement.mark_pool_block_inactive(block_hash="66" * 32, active_tip_height=1008)["inactive_count"]
assert_equal(inactive_count, 1, "height-mature immature confirmed block can be quarantined")
reactivated_count = replacement.reactivate_pool_block(block_hash="66" * 32, active_tip_height=1008)["reactivated_count"]
assert_equal(reactivated_count, 1, "height-mature re-quarantined block reactivation count")
matured_count = replacement._run_json("SELECT json_build_object('count', qbit_mark_mature_pool_payouts(1008));")["count"]
assert_equal(matured_count, 2, "mature payout entry count")
carry_states = replacement._run_json(
    """
SELECT COALESCE(json_agg(DISTINCT maturity_state ORDER BY maturity_state), '[]'::json)
FROM qbit_payout_carry_forward
WHERE block_hash = '""" + "66" * 32 + """';
"""
)
assert_equal(carry_states, ["mature"], "mature payout sweep includes carry-forward rows")
assert_equal(
    replacement.dashboard_miner_pending_maturity_bits(recipient_id="miner-b"),
    0,
    "pending maturity excludes mature outputs",
)
try:
    replacement.mark_pool_block_inactive(block_hash="66" * 32, active_tip_height=1008)
except RuntimeError as exc:
    if "refusing to mark mature pool block inactive" not in str(exc):
        raise
else:
    raise SystemExit("mature block quarantine unexpectedly succeeded")
try:
    replacement.reverse_immature_block(block_hash="66" * 32, active_tip_height=1008)
except RuntimeError as exc:
    if "refusing to reverse mature pool block" not in str(exc):
        raise
else:
    raise SystemExit("mature block reversal unexpectedly succeeded")

reorg_carry_block_a = "aa" * 32
reorg_carry_block_b = "ab" * 32
reorg_carry_miner = "miner-reorg-carry"
reorg_carry_bundle_a = copy.deepcopy(bundle)
reorg_carry_bundle_a["signed_coinbase_manifest"]["manifest"]["payout_count"] = 1
reorg_carry_bundle_a["payout_policy_manifest"]["accounts"] = [
    {
        "recipient_id": reorg_carry_miner,
        "order_key": "reorg-carry",
        "p2mr_program_hex": "cc" * 32,
        "gross_amount_sats": 1000,
        "prior_balance_sats": 0,
        "candidate_balance_sats": 1000,
        "onchain_amount_sats": 0,
        "carry_forward_balance_sats": 1000,
        "action": "accrued",
    }
]
reorg_carry_bundle_b = copy.deepcopy(reorg_carry_bundle_a)
reorg_carry_bundle_b["payout_policy_manifest"]["accounts"][0]["gross_amount_sats"] = 250
reorg_carry_bundle_b["payout_policy_manifest"]["accounts"][0]["prior_balance_sats"] = 1000
reorg_carry_bundle_b["payout_policy_manifest"]["accounts"][0]["candidate_balance_sats"] = 1250
reorg_carry_bundle_b["payout_policy_manifest"]["accounts"][0]["carry_forward_balance_sats"] = 1250
reorg_carry_report_a = audit_report_for(reorg_carry_bundle_a)
reorg_carry_report_b = audit_report_for(reorg_carry_bundle_b)
replacement.persist_accepted_block(
    block_hash=reorg_carry_block_a,
    block_height=90,
    parent_hash="66" * 32,
    final_bundle=reorg_carry_bundle_a,
    audit_report=reorg_carry_report_a,
)
replacement.confirm_accepted_block(block_hash=reorg_carry_block_a, active_tip_height=90)
replacement.persist_accepted_block(
    block_hash=reorg_carry_block_b,
    block_height=91,
    parent_hash=reorg_carry_block_a,
    final_bundle=reorg_carry_bundle_b,
    audit_report=reorg_carry_report_b,
)
replacement.confirm_accepted_block(block_hash=reorg_carry_block_b, active_tip_height=91)
assert_equal(
    replacement.reverse_immature_block(block_hash=reorg_carry_block_a, active_tip_height=91)["reversed_count"],
    3,
    "reorg carry block A reversal count",
)
reorg_carry_balances = replacement._run_json(
    """
SELECT COALESCE(json_agg(json_build_object(
    'recipient_id', miner_id,
    'balance_sats', balance_sats::text
) ORDER BY miner_id), '[]'::json)
FROM qbit_current_carry_forward_balances()
WHERE miner_id = '""" + reorg_carry_miner + """';
"""
)
assert_equal(
    [(row["recipient_id"], int(row["balance_sats"])) for row in reorg_carry_balances],
    [(reorg_carry_miner, 250)],
    "later carry row remains independently valid after earlier row reversal",
)
integrity_report = replacement.carry_forward_integrity_report()
assert_equal(integrity_report["mismatch_count"], 1, "replayed prior mismatch count")
assert_equal(
    integrity_report["audit_chain_version"],
    "qbit.prism.carry-forward-active-delta-chain.v1",
    "carry-forward audit chain version",
)
if integrity_report["audit_row_count"] <= 0:
    raise SystemExit("carry-forward audit chain row count unexpectedly empty")
if len(integrity_report["audit_head_sha256"]) != 64:
    raise SystemExit("carry-forward audit head is not a sha256 hex digest")
assert_equal(
    integrity_report["mismatches"][0]["recipient_id"],
    reorg_carry_miner,
    "replayed prior mismatch recipient",
)
assert_equal(
    integrity_report["mismatches"][0]["mismatch_reason"],
    "prior_balance,candidate_balance,carry_forward_balance",
    "replayed prior mismatch reason",
)
assert_equal(
    replacement.reverse_immature_block(block_hash=reorg_carry_block_b, active_tip_height=91)["reversed_count"],
    3,
    "reorg carry block B cleanup reversal count",
)
assert_equal(
    replacement._run_json(
        """
SELECT COALESCE(json_agg(miner_id ORDER BY miner_id), '[]'::json)
FROM qbit_current_carry_forward_balances()
WHERE miner_id = '""" + reorg_carry_miner + """';
"""
    ),
    [],
    "reorg carry cleanup removes isolated miner from current balances",
)
assert_equal(
    replacement.carry_forward_integrity_report()["mismatch_count"],
    0,
    "reorg carry cleanup clears integrity mismatches",
)

force_expired_idle_lease(replacement)
final_writer = PsqlShareLedger(
    psql_command=psql,
    writer_id="writer-c",
    writer_epoch=1,
    audit_bundle_canonicalizer=fake_audit_bundle_bytes,
)
try:
    replacement.persist_accepted_block(
        block_hash="88" * 32,
        block_height=9,
        parent_hash="66" * 32,
        final_bundle=bundle,
        audit_report=report,
    )
except RuntimeError as exc:
    if "writer lease is not active" not in str(exc):
        raise
else:
    raise SystemExit("stale writer persisted an accepted block after replacement acquired the lease")

stale_bundle = copy.deepcopy(bundle)
stale_bundle["payout_policy_manifest"]["accounts"][0]["gross_amount_sats"] = 250
stale_bundle["payout_policy_manifest"]["accounts"][0]["candidate_balance_sats"] = 250
stale_bundle["payout_policy_manifest"]["accounts"][0]["carry_forward_balance_sats"] = 250
stale_report = audit_report_for(stale_bundle)
final_writer.persist_accepted_block(
    block_hash="77" * 32,
    block_height=6,
    parent_hash="66" * 32,
    final_bundle=stale_bundle,
    audit_report=stale_report,
)
try:
    replacement.reverse_immature_block(block_hash="77" * 32, active_tip_height=6)
except RuntimeError as exc:
    if "writer lease is not active" not in str(exc):
        raise
else:
    raise SystemExit("stale writer reversed a block after replacement acquired the lease")
assert_equal(
    [(row["recipient_id"], row["balance_sats"]) for row in final_writer.current_owed_balances()],
    [("miner-a", 1000)],
    "out-of-order lower-height carry-forward persistence must not rewind current balance",
)
assert_equal(
    final_writer.reverse_immature_block(block_hash="77" * 32, active_tip_height=6)["reversed_count"],
    5,
    "active replacement writer can reverse its prepared block",
)

final_writer._run_sql(
    """
UPDATE qbit_ledger_writer_lease
SET writer_id = 'deploy-writer',
    writer_epoch = 7,
    writer_session_token = 'old-deploy-session',
    lease_expires_at = clock_timestamp() + interval '0.2 seconds',
    updated_at = clock_timestamp()
WHERE singleton;
"""
)
deploy_writer = PsqlShareLedger(
    psql_command=psql,
    writer_id="deploy-writer",
    writer_epoch=7,
    lease_retry_max_sleep_seconds=0.05,
)
deploy_lease = deploy_writer._run_json(
    """
SELECT json_build_object(
    'writer_id', writer_id,
    'writer_epoch', writer_epoch,
    'writer_session_token', writer_session_token
)
FROM qbit_ledger_writer_lease
WHERE singleton;
"""
)
assert_equal(
    [
        deploy_lease["writer_id"],
        deploy_lease["writer_epoch"],
        deploy_lease["writer_session_token"],
    ],
    ["deploy-writer", 7, deploy_writer._writer_session_token],
    "same writer predecessor startup waits for lease expiry and then acquires",
)

with tempfile.TemporaryDirectory() as audit_body_dir:
    external_writer = PsqlShareLedger(
        psql_command=psql,
        writer_id="deploy-writer",
        writer_epoch=7,
        writer_session_token=deploy_writer._writer_session_token,
        audit_body_dir=audit_body_dir,
        audit_bundle_canonicalizer=fake_audit_bundle_bytes,
    )
    external_block_hash = "d9" * 32
    commitment_leaf = "ab" * 32
    witness_leaf = "cd" * 32
    external_bundle = copy.deepcopy(bundle)
    external_bundle["schema"] = "qbit.prism.audit-bundle.v1"
    external_bundle["found_block"] = {
        "network_difficulty": 2500,
        "bits": "207fffff",
        "coinbase_value_sats": 123456,
    }
    external_bundle["audit_commitment_leaves_hex"] = [commitment_leaf]
    external_bundle["witness_merkle_leaves_hex"] = [witness_leaf]
    external_report = {
        "coinbase_txid": "da" * 32,
        "coinbase_manifest_sha256_hex": "98" * 32,
        "audit_bundle_sha256_hex": fake_audit_bundle_sha256(external_bundle),
        "coinbase_tx_hex": "01",
    }
    external_persist = external_writer.persist_accepted_block(
        block_hash=external_block_hash,
        block_height=12,
        parent_hash="97" * 32,
        final_bundle=external_bundle,
        audit_report=external_report,
    )
    assert_equal(
        [
            external_persist["block_count"],
            external_persist["bundle_count"],
            external_persist["payout_entry_count"],
            external_persist["carry_forward_count"],
        ],
        [1, 1, 2, 2],
        "externalized accepted block persistence counts",
    )
    body_files = sorted(Path(audit_body_dir).glob("prism-audit-bundle-body-*.json"))
    assert_equal(len(body_files), 1, "externalized audit body writes exactly one file")
    if external_report["audit_bundle_sha256_hex"] not in body_files[0].name:
        raise SystemExit("externalized audit body filename does not include bundle sha256")

    external_row = external_writer._run_json(
        """
SELECT json_build_object(
    'inline_body', audit_bundle IS NOT NULL,
    'body_uri', body_uri,
    'network_difficulty', found_block_network_difficulty::text,
    'bits', found_block_bits,
    'coinbase_value_sats', found_block_coinbase_value_sats,
    'commitment_leaf_count', jsonb_array_length(audit_commitment_leaves_hex),
    'witness_leaf_count', jsonb_array_length(witness_merkle_leaves_hex)
)
FROM qbit_pool_audit_bundles
WHERE block_hash = '""" + external_block_hash + """';
"""
    )
    assert_equal(external_row["inline_body"], False, "externalized row does not keep inline audit_bundle")
    assert_equal(external_row["body_uri"], str(body_files[0].resolve()), "externalized row points at body file")
    assert_equal(
        [
            external_row["network_difficulty"],
            external_row["bits"],
            external_row["coinbase_value_sats"],
            external_row["commitment_leaf_count"],
            external_row["witness_leaf_count"],
        ],
        ["2500", "207fffff", 123456, 1, 1],
        "externalized row stores promoted metadata",
    )
    assert_equal(
        external_writer.audit_bundle(block_hash=external_block_hash)["audit_bundle"],
        external_bundle,
        "audit_bundle reader resolves external body",
    )
    assert_equal(
        external_writer.audit_bundle_by_commitment(commitment_leaf_hex=commitment_leaf)["audit_bundle"],
        external_bundle,
        "commitment reader resolves external body from promoted leaves",
    )
    assert_equal(
        external_writer.dashboard_public_artifact(sha256=external_report["audit_bundle_sha256_hex"]),
        external_bundle,
        "public artifact reader resolves external body",
    )
    external_blocks = external_writer.dashboard_blocks(page=1, limit=1)
    assert_equal(
        [
            external_blocks["rows"][0]["hash"],
            external_blocks["rows"][0]["network_difficulty"],
            external_blocks["rows"][0]["bits"],
            external_blocks["rows"][0]["coinbase_value_bits"],
        ],
        [external_block_hash, "2500", "207fffff", 123456],
        "dashboard blocks reads promoted metadata without inline bundle",
    )
    duplicate_external = external_writer.persist_accepted_block(
        block_hash=external_block_hash,
        block_height=12,
        parent_hash="97" * 32,
        final_bundle=external_bundle,
        audit_report=external_report,
    )
    assert_equal(duplicate_external["bundle_count"], 1, "externalized duplicate verifies existing bundle")
    assert_equal(
        len(list(Path(audit_body_dir).glob("prism-audit-bundle-body-*.json"))),
        1,
        "externalized duplicate reuses body file",
    )
    body_files[0].unlink()
    force_expired_idle_lease(external_writer)
    external_successor = PsqlShareLedger(
        psql_command=psql,
        writer_id="external-successor",
        writer_epoch=1,
        audit_body_dir=audit_body_dir,
        audit_bundle_canonicalizer=fake_audit_bundle_bytes,
    )
    try:
        external_writer.persist_accepted_block(
            block_hash=external_block_hash,
            block_height=12,
            parent_hash="97" * 32,
            final_bundle=external_bundle,
            audit_report=external_report,
        )
    except RuntimeError as exc:
        if "writer lease is not active" not in str(exc):
            raise
    else:
        raise SystemExit("stale externalized duplicate restored a missing body after losing lease")
    assert_equal(
        len(list(Path(audit_body_dir).glob("prism-audit-bundle-body-*.json"))),
        0,
        "stale externalized duplicate does not restore missing body before lease",
    )
    restored_external = external_successor.persist_accepted_block(
        block_hash=external_block_hash,
        block_height=12,
        parent_hash="97" * 32,
        final_bundle=external_bundle,
        audit_report=external_report,
    )
    assert_equal(
        restored_external["bundle_count"],
        1,
        "active successor externalized duplicate restores missing body after lease",
    )
    restored_body_files = sorted(Path(audit_body_dir).glob("prism-audit-bundle-body-*.json"))
    assert_equal(len(restored_body_files), 1, "externalized duplicate restores missing body file")
    assert_equal(
        external_successor.audit_bundle(block_hash=external_block_hash)["audit_bundle"],
        external_bundle,
        "restored external body remains readable",
    )
    conflicting_bundle = copy.deepcopy(external_bundle)
    conflicting_bundle["found_block"]["coinbase_value_sats"] = 123457
    conflicting_report = {
        **external_report,
        "audit_bundle_sha256_hex": fake_audit_bundle_sha256(conflicting_bundle),
    }
    try:
        external_successor.persist_accepted_block(
            block_hash=external_block_hash,
            block_height=12,
            parent_hash="97" * 32,
            final_bundle=conflicting_bundle,
            audit_report=conflicting_report,
        )
    except RuntimeError as exc:
        if "existing audit bundle does not match payload" not in str(exc):
            raise
    else:
        raise SystemExit("externalized conflicting duplicate unexpectedly succeeded")
    assert_equal(
        len(list(Path(audit_body_dir).glob("prism-audit-bundle-body-*.json"))),
        1,
        "externalized conflicting duplicate does not write an orphan body file",
    )

print("prism postgres ledger PASS shares=14 lease=replay startup-retry persist-fence sql-window maturity=reorg carry-replay integrity")
PY
)
