#!/usr/bin/env python3
"""Real-PostgreSQL gate for chunked candidate bodies (issue #255).

Runs against a disposable database through the transport the environment
offers: ``PRISM_TEST_PSQL_COMMAND`` (the psql subprocess backend) and,
when ``PRISM_TEST_DATABASE_URL`` is set and psycopg imports, the native
pooled client as well. Every check is a semantic gate on the shipped
``PsqlShareLedger``: the additive migration applies twice, both durable
write routes stage outside the writer gate and publish under the fence,
the metadata-first page answers exhaustion, hydration reproduces the
share array from bounded chunk pages, terminalization detaches the body,
the janitor reclaims it in bounded steps, the database's own triggers
refuse every mutation of a sealed body, and a body an outbox row
references cannot be retired.

Usage: ``python3 -m tests.prism_postgres_candidate_gate``; the wrapper
``test/test-prism-postgres-candidate-storage.sh`` provisions the container.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.share_ledger import (  # noqa: E402
    PendingShare,
    PsqlShareLedger,
    block_candidate_identity_sha256,
)

CHECKS: list[str] = []


def check(condition: object, label: str) -> None:
    if not condition:
        raise SystemExit(f"prism postgres candidate gate: FAIL {label}")
    CHECKS.append(label)
    print(f"prism postgres candidate gate: ok {label}", flush=True)


def psql_json(command: str, sql: str) -> object:
    completed = subprocess.run(
        [*shlex.split(command), "--tuples-only", "--no-align", "--quiet"],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip())
    output = completed.stdout.strip()
    return json.loads(output.splitlines()[-1]) if output else None


def psql_expect_error(command: str, sql: str, fragment: str, label: str) -> None:
    completed = subprocess.run(
        [*shlex.split(command), "--quiet", "--set", "ON_ERROR_STOP=1"],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    refused = completed.returncode != 0 and fragment in completed.stderr
    if not refused:
        print(
            f"prism postgres candidate gate: unexpected outcome for {label}: "
            f"exit={completed.returncode} stderr={completed.stderr.strip()[:400]!r} "
            f"stdout={completed.stdout.strip()[:200]!r}",
            flush=True,
        )
    check(refused, f"{label} ({fragment})")


def share(index: int) -> dict[str, object]:
    return {
        "share_seq": index,
        "share_id": f"miner-{index % 5}:é{index}",
        "miner_id": f"m{index % 5}",
        "order_key": "k",
        "p2mr_program_hex": "ab" * 32,
        "share_difficulty": 10**20 + index,
        "network_difficulty": 1000,
        "template_height": 9,
        "job_id": "job-a",
        "job_issued_at_ms": 1_700_000_000_000 + index,
        "accepted_at_ms": 1_700_000_000_001 + index,
        "ntime": 1_700_000_000,
        "credit_policy": None,
    }


def intent(block_hash: str, share_count: int, *, credit: bool) -> dict[str, object]:
    pending = {
        "share_id": f"miner-0:{block_hash}",
        "miner_id": "m0",
        "order_key": "m0",
        "p2mr_program_hex": "ab" * 32,
        "share_difficulty": 100,
        "network_difficulty": 1000,
        "template_height": 9,
        "job_id": "job-a",
        "job_issued_at_ms": 1_700_000_000_000,
        "accepted_at_ms": 1_700_000_000_500,
        "ntime": 1_700_000_000,
        "credit_policy": None,
    }
    return {
        "schema": "qbit.prism.block-candidate-intent.v1",
        "block_hash_hex": block_hash,
        "block_hex": "00" * 200,
        "coinbase_tx_hex": "01",
        "parent_hash": "11" * 32,
        "expected_height": 10,
        "template": {"previousblockhash": "11" * 32, "height": 10, "coinbasevalue": 5},
        "shares_json": [share(index) for index in range(share_count)],
        "prior_balances": [{"recipient_id": f"r{index}", "balance_sats": index} for index in range(3000)],
        "found_block": {"network_difficulty": 1000},
        "prospective_prior_balances": None,
        "witness_merkle_leaves_hex": ["cc" * 32] * 5,
        "extranonce1_hex": "00",
        "extranonce2_hex": "01",
        "username": "miner-0",
        "pending_share": pending,
        "credit_share_on_accept": credit,
        "collection_only": False,
    }


def pending_share_for(fields: dict[str, object]) -> PendingShare:
    return PendingShare(**dict(fields["pending_share"]))  # type: ignore[arg-type]


def run_gate(psql_command: str, database_url: str | None, *, native: bool) -> None:
    label = "native" if native else "psql"
    ledger = PsqlShareLedger(
        psql_command=psql_command,
        database_url=database_url if native else None,
        native_client_mode="1" if native else "0",
        writer_id=f"writer-{label}",
        writer_epoch=1,
        initialize_schema=True,
        candidate_spool_dir=os.environ.get("PRISM_TEST_SPOOL_DIR"),
    )
    try:
        # The migration is idempotent: apply it a second time through psql.
        root = Path(__file__).resolve().parents[1]
        psql_json(psql_command, (root / "crates/qbit-prism/sql/002_candidate_bodies.sql").read_text() + "\nSELECT json_build_object('ok', true);")
        check(ledger.verify_candidate_schema() == {"declared": 2, "has_body_table": True}, f"{label}: schema capability declared")

        block_a = hashlib.sha256(f"{label}-a-{uuid.uuid4().hex}".encode()).hexdigest()
        fields = intent(block_a, 3000, credit=True)
        oracle_digest = block_candidate_identity_sha256(fields)
        started = time.monotonic()
        result = ledger.persist_block_candidate_intent(fields)
        check(result.inserted and result.state == "pending", f"{label}: standalone persist publishes a chunked body")
        again = ledger.persist_block_candidate_intent(intent(block_a, 3000, credit=True))
        check(not again.inserted and again.state == "pending", f"{label}: exact standalone replay is idempotent")
        try:
            ledger.persist_block_candidate_intent({**intent(block_a, 3000, credit=True), "username": "other"})
            check(False, f"{label}: differing payload is refused")
        except RuntimeError as exc:
            check("payload mismatch" in str(exc), f"{label}: differing payload is refused")
        print(f"prism postgres candidate gate: staged 3000-share body in {time.monotonic() - started:.2f}s", flush=True)

        row = psql_json(
            psql_command,
            f"SELECT json_build_object('storage_version', storage_version, 'candidate_sha256', candidate_sha256, 'body_id', body_id, 'state', state, 'candidate_is_null', candidate IS NULL) FROM qbit_block_candidate_outbox WHERE block_hash = '{block_a}';",
        )
        check(row["storage_version"] == 2 and row["candidate_is_null"] and row["candidate_sha256"] == oracle_digest, f"{label}: outbox row is version 2 with the historical digest")
        body_id = row["body_id"]
        manifest = psql_json(psql_command, f"SELECT json_build_object('state', state, 'chunk_count', chunk_count, 'span_count', span_count, 'page_count', page_count) FROM qbit_block_candidate_body WHERE body_id = '{body_id}';")
        check(manifest["state"] == "sealed" and manifest["chunk_count"] >= 1 and manifest["span_count"] >= 2 and manifest["page_count"] >= 2, f"{label}: manifest is sealed with spans and pages")

        page = ledger.pending_block_candidate_headers(limit=32)
        check(len(page.rows) == 1 and page.exhausted and page.rows[0]["block_hash"] == block_a, f"{label}: header page enumerates the row and answers exhaustion")
        header = page.rows[0]["header"]
        check(header["parent_hash"] == "11" * 32 and header["expected_height"] == 10 and header["pending_share"]["accepted_at_ms"] == 1_700_000_000_500 and header["oversized"] is False, f"{label}: bounded header carries the typed facts")
        check(page.rows[0]["body"]["state"] == "sealed" and page.rows[0]["body"]["body_id"] == body_id, f"{label}: header carries the sealed body reference")

        hydrated = ledger.hydrate_block_candidate_intent(page.rows[0])
        check(len(hydrated["shares_json"]) == 3000 and hydrated["shares_json"][2999] == share(2999) and hydrated["shares_json"][0] == share(0), f"{label}: hydration reproduces the share array")
        check(hydrated["pending_share"]["accepted_at_ms"] == 1_700_000_000_500 and hydrated.candidate_sha256 == oracle_digest, f"{label}: hydration restores the stamp and identity")
        check(list(hydrated["prior_balances"]) == fields["prior_balances"], f"{label}: large balances decode through the spool adapter")
        spool_path = hydrated.body.path
        hydrated.body.close()
        check(not os.path.exists(spool_path), f"{label}: spool is unlinked on close")

        # Immutability at the database boundary.
        psql_expect_error(psql_command, f"INSERT INTO qbit_block_candidate_body_chunk (body_id, ordinal, chunk, chunk_sha256) VALUES ('{body_id}', 9999, '\\x00', '{'0' * 64}');", "accepts no parts", f"{label}: chunk insert after seal is refused")
        psql_expect_error(psql_command, f"UPDATE qbit_block_candidate_body_chunk SET chunk = '\\x00' WHERE body_id = '{body_id}' AND ordinal = 0;", "immutable", f"{label}: chunk update is refused")
        psql_expect_error(psql_command, f"DELETE FROM qbit_block_candidate_body_chunk WHERE body_id = '{body_id}' AND ordinal = 0;", "cannot be deleted", f"{label}: sealed chunk delete is refused")
        psql_expect_error(psql_command, f"UPDATE qbit_block_candidate_body SET byte_count = byte_count + 1 WHERE body_id = '{body_id}';", "is sealed", f"{label}: sealed manifest scalars are immutable")
        psql_expect_error(psql_command, f"UPDATE qbit_block_candidate_body SET state = 'retired', retired_at = clock_timestamp() WHERE body_id = '{body_id}';", "referenced by a pending outbox row", f"{label}: a referenced body cannot be retired")
        psql_expect_error(psql_command, f"INSERT INTO qbit_block_candidate_body_page (body_id, field, page_ordinal, body_offset, record_index) VALUES ('{body_id}', 'x', 99999, 0, 0);", "accepts no parts", f"{label}: page insert after seal is refused")
        check(ledger.retire_orphan_candidate_bodies() == (), f"{label}: orphan retirement leaves the referenced body alone")

        # Credit-on-accept: the share append joins the published body without re-uploading.
        pending = pending_share_for(fields)
        records = ledger.append_batch([(pending, intent(block_a, 3000, credit=True))])
        check(len(records) == 1 and records[0].candidate_outbox_state == "pending", f"{label}: credit append links the existing body")
        linked = psql_json(psql_command, f"SELECT json_build_object('share_id', share_id, 'body_id', body_id) FROM qbit_block_candidate_outbox WHERE block_hash = '{block_a}';")
        check(linked["share_id"] == pending.share_id and linked["body_id"] == body_id, f"{label}: outbox row carries the share and the same body")
        replay = ledger.append_batch([(pending, intent(block_a, 3000, credit=True))])
        check(replay[0].share_seq == records[0].share_seq, f"{label}: exact replay of the credit append is idempotent")
        try:
            ledger.append_batch([(pending, {**intent(block_a, 3000, credit=True), "username": "other"})])
            check(False, f"{label}: differing credit payload is refused")
        except RuntimeError as exc:
            check("payload mismatch" in str(exc), f"{label}: differing credit payload is refused")

        # Async route: a fresh share with a new candidate in one fenced statement.
        block_b = hashlib.sha256(f"{label}-b-{uuid.uuid4().hex}".encode()).hexdigest()
        fields_b = intent(block_b, 700, credit=False)
        records_b = ledger.append_batch([(pending_share_for(fields_b), fields_b)])
        check(records_b[0].candidate_outbox_state == "pending", f"{label}: share append publishes a new chunked candidate")
        page = ledger.pending_block_candidate_headers(limit=1)
        check(len(page.rows) == 1 and not page.exhausted, f"{label}: a full page is not exhaustion")
        page2 = ledger.pending_block_candidate_headers(limit=1, after_cursor=page.next_cursor)
        check(len(page2.rows) == 1 and page2.exhausted, f"{label}: the cursor walks to the last row and proves exhaustion")

        # Terminalization detaches the body; the janitor reclaims it in bounded steps.
        check(ledger.mark_block_candidate_submitted(block_hash=block_a), f"{label}: terminal write succeeds")
        detached = psql_json(psql_command, f"SELECT json_build_object('body_id', body_id, 'retired_body_id', retired_body_id, 'state', (SELECT state FROM qbit_block_candidate_body WHERE body_id = '{body_id}')) FROM qbit_block_candidate_outbox WHERE block_hash = '{block_a}';")
        check(detached["body_id"] is None and detached["retired_body_id"] == body_id and detached["state"] == "retired", f"{label}: terminal write detaches and retires the body")
        steps = 0
        reclaimed = False
        while steps < 500:
            outcome = ledger.reap_retired_candidate_bodies(max_chunks=2)
            steps += 1
            if outcome["deleted_bodies"]:
                reclaimed = True
                break
            if outcome["body_id"] is None:
                break
        remaining = psql_json(psql_command, f"SELECT json_build_object('body', EXISTS (SELECT 1 FROM qbit_block_candidate_body WHERE body_id = '{body_id}'), 'chunks', EXISTS (SELECT 1 FROM qbit_block_candidate_body_chunk WHERE body_id = '{body_id}'), 'pages', EXISTS (SELECT 1 FROM qbit_block_candidate_body_page WHERE body_id = '{body_id}'));")
        check(reclaimed and not any(remaining.values()) and steps > 3, f"{label}: janitor reclaims the retired body in {steps} bounded steps")

        # A pre-#255 terminal write cannot touch a version-2 row (rollback floor).
        psql_expect_error(psql_command, f"UPDATE qbit_block_candidate_outbox SET state = 'abandoned', completed_at = clock_timestamp(), candidate = NULL WHERE block_hash = '{block_b}';", "dual_format_check", f"{label}: legacy terminal write is refused by the schema")
        check(ledger.mark_block_candidate_abandoned(block_hash=block_b, error="gate"), f"{label}: fenced abandon detaches the second body")
        # Drain the second body too, then a step with nothing to do.
        for _ in range(500):
            if not ledger.reap_retired_candidate_bodies(max_chunks=64)["body_id"]:
                break
        check(ledger.reap_retired_candidate_bodies()["body_id"] is None, f"{label}: janitor idles with nothing retired")
    finally:
        ledger.release_writer_lease()
        ledger.close()


def main() -> int:
    psql_command = os.environ["PRISM_TEST_PSQL_COMMAND"]
    database_url = os.environ.get("PRISM_TEST_DATABASE_URL")
    run_gate(psql_command, database_url, native=False)
    native_available = False
    if database_url:
        try:
            import psycopg  # noqa: F401

            native_available = True
        except ImportError:
            print("prism postgres candidate gate: psycopg unavailable; native transport skipped", flush=True)
    if native_available:
        run_gate(psql_command, database_url, native=True)
    print(f"prism postgres candidate gate: PASS ({len(CHECKS)} checks)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
