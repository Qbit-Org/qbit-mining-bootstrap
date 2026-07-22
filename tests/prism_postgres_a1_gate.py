"""Non-discovered PostgreSQL/A1 integration gate.

This helper is invoked by ``test/test-prism-postgres-ledger.sh``.  It lives
outside unittest discovery because every scenario requires an explicitly
provisioned PostgreSQL target.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import tempfile
import threading
from typing import Any

from lab.prism.share_ledger import PsqlShareLedger, SingleWriterShareLedger


PSQL_TIMEOUT_SECONDS = 30.0
PSQL_OUTPUT_LIMIT_BYTES = 1 << 20
SCHEMA_PATTERN = re.compile(r"qbit_a1_[a-z0-9_]+")
BASE_PSQL_COMMAND = os.environ.get("PRISM_PSQL_COMMAND", "")
if not BASE_PSQL_COMMAND:
    raise SystemExit("PRISM_PSQL_COMMAND is required")
BASE_PSQL_ARGV = shlex.split(BASE_PSQL_COMMAND)
if not BASE_PSQL_ARGV:
    raise SystemExit("PRISM_PSQL_COMMAND is empty")

RUN_TOKEN = os.urandom(16).hex()
RUN_MARKER = f"qbit-a1-test:{RUN_TOKEN}"
OWNED_SCHEMAS: list[tuple[str, str]] = []
ACTIVE_CHILDREN: set[subprocess.Popen[str]] = set()
ACTIVE_CHILDREN_LOCK = threading.Lock()


def fake_bundle_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


PERSIST_BUNDLE: dict[str, object] = {
    "signed_coinbase_manifest": {"manifest": {"payout_count": 1}},
    "payout_policy_manifest": {
        "accounts": [
            {
                "recipient_id": "a1-parity-miner",
                "order_key": "a1-parity-order",
                "p2mr_program_hex": "42" * 32,
                "gross_amount_sats": 1000,
                "prior_balance_sats": 0,
                "candidate_balance_sats": 1000,
                "onchain_amount_sats": 0,
                "carry_forward_balance_sats": 1000,
                "action": "accrued",
            }
        ]
    },
}
PERSIST_BUNDLE_SHA256 = hashlib.sha256(
    fake_bundle_bytes(PERSIST_BUNDLE)
).hexdigest()
PERSIST_REPORT: dict[str, object] = {
    "coinbase_txid": "20" * 32,
    "coinbase_manifest_sha256_hex": "30" * 32,
    "audit_bundle_sha256_hex": PERSIST_BUNDLE_SHA256,
    "coinbase_tx_hex": "00",
}


class GateFailure(RuntimeError):
    pass


def assert_equal(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise GateFailure(f"{message}: expected {expected!r}, got {actual!r}")


def _terminate_and_reap(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        process.wait()
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=2.0)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=2.0)


def cleanup_active_children() -> None:
    with ACTIVE_CHILDREN_LOCK:
        children = list(ACTIVE_CHILDREN)
    for process in children:
        _terminate_and_reap(process)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        with ACTIVE_CHILDREN_LOCK:
            ACTIVE_CHILDREN.discard(process)


def run_psql(sql: str, *, schema: str | None = None) -> str:
    if schema is not None and SCHEMA_PATTERN.fullmatch(schema) is None:
        raise GateFailure(f"invalid scoped schema: {schema!r}")
    scoped_sql = sql
    if schema is not None:
        scoped_sql = (
            "SET statement_timeout = '20s';\n"
            "SET lock_timeout = '10s';\n"
            f'SET search_path TO "{schema}", pg_catalog;\n'
            + sql
        )
    command = [
        *BASE_PSQL_ARGV,
        "--no-psqlrc",
        "--set",
        "ON_ERROR_STOP=1",
        "--tuples-only",
        "--no-align",
        "--quiet",
    ]
    with (
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stdout_file,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr_file,
    ):
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            start_new_session=True,
        )
        with ACTIVE_CHILDREN_LOCK:
            ACTIVE_CHILDREN.add(process)
        try:
            process.communicate(
                scoped_sql,
                timeout=PSQL_TIMEOUT_SECONDS,
            )
        except BaseException:
            _terminate_and_reap(process)
            raise
        finally:
            if process.poll() is None:
                _terminate_and_reap(process)
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            with ACTIVE_CHILDREN_LOCK:
                ACTIVE_CHILDREN.discard(process)
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read(PSQL_OUTPUT_LIMIT_BYTES + 1)
        stderr = stderr_file.read(PSQL_OUTPUT_LIMIT_BYTES + 1)
        if (
            len(stdout.encode("utf-8")) > PSQL_OUTPUT_LIMIT_BYTES
            or len(stderr.encode("utf-8")) > PSQL_OUTPUT_LIMIT_BYTES
        ):
            raise GateFailure("psql output exceeded 1 MiB limit")
        if process.returncode != 0:
            raise GateFailure(
                f"psql failed with exit {process.returncode}: {stderr.strip()}"
            )
        return stdout


def run_json(sql: str, *, schema: str | None = None) -> Any:
    output = run_psql(sql, schema=schema).strip()
    if not output:
        raise GateFailure("psql query returned no JSON")
    return json.loads(output.splitlines()[-1])


def _schema_registered(schema: str) -> bool:
    return any(name == schema for name, _marker in OWNED_SCHEMAS)


def create_owned_schema(label: str) -> str:
    if re.fullmatch(r"[a-z0-9_]+", label) is None:
        raise GateFailure(f"invalid schema label: {label!r}")
    schema = f"qbit_a1_{label}_{os.getpid()}_{os.urandom(5).hex()}"
    if SCHEMA_PATTERN.fullmatch(schema) is None or len(schema) > 63:
        raise GateFailure(f"invalid generated schema: {schema!r}")
    # Register authority before the transaction starts. If this call is
    # interrupted, run_psql kills/reaps its child before atexit examines the
    # durable marker.
    OWNED_SCHEMAS.append((schema, RUN_MARKER))
    run_psql(
        f"""
BEGIN;
CREATE SCHEMA "{schema}";
COMMENT ON SCHEMA "{schema}" IS '{RUN_MARKER}';
COMMIT;
"""
    )
    return schema


def cleanup_owned_schemas() -> None:
    while OWNED_SCHEMAS:
        schema, expected_marker = OWNED_SCHEMAS[-1]
        if SCHEMA_PATTERN.fullmatch(schema) is None:
            raise GateFailure(f"refusing to drop invalid schema: {schema!r}")
        marker = run_json(
            f"""
SELECT json_build_object(
    'marker', (
        SELECT obj_description(oid, 'pg_namespace')
        FROM pg_namespace
        WHERE nspname = '{schema}'
    )
);
"""
        )["marker"]
        if marker is None:
            OWNED_SCHEMAS.pop()
            continue
        if marker != expected_marker:
            raise GateFailure(
                f"refusing to drop schema with wrong marker: {schema!r}"
            )
        run_psql(f'DROP SCHEMA "{schema}" CASCADE;')
        OWNED_SCHEMAS.pop()


def _signal_exit(signum: int, _frame: object) -> None:
    # Raising first lets the active run_psql frame kill and reap its exact
    # process group. Registered atexit cleanup runs only after that unwind.
    raise SystemExit(128 + signum)


signal.signal(signal.SIGTERM, _signal_exit)
signal.signal(signal.SIGINT, _signal_exit)
atexit.register(cleanup_owned_schemas)
atexit.register(cleanup_active_children)


class ScopedPsqlLedger(PsqlShareLedger):
    def __init__(self, *, test_schema: str, **kwargs: object) -> None:
        if not _schema_registered(test_schema):
            raise GateFailure("ledger requires a registered owned schema")
        self._test_schema = test_schema
        kwargs["psql_command"] = BASE_PSQL_COMMAND
        kwargs["native_client_mode"] = "psql"
        super().__init__(**kwargs)  # type: ignore[arg-type]

    def _run_sql(self, sql: str) -> str:
        if not _schema_registered(self._test_schema):
            raise GateFailure("ledger schema authority was revoked")
        return run_psql(sql, schema=self._test_schema)


def public_sentinel() -> dict[str, object]:
    return run_json(
        """
SELECT json_build_object(
    'pool_oid', 'public.qbit_pool_blocks'::regclass::oid,
    'pool_rows', (
        SELECT md5(COALESCE(
            string_agg(row_to_json(row_value)::text, ',' ORDER BY block_hash),
            ''
        ))
        FROM public.qbit_pool_blocks row_value
    ),
    'sequence_oid',
        'public.qbit_audit_publication_sequence_seq'::regclass::oid,
    'sequence_definition', (
        SELECT json_build_object(
            'type_oid', sequence.seqtypid,
            'start', sequence.seqstart,
            'increment', sequence.seqincrement,
            'max', sequence.seqmax,
            'min', sequence.seqmin,
            'cache', sequence.seqcache,
            'cycle', sequence.seqcycle,
            'persistence', relation.relpersistence,
            'owner', relation.relowner
        )
        FROM pg_catalog.pg_sequence sequence
        JOIN pg_catalog.pg_class relation
          ON relation.oid = sequence.seqrelid
        WHERE sequence.seqrelid =
            'public.qbit_audit_publication_sequence_seq'::regclass
    ),
    'sequence_state', (
        SELECT row_to_json(state)
        FROM (
            SELECT last_value, is_called
            FROM public.qbit_audit_publication_sequence_seq
        ) state
    ),
    'index_oid',
        'public.qbit_pool_blocks_audit_publication_sequence_idx'::regclass::oid,
    'index_filenode', pg_relation_filenode(
        'public.qbit_pool_blocks_audit_publication_sequence_idx'::regclass
    ),
    'index_definition', pg_get_indexdef(
        'public.qbit_pool_blocks_audit_publication_sequence_idx'::regclass
    ),
    'index_catalog', (
        SELECT json_build_object(
            'kind', relation.relkind,
            'persistence', relation.relpersistence,
            'owner', relation.relowner,
            'same_owner_as_table', relation.relowner = (
                SELECT pool_blocks.relowner
                FROM pg_catalog.pg_class pool_blocks
                JOIN pg_catalog.pg_namespace pool_namespace
                  ON pool_namespace.oid = pool_blocks.relnamespace
                WHERE pool_namespace.nspname = 'public'
                  AND pool_blocks.relname = 'qbit_pool_blocks'
            ),
            'unique', index_definition.indisunique,
            'valid', index_definition.indisvalid,
            'ready', index_definition.indisready,
            'live', index_definition.indislive,
            'immediate', index_definition.indimmediate,
            'primary', index_definition.indisprimary,
            'exclusion', index_definition.indisexclusion,
            'clustered', index_definition.indisclustered,
            'replica_identity', index_definition.indisreplident,
            'nulls_not_distinct', index_definition.indnullsnotdistinct,
            'key_count', index_definition.indnkeyatts,
            'attribute_count', index_definition.indnatts,
            'keys', index_definition.indkey::text,
            'classes', index_definition.indclass::text,
            'collations', index_definition.indcollation::text,
            'options', index_definition.indoption::text,
            'expressions', index_definition.indexprs::text,
            'predicate', index_definition.indpred::text
        )
        FROM pg_catalog.pg_index index_definition
        JOIN pg_catalog.pg_class relation
          ON relation.oid = index_definition.indexrelid
        WHERE index_definition.indexrelid =
            'public.qbit_pool_blocks_audit_publication_sequence_idx'::regclass
    ),
    'constraint', (
        SELECT json_build_object(
            'oid', oid,
            'conbin', conbin::text,
            'definition', pg_get_constraintdef(oid, true),
            'validated', convalidated,
            'name', conname,
            'type', contype,
            'keys', conkey::text,
            'local', conislocal,
            'inherited_count', coninhcount,
            'no_inherit', connoinherit,
            'deferrable', condeferrable,
            'deferred', condeferred
        )
        FROM pg_constraint
        WHERE conrelid = 'public.qbit_pool_blocks'::regclass
          AND conname =
              'qbit_pool_blocks_audit_publication_sequence_check'
    )
);
"""
    )


def marker_schema_count() -> int:
    value = run_json(
        f"""
SELECT json_build_object(
    'count', count(*)
)
FROM pg_namespace
WHERE obj_description(oid, 'pg_namespace') = '{RUN_MARKER}';
"""
    )["count"]
    return int(value)


def allocator_state(ledger: ScopedPsqlLedger) -> dict[str, object]:
    return ledger._run_json(
        """
SELECT json_build_object(
    'last_value', last_value,
    'is_called', is_called
)
FROM qbit_audit_publication_sequence_seq;
"""
    )


def seed_prepared_direct(
    postgres: ScopedPsqlLedger,
    memory: SingleWriterShareLedger,
    *,
    block_hash: str,
    block_height: int,
) -> None:
    postgres._run_sql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state,
    maturity_state
) VALUES (
    '{block_hash}',
    {block_height},
    '{'10' * 32}',
    '{'20' * 32}',
    '{'30' * 32}',
    'prepared',
    'immature'
)
ON CONFLICT (block_hash) DO NOTHING;
"""
    )
    memory.persist_accepted_block(
        block_hash=block_hash,
        block_height=block_height,
        parent_hash="10" * 32,
        final_bundle={},
        audit_report={},
    )


def normalized_response(payload: dict[str, int | str]) -> dict[str, int | str]:
    return {**payload, "backend": "ledger"}


def assert_states_equal(
    postgres: ScopedPsqlLedger,
    memory: SingleWriterShareLedger,
    block_hashes: list[str],
    message: str,
) -> None:
    for block_hash in block_hashes:
        assert_equal(
            postgres.pool_block_state(block_hash=block_hash),
            memory.pool_block_state(block_hash=block_hash),
            f"{message} state {block_hash}",
        )
    assert_equal(
        postgres.audit_publication_sequence_floor(),
        memory.audit_publication_sequence_floor(),
        f"{message} durable floor",
    )


def call_both(
    postgres: ScopedPsqlLedger,
    memory: SingleWriterShareLedger,
    method: str,
    *,
    block_hash: str,
    active_tip_height: int,
    known_hashes: list[str],
    message: str,
) -> dict[str, int | str]:
    postgres_result = getattr(postgres, method)(
        block_hash=block_hash,
        active_tip_height=active_tip_height,
    )
    memory_result = getattr(memory, method)(
        block_hash=block_hash,
        active_tip_height=active_tip_height,
    )
    assert_equal(
        normalized_response(postgres_result),
        normalized_response(memory_result),
        f"{message} response",
    )
    assert_states_equal(postgres, memory, known_hashes, message)
    return postgres_result


def test_exact_transition_parity() -> None:
    schema = create_owned_schema("parity")
    postgres = ScopedPsqlLedger(
        test_schema=schema,
        writer_id="a1-parity",
        writer_epoch=1,
        initialize_schema=True,
        audit_bundle_canonicalizer=fake_bundle_bytes,
    )
    memory = SingleWriterShareLedger()
    try:
        assert_equal(
            postgres._run_json(
                "SELECT json_build_object('schema', current_schema());"
            )["schema"],
            schema,
            "parity current schema",
        )
        assert_equal(allocator_state(postgres), {"last_value": 1, "is_called": False}, "parity fresh allocator")
        block_a = "a1" * 32
        block_b = "b1" * 32
        block_c = "c1" * 32
        known = [block_a]
        seed_prepared_direct(postgres, memory, block_hash=block_a, block_height=10)
        seed_prepared_direct(postgres, memory, block_hash=block_a, block_height=10)
        assert_states_equal(postgres, memory, known, "duplicate direct prepared seed")

        api_hash = "d1" * 32
        expected_inline_body_bytes = fake_bundle_bytes(PERSIST_BUNDLE)
        expected_inline_body_byte_len = len(expected_inline_body_bytes)
        postgres_first_persist = postgres.persist_accepted_block(
            block_hash=api_hash,
            block_height=30,
            parent_hash="10" * 32,
            final_bundle=PERSIST_BUNDLE,
            audit_report=PERSIST_REPORT,
        )
        memory_first_persist = memory.persist_accepted_block(
            block_hash=api_hash,
            block_height=30,
            parent_hash="10" * 32,
            final_bundle=PERSIST_BUNDLE,
            audit_report=PERSIST_REPORT,
        )
        assert_equal(
            postgres_first_persist,
            {
                "backend": "postgres-psql",
                "share_count": 0,
                "block_count": 1,
                "bundle_count": 1,
                "payout_entry_count": 1,
                "carry_forward_count": 1,
                "onchain_output_count": 1,
                "audit_bundle_sha256": PERSIST_BUNDLE_SHA256,
                "body_uri": "",
                "audit_body_byte_len": expected_inline_body_byte_len,
            },
            "PostgreSQL first persist API response",
        )
        persisted_inline_body = postgres._run_json(
            f"""
SELECT json_build_object(
    'audit_bundle_present', audit_bundle IS NOT NULL,
    'body_uri_is_null', body_uri IS NULL,
    'audit_body_byte_len', audit_body_byte_len,
    'audit_bundle_sha256', audit_bundle_sha256,
    'audit_bundle', audit_bundle
)
FROM qbit_pool_audit_bundles
WHERE block_hash = '{api_hash}';
"""
        )
        assert_equal(
            persisted_inline_body,
            {
                "audit_bundle_present": True,
                "body_uri_is_null": True,
                "audit_body_byte_len": expected_inline_body_byte_len,
                "audit_bundle_sha256": hashlib.sha256(
                    expected_inline_body_bytes
                ).hexdigest(),
                "audit_bundle": PERSIST_BUNDLE,
            },
            "PostgreSQL inline audit body row",
        )
        assert_equal(
            memory_first_persist,
            {
                "backend": "memory",
                "share_count": 0,
                "block_count": 0,
                "payout_entry_count": 0,
                "carry_forward_count": 0,
            },
            "memory first persist API response",
        )
        postgres_duplicate_persist = postgres.persist_accepted_block(
            block_hash=api_hash,
            block_height=30,
            parent_hash="10" * 32,
            final_bundle=PERSIST_BUNDLE,
            audit_report=PERSIST_REPORT,
        )
        memory_duplicate_persist = memory.persist_accepted_block(
            block_hash=api_hash,
            block_height=30,
            parent_hash="10" * 32,
            final_bundle=PERSIST_BUNDLE,
            audit_report=PERSIST_REPORT,
        )
        assert_equal(
            postgres_duplicate_persist,
            postgres_first_persist,
            "PostgreSQL duplicate persist exact response",
        )
        assert_equal(
            memory_duplicate_persist,
            memory_first_persist,
            "memory duplicate persist exact response",
        )
        known.append(api_hash)
        assert_states_equal(postgres, memory, known, "public persist API parity")

        before = allocator_state(postgres)
        call_both(postgres, memory, "confirm_accepted_block", block_hash=block_a, active_tip_height=9, known_hashes=known, message="wrong-height confirmation")
        assert_equal(allocator_state(postgres), before, "wrong-height confirmation allocator immobility")
        confirmed = call_both(postgres, memory, "confirm_accepted_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="first confirmation")
        assert_equal(confirmed["audit_publication_sequence"], 1, "first event ordinal")
        before = allocator_state(postgres)
        replay = call_both(postgres, memory, "confirm_accepted_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="exact confirmation replay")
        assert_equal(replay["audit_publication_sequence"], 1, "replay ordinal")
        for replay_round in range(2, 4):
            replay = call_both(
                postgres,
                memory,
                "confirm_accepted_block",
                block_hash=block_a,
                active_tip_height=10,
                known_hashes=known,
                message=f"exact confirmation replay round {replay_round}",
            )
            assert_equal(
                replay["audit_publication_sequence"],
                1,
                f"replay round {replay_round} ordinal",
            )
        assert_equal(allocator_state(postgres), before, "exact replay allocator immobility")
        before = allocator_state(postgres)
        call_both(postgres, memory, "reactivate_pool_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="confirmed count-zero reactivation")
        assert_equal(allocator_state(postgres), before, "count-zero reactivation allocator immobility")
        call_both(postgres, memory, "mark_pool_block_inactive", block_hash=block_a, active_tip_height=10, known_hashes=known, message="inactive transition")
        before = allocator_state(postgres)
        call_both(
            postgres,
            memory,
            "confirm_accepted_block",
            block_hash=block_a,
            active_tip_height=10,
            known_hashes=known,
            message="inactive confirmation",
        )
        assert_equal(
            allocator_state(postgres),
            before,
            "inactive confirmation allocator immobility",
        )
        call_both(postgres, memory, "reactivate_pool_block", block_hash=block_a, active_tip_height=9, known_hashes=known, message="wrong-height reactivation")
        assert_equal(allocator_state(postgres), before, "wrong-height reactivation allocator immobility")
        postgres.release_writer_lease()
        postgres.close()
        postgres = ScopedPsqlLedger(
            test_schema=schema,
            writer_id="a1-parity-reactivation-restart",
            writer_epoch=1,
            audit_bundle_canonicalizer=fake_bundle_bytes,
        )
        assert_states_equal(
            postgres,
            memory,
            known,
            "inactive restart serializer",
        )
        assert_equal(
            allocator_state(postgres),
            before,
            "inactive restart allocator preservation",
        )
        reactivated = call_both(postgres, memory, "reactivate_pool_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="reactivation")
        assert_equal(reactivated["audit_publication_sequence"], 1, "reactivation preserves published ordinal")
        assert_equal(allocator_state(postgres), before, "reactivation allocator immobility")
        before = allocator_state(postgres)
        reactivated_confirmation = call_both(
            postgres,
            memory,
            "confirm_accepted_block",
            block_hash=block_a,
            active_tip_height=10,
            known_hashes=known,
            message="reactivated confirmation replay",
        )
        assert_equal(
            reactivated_confirmation["audit_publication_sequence"],
            1,
            "reactivated confirmation replay ordinal",
        )
        call_both(postgres, memory, "reactivate_pool_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="reactivation replay")
        assert_equal(allocator_state(postgres), before, "reactivation replay allocator immobility")
        call_both(postgres, memory, "mark_pool_block_inactive", block_hash=block_a, active_tip_height=10, known_hashes=known, message="second inactive transition")
        call_both(postgres, memory, "reverse_immature_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="terminal reversal")
        before = allocator_state(postgres)
        postgres.release_writer_lease()
        postgres.close()
        postgres = ScopedPsqlLedger(
            test_schema=schema,
            writer_id="a1-parity-reversed-restart",
            writer_epoch=1,
            audit_bundle_canonicalizer=fake_bundle_bytes,
        )
        assert_states_equal(postgres, memory, known, "reversed restart serializer")
        assert_equal(
            allocator_state(postgres),
            before,
            "reversed restart allocator preservation",
        )
        call_both(postgres, memory, "confirm_accepted_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="reversed confirmation")
        call_both(postgres, memory, "reactivate_pool_block", block_hash=block_a, active_tip_height=10, known_hashes=known, message="reversed reactivation")
        assert_equal(allocator_state(postgres), before, "terminal transition allocator immobility")

        known.append(block_b)
        seed_prepared_direct(postgres, memory, block_hash=block_b, block_height=1)
        lower = call_both(postgres, memory, "confirm_accepted_block", block_hash=block_b, active_tip_height=1, known_hashes=known, message="later lower-height confirmation")
        assert_equal(lower["audit_publication_sequence"], 2, "event order dominates block height")

        known.append(block_c)
        seed_prepared_direct(postgres, memory, block_hash=block_c, block_height=20)
        before = allocator_state(postgres)
        call_both(postgres, memory, "reject_prepared_block", block_hash=block_c, active_tip_height=20, known_hashes=known, message="prepared rejection")
        call_both(postgres, memory, "reject_prepared_block", block_hash=block_c, active_tip_height=20, known_hashes=known, message="rejection replay")
        assert_equal(allocator_state(postgres), before, "rejection allocator immobility")
        block_d = "e1" * 32
        known.append(block_d)
        seed_prepared_direct(postgres, memory, block_hash=block_d, block_height=0)
        final_confirmation = call_both(
            postgres,
            memory,
            "confirm_accepted_block",
            block_hash=block_d,
            active_tip_height=0,
            known_hashes=known,
            message="post-replay fresh confirmation",
        )
        assert_equal(
            final_confirmation["audit_publication_sequence"],
            3,
            "post-replay exact next ordinal",
        )
        assert_equal(postgres.audit_publication_sequence_floor(), 3, "parity final durable floor")
    finally:
        postgres.release_writer_lease()
        postgres.close()


LEGACY_POOL_BLOCKS_SQL = """
CREATE TABLE qbit_pool_blocks (
    block_hash text PRIMARY KEY,
    block_height bigint NOT NULL CHECK (block_height >= 0),
    parent_hash text NOT NULL,
    coinbase_txid text NOT NULL,
    payout_manifest_sha256 text NOT NULL,
    found_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    chain_state text NOT NULL DEFAULT 'prepared'
        CHECK (chain_state IN ('prepared', 'confirmed', 'inactive', 'rejected', 'reversed')),
    maturity_state text NOT NULL DEFAULT 'immature'
        CHECK (maturity_state IN ('immature', 'mature', 'reversed')),
    matured_at timestamptz,
    disconnected_at timestamptz,
    CHECK ((maturity_state = 'mature') = (matured_at IS NOT NULL)),
    CHECK ((maturity_state = 'reversed') = (disconnected_at IS NOT NULL))
);
"""


def assert_empty_serializer_case(*, legacy: bool) -> None:
    label = "empty_legacy" if legacy else "empty_fresh"
    schema = create_owned_schema(label)
    if legacy:
        run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
    first = ScopedPsqlLedger(
        test_schema=schema,
        writer_id=f"a1-{label}-first",
        writer_epoch=1,
        initialize_schema=True,
    )
    try:
        assert_equal(first.audit_publication_sequence_floor(), 0, f"{label} floor")
        assert_equal(allocator_state(first), {"last_value": 1, "is_called": False}, f"{label} allocator")
    finally:
        first.release_writer_lease()
        first.close()
    second = ScopedPsqlLedger(
        test_schema=schema,
        writer_id=f"a1-{label}-second",
        writer_epoch=1,
        initialize_schema=True,
    )
    try:
        assert_equal(second.audit_publication_sequence_floor(), 0, f"{label} rerun floor")
        assert_equal(allocator_state(second), {"last_value": 1, "is_called": False}, f"{label} rerun allocator")
        block_hash = hashlib.sha256(label.encode()).hexdigest()
        memory = SingleWriterShareLedger()
        before = allocator_state(second)
        for method, operation in (
            ("confirm_accepted_block", "confirmation"),
            ("reactivate_pool_block", "reactivation"),
            ("mark_pool_block_inactive", "inactive transition"),
            ("reverse_immature_block", "reversal"),
            ("reject_prepared_block", "rejection"),
        ):
            call_both(
                second,
                memory,
                method,
                block_hash=block_hash,
                active_tip_height=1,
                known_hashes=[block_hash],
                message=f"{label} missing {operation}",
            )
        assert_equal(
            second.pool_block_state(block_hash=block_hash),
            None,
            f"{label} missing state serializer",
        )
        assert_equal(
            allocator_state(second),
            before,
            f"{label} missing-row allocator immobility",
        )
        seed_prepared_direct(second, memory, block_hash=block_hash, block_height=1)
        seed_prepared_direct(second, memory, block_hash=block_hash, block_height=1)
        assert_states_equal(
            second,
            memory,
            [block_hash],
            f"{label} nullable prepared serializer",
        )
        assert_equal(
            second.pool_block_state(block_hash=block_hash)[
                "audit_publication_sequence"
            ],
            None,
            f"{label} explicit prepared nullable ordinal",
        )
        before = allocator_state(second)
        call_both(
            second,
            memory,
            "confirm_accepted_block",
            block_hash=block_hash,
            active_tip_height=0,
            known_hashes=[block_hash],
            message=f"{label} wrong-height confirmation",
        )
        call_both(
            second,
            memory,
            "reactivate_pool_block",
            block_hash=block_hash,
            active_tip_height=1,
            known_hashes=[block_hash],
            message=f"{label} prepared count-zero reactivation",
        )
        assert_equal(
            allocator_state(second),
            before,
            f"{label} nullable zero-op allocator immobility",
        )
        result = call_both(
            second,
            memory,
            "confirm_accepted_block",
            block_hash=block_hash,
            active_tip_height=1,
            known_hashes=[block_hash],
            message=f"{label} first confirmation",
        )
        assert_equal(result["audit_publication_sequence"], 1, f"{label} first ordinal")
        assert_equal(second.audit_publication_sequence_floor(), 1, f"{label} confirmed floor")
    finally:
        second.release_writer_lease()
        second.close()


def transition_binding_state_sql(
    *,
    target_schema: str,
    decoy_schema: str,
) -> str:
    return f"""
SELECT json_build_object(
    'target_sequence', (
        SELECT json_build_object('last_value', last_value, 'is_called', is_called)
        FROM "{target_schema}".qbit_audit_publication_sequence_seq
    ),
    'decoy_sequence', (
        SELECT json_build_object('last_value', last_value, 'is_called', is_called)
        FROM {decoy_schema}.qbit_audit_publication_sequence_seq
    ),
    'target_rows', (
        SELECT json_object_agg(
            block_hash,
            json_build_object(
                'chain_state', chain_state,
                'audit_publication_sequence', audit_publication_sequence
            )
        )
        FROM "{target_schema}".qbit_pool_blocks
        WHERE block_hash IN ('{'71' * 32}', '{'72' * 32}')
    ),
    'decoy_rows', (
        SELECT json_object_agg(
            block_hash,
            json_build_object(
                'chain_state', chain_state,
                'audit_publication_sequence', audit_publication_sequence
            )
        )
        FROM {decoy_schema}.qbit_pool_blocks
    ),
    'target_lease_changed', (
        SELECT before.value <>
            row_to_json(current_lease)::text
        FROM a1_target_lease_before before,
             "{target_schema}".qbit_ledger_writer_lease current_lease
    ),
    'target_lease', (
        SELECT json_build_object(
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token,
            'active', lease_expires_at > clock_timestamp(),
            'expiry_after_update', lease_expires_at > updated_at
        )
        FROM "{target_schema}".qbit_ledger_writer_lease
    ),
    'decoy_lease_unchanged', (
        SELECT before.value = row_to_json(current_lease)::text
        FROM a1_decoy_lease_before before,
             {decoy_schema}.qbit_ledger_writer_lease current_lease
    ),
    'decoy_lease', (
        SELECT json_build_object(
            'writer_id', writer_id,
            'writer_epoch', writer_epoch,
            'writer_session_token', writer_session_token
        )
        FROM {decoy_schema}.qbit_ledger_writer_lease
    ),
    'proconfig', (
        SELECT json_object_agg(procedure.proname, procedure.proconfig[1])
        FROM pg_catalog.pg_proc procedure
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = procedure.pronamespace
        WHERE procedure.oid IN (
            pg_catalog.to_regprocedure(
                '"{target_schema}".qbit_confirm_pool_block('
                'text,bigint,text,bigint,text,interval)'
            ),
            pg_catalog.to_regprocedure(
                '"{target_schema}".qbit_reactivate_pool_block('
                'text,bigint,text,bigint,text,interval)'
            )
        )
    )
);
"""


def seed_transition_binding_target(ledger: ScopedPsqlLedger) -> None:
    ledger._run_sql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash,
    audit_publication_sequence,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    chain_state,
    maturity_state
) VALUES
    ('{'71' * 32}', NULL, 10, '{'10' * 32}', '{'20' * 32}',
     '{'30' * 32}', 'prepared', 'immature'),
    ('{'72' * 32}', NULL, 20, '{'10' * 32}', '{'20' * 32}',
     '{'30' * 32}', 'prepared', 'immature');
"""
    )
    confirmed = ledger.confirm_accepted_block(
        block_hash="72" * 32,
        active_tip_height=20,
    )
    assert_equal(
        confirmed["audit_publication_sequence"],
        1,
        "binding seed confirmation ordinal",
    )
    assert_equal(
        ledger.mark_pool_block_inactive(
            block_hash="72" * 32,
            active_tip_height=20,
        )["inactive_count"],
        1,
        "binding seed inactive transition",
    )


def decoy_objects_sql(*, temporary: bool) -> str:
    temporary_keyword = "TEMPORARY " if temporary else ""
    return f"""
CREATE {temporary_keyword}TABLE qbit_ledger_writer_lease (
    singleton boolean PRIMARY KEY,
    writer_id text NOT NULL,
    writer_epoch bigint NOT NULL,
    writer_session_token text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE {temporary_keyword}TABLE qbit_pool_blocks (
    block_hash text PRIMARY KEY,
    audit_publication_sequence bigint,
    block_height bigint NOT NULL,
    chain_state text NOT NULL,
    maturity_state text NOT NULL
);
CREATE {temporary_keyword}SEQUENCE qbit_audit_publication_sequence_seq
    AS bigint START WITH 700;
INSERT INTO qbit_ledger_writer_lease VALUES (
    true,
    'a1-binding',
    1,
    'a1-binding-token',
    '2040-01-01T00:00:00Z',
    '2000-01-01T00:00:00Z'
);
INSERT INTO qbit_pool_blocks VALUES
    ('{'71' * 32}', NULL, 10, 'prepared', 'immature'),
    ('{'72' * 32}', 700, 20, 'inactive', 'immature');
"""


def assert_transition_binding_case(*, temporary: bool) -> None:
    label = "binding_temp" if temporary else "binding_ordinary"
    target_schema = create_owned_schema(label)
    target = ScopedPsqlLedger(
        test_schema=target_schema,
        writer_id="a1-binding",
        writer_epoch=1,
        writer_session_token="a1-binding-token",
        initialize_schema=True,
    )
    try:
        seed_transition_binding_target(target)
        if temporary:
            decoy_schema = "pg_temp"
            caller_setup = decoy_objects_sql(temporary=True)
            caller_path = f'pg_temp, "{target_schema}", pg_catalog'
        else:
            ordinary_decoy = create_owned_schema("binding_decoy")
            run_psql(decoy_objects_sql(temporary=False), schema=ordinary_decoy)
            decoy_schema = f'"{ordinary_decoy}"'
            caller_setup = ""
            caller_path = f'"{ordinary_decoy}", "{target_schema}", pg_catalog'
        expected_config = f"search_path=pg_catalog, {target_schema}, pg_temp"
        result = run_json(
            f"""
SET statement_timeout = '20s';
{caller_setup}
SET search_path TO {caller_path};
CREATE TEMPORARY TABLE a1_target_lease_before (value text NOT NULL);
INSERT INTO a1_target_lease_before
SELECT row_to_json(target_lease)::text
FROM "{target_schema}".qbit_ledger_writer_lease target_lease;
CREATE TEMPORARY TABLE a1_decoy_lease_before (value text NOT NULL);
INSERT INTO a1_decoy_lease_before
SELECT row_to_json(decoy_lease)::text
FROM {decoy_schema}.qbit_ledger_writer_lease decoy_lease;
SELECT "{target_schema}".qbit_confirm_pool_block(
    '{'71' * 32}', 10, 'a1-binding', 1, 'a1-binding-token', interval '5 minutes'
);
SELECT "{target_schema}".qbit_reactivate_pool_block(
    '{'72' * 32}', 20, 'a1-binding', 1, 'a1-binding-token', interval '5 minutes'
);
{transition_binding_state_sql(target_schema=target_schema, decoy_schema=decoy_schema)}
"""
        )
        assert_equal(
            result["proconfig"],
            {
                "qbit_confirm_pool_block": expected_config,
                "qbit_reactivate_pool_block": expected_config,
            },
            f"{label} exact transition proconfig",
        )
        assert_equal(
            result["target_sequence"],
            {"last_value": 2, "is_called": True},
            f"{label} target allocator",
        )
        assert_equal(
            result["decoy_sequence"],
            {"last_value": 700, "is_called": False},
            f"{label} decoy allocator preservation",
        )
        assert_equal(
            result["target_rows"],
            {
                "71" * 32: {
                    "chain_state": "confirmed",
                    "audit_publication_sequence": 2,
                },
                "72" * 32: {
                    "chain_state": "confirmed",
                    "audit_publication_sequence": 1,
                },
            },
            f"{label} target row transitions",
        )
        assert_equal(
            result["decoy_rows"],
            {
                "71" * 32: {
                    "chain_state": "prepared",
                    "audit_publication_sequence": None,
                },
                "72" * 32: {
                    "chain_state": "inactive",
                    "audit_publication_sequence": 700,
                },
            },
            f"{label} decoy row preservation",
        )
        assert_equal(
            result["target_lease_changed"],
            True,
            f"{label} target lease renewal",
        )
        assert_equal(
            result["target_lease"],
            {
                "writer_id": "a1-binding",
                "writer_epoch": 1,
                "writer_session_token": "a1-binding-token",
                "active": True,
                "expiry_after_update": True,
            },
            f"{label} target lease identity",
        )
        assert_equal(
            result["decoy_lease_unchanged"],
            True,
            f"{label} decoy lease byte preservation",
        )
        assert_equal(
            result["decoy_lease"],
            {
                "writer_id": "a1-binding",
                "writer_epoch": 1,
                "writer_session_token": "a1-binding-token",
            },
            f"{label} decoy lease identity",
        )
    finally:
        target.release_writer_lease()
        target.close()


def test_durable_floor_ignores_allocator_gaps() -> None:
    schema = create_owned_schema("durable_floor")
    ledger = ScopedPsqlLedger(
        test_schema=schema,
        writer_id="a1-durable-floor",
        writer_epoch=1,
        initialize_schema=True,
    )
    memory_seed = SingleWriterShareLedger()
    block_a = "61" * 32
    block_b = "62" * 32
    rejected = "63" * 32
    try:
        assert_equal(ledger.audit_publication_sequence_floor(), 0, "empty floor")
        seed_prepared_direct(
            ledger,
            memory_seed,
            block_hash=block_a,
            block_height=10,
        )
        assert_equal(
            ledger.pool_block_state(block_hash=block_a)[
                "audit_publication_sequence"
            ],
            None,
            "prepared floor row is nullable",
        )
        committed_gap = ledger._run_json(
            """
SELECT json_build_object(
    'value', nextval('qbit_audit_publication_sequence_seq')
);
"""
        )["value"]
        assert_equal(committed_gap, 1, "committed unattached allocator gap")
        assert_equal(ledger.audit_publication_sequence_floor(), 0, "gap floor")
        ledger._run_sql(
            """
BEGIN;
SELECT nextval('qbit_audit_publication_sequence_seq');
ROLLBACK;
"""
        )
        assert_equal(
            allocator_state(ledger),
            {"last_value": 2, "is_called": True},
            "rolled-back allocator gap persists",
        )
        assert_equal(
            ledger.audit_publication_sequence_floor(),
            0,
            "rolled-back gap is not authority",
        )
        confirmed = ledger.confirm_accepted_block(
            block_hash=block_a,
            active_tip_height=10,
        )
        assert_equal(
            confirmed["audit_publication_sequence"],
            3,
            "confirmation follows both allocator gaps",
        )
        before = allocator_state(ledger)
        for _round in range(3):
            replay = ledger.confirm_accepted_block(
                block_hash=block_a,
                active_tip_height=10,
            )
            assert_equal(
                replay["audit_publication_sequence"],
                3,
                "gap-vector confirmation replay",
            )
        assert_equal(allocator_state(ledger), before, "gap replay immobility")
        ledger.mark_pool_block_inactive(
            block_hash=block_a,
            active_tip_height=10,
        )
        assert_equal(ledger.audit_publication_sequence_floor(), 3, "inactive floor")
        reactivated = ledger.reactivate_pool_block(
            block_hash=block_a,
            active_tip_height=10,
        )
        assert_equal(
            reactivated["audit_publication_sequence"],
            3,
            "reactivation preserves ordinal after gaps",
        )
        assert_equal(
            allocator_state(ledger),
            before,
            "gap-vector reactivation allocator immobility",
        )
        before = allocator_state(ledger)
        assert_equal(
            ledger.reactivate_pool_block(
                block_hash=block_a,
                active_tip_height=10,
            ),
            {"backend": "postgres-psql", "reactivated_count": 0},
            "gap-vector reactivation replay",
        )
        assert_equal(allocator_state(ledger), before, "reactivation replay immobility")
        ledger.mark_pool_block_inactive(
            block_hash=block_a,
            active_tip_height=10,
        )
        ledger.reverse_immature_block(
            block_hash=block_a,
            active_tip_height=10,
        )
        assert_equal(ledger.audit_publication_sequence_floor(), 3, "reversed floor")
        seed_prepared_direct(
            ledger,
            memory_seed,
            block_hash=rejected,
            block_height=20,
        )
        before = allocator_state(ledger)
        assert_equal(
            ledger.reject_prepared_block(
                block_hash=rejected,
                active_tip_height=20,
            )["rejected_count"],
            1,
            "rejected nullable floor row",
        )
        assert_equal(allocator_state(ledger), before, "rejection allocator immobility")
        assert_equal(ledger.audit_publication_sequence_floor(), 3, "rejected floor")
        assert_equal(
            ledger._run_json(
                """
SELECT json_build_object(
    'value', nextval('qbit_audit_publication_sequence_seq')
);
"""
            )["value"],
            4,
            "second unattached allocator gap",
        )
        seed_prepared_direct(
            ledger,
            memory_seed,
            block_hash=block_b,
            block_height=1,
        )
        assert_equal(
            ledger.confirm_accepted_block(
                block_hash=block_b,
                active_tip_height=1,
            )["audit_publication_sequence"],
            5,
            "post-gap event ordinal",
        )
        ledger._run_sql(
            f"""
INSERT INTO qbit_pool_blocks (
    block_hash, audit_publication_sequence, block_height, parent_hash,
    coinbase_txid, payout_manifest_sha256, chain_state, maturity_state
) VALUES
    ('{'64' * 32}', 9000000000, 30, '{'10' * 32}', '{'20' * 32}',
     '{'30' * 32}', 'prepared', 'immature'),
    ('{'65' * 32}', 9000000001, 31, '{'10' * 32}', '{'20' * 32}',
     '{'30' * 32}', 'rejected', 'immature');
"""
        )
        assert_equal(
            ledger.audit_publication_sequence_floor(),
            9_000_000_001,
            "partial-state bigint durable floor",
        )
    finally:
        ledger.release_writer_lease()
        ledger.close()
    restart = ScopedPsqlLedger(
        test_schema=schema,
        writer_id="a1-durable-floor-restart",
        writer_epoch=1,
        initialize_schema=True,
    )
    try:
        assert_equal(
            restart.audit_publication_sequence_floor(),
            9_000_000_001,
            "restart bigint durable floor",
        )
        assert_equal(
            restart.pool_block_state(block_hash=block_a),
            {
                "block_hash": block_a,
                "block_height": 10,
                "parent_hash": "10" * 32,
                "chain_state": "reversed",
                "maturity_state": "reversed",
                "audit_publication_sequence": 3,
            },
            "restart exact reversed serializer",
        )
        assert_equal(
            restart.pool_block_state(block_hash=rejected)[
                "audit_publication_sequence"
            ],
            None,
            "restart exact rejected nullable serializer",
        )
    finally:
        restart.release_writer_lease()
        restart.close()


def main() -> None:
    public_before = public_sentinel()
    failure: BaseException | None = None
    try:
        test_exact_transition_parity()
        assert_empty_serializer_case(legacy=False)
        assert_empty_serializer_case(legacy=True)
        assert_transition_binding_case(temporary=False)
        assert_transition_binding_case(temporary=True)
        test_durable_floor_ignores_allocator_gaps()
    except BaseException as error:
        failure = error
    try:
        cleanup_active_children()
        cleanup_owned_schemas()
        assert_equal(marker_schema_count(), 0, "owned schema marker cleanup")
        assert_equal(public_sentinel(), public_before, "public sentinel preservation")
    except BaseException as cleanup_error:
        if failure is None:
            raise
        raise GateFailure(
            f"scenario failed with {failure!r}; cleanup also failed with "
            f"{cleanup_error!r}"
        ) from cleanup_error
    else:
        atexit.unregister(cleanup_active_children)
        atexit.unregister(cleanup_owned_schemas)
    if failure is not None:
        raise failure
    print(
        "prism postgres A1 gate PASS "
        "exact-transition-parity empty-fresh empty-legacy "
        "ordinary-decoy-binding temporary-decoy-binding durable-floor-gaps"
    )


if __name__ == "__main__":
    main()
