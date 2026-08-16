"""Non-discovered PostgreSQL ordinal schema-revert integration gate.

Round-trips crates/qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql:
forward-migrate a schema holding confirmed rows, revert it to its
pre-ordinal shape, confirm blocks pre-ordinal-style against the reverted
schema, then re-apply the forward migration and require a correct backfill
with no double-assigned ordinals. Invoked by
``test/test-prism-postgres-ledger.sh``.
"""

from __future__ import annotations

from pathlib import Path

from tests import prism_postgres_a1_gate as support
from tests.prism_postgres_a1_gate import (
    GateFailure,
    LEGACY_POOL_BLOCKS_SQL,
    ScopedPsqlLedger,
    assert_equal,
    create_owned_schema,
    run_json,
    run_psql,
)

REVERT_SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "crates/qbit-prism/sql/001_share_ledger_revert_audit_publication_sequence.sql"
)
REVERT_SCRIPT_SQL = REVERT_SCRIPT_PATH.read_text(encoding="utf-8")

REVERT_HASH_A = "91" * 32
REVERT_HASH_B = "92" * 32
REVERT_HASH_C = "93" * 32
REVERT_HASH_INACTIVE = "94" * 32
REVERT_HASH_REVERSED = "95" * 32
REVERT_HASH_PREPARED = "96" * 32
REVERT_HASH_LEGACY_UPDATE = "97" * 32
REVERT_HASH_POST_REAPPLY = "98" * 32


def apply_revert(schema: str) -> None:
    run_psql(REVERT_SCRIPT_SQL, schema=schema)


def seed_legacy_pool_blocks(schema: str) -> None:
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
    run_psql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash, block_height, parent_hash, coinbase_txid,
    payout_manifest_sha256, found_at, chain_state, maturity_state,
    matured_at, disconnected_at
) VALUES
    -- Insertion order intentionally differs from deterministic backfill
    -- order. A/B share a timestamp and sort by block hash.
    ('{REVERT_HASH_C}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
     '2020-01-01T00:00:02Z', 'confirmed', 'immature', NULL, NULL),
    ('{REVERT_HASH_B}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
     '2020-01-01T00:00:01Z', 'confirmed', 'immature', NULL, NULL),
    ('{REVERT_HASH_A}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
     '2020-01-01T00:00:01Z', 'confirmed', 'immature', NULL, NULL),
    ('{REVERT_HASH_INACTIVE}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
     '2020-01-01T00:00:03Z', 'inactive', 'immature', NULL, NULL),
    ('{REVERT_HASH_REVERSED}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
     '2020-01-01T00:00:03Z', 'reversed', 'reversed', NULL,
     '2020-01-02T00:00:00Z'),
    ('{REVERT_HASH_PREPARED}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
     '2020-01-01T00:00:04Z', 'prepared', 'immature', NULL, NULL);
""",
        schema=schema,
    )


def insert_prepared_block(schema: str, block_hash: str, found_at: str) -> None:
    run_psql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash, block_height, parent_hash, coinbase_txid,
    payout_manifest_sha256, found_at, chain_state, maturity_state
) VALUES (
    '{block_hash}', 10, '{'10' * 32}', '{'20' * 32}', '{'30' * 32}',
    '{found_at}', 'prepared', 'immature'
);
""",
        schema=schema,
    )


def ordinal_catalog(schema: str) -> dict[str, object]:
    return run_json(
        """
SELECT json_build_object(
    'column_present', EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = to_regclass('qbit_pool_blocks')
          AND attname = 'audit_publication_sequence'
          AND attnum > 0
          AND NOT attisdropped
    ),
    'sequence_present',
        to_regclass('qbit_audit_publication_sequence_seq') IS NOT NULL,
    'index_present',
        to_regclass('qbit_pool_blocks_audit_publication_sequence_idx')
        IS NOT NULL,
    'constraint_present', EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = to_regclass('qbit_pool_blocks')
          AND conname = 'qbit_pool_blocks_audit_publication_sequence_check'
    ),
    'trigger_present', EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = to_regclass('qbit_pool_blocks')
          AND tgname = 'qbit_pool_blocks_assign_publication_ordinal'
          AND NOT tgisinternal
    ),
    'trigger_function_present', EXISTS (
        SELECT 1
        FROM pg_proc function_definition
        JOIN pg_namespace namespace
          ON namespace.oid = function_definition.pronamespace
        WHERE namespace.nspname = current_schema()
          AND function_definition.proname =
              'qbit_pool_blocks_assign_publication_ordinal'
    ),
    'confirm_mentions_ordinal', COALESCE((
        SELECT function_definition.prosrc LIKE '%audit_publication_sequence%'
        FROM pg_proc function_definition
        JOIN pg_namespace namespace
          ON namespace.oid = function_definition.pronamespace
        WHERE namespace.nspname = current_schema()
          AND function_definition.proname = 'qbit_confirm_pool_block'
    ), false),
    'confirm_search_path_pinned', COALESCE((
        SELECT function_definition.proconfig IS NOT NULL
        FROM pg_proc function_definition
        JOIN pg_namespace namespace
          ON namespace.oid = function_definition.pronamespace
        WHERE namespace.nspname = current_schema()
          AND function_definition.proname = 'qbit_confirm_pool_block'
    ), false),
    'reactivate_search_path_pinned', COALESCE((
        SELECT function_definition.proconfig IS NOT NULL
        FROM pg_proc function_definition
        JOIN pg_namespace namespace
          ON namespace.oid = function_definition.pronamespace
        WHERE namespace.nspname = current_schema()
          AND function_definition.proname = 'qbit_reactivate_pool_block'
    ), false)
);
""",
        schema=schema,
    )


MIGRATED_CATALOG = {
    "column_present": True,
    "sequence_present": True,
    "index_present": True,
    "constraint_present": True,
    "trigger_present": True,
    "trigger_function_present": True,
    "confirm_mentions_ordinal": True,
    "confirm_search_path_pinned": True,
    "reactivate_search_path_pinned": True,
}
REVERTED_CATALOG = {
    "column_present": False,
    "sequence_present": False,
    "index_present": False,
    "constraint_present": False,
    "trigger_present": False,
    "trigger_function_present": False,
    "confirm_mentions_ordinal": False,
    "confirm_search_path_pinned": False,
    "reactivate_search_path_pinned": False,
}


def ordinal_rows(schema: str) -> dict[str, int | None]:
    rows = run_json(
        """
SELECT COALESCE(json_agg(json_build_object(
    'block_hash', block_hash,
    'audit_publication_sequence', audit_publication_sequence
) ORDER BY block_hash), '[]'::json)
FROM qbit_pool_blocks;
""",
        schema=schema,
    )
    return {
        str(row["block_hash"]): row["audit_publication_sequence"]
        for row in rows
    }


def duplicate_ordinal_count(schema: str) -> int:
    return int(
        run_json(
            """
SELECT json_build_object('duplicates', count(*))
FROM (
    SELECT audit_publication_sequence
    FROM qbit_pool_blocks
    WHERE audit_publication_sequence IS NOT NULL
    GROUP BY audit_publication_sequence
    HAVING count(*) > 1
) duplicated;
""",
            schema=schema,
        )["duplicates"]
    )


def allocator_state(schema: str) -> dict[str, object]:
    return run_json(
        """
SELECT json_build_object(
    'last_value', last_value,
    'is_called', is_called
)
FROM qbit_audit_publication_sequence_seq;
""",
        schema=schema,
    )


def install_writer_lease(schema: str, *, writer_id: str, token: str) -> None:
    run_psql(
        f"""
DELETE FROM qbit_ledger_writer_lease;
INSERT INTO qbit_ledger_writer_lease (
    writer_id, writer_epoch, writer_session_token, lease_expires_at
) VALUES (
    '{writer_id}', 1, '{token}', clock_timestamp() + interval '5 minutes'
);
""",
        schema=schema,
    )


def expire_writer_lease(schema: str) -> None:
    run_psql(
        """
UPDATE qbit_ledger_writer_lease
SET updated_at = clock_timestamp() - interval '6 minutes',
    lease_expires_at = clock_timestamp() - interval '1 minute';
""",
        schema=schema,
    )


def initialize_forward_schema(schema: str, *, writer_id: str) -> None:
    ledger = ScopedPsqlLedger(
        test_schema=schema,
        writer_id=writer_id,
        writer_epoch=1,
        initialize_schema=True,
    )
    try:
        pass
    finally:
        ledger.release_writer_lease()
        ledger.close()


def test_revert_round_trip() -> None:
    schema = create_owned_schema("revert_round_trip")
    seed_legacy_pool_blocks(schema)
    initialize_forward_schema(schema, writer_id="a1-revert-forward")

    assert_equal(
        ordinal_catalog(schema), MIGRATED_CATALOG, "round-trip migrated catalog"
    )
    assert_equal(
        ordinal_rows(schema),
        {
            REVERT_HASH_A: 1,
            REVERT_HASH_B: 2,
            REVERT_HASH_C: 3,
            REVERT_HASH_INACTIVE: 4,
            REVERT_HASH_REVERSED: None,
            REVERT_HASH_PREPARED: None,
        },
        "round-trip forward backfill ordinals",
    )

    apply_revert(schema)
    assert_equal(
        ordinal_catalog(schema), REVERTED_CATALOG, "round-trip reverted catalog"
    )
    assert_equal(
        run_json(
            """
SELECT json_build_object(
    'confirmed', (
        SELECT count(*) FROM qbit_pool_blocks WHERE chain_state = 'confirmed'
    ),
    'total', (SELECT count(*) FROM qbit_pool_blocks)
);
""",
            schema=schema,
        ),
        {"confirmed": 3, "total": 6},
        "revert keeps every pool-block row",
    )

    # Pre-ordinal code confirms through qbit_confirm_pool_block's plain
    # chain_state UPDATE; the restored function must work with no ordinal
    # machinery present.
    install_writer_lease(
        schema, writer_id="revert-legacy-writer", token="revert-legacy-session"
    )
    confirm_sql = (
        "SELECT json_build_object('confirmed', qbit_confirm_pool_block("
        f"'{REVERT_HASH_PREPARED}', 10, 'revert-legacy-writer', 1, "
        "'revert-legacy-session'));"
    )
    assert_equal(
        int(run_json(confirm_sql, schema=schema)["confirmed"]),
        1,
        "restored confirm function confirms against reverted schema",
    )
    assert_equal(
        int(run_json(confirm_sql, schema=schema)["confirmed"]),
        1,
        "restored confirm function replays an exact confirmation",
    )
    # The rawest pre-ordinal shape: a direct chain_state UPDATE.
    insert_prepared_block(
        schema, REVERT_HASH_LEGACY_UPDATE, "2020-01-01T00:00:05Z"
    )
    run_psql(
        f"""
UPDATE qbit_pool_blocks
SET chain_state = 'confirmed'
WHERE block_hash = '{REVERT_HASH_LEGACY_UPDATE}';
""",
        schema=schema,
    )
    assert_equal(
        run_json(
            f"""
SELECT json_build_object(
    'chain_state', (
        SELECT chain_state FROM qbit_pool_blocks
        WHERE block_hash = '{REVERT_HASH_LEGACY_UPDATE}'
    )
);
""",
            schema=schema,
        )["chain_state"],
        "confirmed",
        "plain chain_state UPDATE confirms against reverted schema",
    )

    # Idempotent style: a second run of the revert is a no-op success.
    apply_revert(schema)
    assert_equal(
        ordinal_catalog(schema),
        REVERTED_CATALOG,
        "second revert run is a no-op",
    )

    # Re-apply the forward migration: the backfill must renumber every
    # confirmed and inactive row deterministically by (found_at, block_hash),
    # including the two rows confirmed pre-ordinal-style after the revert.
    expire_writer_lease(schema)
    initialize_forward_schema(schema, writer_id="a1-revert-reapply")
    assert_equal(
        ordinal_catalog(schema), MIGRATED_CATALOG, "re-applied catalog"
    )
    reapplied = ordinal_rows(schema)
    assert_equal(
        reapplied,
        {
            REVERT_HASH_A: 1,
            REVERT_HASH_B: 2,
            REVERT_HASH_C: 3,
            REVERT_HASH_INACTIVE: 4,
            REVERT_HASH_PREPARED: 5,
            REVERT_HASH_REVERSED: None,
            REVERT_HASH_LEGACY_UPDATE: 6,
        },
        "re-applied forward backfill ordinals",
    )
    assert_equal(duplicate_ordinal_count(schema), 0, "re-applied uniqueness")
    assert_equal(
        allocator_state(schema),
        {"last_value": 6, "is_called": True},
        "re-applied allocator floor",
    )

    # The assignment trigger continues past the backfilled floor for a
    # pre-ordinal-style confirmation and does not disturb existing ordinals.
    insert_prepared_block(
        schema, REVERT_HASH_POST_REAPPLY, "2020-01-01T00:00:06Z"
    )
    run_psql(
        f"""
UPDATE qbit_pool_blocks
SET chain_state = 'confirmed'
WHERE block_hash = '{REVERT_HASH_POST_REAPPLY}';
""",
        schema=schema,
    )
    after_trigger = ordinal_rows(schema)
    assert_equal(
        after_trigger,
        {**reapplied, REVERT_HASH_POST_REAPPLY: 7},
        "trigger assigns the next ordinal after re-apply",
    )
    assert_equal(duplicate_ordinal_count(schema), 0, "post-trigger uniqueness")

    # A further schema apply must not re-assign or double-assign anything.
    expire_writer_lease(schema)
    initialize_forward_schema(schema, writer_id="a1-revert-stability")
    assert_equal(
        ordinal_rows(schema),
        after_trigger,
        "third schema apply leaves every ordinal unchanged",
    )
    assert_equal(
        allocator_state(schema),
        {"last_value": 7, "is_called": True},
        "third schema apply leaves the allocator unchanged",
    )


def test_revert_refuses_unknown_dependents() -> None:
    schema = create_owned_schema("revert_dependent")
    seed_legacy_pool_blocks(schema)
    initialize_forward_schema(schema, writer_id="a1-revert-dependent")
    migrated_rows = ordinal_rows(schema)
    run_psql(
        """
CREATE VIEW qbit_pool_blocks_ordinal_view AS
SELECT block_hash, audit_publication_sequence
FROM qbit_pool_blocks;
""",
        schema=schema,
    )
    try:
        apply_revert(schema)
    except GateFailure as failure:
        message = str(failure)
        if "audit_publication_sequence" not in message or "depend" not in message:
            raise
    else:
        raise GateFailure(
            "revert with a dependent view unexpectedly succeeded"
        )
    # The failed revert must roll back completely: nothing half-reverted.
    assert_equal(
        ordinal_catalog(schema),
        MIGRATED_CATALOG,
        "failed revert leaves the migrated catalog intact",
    )
    assert_equal(
        ordinal_rows(schema),
        migrated_rows,
        "failed revert leaves every ordinal intact",
    )
    run_psql("DROP VIEW qbit_pool_blocks_ordinal_view;", schema=schema)
    apply_revert(schema)
    assert_equal(
        ordinal_catalog(schema),
        REVERTED_CATALOG,
        "revert succeeds once the dependent view is gone",
    )


def test_revert_noop_and_missing_table() -> None:
    schema = create_owned_schema("revert_legacy_noop")
    seed_legacy_pool_blocks(schema)
    # Never migrated: the revert must be a no-op that still converges on the
    # pre-ordinal shape (including the restored confirmation function).
    apply_revert(schema)
    assert_equal(
        ordinal_catalog(schema),
        REVERTED_CATALOG,
        "revert of a never-migrated schema is a no-op",
    )
    assert_equal(
        run_json(
            "SELECT json_build_object("
            "'total', (SELECT count(*) FROM qbit_pool_blocks));",
            schema=schema,
        )["total"],
        6,
        "no-op revert keeps every legacy row",
    )

    empty_schema = create_owned_schema("revert_missing_table")
    try:
        apply_revert(empty_schema)
    except GateFailure as failure:
        if "missing qbit_pool_blocks in current schema" not in str(failure):
            raise
    else:
        raise GateFailure(
            "revert without qbit_pool_blocks unexpectedly succeeded"
        )


def main() -> None:
    public_before = support.public_sentinel()
    failure: BaseException | None = None
    try:
        test_revert_round_trip()
        test_revert_refuses_unknown_dependents()
        test_revert_noop_and_missing_table()
    except BaseException as error:
        failure = error
    try:
        support.cleanup_active_children()
        support.cleanup_owned_schemas()
        support.assert_equal(
            support.marker_schema_count(), 0, "revert marker cleanup"
        )
        support.assert_equal(
            support.public_sentinel(),
            public_before,
            "revert public preservation",
        )
    except BaseException as cleanup_error:
        if failure is None:
            raise
        raise GateFailure(
            f"revert scenario failed with {failure!r}; "
            f"cleanup also failed with {cleanup_error!r}"
        ) from cleanup_error
    else:
        support.atexit.unregister(support.cleanup_active_children)
        support.atexit.unregister(support.cleanup_owned_schemas)
    if failure is not None:
        raise failure
    print(
        "prism postgres A1 revert gate PASS "
        "round-trip legacy-confirm loud-failure noop-idempotent"
    )


if __name__ == "__main__":
    main()
