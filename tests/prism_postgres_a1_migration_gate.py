"""Non-discovered PostgreSQL ordinal migration integration gate."""

from __future__ import annotations

import hashlib
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from tests import prism_postgres_a1_gate as support
from tests.prism_postgres_a1_gate import (
    ACTIVE_CHILDREN,
    ACTIVE_CHILDREN_LOCK,
    BASE_PSQL_ARGV,
    GateFailure,
    LEGACY_POOL_BLOCKS_SQL,
    PSQL_OUTPUT_LIMIT_BYTES,
    PSQL_TIMEOUT_SECONDS,
    RUN_TOKEN,
    SCHEMA_PATTERN,
    ScopedPsqlLedger,
    _schema_registered,
    _terminate_and_reap,
    assert_equal,
    create_owned_schema,
    run_json,
    run_psql,
)

MIGRATION_HASH_A = "81" * 32
MIGRATION_HASH_B = "82" * 32
MIGRATION_HASH_C = "83" * 32
MIGRATION_HASH_OTHER = "84" * 32
BIGINT_MAX = 9_223_372_036_854_775_807


def add_ordinal_column(schema: str) -> None:
    run_psql(
        "ALTER TABLE qbit_pool_blocks "
        "ADD COLUMN audit_publication_sequence bigint;",
        schema=schema,
    )


def create_ordinal_sequence(
    schema: str,
    *,
    last_value: int = 1,
    is_called: bool = False,
) -> None:
    run_psql(
        f"""
CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint;
SELECT setval(
    'qbit_audit_publication_sequence_seq'::regclass,
    {last_value},
    {'true' if is_called else 'false'}
);
""",
        schema=schema,
    )


def seed_migration_rows(
    schema: str,
    *,
    ordinals: tuple[int | None, int | None, int | None] | None,
    include_other_states: bool = True,
) -> None:
    ordinal_column = (
        ", audit_publication_sequence" if ordinals is not None else ""
    )
    ordinal_values = (
        ["NULL" if value is None else str(value) for value in ordinals]
        if ordinals is not None
        else [None, None, None]
    )

    def row(
        block_hash: str,
        found_at: str,
        ordinal: str | None,
        state: str,
        maturity: str = "immature",
    ) -> str:
        ordinal_sql = f", {ordinal}" if ordinal is not None else ""
        disconnected = (
            ", '2020-01-02T00:00:00Z'" if maturity == "reversed" else ", NULL"
        )
        return (
            f"('{block_hash}', 10, '{'10' * 32}', '{'20' * 32}', "
            f"'{'30' * 32}', '{found_at}', '{state}', '{maturity}', "
            f"NULL{disconnected}{ordinal_sql})"
        )

    rows = [
        # Insertion order intentionally differs from deterministic migration
        # order. A/B share a timestamp and sort by block hash.
        row(
            MIGRATION_HASH_C,
            "2020-01-01T00:00:02Z",
            ordinal_values[2],
            "confirmed",
        ),
        row(
            MIGRATION_HASH_B,
            "2020-01-01T00:00:01Z",
            ordinal_values[1],
            "confirmed",
        ),
        row(
            MIGRATION_HASH_A,
            "2020-01-01T00:00:01Z",
            ordinal_values[0],
            "confirmed",
        ),
    ]
    if include_other_states:
        rows.extend(
            [
                row(
                    MIGRATION_HASH_OTHER,
                    "2020-01-01T00:00:03Z",
                    "NULL" if ordinals is not None else None,
                    "prepared",
                ),
                row(
                    "85" * 32,
                    "2020-01-01T00:00:04Z",
                    "NULL" if ordinals is not None else None,
                    "inactive",
                ),
                row(
                    "86" * 32,
                    "2020-01-01T00:00:05Z",
                    "NULL" if ordinals is not None else None,
                    "rejected",
                    "reversed",
                ),
                row(
                    "87" * 32,
                    "2020-01-01T00:00:06Z",
                    "NULL" if ordinals is not None else None,
                    "reversed",
                    "reversed",
                ),
            ]
        )
    run_psql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    found_at,
    chain_state,
    maturity_state,
    matured_at,
    disconnected_at
    {ordinal_column}
) VALUES
{', '.join(rows)};
""",
        schema=schema,
    )


def migration_snapshot(schema: str) -> dict[str, object]:
    if SCHEMA_PATTERN.fullmatch(schema) is None:
        raise GateFailure(f"invalid migration snapshot schema: {schema!r}")
    snapshot = run_json(
        f"""
SELECT json_build_object(
    'rows', (
        SELECT COALESCE(
            json_agg(to_jsonb(block) ORDER BY block_hash),
            '[]'::json
        )
        FROM "{schema}".qbit_pool_blocks block
    ),
    'column', (
        SELECT json_build_object(
            'attnum', attribute.attnum,
            'type_oid', attribute.atttypid::pg_catalog.int8,
            'nullable', NOT attribute.attnotnull,
            'has_default', attribute.atthasdef,
            'identity', attribute.attidentity,
            'generated', attribute.attgenerated,
            'collation', attribute.attcollation::pg_catalog.int8
        )
        FROM pg_catalog.pg_attribute attribute
        JOIN pg_catalog.pg_class relation
          ON relation.oid = attribute.attrelid
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = '{schema}'
          AND relation.relname = 'qbit_pool_blocks'
          AND attribute.attname = 'audit_publication_sequence'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
    ),
    'sequence_catalog', (
        SELECT json_build_object(
            'oid', relation.oid,
            'kind', relation.relkind,
            'persistence', relation.relpersistence,
            'owner', relation.relowner,
            'type_oid', sequence.seqtypid::pg_catalog.int8,
            'start', sequence.seqstart,
            'increment', sequence.seqincrement,
            'max', sequence.seqmax,
            'min', sequence.seqmin,
            'cache', sequence.seqcache,
            'cycle', sequence.seqcycle,
            'owned_dependencies', (
                SELECT count(*)
                FROM pg_catalog.pg_depend dependency
                WHERE dependency.classid = 'pg_catalog.pg_class'::regclass
                  AND dependency.objid = relation.oid
                  AND dependency.refclassid = 'pg_catalog.pg_class'::regclass
                  AND dependency.refobjsubid > 0
                  AND dependency.deptype IN ('a', 'i')
            ),
            'same_owner_as_table', relation.relowner = (
                SELECT pool_blocks.relowner
                FROM pg_catalog.pg_class pool_blocks
                JOIN pg_catalog.pg_namespace pool_namespace
                  ON pool_namespace.oid = pool_blocks.relnamespace
                WHERE pool_namespace.nspname = '{schema}'
                  AND pool_blocks.relname = 'qbit_pool_blocks'
            )
        )
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = relation.relnamespace
        LEFT JOIN pg_catalog.pg_sequence sequence
          ON sequence.seqrelid = relation.oid
        WHERE namespace.nspname = '{schema}'
          AND relation.relname = 'qbit_audit_publication_sequence_seq'
    ),
    'indexes', (
        SELECT COALESCE(json_agg(index_row ORDER BY index_row.name), '[]'::json)
        FROM (
            SELECT
                index_relation.relname AS name,
                index_relation.oid,
                pg_catalog.pg_relation_filenode(index_relation.oid) AS filenode,
                index_relation.relkind AS kind,
                index_relation.relpersistence AS persistence,
                index_relation.relowner AS owner,
                pg_catalog.pg_get_indexdef(index_relation.oid) AS definition,
                index_definition.indisunique AS unique,
                index_definition.indisvalid AS valid,
                index_definition.indisready AS ready,
                index_definition.indislive AS live,
                index_definition.indimmediate AS immediate,
                index_definition.indisprimary AS primary,
                index_definition.indisexclusion AS exclusion,
                index_definition.indisclustered AS clustered,
                index_definition.indisreplident AS replica_identity,
                index_definition.indnullsnotdistinct AS nulls_not_distinct,
                index_definition.indnkeyatts AS key_count,
                index_definition.indnatts AS attribute_count,
                index_definition.indkey::text AS keys,
                index_definition.indclass::text AS classes,
                index_definition.indcollation::text AS collations,
                index_definition.indoption::text AS options,
                index_definition.indexprs::text AS expressions,
                index_definition.indpred::text AS predicate
            FROM pg_catalog.pg_index index_definition
            JOIN pg_catalog.pg_class index_relation
              ON index_relation.oid = index_definition.indexrelid
            JOIN pg_catalog.pg_class table_relation
              ON table_relation.oid = index_definition.indrelid
            JOIN pg_catalog.pg_namespace namespace
              ON namespace.oid = table_relation.relnamespace
            WHERE namespace.nspname = '{schema}'
              AND table_relation.relname = 'qbit_pool_blocks'
              AND EXISTS (
                  SELECT 1
                  FROM unnest(index_definition.indkey) AS key(attnum)
                  JOIN pg_catalog.pg_attribute attribute
                    ON attribute.attrelid = table_relation.oid
                   AND attribute.attnum = key.attnum
                  WHERE attribute.attname = 'audit_publication_sequence'
              )
        ) index_row
    ),
    'named_index_relation', (
        SELECT json_build_object(
            'oid', relation.oid,
            'kind', relation.relkind,
            'persistence', relation.relpersistence,
            'owner', relation.relowner,
            'filenode', pg_catalog.pg_relation_filenode(relation.oid)
        )
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = '{schema}'
          AND relation.relname =
              'qbit_pool_blocks_audit_publication_sequence_idx'
    ),
    'constraints', (
        SELECT COALESCE(
            json_agg(constraint_row ORDER BY constraint_row.name),
            '[]'::json
        )
        FROM (
            SELECT
                constraint_definition.conname AS name,
                constraint_definition.oid,
                constraint_definition.contype AS type,
                constraint_definition.conbin::text AS expression_tree,
                pg_catalog.pg_get_constraintdef(
                    constraint_definition.oid,
                    true
                ) AS definition,
                constraint_definition.convalidated AS validated,
                constraint_definition.conkey::text AS keys,
                constraint_definition.conislocal AS local,
                constraint_definition.coninhcount AS inherited_count,
                constraint_definition.connoinherit AS no_inherit,
                constraint_definition.condeferrable AS deferrable,
                constraint_definition.condeferred AS deferred
            FROM pg_catalog.pg_constraint constraint_definition
            JOIN pg_catalog.pg_class table_relation
              ON table_relation.oid = constraint_definition.conrelid
            JOIN pg_catalog.pg_namespace namespace
              ON namespace.oid = table_relation.relnamespace
            WHERE namespace.nspname = '{schema}'
              AND table_relation.relname = 'qbit_pool_blocks'
              AND (
                  constraint_definition.conname =
                    'qbit_pool_blocks_audit_publication_sequence_check'
                  OR pg_catalog.pg_get_constraintdef(
                      constraint_definition.oid,
                      true
                  ) LIKE '%audit_publication_sequence%'
              )
        ) constraint_row
    )
);
"""
    )
    sequence_catalog = snapshot["sequence_catalog"]
    if sequence_catalog is None or sequence_catalog["kind"] != "S":
        snapshot["sequence_state"] = None
    else:
        snapshot["sequence_state"] = run_json(
            f"""
SELECT json_build_object('last_value', last_value, 'is_called', is_called)
FROM "{schema}".qbit_audit_publication_sequence_seq;
"""
        )
    return snapshot


def assert_advisory_lock_available(message: str) -> None:
    result = run_json(
        """
WITH acquired AS (
    SELECT pg_catalog.pg_try_advisory_lock(
        pg_catalog.hashtext('qbit_audit_publication_sequence_migration')
    ) AS ok
)
SELECT json_build_object(
    'acquired', (SELECT ok FROM acquired),
    'released', CASE
        WHEN (SELECT ok FROM acquired) THEN pg_catalog.pg_advisory_unlock(
            pg_catalog.hashtext('qbit_audit_publication_sequence_migration')
        )
        ELSE false
    END
);
"""
    )
    assert_equal(
        result,
        {"acquired": True, "released": True},
        message,
    )


def initialize_migration_schema(
    schema: str,
    *,
    writer_id: str,
) -> ScopedPsqlLedger:
    return ScopedPsqlLedger(
        test_schema=schema,
        writer_id=writer_id,
        writer_epoch=1,
        initialize_schema=True,
    )


def assert_valid_migration_case(
    *,
    schema: str,
    label: str,
    expected_ordinals: dict[str, int | None],
    expected_next_ordinal: int | None,
    expected_sequence_state: dict[str, object] | None = None,
) -> dict[str, object]:
    if not _schema_registered(schema):
        raise GateFailure(f"{label} requires an explicit registered schema")
    ledger = initialize_migration_schema(schema, writer_id=f"a1-{label}-first")
    try:
        first = migration_snapshot(schema)
        rows = {
            str(row["block_hash"]): row.get("audit_publication_sequence")
            for row in first["rows"]  # type: ignore[union-attr]
        }
        for block_hash, expected in expected_ordinals.items():
            assert_equal(rows[block_hash], expected, f"{label} ordinal {block_hash}")
        if expected_sequence_state is not None:
            assert_equal(
                first["sequence_state"],
                expected_sequence_state,
                f"{label} exact allocator state",
            )
        assert_equal(len(first["indexes"]), 1, f"{label} canonical index count")
        assert_equal(len(first["constraints"]), 1, f"{label} canonical constraint count")
        assert_equal(
            first["column"],
            {
                "attnum": first["column"]["attnum"],  # type: ignore[index]
                "type_oid": 20,
                "nullable": True,
                "has_default": False,
                "identity": "",
                "generated": "",
                "collation": 0,
            },
            f"{label} exact ordinal column catalog",
        )
        sequence_catalog = first["sequence_catalog"]
        assert sequence_catalog is not None
        assert_equal(
            {
                key: sequence_catalog[key]  # type: ignore[index]
                for key in (
                    "kind",
                    "persistence",
                    "type_oid",
                    "start",
                    "increment",
                    "max",
                    "min",
                    "cache",
                    "cycle",
                    "owned_dependencies",
                    "same_owner_as_table",
                )
            },
            {
                "kind": "S",
                "persistence": "p",
                "type_oid": 20,
                "start": 1,
                "increment": 1,
                "max": BIGINT_MAX,
                "min": 1,
                "cache": 1,
                "cycle": False,
                "owned_dependencies": 0,
                "same_owner_as_table": True,
            },
            f"{label} exact ordinal sequence catalog",
        )
        index = first["indexes"][0]  # type: ignore[index]
        assert_equal(
            {
                key: index[key]
                for key in (
                    "name",
                    "kind",
                    "persistence",
                    "unique",
                    "valid",
                    "ready",
                    "live",
                    "immediate",
                    "primary",
                    "exclusion",
                    "clustered",
                    "replica_identity",
                    "nulls_not_distinct",
                    "key_count",
                    "attribute_count",
                    "collations",
                    "options",
                    "expressions",
                    "predicate",
                )
            },
            {
                "name": "qbit_pool_blocks_audit_publication_sequence_idx",
                "kind": "i",
                "persistence": "p",
                "unique": True,
                "valid": True,
                "ready": True,
                "live": True,
                "immediate": True,
                "primary": False,
                "exclusion": False,
                "clustered": False,
                "replica_identity": False,
                "nulls_not_distinct": False,
                "key_count": 1,
                "attribute_count": 1,
                "collations": "0",
                "options": "0",
                "expressions": None,
                "predicate": None,
            },
            f"{label} exact ordinal index catalog",
        )
        assert_equal(
            index["definition"],
            "CREATE UNIQUE INDEX "
            "qbit_pool_blocks_audit_publication_sequence_idx ON "
            f"{schema}.qbit_pool_blocks USING btree "
            "(audit_publication_sequence)",
            f"{label} exact ordinal index definition",
        )
        constraint = first["constraints"][0]  # type: ignore[index]
        assert_equal(
            {
                key: constraint[key]
                for key in (
                    "name",
                    "type",
                    "definition",
                    "validated",
                    "local",
                    "inherited_count",
                    "no_inherit",
                    "deferrable",
                    "deferred",
                )
            },
            {
                "name": "qbit_pool_blocks_audit_publication_sequence_check",
                "type": "c",
                "definition": "CHECK ((audit_publication_sequence IS NULL OR "
                "audit_publication_sequence > 0) AND (chain_state <> "
                "'confirmed'::text OR audit_publication_sequence IS NOT NULL))",
                "validated": True,
                "local": True,
                "inherited_count": 0,
                "no_inherit": False,
                "deferrable": False,
                "deferred": False,
            },
            f"{label} exact ordinal constraint catalog",
        )
    finally:
        ledger.release_writer_lease()
        ledger.close()
    for rerun_round in (1, 2):
        rerun = initialize_migration_schema(
            schema,
            writer_id=f"a1-{label}-rerun-{rerun_round}",
        )
        try:
            assert_equal(
                migration_snapshot(schema),
                first,
                f"{label} idempotent catalog rerun {rerun_round}",
            )
        finally:
            rerun.release_writer_lease()
            rerun.close()
    legacy_inactive_hash = "85" * 32
    legacy_inactive_sequence = expected_ordinals.get(legacy_inactive_hash)
    if legacy_inactive_sequence is not None:
        reactivator = ScopedPsqlLedger(
            test_schema=schema,
            writer_id=f"a1-{label}-legacy-reactivation",
            writer_epoch=1,
        )
        try:
            reactivated = reactivator.reactivate_pool_block(
                block_hash=legacy_inactive_hash,
                active_tip_height=10,
            )
            assert_equal(
                reactivated["audit_publication_sequence"],
                legacy_inactive_sequence,
                f"{label} legacy inactive reactivation preserves backfilled ordinal",
            )
        finally:
            reactivator.release_writer_lease()
            reactivator.close()
    if expected_next_ordinal is not None:
        final = ScopedPsqlLedger(
            test_schema=schema,
            writer_id=f"a1-{label}-next",
            writer_epoch=1,
        )
        try:
            block_hash = hashlib.sha256(f"{label}-next".encode()).hexdigest()
            final._run_sql(
                f"""
INSERT INTO qbit_pool_blocks (
    block_hash, block_height, parent_hash, coinbase_txid,
    payout_manifest_sha256, chain_state, maturity_state
) VALUES (
    '{block_hash}', 99, '{'10' * 32}', '{'20' * 32}',
    '{'30' * 32}', 'prepared', 'immature'
);
"""
            )
            confirmation = final.confirm_accepted_block(
                block_hash=block_hash,
                active_tip_height=99,
            )
            assert_equal(
                confirmation["audit_publication_sequence"],
                expected_next_ordinal,
                f"{label} next production ordinal",
            )
        finally:
            final.release_writer_lease()
            final.close()
    assert_advisory_lock_available(f"{label} advisory lock released")
    return first


def insert_migration_row(
    schema: str,
    *,
    block_hash: str,
    ordinal: int | None,
    found_at: str,
    chain_state: str = "confirmed",
) -> None:
    ordinal_sql = "NULL" if ordinal is None else str(ordinal)
    run_psql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash,
    audit_publication_sequence,
    block_height,
    parent_hash,
    coinbase_txid,
    payout_manifest_sha256,
    found_at,
    chain_state,
    maturity_state
) VALUES (
    '{block_hash}',
    {ordinal_sql},
    10,
    '{'10' * 32}',
    '{'20' * 32}',
    '{'30' * 32}',
    '{found_at}',
    '{chain_state}',
    'immature'
);
""",
        schema=schema,
    )


def assert_initializer_rejects_unchanged(
    schema: str,
    *,
    label: str,
    error_fragment: str,
) -> dict[str, object]:
    before = migration_snapshot(schema)
    try:
        initialize_migration_schema(schema, writer_id=f"a1-{label}-reject")
    except GateFailure as error:
        if error_fragment not in str(error):
            raise GateFailure(
                f"{label} wrong failure: expected {error_fragment!r}, got {error!r}"
            ) from error
    else:
        raise GateFailure(f"{label} initializer unexpectedly succeeded")
    assert_equal(
        migration_snapshot(schema),
        before,
        f"{label} transactional rollback snapshot",
    )
    assert_advisory_lock_available(f"{label} rejecting advisory lock released")
    return before


def create_canonical_ordinal_constraint(schema: str, *, not_valid: bool = False) -> None:
    run_psql(
        """
ALTER TABLE qbit_pool_blocks
ADD CONSTRAINT qbit_pool_blocks_audit_publication_sequence_check
CHECK (
    (audit_publication_sequence IS NULL OR audit_publication_sequence > 0)
    AND (chain_state <> 'confirmed' OR audit_publication_sequence IS NOT NULL)
)
"""
        + (" NOT VALID;" if not_valid else ";"),
        schema=schema,
    )


def test_m0_m11_migration_matrix() -> None:
    other_null_expected = {
        MIGRATION_HASH_OTHER: None,
        "85" * 32: 4,
        "86" * 32: None,
        "87" * 32: None,
    }
    common_null_expected = {
        MIGRATION_HASH_A: 1,
        MIGRATION_HASH_B: 2,
        MIGRATION_HASH_C: 3,
        **other_null_expected,
    }

    m0 = create_owned_schema("m0")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m0)
    seed_migration_rows(m0, ordinals=None)
    assert_valid_migration_case(
        schema=m0,
        label="m0",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )

    m1 = create_owned_schema("m1")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m1)
    add_ordinal_column(m1)
    seed_migration_rows(m1, ordinals=(None, None, None))
    assert_valid_migration_case(
        schema=m1,
        label="m1",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )

    m2 = create_owned_schema("m2")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m2)
    create_ordinal_sequence(m2)
    seed_migration_rows(m2, ordinals=None)
    assert_valid_migration_case(
        schema=m2,
        label="m2",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )

    m3 = create_owned_schema("m3")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m3)
    add_ordinal_column(m3)
    create_ordinal_sequence(m3)
    seed_migration_rows(m3, ordinals=(None, None, None))
    assert_valid_migration_case(
        schema=m3,
        label="m3",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )

    m4 = create_owned_schema("m4")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m4)
    add_ordinal_column(m4)
    create_ordinal_sequence(m4)
    seed_migration_rows(m4, ordinals=(None, None, None))
    run_psql(
        "CREATE UNIQUE INDEX "
        "qbit_pool_blocks_audit_publication_sequence_idx "
        "ON qbit_pool_blocks (audit_publication_sequence);",
        schema=m4,
    )
    m4_before = migration_snapshot(m4)
    m4_first = assert_valid_migration_case(
        schema=m4,
        label="m4",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )
    assert_equal(
        m4_first["indexes"],
        m4_before["indexes"],
        "M4 first migration preserves correct index OID/filenode/catalog",
    )
    assert_equal(
        m4_first["named_index_relation"],
        m4_before["named_index_relation"],
        "M4 first migration preserves correct named index relation",
    )

    m5 = create_owned_schema("m5")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m5)
    add_ordinal_column(m5)
    create_ordinal_sequence(m5, last_value=3, is_called=True)
    seed_migration_rows(m5, ordinals=(1, 2, 3))
    create_canonical_ordinal_constraint(m5)
    m5_before = migration_snapshot(m5)
    m5_first = assert_valid_migration_case(
        schema=m5,
        label="m5",
        expected_ordinals={
            MIGRATION_HASH_A: 1,
            MIGRATION_HASH_B: 2,
            MIGRATION_HASH_C: 3,
            **other_null_expected,
        },
        expected_next_ordinal=5,
    )
    assert_equal(
        m5_first["constraints"],
        m5_before["constraints"],
        "M5 first migration preserves correct constraint OID/conbin/catalog",
    )

    m6 = create_owned_schema("m6")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m6)
    add_ordinal_column(m6)
    create_ordinal_sequence(m6)
    seed_migration_rows(m6, ordinals=(9002, None, None))
    assert_valid_migration_case(
        schema=m6,
        label="m6",
        expected_ordinals={
            MIGRATION_HASH_A: 9002,
            MIGRATION_HASH_B: 9003,
            MIGRATION_HASH_C: 9004,
            MIGRATION_HASH_OTHER: None,
            "85" * 32: 9005,
            "86" * 32: None,
            "87" * 32: None,
        },
        expected_next_ordinal=9006,
        expected_sequence_state={"last_value": 9005, "is_called": True},
    )

    for label, called, inactive_ordinal, expected_next in (
        ("m7a", True, 12, 13),
        ("m8a", False, 11, 12),
    ):
        schema = create_owned_schema(label)
        run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
        add_ordinal_column(schema)
        create_ordinal_sequence(schema, last_value=11, is_called=called)
        seed_migration_rows(schema, ordinals=(5, 6, 7))
        assert_valid_migration_case(
            schema=schema,
            label=label,
            expected_ordinals={
                MIGRATION_HASH_A: 5,
                MIGRATION_HASH_B: 6,
                MIGRATION_HASH_C: 7,
                MIGRATION_HASH_OTHER: None,
                "85" * 32: inactive_ordinal,
                "86" * 32: None,
                "87" * 32: None,
            },
            expected_next_ordinal=expected_next,
            expected_sequence_state={
                "last_value": inactive_ordinal,
                "is_called": True,
            },
        )

    for label, called, expected, inactive_ordinal, expected_next in (
        ("m7b", True, (12, 13, 14), 15, 16),
        ("m8b", False, (11, 12, 13), 14, 15),
    ):
        schema = create_owned_schema(label)
        run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
        add_ordinal_column(schema)
        create_ordinal_sequence(schema, last_value=11, is_called=called)
        seed_migration_rows(schema, ordinals=(None, None, None))
        run_psql(
            f"""
UPDATE qbit_pool_blocks
SET audit_publication_sequence = 7
WHERE block_hash = '{MIGRATION_HASH_OTHER}';
""",
            schema=schema,
        )
        assert_valid_migration_case(
            schema=schema,
            label=label,
            expected_ordinals={
                MIGRATION_HASH_A: expected[0],
                MIGRATION_HASH_B: expected[1],
                MIGRATION_HASH_C: expected[2],
                MIGRATION_HASH_OTHER: 7,
                "85" * 32: inactive_ordinal,
                "86" * 32: None,
                "87" * 32: None,
            },
            expected_next_ordinal=expected_next,
            expected_sequence_state={
                "last_value": inactive_ordinal,
                "is_called": True,
            },
        )

    m9 = create_owned_schema("m9")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m9)
    add_ordinal_column(m9)
    seed_migration_rows(m9, ordinals=(0, None, None))
    assert_initializer_rejects_unchanged(
        m9,
        label="m9",
        error_fragment="invalid non-positive audit publication sequence",
    )

    m10 = create_owned_schema("m10")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m10)
    add_ordinal_column(m10)
    seed_migration_rows(m10, ordinals=(4, 4, None))
    assert_initializer_rejects_unchanged(
        m10,
        label="m10",
        error_fragment="duplicate audit publication sequence",
    )

    m11 = create_owned_schema("m11")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11)
    create_ordinal_sequence(m11, last_value=19, is_called=True)
    seed_migration_rows(m11, ordinals=None)
    run_psql(
        """
ALTER TABLE qbit_pool_blocks
ADD CONSTRAINT qbit_pool_blocks_audit_publication_sequence_check
CHECK (block_height < 1000);
""",
        schema=m11,
    )
    assert_initializer_rejects_unchanged(
        m11,
        label="m11_wrong_constraint",
        error_fragment="invalid audit publication sequence constraint definition",
    )
    run_psql(
        "ALTER TABLE qbit_pool_blocks DROP CONSTRAINT "
        "qbit_pool_blocks_audit_publication_sequence_check;",
        schema=m11,
    )
    assert_valid_migration_case(
        schema=m11,
        label="m11",
        expected_ordinals={
            MIGRATION_HASH_A: 20,
            MIGRATION_HASH_B: 21,
            MIGRATION_HASH_C: 22,
            "85" * 32: 23,
        },
        expected_next_ordinal=24,
    )

    m11_alt = create_owned_schema("m11_alt")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_alt)
    add_ordinal_column(m11_alt)
    create_ordinal_sequence(m11_alt, last_value=3, is_called=True)
    seed_migration_rows(m11_alt, ordinals=(1, 2, 3))
    run_psql(
        "CREATE UNIQUE INDEX qbit_audit_publication_sequence_alternate "
        "ON qbit_pool_blocks (audit_publication_sequence);",
        schema=m11_alt,
    )
    assert_initializer_rejects_unchanged(
        m11_alt,
        label="m11_alternate_index",
        error_fragment="duplicate audit publication sequence index definition",
    )
    run_psql(
        "DROP INDEX qbit_audit_publication_sequence_alternate;",
        schema=m11_alt,
    )
    assert_valid_migration_case(
        schema=m11_alt,
        label="m11_alt",
        expected_ordinals={
            MIGRATION_HASH_A: 1,
            MIGRATION_HASH_B: 2,
            MIGRATION_HASH_C: 3,
            "85" * 32: 4,
        },
        expected_next_ordinal=5,
    )

    m11_named = create_owned_schema("m11_named")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_named)
    add_ordinal_column(m11_named)
    create_ordinal_sequence(m11_named, last_value=3, is_called=True)
    seed_migration_rows(m11_named, ordinals=(1, 2, 3))
    run_psql(
        "CREATE INDEX qbit_pool_blocks_audit_publication_sequence_idx "
        "ON qbit_pool_blocks (audit_publication_sequence);",
        schema=m11_named,
    )
    assert_initializer_rejects_unchanged(
        m11_named,
        label="m11_wrong_named_index",
        error_fragment="invalid audit publication sequence index definition",
    )
    run_psql(
        "DROP INDEX qbit_pool_blocks_audit_publication_sequence_idx;",
        schema=m11_named,
    )
    assert_valid_migration_case(
        schema=m11_named,
        label="m11_named",
        expected_ordinals={
            MIGRATION_HASH_A: 1,
            MIGRATION_HASH_B: 2,
            MIGRATION_HASH_C: 3,
            "85" * 32: 4,
        },
        expected_next_ordinal=5,
    )

    m11_sequence = create_owned_schema("m11_sequence")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_sequence)
    add_ordinal_column(m11_sequence)
    run_psql(
        "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS integer;",
        schema=m11_sequence,
    )
    seed_migration_rows(m11_sequence, ordinals=(None, None, None))
    assert_initializer_rejects_unchanged(
        m11_sequence,
        label="m11_wrong_sequence",
        error_fragment="invalid audit publication sequence definition",
    )
    run_psql(
        "DROP SEQUENCE qbit_audit_publication_sequence_seq;",
        schema=m11_sequence,
    )
    create_ordinal_sequence(m11_sequence)
    assert_valid_migration_case(
        schema=m11_sequence,
        label="m11_sequence",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )

    m11_column = create_owned_schema("m11_column")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_column)
    run_psql(
        "ALTER TABLE qbit_pool_blocks "
        "ADD COLUMN audit_publication_sequence integer;",
        schema=m11_column,
    )
    seed_migration_rows(m11_column, ordinals=(None, None, None))
    assert_initializer_rejects_unchanged(
        m11_column,
        label="m11_wrong_column",
        error_fragment="invalid audit publication sequence column definition",
    )
    run_psql(
        "ALTER TABLE qbit_pool_blocks ALTER COLUMN "
        "audit_publication_sequence TYPE bigint;",
        schema=m11_column,
    )
    assert_valid_migration_case(
        schema=m11_column,
        label="m11_column",
        expected_ordinals=common_null_expected,
        expected_next_ordinal=5,
    )

    m11_not_valid = create_owned_schema("m11_not_valid")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_not_valid)
    add_ordinal_column(m11_not_valid)
    create_ordinal_sequence(m11_not_valid, last_value=3, is_called=True)
    seed_migration_rows(m11_not_valid, ordinals=(1, 2, 3))
    create_canonical_ordinal_constraint(m11_not_valid, not_valid=True)
    before_not_valid = migration_snapshot(m11_not_valid)
    not_valid_constraint = before_not_valid["constraints"][0]  # type: ignore[index]
    assert_equal(
        not_valid_constraint["validated"],
        False,
        "M11 preexisting canonical constraint starts NOT VALID",
    )
    normalized = assert_valid_migration_case(
        schema=m11_not_valid,
        label="m11_not_valid",
        expected_ordinals={
            MIGRATION_HASH_A: 1,
            MIGRATION_HASH_B: 2,
            MIGRATION_HASH_C: 3,
            **other_null_expected,
        },
        expected_next_ordinal=5,
    )
    assert_equal(
        normalized["constraints"][0]["oid"],  # type: ignore[index]
        not_valid_constraint["oid"],
        "M11 NOT VALID normalization preserves constraint OID",
    )

    m11_alt_constraint = create_owned_schema("m11_alt_constraint")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_alt_constraint)
    add_ordinal_column(m11_alt_constraint)
    create_ordinal_sequence(m11_alt_constraint, last_value=3, is_called=True)
    seed_migration_rows(m11_alt_constraint, ordinals=(1, 2, 3))
    run_psql(
        """
ALTER TABLE qbit_pool_blocks
ADD CONSTRAINT qbit_audit_publication_sequence_alternate_check
CHECK (
    (audit_publication_sequence IS NULL OR audit_publication_sequence > 0)
    AND (chain_state <> 'confirmed' OR audit_publication_sequence IS NOT NULL)
);
""",
        schema=m11_alt_constraint,
    )
    assert_initializer_rejects_unchanged(
        m11_alt_constraint,
        label="m11_alternate_constraint",
        error_fragment="duplicate audit publication sequence constraint definition",
    )
    run_psql(
        "ALTER TABLE qbit_pool_blocks DROP CONSTRAINT "
        "qbit_audit_publication_sequence_alternate_check;",
        schema=m11_alt_constraint,
    )
    assert_valid_migration_case(
        schema=m11_alt_constraint,
        label="m11_alt_constraint",
        expected_ordinals={
            MIGRATION_HASH_A: 1,
            MIGRATION_HASH_B: 2,
            MIGRATION_HASH_C: 3,
            **other_null_expected,
        },
        expected_next_ordinal=5,
    )

    m11_relkind = create_owned_schema("m11_relkind")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=m11_relkind)
    add_ordinal_column(m11_relkind)
    create_ordinal_sequence(m11_relkind, last_value=3, is_called=True)
    seed_migration_rows(m11_relkind, ordinals=(1, 2, 3))
    run_psql(
        "CREATE TABLE qbit_pool_blocks_audit_publication_sequence_idx "
        "(sentinel integer);",
        schema=m11_relkind,
    )
    assert_initializer_rejects_unchanged(
        m11_relkind,
        label="m11_wrong_index_relkind",
        error_fragment="invalid audit publication sequence index definition",
    )
    run_psql(
        "DROP TABLE qbit_pool_blocks_audit_publication_sequence_idx;",
        schema=m11_relkind,
    )
    assert_valid_migration_case(
        schema=m11_relkind,
        label="m11_relkind",
        expected_ordinals={
            MIGRATION_HASH_A: 1,
            MIGRATION_HASH_B: 2,
            MIGRATION_HASH_C: 3,
            **other_null_expected,
        },
        expected_next_ordinal=5,
    )


def prepare_ordinal_migration_schema(
    label: str,
    *,
    sequence_last: int,
    sequence_called: bool,
) -> str:
    schema = create_owned_schema(label)
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
    add_ordinal_column(schema)
    create_ordinal_sequence(
        schema,
        last_value=sequence_last,
        is_called=sequence_called,
    )
    return schema


def assert_next_allocation_exhausted(schema: str, *, label: str) -> None:
    block_hash = hashlib.sha256(f"{label}-exhausted".encode()).hexdigest()
    run_psql(
        f"""
INSERT INTO qbit_pool_blocks (
    block_hash, block_height, parent_hash, coinbase_txid,
    payout_manifest_sha256, chain_state, maturity_state
) VALUES (
    '{block_hash}', 101, '{'10' * 32}', '{'20' * 32}',
    '{'30' * 32}', 'prepared', 'immature'
);
""",
        schema=schema,
    )
    ledger = ScopedPsqlLedger(
        test_schema=schema,
        writer_id=f"a1-{label}-exhausted",
        writer_epoch=1,
    )
    try:
        before = migration_snapshot(schema)
        try:
            ledger.confirm_accepted_block(
                block_hash=block_hash,
                active_tip_height=101,
            )
        except GateFailure as error:
            if "reached maximum value" not in str(error):
                raise GateFailure(
                    f"{label} wrong allocator exhaustion: {error!r}"
                ) from error
        else:
            raise GateFailure(f"{label} exhausted allocator unexpectedly advanced")
        assert_equal(
            migration_snapshot(schema),
            before,
            f"{label} exhaustion leaves rows/catalog/allocator unchanged",
        )
    finally:
        ledger.release_writer_lease()
        ledger.close()


def test_migration_bigint_boundaries() -> None:
    max_called = prepare_ordinal_migration_schema(
        "bound_max_called",
        sequence_last=BIGINT_MAX,
        sequence_called=True,
    )
    for offset, block_hash in zip(
        (-2, -1, 0),
        (MIGRATION_HASH_A, MIGRATION_HASH_B, MIGRATION_HASH_C),
        strict=True,
    ):
        insert_migration_row(
            max_called,
            block_hash=block_hash,
            ordinal=BIGINT_MAX + offset,
            found_at=f"2020-01-01T00:00:0{offset + 3}Z",
        )
    assert_valid_migration_case(
        schema=max_called,
        label="bound_max_called",
        expected_ordinals={
            MIGRATION_HASH_A: BIGINT_MAX - 2,
            MIGRATION_HASH_B: BIGINT_MAX - 1,
            MIGRATION_HASH_C: BIGINT_MAX,
        },
        expected_next_ordinal=None,
        expected_sequence_state={"last_value": BIGINT_MAX, "is_called": True},
    )
    assert_next_allocation_exhausted(
        max_called,
        label="bound_max_called",
    )

    max_uncalled_one = prepare_ordinal_migration_schema(
        "bound_max_one",
        sequence_last=BIGINT_MAX,
        sequence_called=False,
    )
    insert_migration_row(
        max_uncalled_one,
        block_hash=MIGRATION_HASH_A,
        ordinal=None,
        found_at="2020-01-01T00:00:01Z",
    )
    assert_valid_migration_case(
        schema=max_uncalled_one,
        label="bound_max_one",
        expected_ordinals={MIGRATION_HASH_A: BIGINT_MAX},
        expected_next_ordinal=None,
        expected_sequence_state={"last_value": BIGINT_MAX, "is_called": True},
    )
    assert_next_allocation_exhausted(
        max_uncalled_one,
        label="bound_max_one",
    )

    max_uncalled_two = prepare_ordinal_migration_schema(
        "bound_max_two",
        sequence_last=BIGINT_MAX,
        sequence_called=False,
    )
    for block_hash, found_at in (
        (MIGRATION_HASH_A, "2020-01-01T00:00:01Z"),
        (MIGRATION_HASH_B, "2020-01-01T00:00:02Z"),
    ):
        insert_migration_row(
            max_uncalled_two,
            block_hash=block_hash,
            ordinal=None,
            found_at=found_at,
        )
    assert_initializer_rejects_unchanged(
        max_uncalled_two,
        label="bound_max_two",
        error_fragment="audit publication sequence exhausted",
    )

    durable_max_pending = prepare_ordinal_migration_schema(
        "bound_durable_max",
        sequence_last=1,
        sequence_called=False,
    )
    insert_migration_row(
        durable_max_pending,
        block_hash=MIGRATION_HASH_A,
        ordinal=BIGINT_MAX,
        found_at="2020-01-01T00:00:01Z",
    )
    insert_migration_row(
        durable_max_pending,
        block_hash=MIGRATION_HASH_B,
        ordinal=None,
        found_at="2020-01-01T00:00:02Z",
    )
    assert_initializer_rejects_unchanged(
        durable_max_pending,
        label="bound_durable_max",
        error_fragment="audit publication sequence exhausted",
    )

    near_max_three = prepare_ordinal_migration_schema(
        "bound_near_three",
        sequence_last=BIGINT_MAX - 2,
        sequence_called=False,
    )
    for block_hash, second in (
        (MIGRATION_HASH_C, 2),
        (MIGRATION_HASH_B, 1),
        (MIGRATION_HASH_A, 1),
    ):
        insert_migration_row(
            near_max_three,
            block_hash=block_hash,
            ordinal=None,
            found_at=f"2020-01-01T00:00:0{second}Z",
        )
    assert_valid_migration_case(
        schema=near_max_three,
        label="bound_near_three",
        expected_ordinals={
            MIGRATION_HASH_A: BIGINT_MAX - 2,
            MIGRATION_HASH_B: BIGINT_MAX - 1,
            MIGRATION_HASH_C: BIGINT_MAX,
        },
        expected_next_ordinal=None,
        expected_sequence_state={"last_value": BIGINT_MAX, "is_called": True},
    )

    near_max_four = prepare_ordinal_migration_schema(
        "bound_near_four",
        sequence_last=BIGINT_MAX - 2,
        sequence_called=False,
    )
    for index, block_hash in enumerate(
        (MIGRATION_HASH_A, MIGRATION_HASH_B, MIGRATION_HASH_C, "88" * 32),
        start=1,
    ):
        insert_migration_row(
            near_max_four,
            block_hash=block_hash,
            ordinal=None,
            found_at=f"2020-01-01T00:00:0{index}Z",
        )
    assert_initializer_rejects_unchanged(
        near_max_four,
        label="bound_near_four",
        error_fragment="audit publication sequence exhausted",
    )


def test_invalid_sequence_and_column_definitions() -> None:
    sequence_definitions = {
        "seq_start": "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint START WITH 2",
        "seq_increment": "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint INCREMENT BY 2",
        "seq_min": "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint MINVALUE 2 START WITH 2",
        "seq_max": "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint MAXVALUE 100",
        "seq_cache": "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint CACHE 2",
        "seq_cycle": "CREATE SEQUENCE qbit_audit_publication_sequence_seq AS bigint CYCLE",
        "seq_unlogged": "CREATE UNLOGGED SEQUENCE qbit_audit_publication_sequence_seq AS bigint",
    }
    for label, definition in sequence_definitions.items():
        schema = create_owned_schema(label)
        run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
        add_ordinal_column(schema)
        run_psql(definition + ";", schema=schema)
        seed_migration_rows(
            schema,
            ordinals=(1, 2, 3),
            include_other_states=False,
        )
        assert_initializer_rejects_unchanged(
            schema,
            label=label,
            error_fragment="invalid audit publication sequence definition",
        )

    owned = create_owned_schema("seq_owned")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=owned)
    add_ordinal_column(owned)
    create_ordinal_sequence(owned)
    run_psql(
        "ALTER SEQUENCE qbit_audit_publication_sequence_seq "
        "OWNED BY qbit_pool_blocks.audit_publication_sequence;",
        schema=owned,
    )
    seed_migration_rows(owned, ordinals=(1, 2, 3))
    assert_initializer_rejects_unchanged(
        owned,
        label="seq_owned",
        error_fragment="invalid audit publication sequence definition",
    )

    wrong_kind = create_owned_schema("seq_wrong_kind")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=wrong_kind)
    add_ordinal_column(wrong_kind)
    run_psql(
        "CREATE TABLE qbit_audit_publication_sequence_seq (sentinel integer);",
        schema=wrong_kind,
    )
    seed_migration_rows(wrong_kind, ordinals=(1, 2, 3))
    assert_initializer_rejects_unchanged(
        wrong_kind,
        label="seq_wrong_kind",
        error_fragment="invalid audit publication sequence definition",
    )

    column_definitions = {
        "column_not_null": "bigint NOT NULL",
        "column_default": "bigint DEFAULT 1",
        "column_identity": "bigint GENERATED BY DEFAULT AS IDENTITY",
    }
    for label, definition in column_definitions.items():
        schema = create_owned_schema(label)
        run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
        run_psql(
            "ALTER TABLE qbit_pool_blocks ADD COLUMN "
            f"audit_publication_sequence {definition};",
            schema=schema,
        )
        seed_migration_rows(
            schema,
            ordinals=(1, 2, 3),
            include_other_states=False,
        )
        assert_initializer_rejects_unchanged(
            schema,
            label=label,
            error_fragment="invalid audit publication sequence column definition",
        )

    generated = create_owned_schema("column_generated")
    run_psql(LEGACY_POOL_BLOCKS_SQL, schema=generated)
    seed_migration_rows(generated, ordinals=None)
    run_psql(
        "ALTER TABLE qbit_pool_blocks ADD COLUMN "
        "audit_publication_sequence bigint "
        "GENERATED ALWAYS AS (block_height + 1) STORED;",
        schema=generated,
    )
    assert_initializer_rejects_unchanged(
        generated,
        label="column_generated",
        error_fragment="invalid audit publication sequence column definition",
    )
    # Owner mismatch needs CREATE/SET ROLE authority that the supported
    # external-psql mode does not promise. Production still validates exact
    # same-owner identity, and every portable sequence property is exercised.


def _psql_process_command() -> list[str]:
    return [
        *BASE_PSQL_ARGV,
        "--no-psqlrc",
        "--set",
        "ON_ERROR_STOP=1",
        "--tuples-only",
        "--no-align",
        "--quiet",
    ]


def _register_process(process: subprocess.Popen[str]) -> None:
    with ACTIVE_CHILDREN_LOCK:
        ACTIVE_CHILDREN.add(process)


def _forget_process(process: subprocess.Popen[str]) -> None:
    with ACTIVE_CHILDREN_LOCK:
        ACTIVE_CHILDREN.discard(process)


def _wait_for_advisory_holder(
    process: subprocess.Popen[str],
    *,
    application_name: str,
    deadline: float,
) -> None:
    while time.monotonic() < deadline:
        state = run_json(
            f"""
SELECT json_build_object(
    'ready', EXISTS (
        SELECT 1
        FROM pg_catalog.pg_stat_activity activity
        JOIN pg_catalog.pg_locks lock_state
          ON lock_state.pid = activity.pid
        WHERE activity.application_name = '{application_name}'
          AND lock_state.locktype = 'advisory'
          AND lock_state.granted
    )
);
"""
        )
        if state["ready"]:
            return
        if process.poll() is not None:
            break
        time.sleep(0.05)
    raise GateFailure(
        f"advisory holder {application_name!r} not observed; exit={process.poll()}"
    )


def _wait_file_process(
    process: subprocess.Popen[str],
    *,
    stdout_file: Any,
    stderr_file: Any,
    expected_success: bool,
    error_fragment: str | None,
) -> None:
    try:
        process.wait(timeout=PSQL_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as error:
        _terminate_and_reap(process)
        raise GateFailure("tagged psql worker timed out") from error
    finally:
        _forget_process(process)
    stdout_file.seek(0)
    stderr_file.seek(0)
    stdout = stdout_file.read(PSQL_OUTPUT_LIMIT_BYTES + 1)
    stderr = stderr_file.read(PSQL_OUTPUT_LIMIT_BYTES + 1)
    if (
        len(stdout.encode("utf-8")) > PSQL_OUTPUT_LIMIT_BYTES
        or len(stderr.encode("utf-8")) > PSQL_OUTPUT_LIMIT_BYTES
    ):
        raise GateFailure("tagged psql worker output exceeded 1 MiB")
    if expected_success and process.returncode != 0:
        raise GateFailure(
            f"tagged psql worker failed with {process.returncode}: {stderr.strip()}"
        )
    if not expected_success:
        if process.returncode == 0:
            raise GateFailure("rejecting tagged psql worker unexpectedly succeeded")
        if error_fragment is not None and error_fragment not in stderr:
            raise GateFailure(
                f"tagged psql worker wrong failure: {stderr.strip()}"
            )


def test_migration_advisory_waiter() -> None:
    schema_sql = (
        Path(__file__).resolve().parents[1]
        / "crates/qbit-prism/sql/001_share_ledger.sql"
    ).read_text(encoding="utf-8")
    for outcome, rejecting in (("commit", False), ("rollback", True)):
        schema = create_owned_schema(f"migration_wait_{outcome}")
        rejection_before: dict[str, object] | None = None
        if rejecting:
            run_psql(LEGACY_POOL_BLOCKS_SQL, schema=schema)
            add_ordinal_column(schema)
            seed_migration_rows(schema, ordinals=(0, None, None))
            rejection_before = migration_snapshot(schema)
        # PostgreSQL truncates application_name to NAMEDATALEN - 1 bytes.
        # Keep the tags below that limit so catalog coordination uses the
        # exact values the clients set.
        holder_tag = f"a1_mig_holder_{RUN_TOKEN}_{outcome}"
        waiter_tag = f"a1_mig_waiter_{RUN_TOKEN}_{outcome}"
        holder_input = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        holder_stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        waiter_input = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        waiter_stdout = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        waiter_stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        holder: subprocess.Popen[str] | None = None
        waiter: subprocess.Popen[str] | None = None
        try:
            holder_input.write(
                f"""
SET application_name = '{holder_tag}';
BEGIN;
SELECT pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('qbit_audit_publication_sequence_migration')
);
DO $qbit_a1_wait$
DECLARE
    waiter_observed boolean := false;
BEGIN
    FOR attempt IN 1..600 LOOP
        PERFORM pg_catalog.pg_stat_clear_snapshot();
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_stat_activity waiter
            JOIN pg_catalog.pg_locks lock_state
              ON lock_state.pid = waiter.pid
            WHERE waiter.application_name = '{waiter_tag}'
              AND lock_state.locktype = 'advisory'
              AND NOT lock_state.granted
              AND pg_catalog.pg_backend_pid() = ANY(
                  pg_catalog.pg_blocking_pids(waiter.pid)
              )
        )
        INTO waiter_observed;
        EXIT WHEN waiter_observed;
        PERFORM pg_catalog.pg_sleep(0.05);
    END LOOP;
    IF NOT waiter_observed THEN
        RAISE EXCEPTION 'migration advisory waiter not observed';
    END IF;
END;
$qbit_a1_wait$;
{outcome.upper()};
"""
            )
            holder_input.seek(0)
            holder = subprocess.Popen(
                _psql_process_command(),
                stdin=holder_input,
                stdout=subprocess.DEVNULL,
                stderr=holder_stderr,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            _register_process(holder)
            _wait_for_advisory_holder(
                holder,
                application_name=holder_tag,
                deadline=time.monotonic() + PSQL_TIMEOUT_SECONDS,
            )

            waiter_input.write(
                f"""
SET application_name = '{waiter_tag}';
SET statement_timeout = '20s';
SET lock_timeout = '20s';
SET search_path TO "{schema}", pg_catalog;
{schema_sql}
"""
            )
            waiter_input.seek(0)
            waiter = subprocess.Popen(
                _psql_process_command(),
                stdin=waiter_input,
                stdout=waiter_stdout,
                stderr=waiter_stderr,
                text=True,
                start_new_session=True,
            )
            _register_process(waiter)
            try:
                holder.wait(timeout=PSQL_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired as error:
                _terminate_and_reap(holder)
                raise GateFailure("migration advisory holder timed out") from error
            finally:
                _forget_process(holder)
            if holder.returncode != 0:
                holder_stderr.seek(0)
                raise GateFailure(
                    "migration advisory holder failed: "
                    + holder_stderr.read(PSQL_OUTPUT_LIMIT_BYTES + 1).strip()
                )

            _wait_file_process(
                waiter,
                stdout_file=waiter_stdout,
                stderr_file=waiter_stderr,
                expected_success=not rejecting,
                error_fragment=(
                    "invalid non-positive audit publication sequence"
                    if rejecting
                    else None
                ),
            )
            if rejecting:
                assert rejection_before is not None
                assert_equal(
                    migration_snapshot(schema),
                    rejection_before,
                    "rejecting advisory waiter exact rollback",
                )
            else:
                success_snapshot = migration_snapshot(schema)
                assert_equal(
                    success_snapshot["sequence_state"],
                    {"last_value": 1, "is_called": False},
                    "successful advisory waiter empty allocator",
                )
            assert_advisory_lock_available(
                f"migration {outcome} advisory lock no leak"
            )
            remaining = run_json(
                f"""
SELECT json_build_object('count', count(*))
FROM pg_catalog.pg_stat_activity
WHERE application_name IN ('{holder_tag}', '{waiter_tag}');
"""
            )["count"]
            assert_equal(
                remaining,
                0,
                f"migration {outcome} tagged backend cleanup",
            )
        finally:
            for process in (waiter, holder):
                if process is not None:
                    if process.poll() is None:
                        _terminate_and_reap(process)
                    _forget_process(process)
                    for stream in (process.stdin, process.stdout, process.stderr):
                        if stream is not None and not stream.closed:
                            stream.close()
            for stream in (
                holder_input,
                holder_stderr,
                waiter_input,
                waiter_stdout,
                waiter_stderr,
            ):
                if not stream.closed:
                    stream.close()


def main() -> None:
    public_before = support.public_sentinel()
    failure: BaseException | None = None
    try:
        test_m0_m11_migration_matrix()
        test_migration_bigint_boundaries()
        test_invalid_sequence_and_column_definitions()
        test_migration_advisory_waiter()
    except BaseException as error:
        failure = error
    try:
        support.cleanup_active_children()
        support.cleanup_owned_schemas()
        support.assert_equal(support.marker_schema_count(), 0, "migration marker cleanup")
        support.assert_equal(support.public_sentinel(), public_before, "migration public preservation")
    except BaseException as cleanup_error:
        if failure is None:
            raise
        raise GateFailure(
            f"migration scenario failed with {failure!r}; cleanup also failed with {cleanup_error!r}"
        ) from cleanup_error
    else:
        support.atexit.unregister(support.cleanup_active_children)
        support.atexit.unregister(support.cleanup_owned_schemas)
    if failure is not None:
        raise failure
    print(
        "prism postgres A1 migration gate PASS "
        "M0-M11 bigint-bounds invalid-definitions migration-advisory-waiter"
    )


if __name__ == "__main__":
    main()
